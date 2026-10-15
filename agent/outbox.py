"""Durable SQLite outbox and bounded quarantine; initialize explicitly."""
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import ValidationError

from shared.config import (
    HTTP_BATCH_MAX_EVENTS,
    OUTBOX_MAX_PENDING_BYTES,
    QUARANTINE_MAX_BYTES,
    QUARANTINE_MAX_EVENTS,
    QUARANTINE_REASON_MAX_CHARS,
    QUERY_DEFAULT_LIMIT,
    QUERY_MAX_LIMIT,
)
from shared.schema import (
    Event,
    canonical_event_bytes,
    event_hash,
    event_key,
    validate_event,
)

EventKey = tuple[str, str, int]


@dataclass(frozen=True)
class PublishResult:
    accepted: bool
    status: Literal[
        "inserted", "duplicate", "invalid", "conflict", "capacity", "unavailable"
    ]
    reason: str | None = None


@dataclass(frozen=True)
class OutboxStats:
    pending_count: int
    pending_bytes: int
    capacity_rejections: int
    quarantine_count: int = 0
    quarantine_bytes: int = 0
    quarantined_total: int = 0
    quarantine_pruned_total: int = 0


@dataclass(frozen=True)
class QuarantinedEvent:
    quarantine_id: int
    key: EventKey
    event_json: str
    reason: str
    http_status: int | None
    rejected_at_utc: str


class QuarantineCapacityError(Exception):
    """Moving these rows would not preserve them within the quarantine limits."""


class Outbox:
    def __init__(
        self,
        path: str | Path = "data/outbox.sqlite3",
        *,
        max_pending_bytes: int = OUTBOX_MAX_PENDING_BYTES,
        busy_timeout_ms: int = 1_000,
        max_quarantine_events: int = QUARANTINE_MAX_EVENTS,
        max_quarantine_bytes: int = QUARANTINE_MAX_BYTES,
    ) -> None:
        if type(max_pending_bytes) is not int or max_pending_bytes < 0:
            raise ValueError("max_pending_bytes must be a nonnegative integer")
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be a nonnegative integer")
        for name, value in (
            ("max_quarantine_events", max_quarantine_events),
            ("max_quarantine_bytes", max_quarantine_bytes),
        ):
            if type(value) is not int or not 1 <= value < 2**63:
                raise ValueError(f"{name} must be a positive integer below 2**63")
        if str(path) == ":memory:":
            raise ValueError("the outbox requires a persistent database file")
        self.path = Path(path).resolve()
        self.max_pending_bytes = max_pending_bytes
        self.busy_timeout_ms = busy_timeout_ms
        self.max_quarantine_events = max_quarantine_events
        self.max_quarantine_bytes = max_quarantine_bytes

    @contextmanager
    def _connection(self, *, create: bool = False) -> Iterator[sqlite3.Connection]:
        # One connection per operation; only initialize() may create the database.
        database = str(self.path) if create else self.path.as_uri() + "?mode=rw"
        conn = sqlite3.connect(
            database,
            uri=not create,
            timeout=self.busy_timeout_ms / 1_000,
            autocommit=True,
        )
        try:
            # autocommit=True requires explicit SQL COMMIT/ROLLBACK.
            conn.execute("PRAGMA synchronous=FULL")
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _transaction(self, conn: sqlite3.Connection) -> Iterator[None]:
        # Reserve the writer before reading capacity to prevent competing overfills.
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    def initialize(self) -> None:
        """Create the database/tables, preserving any existing pending events."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection(create=True) as conn:
            # WAL persists; synchronous=FULL is set on every connection.
            mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if mode.lower() != "wal":
                raise sqlite3.OperationalError("could not enable WAL mode")
            with self._transaction(conn):
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS pending (
                        source_id TEXT NOT NULL,
                        boot_id TEXT NOT NULL,
                        seq INTEGER NOT NULL,
                        event_json TEXT NOT NULL,
                        content_hash TEXT NOT NULL,
                        event_bytes INTEGER NOT NULL CHECK (event_bytes >= 0),
                        received_at_utc TEXT NOT NULL,
                        PRIMARY KEY (source_id, boot_id, seq)
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS pending_receipt_order
                    ON pending (received_at_utc, source_id, boot_id, seq)
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS counters (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        capacity_rejections INTEGER NOT NULL DEFAULT 0
                    )
                """)
                conn.execute("INSERT OR IGNORE INTO counters (id) VALUES (1)")
                # Preserve pending rows when migrating older databases.
                columns = {row[1] for row in conn.execute("PRAGMA table_info(counters)")}
                for name in ("quarantined_total", "quarantine_pruned_total"):
                    if name not in columns:
                        conn.execute(f"ALTER TABLE counters ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS quarantine (
                        quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_id TEXT NOT NULL,
                        boot_id TEXT NOT NULL,
                        seq INTEGER NOT NULL,
                        event_json TEXT NOT NULL,
                        content_hash TEXT NOT NULL,
                        event_bytes INTEGER NOT NULL CHECK (event_bytes >= 0),
                        received_at_utc TEXT NOT NULL,
                        rejected_at_utc TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        http_status INTEGER
                    )
                """)
                self._prune_quarantine(conn)

    def publish(self, raw_event: Any) -> PublishResult:
        """Validate and durably enqueue, or explicitly report rejection."""
        try:
            event = validate_event(raw_event)
        except ValidationError as exc:
            return PublishResult(False, "invalid", f"{exc.error_count()} validation errors")

        # Prepare content outside the write lock.
        key = event_key(event)
        content = canonical_event_bytes(event)
        event_json = content.decode("utf-8")
        content_hash = event_hash(event)
        received_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")

        try:
            with self._connection() as conn:
                with self._transaction(conn):
                    existing = conn.execute(
                        """SELECT event_json, content_hash FROM pending
                           WHERE source_id = ? AND boot_id = ? AND seq = ?""",
                        key,
                    ).fetchone()
                    if existing is not None:
                        if existing == (event_json, content_hash):
                            result = PublishResult(True, "duplicate")
                        else:
                            result = PublishResult(False, "conflict", "key has different content")
                    else:
                        pending_bytes = conn.execute(
                            "SELECT COALESCE(SUM(event_bytes), 0) FROM pending"
                        ).fetchone()[0]
                        if pending_bytes + len(content) > self.max_pending_bytes:
                            conn.execute("""
                                UPDATE counters
                                SET capacity_rejections = capacity_rejections + 1
                                WHERE id = 1
                            """)
                            result = PublishResult(False, "capacity", "pending byte limit exceeded")
                        else:
                            conn.execute(
                                """INSERT INTO pending
                                   (source_id, boot_id, seq, event_json, content_hash,
                                    event_bytes, received_at_utc)
                                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                (*key, event_json, content_hash, len(content), received_at),
                            )
                            result = PublishResult(True, "inserted")
                # Commit the event or capacity-rejection counter before returning.
            return result
        except sqlite3.Error as exc:
            return PublishResult(False, "unavailable", str(exc)[:160])


    def peek_batch(self, limit: int = HTTP_BATCH_MAX_EVENTS) -> list[Event]:
        """Read a bounded, deterministic batch WITHOUT deleting its rows."""
        if type(limit) is not int or not 1 <= limit <= HTTP_BATCH_MAX_EVENTS:
            raise ValueError(f"limit must be an integer in 1..{HTTP_BATCH_MAX_EVENTS}")
        with self._connection() as conn:
            rows = conn.execute(
                """SELECT event_json FROM pending
                   ORDER BY received_at_utc, source_id, boot_id, seq LIMIT ?""",
                (limit,),
            ).fetchall()
        return [validate_event(json.loads(row[0])) for row in rows]

    def acknowledge(self, keys: list[EventKey]) -> int:
        """Delete only these acknowledged keys; return the number removed."""
        # Validate all keys before deleting any rows.
        self._validate_keys(keys)
        deleted = 0
        with self._connection() as conn:
            with self._transaction(conn):
                for key in keys:
                    cursor = conn.execute(
                        "DELETE FROM pending WHERE source_id = ? AND boot_id = ? AND seq = ?",
                        key,
                    )
                    deleted += cursor.rowcount
        # Return only after deletion COMMIT; repeated ACKs remove zero rows.
        return deleted

    @staticmethod
    def _validate_keys(keys: list[EventKey]) -> None:
        if not isinstance(keys, list) or len(keys) > HTTP_BATCH_MAX_EVENTS:
            raise ValueError("acknowledgment exceeds the batch count limit")
        for key in keys:
            if (
                not isinstance(key, tuple)
                or len(key) != 3
                or not isinstance(key[0], str)
                or not isinstance(key[1], str)
                or type(key[2]) is not int
                or not 0 <= key[2] < 2**63
            ):
                raise ValueError("each key must be a (source_id, boot_id, seq) tuple")

    def quarantine(
        self,
        keys: list[EventKey],
        *,
        reason: str,
        http_status: int | None = None,
    ) -> int:
        """Atomically archive rejected pending rows, then free pending capacity."""
        self._validate_keys(keys)
        if not isinstance(reason, str) or not reason.strip() or len(reason) > QUARANTINE_REASON_MAX_CHARS:
            raise ValueError(f"reason must be nonblank and at most {QUARANTINE_REASON_MAX_CHARS} characters")
        if http_status is not None and (type(http_status) is not int or not 100 <= http_status <= 599):
            raise ValueError("http_status must be an integer in 100..599 or None")
        rejected_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        with self._connection() as conn:
            with self._transaction(conn):
                # Deduplicate keys so archives and counters count each row once.
                rows = []
                for key in dict.fromkeys(keys):
                    row = conn.execute(
                        """SELECT source_id, boot_id, seq, event_json, content_hash,
                                  event_bytes, received_at_utc FROM pending
                           WHERE source_id = ? AND boot_id = ? AND seq = ?""",
                        key,
                    ).fetchone()
                    if row is not None:
                        rows.append(row)
                # If newly rejected rows cannot fit together, leave them pending.
                if len(rows) > self.max_quarantine_events or sum(row[5] for row in rows) > self.max_quarantine_bytes:
                    raise QuarantineCapacityError("rejected rows exceed quarantine retention capacity")
                for row in rows:
                    conn.execute(
                        """INSERT INTO quarantine
                           (source_id, boot_id, seq, event_json, content_hash,
                            event_bytes, received_at_utc, rejected_at_utc, reason, http_status)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (*row, rejected_at, reason, http_status),
                    )
                    conn.execute(
                        "DELETE FROM pending WHERE source_id = ? AND boot_id = ? AND seq = ?",
                        row[:3],
                    )
                conn.execute(
                    "UPDATE counters SET quarantined_total = quarantined_total + ? WHERE id = 1",
                    (len(rows),),
                )
                self._prune_quarantine(conn)
        return len(rows)

    def _prune_quarantine(self, conn: sqlite3.Connection) -> None:
        count, size = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(event_bytes), 0) FROM quarantine"
        ).fetchone()
        pruned = 0
        while count > self.max_quarantine_events or size > self.max_quarantine_bytes:
            oldest_id, oldest_bytes = conn.execute(
                "SELECT quarantine_id, event_bytes FROM quarantine ORDER BY quarantine_id LIMIT 1"
            ).fetchone()
            conn.execute("DELETE FROM quarantine WHERE quarantine_id = ?", (oldest_id,))
            count -= 1
            size -= oldest_bytes
            pruned += 1
        if pruned:
            conn.execute(
                "UPDATE counters SET quarantine_pruned_total = quarantine_pruned_total + ? WHERE id = 1",
                (pruned,),
            )

    def peek_quarantine(self, limit: int = QUERY_DEFAULT_LIMIT) -> list[QuarantinedEvent]:
        """Inspect newest retained failures, including their exact source JSON."""
        if type(limit) is not int or not 1 <= limit <= QUERY_MAX_LIMIT:
            raise ValueError(f"limit must be an integer in 1..{QUERY_MAX_LIMIT}")
        with self._connection() as conn:
            rows = conn.execute(
                """SELECT quarantine_id, source_id, boot_id, seq, event_json,
                          reason, http_status, rejected_at_utc
                   FROM quarantine ORDER BY quarantine_id DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        # Keep JSON as text so later schema changes cannot hide archived failures.
        return [QuarantinedEvent(row[0], tuple(row[1:4]), *row[4:]) for row in rows]

    def stats(self) -> OutboxStats:
        """Return bounded diagnostics; serialized bytes are not physical disk use."""
        with self._connection() as conn:
        # One query gives a consistent snapshot.
            row = conn.execute("""
                SELECT COUNT(*), COALESCE(SUM(event_bytes), 0),
                       (SELECT capacity_rejections FROM counters WHERE id = 1),
                       (SELECT COUNT(*) FROM quarantine),
                       (SELECT COALESCE(SUM(event_bytes), 0) FROM quarantine),
                       (SELECT quarantined_total FROM counters WHERE id = 1),
                       (SELECT quarantine_pruned_total FROM counters WHERE id = 1)
                FROM pending
            """).fetchone()
        return OutboxStats(*row)
