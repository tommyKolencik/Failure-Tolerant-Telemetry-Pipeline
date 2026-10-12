"""Step 2: persist accepted events until the collector acknowledges delivery.

Read in order: results -> connection/transaction helpers -> public operations.
This module uses the shared schema and SQLite; it makes no network requests.
Constructing Outbox does not touch storage. Call initialize() before using it.
"""
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import ValidationError

from shared.config import HTTP_BATCH_MAX_EVENTS, OUTBOX_MAX_PENDING_BYTES
from shared.schema import (
    Event,
    canonical_event_bytes,
    event_hash,
    event_key,
    validate_event,
)

EventKey = tuple[str, str, int]


# A dataclass is a small Python result container; it does not validate raw events
# like a Pydantic model. frozen=True prevents reassignment of these result fields.
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


class Outbox:
    def __init__(
        self,
        path: str | Path = "data/outbox.sqlite3",
        *,
        max_pending_bytes: int = OUTBOX_MAX_PENDING_BYTES,
        busy_timeout_ms: int = 1_000,
    ) -> None:
        # The starter specifies a finite timeout, but not its value. One second
        # is a provisional, configurable default; revisit during experiments.
        if type(max_pending_bytes) is not int or max_pending_bytes < 0:
            raise ValueError("max_pending_bytes must be a nonnegative integer")
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be a nonnegative integer")
        # Store configuration, not a live connection shared between threads.
        # Resolving the path keeps it stable if the working directory changes.
        if str(path) == ":memory:":
            raise ValueError("the outbox requires a persistent database file")
        self.path = Path(path).resolve()
        self.max_pending_bytes = max_pending_bytes
        self.busy_timeout_ms = busy_timeout_ms

    @contextmanager
    def _connection(self, *, create: bool = False) -> Iterator[sqlite3.Connection]:
        # @contextmanager lets callers use `with self._connection() as conn:`.
        # Code before yield opens/configures; finally runs when the block exits.
        # Each operation opens, uses, and closes its connection on its own thread.
        # mode=rw opens an existing file; only initialize may create a new one.
        # as_uri safely encodes the path, including spaces or ? characters.
        database = str(self.path) if create else self.path.as_uri() + "?mode=rw"
        conn = sqlite3.connect(
            database,
            uri=not create,
            timeout=self.busy_timeout_ms / 1_000,
            autocommit=True,
        )
        try:
            # Python 3.12 autocommit=True leaves transaction control to our SQL.
            # Use SQL BEGIN/COMMIT/ROLLBACK, not conn.commit()/conn.rollback(),
            # which have no effect with this setting.
            # Docs: https://docs.python.org/3.12/library/sqlite3.html#transaction-control
            conn.execute("PRAGMA synchronous=FULL")
            yield conn
        finally:
            # sqlite3's own `with conn:` does not close the connection.
            conn.close()

    @contextmanager
    def _transaction(self, conn: sqlite3.Connection) -> Iterator[None]:
        # IMMEDIATE obtains the write reservation BEFORE reading capacity.
        # Competing publishers cannot both see the same available space and
        # overfill the queue. Other writers wait up to the configured timeout.
        # Docs: https://www.sqlite.org/lang_transaction.html
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            conn.execute("COMMIT")
        except BaseException:
            # Also roll back interrupted work. A failure must never become ACK.
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    def initialize(self) -> None:
        """Create the database/tables, preserving any existing pending events."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection(create=True) as conn:
            # WAL allows readers alongside a writer. FULL requests durable
            # synchronization at commit. These assume a working disk/filesystem.
            # WAL mode persists on this database; FULL is set on every connection.
            # Docs: https://www.sqlite.org/pragma.html#pragma_synchronous
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
                # One durable counter row. IF NOT EXISTS / OR IGNORE makes
                # initialization repeatable without resetting data or counters.
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS counters (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        capacity_rejections INTEGER NOT NULL DEFAULT 0
                    )
                """)
                conn.execute("INSERT OR IGNORE INTO counters (id) VALUES (1)")

    def publish(self, raw_event: Any) -> PublishResult:
        """Validate and durably enqueue, or explicitly report rejection."""
        try:
            # Model objects are mutable by default. Convert supplied models back
            # to data so validation checks their current fields as well.
            if isinstance(raw_event, Event):
                raw_event = raw_event.model_dump(mode="python")
            event = validate_event(raw_event)
        except ValidationError as exc:
            # Keep diagnostics small and do not echo the entire invalid payload.
            return PublishResult(False, "invalid", f"{exc.error_count()} validation errors")

        # Validation/serialization happen before taking the write lock.
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
                    # ? placeholders bind data separately from SQL instructions.
                    if existing is not None:
                        if existing == (event_json, content_hash):
                            result = PublishResult(True, "duplicate")
                        else:
                            result = PublishResult(False, "conflict", "key has different content")
                    else:
                        pending_bytes = conn.execute(
                            "SELECT COALESCE(SUM(event_bytes), 0) FROM pending"
                        ).fetchone()[0]
                        # SUM on no rows is NULL; COALESCE substitutes zero.
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
                # The transaction's COMMIT has completed before this return.
                # Capacity rejections also commit, to preserve their counter.
            return result
        except sqlite3.Error as exc:
            return PublishResult(False, "unavailable", str(exc)[:160])

        # Duplicate means the SAME event is already pending, so it needs no
        # additional bytes, even at capacity. After acknowledgment, its local
        # key is forgotten; permanent deduplication belongs to the collector.

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
        # Decode the saved content without changing boot IDs, sequences or uptime.
        # The uploader can obtain keys with event_key(event).
        return [validate_event(json.loads(row[0])) for row in rows]

    def acknowledge(self, keys: list[EventKey]) -> int:
        """Delete only these acknowledged keys; return the number removed."""
        # An uploader ACK corresponds to one bounded batch. Validate all keys
        # before writing, so a malformed later key cannot cause partial deletion.
        if len(keys) > HTTP_BATCH_MAX_EVENTS:
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
        deleted = 0
        with self._connection() as conn:
            with self._transaction(conn):
                for key in keys:
                    cursor = conn.execute(
                        "DELETE FROM pending WHERE source_id = ? AND boot_id = ? AND seq = ?",
                        key,
                    )
                    deleted += cursor.rowcount
        # Repeating an ACK deletes zero rows, safely. Storage failures raise;
        # callers must not interpret a failed transaction as successful deletion.
        # Call this only after the collector's durable ACK, never on a timeout.
        return deleted

    def stats(self) -> OutboxStats:
        """Return bounded diagnostics; serialized bytes are not physical disk use."""
        with self._connection() as conn:
            # A single query gives a consistent snapshot of backlog and counter.
            row = conn.execute("""
                SELECT COUNT(*), COALESCE(SUM(event_bytes), 0),
                       (SELECT capacity_rejections FROM counters WHERE id = 1)
                FROM pending
            """).fetchone()
        return OutboxStats(*row)


# TODO before uploader integration:
# - Choose bounded quarantine retention and what a permanent HTTP rejection
#   moves there. Count quarantined events separately from capacity rejections.
# - Cap the full HTTP wrapper's bytes in the uploader, not just event count.
# - Measure DB/WAL file size separately from pending serialized bytes. Deletion
#   frees logical queue capacity but does not necessarily shrink database files.
# Only capacity rejections have a durable counter in this checkpoint; invalid,
# conflict and unavailable results are reported to the caller without logging.
