"""Atomic collector storage with retained-key deduplication and bounded queries."""
import json
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from shared.config import (
    COLLECTOR_MAX_RETAINED_EVENTS,
    HTTP_BATCH_MAX_EVENTS,
    QUERY_DEFAULT_LIMIT,
    QUERY_MAX_LIMIT,
    STATS_MAX_EVENTS,
    STATS_WINDOW_SECONDS,
    SUPPORTED_EVENT_KINDS,
)
from shared.schema import (
    BatchRequest, Event, EventEnvelope, canonical_event_bytes, event_hash, event_key, validate_event,
)

EventKey = tuple[str, str, int]


@dataclass(frozen=True)
class BatchResult:
    inserted: int
    duplicates: int


@dataclass(frozen=True)
class StoredEvent:
    event_id: int
    event: Event
    received_at_utc: str


@dataclass(frozen=True)
class EventPage:
    events: list[StoredEvent]
    next_cursor: int | None


@dataclass(frozen=True)
class HttpRequestStats:
    sampled_count: int
    error_count: int  # HTTP 4xx/5xx, including our cancellation convention 499.
    status_classes: dict[str, int]
    mean_duration_ms: float | None
    p50_duration_ms: float | None
    p95_duration_ms: float | None
    p99_duration_ms: float | None


@dataclass(frozen=True)
class RecentStats:
    start_at_utc: str
    end_at_utc: str
    row_limit: int
    sampled_count: int
    capped: bool
    by_kind: dict[str, int]
    http_requests: HttpRequestStats


@dataclass(frozen=True)
class CollectorStats:
    inserted_total: int
    duplicate_total: int
    pruned_total: int
    retained_count: int
    retained_bytes: int
    max_retained_events: int
    last_received_at_utc: str | None
    recent: RecentStats
    dedup_scope: Literal["retained_events"] = "retained_events"


class ConflictError(Exception):
    """An event key already exists with different source content."""

    def __init__(self, key: EventKey) -> None:
        self.key = key
        super().__init__(f"event key has different content: {key}")


class InvalidQueryError(ValueError):
    """Invalid filters/bounds, separate from malformed content in storage."""


class CollectorStore:
    def __init__(
        self,
        path: str | Path = "data/collector.sqlite3",
        *,
        busy_timeout_ms: int = 1_000,
        max_retained_events: int = COLLECTOR_MAX_RETAINED_EVENTS,
        stats_window_seconds: int = STATS_WINDOW_SECONDS,
        stats_max_events: int = STATS_MAX_EVENTS,
    ) -> None:
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be a nonnegative integer")
        if str(path) == ":memory:":
            raise ValueError("the collector requires a persistent database file")
        # Retention must fit a full batch so every ACKed key remains stored.
        if (
            type(max_retained_events) is not int
            or not HTTP_BATCH_MAX_EVENTS <= max_retained_events < 2**63
        ):
            raise ValueError(f"max_retained_events must be an integer >= {HTTP_BATCH_MAX_EVENTS} and < 2**63")
        if type(stats_window_seconds) is not int or not 1 <= stats_window_seconds <= 86_400:
            raise ValueError("stats_window_seconds must be an integer in 1..86400")
        if type(stats_max_events) is not int or not 1 <= stats_max_events < 2**63 - 1:
            raise ValueError("stats_max_events must be a positive integer below 2**63 - 1")
        self.path = Path(path).resolve()
        self.busy_timeout_ms = busy_timeout_ms
        self.max_retained_events = max_retained_events
        self.stats_window_seconds = stats_window_seconds
        self.stats_max_events = stats_max_events

    @contextmanager
    def _connection(self, *, create: bool = False) -> Iterator[sqlite3.Connection]:
        # One connection per operation, owned by its calling thread.
        database = str(self.path) if create else self.path.as_uri() + "?mode=rw"
        conn = sqlite3.connect(
            database,
            uri=not create,
            timeout=self.busy_timeout_ms / 1_000,
            autocommit=True,
        )
        try:
            conn.execute("PRAGMA synchronous=FULL")
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _transaction(self, conn: sqlite3.Connection, *, write: bool = True) -> Iterator[None]:
        # Reserve the writer before key checks; reads use a consistent snapshot.
        # autocommit=True requires explicit SQL COMMIT/ROLLBACK.
        conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    def initialize(self) -> None:
        """Create tables without erasing existing events or durable counters."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection(create=True) as conn:
            mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if mode.lower() != "wal":
                raise sqlite3.OperationalError("could not enable WAL mode")
            with self._transaction(conn):
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        source_id TEXT NOT NULL,
                        boot_id TEXT NOT NULL,
                        seq INTEGER NOT NULL,
                        kind TEXT NOT NULL,
                        event_json TEXT NOT NULL,
                        content_hash TEXT NOT NULL,
                        received_at_utc TEXT NOT NULL,
                        ingest_id INTEGER NOT NULL,
                        PRIMARY KEY (source_id, boot_id, seq)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS counters (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        inserted_total INTEGER NOT NULL DEFAULT 0,
                        duplicate_total INTEGER NOT NULL DEFAULT 0,
                        pruned_total INTEGER NOT NULL DEFAULT 0,
                        last_ingest_id INTEGER NOT NULL DEFAULT 0
                    )
                """)
                conn.execute("INSERT OR IGNORE INTO counters (id) VALUES (1)")
                self._migrate_query_columns(conn)
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS events_ingest_id ON events (ingest_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS events_source_cursor ON events (source_id, ingest_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS events_kind_cursor ON events (kind, ingest_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS events_source_kind_cursor ON events (source_id, kind, ingest_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS events_receipt_order ON events (received_at_utc, ingest_id)")
                self._prune(conn)

    def _migrate_query_columns(self, conn: sqlite3.Connection) -> None:
        # Migrate older databases atomically without resetting content or counters.
        event_columns = {row[1] for row in conn.execute("PRAGMA table_info(events)")}
        if "ingest_id" not in event_columns:
            conn.execute("ALTER TABLE events ADD COLUMN ingest_id INTEGER")
        counter_columns = {row[1] for row in conn.execute("PRAGMA table_info(counters)")}
        for name in ("pruned_total", "last_ingest_id"):
            if name not in counter_columns:
                conn.execute(f"ALTER TABLE counters ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0")
        # Seed old IDs from rowid once; the durable counter prevents ID reuse.
        conn.execute("UPDATE events SET ingest_id = rowid WHERE ingest_id IS NULL")
        conn.execute("""
            UPDATE counters
            SET last_ingest_id = MAX(last_ingest_id,
                COALESCE((SELECT MAX(ingest_id) FROM events), 0))
            WHERE id = 1
        """)

    def _prune(self, conn: sqlite3.Connection, protected_ids: list[int] | None = None) -> None:
        # Write transaction only; retention follows insertion order.
        count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        overflow = count - self.max_retained_events
        if overflow > 0:
            # Protect this batch's ACKed keys, including old duplicates.
            protected_ids = [] if protected_ids is None else protected_ids
            exclusion = ""
            if protected_ids:
                placeholders = ",".join("?" for _ in protected_ids)
                exclusion = f"WHERE ingest_id NOT IN ({placeholders}) "
            removed = conn.execute(
                "DELETE FROM events WHERE ingest_id IN ("
                + "SELECT ingest_id FROM events " + exclusion
                + "ORDER BY ingest_id LIMIT ?)",
                (*protected_ids, overflow),
            ).rowcount
            conn.execute(
                "UPDATE counters SET pruned_total = pruned_total + ? WHERE id = 1",
                (removed,),
            )
        # Pruning forgets dedup identity and bounds rows, not physical DB/WAL bytes.

    def insert_batch(self, events: list[Any]) -> BatchResult:
        """Validate everything, then atomically insert or classify duplicates."""
        if not isinstance(events, list) or not 1 <= len(events) <= HTTP_BATCH_MAX_EVENTS:
            raise ValueError(f"events must be a list of 1..{HTTP_BATCH_MAX_EVENTS} items")

        # Revalidate mutable model instances.
        raw_events = [
            event.model_dump(mode="python") if isinstance(event, EventEnvelope) else event
            for event in events
        ]
        batch = BatchRequest.model_validate({"events": raw_events})

        # Prepare the entire batch before taking the write lock.
        prepared = [
            (
                event_key(event),
                event.kind,
                canonical_event_bytes(event).decode("utf-8"),
                event_hash(event),
            )
            for event in batch.events
        ]
        received_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        # Receipt time is separate from COMMIT time and source content.

        inserted = 0
        duplicates = 0
        protected_ids: list[int] = []
        with self._connection() as conn:
            with self._transaction(conn):
                last_id = conn.execute("SELECT last_ingest_id FROM counters WHERE id = 1").fetchone()[0]
                for key, kind, event_json, content_hash in prepared:
                    existing = conn.execute(
                        """SELECT event_json, content_hash, ingest_id FROM events
                           WHERE source_id = ? AND boot_id = ? AND seq = ?""",
                        key,
                    ).fetchone()
                    if existing is None:
                        last_id += 1
                        conn.execute(
                            """INSERT INTO events
                               (source_id, boot_id, seq, kind, event_json,
                                content_hash, received_at_utc, ingest_id)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (*key, kind, event_json, content_hash, received_at, last_id),
                        )
                        inserted += 1
                        protected_ids.append(last_id)
                    elif existing[:2] == (event_json, content_hash):
                        duplicates += 1
                        protected_ids.append(existing[2])
                    else:
                        # A conflict rolls back the entire batch.
                        raise ConflictError(key)

                # Duplicates within this batch see earlier writes; counters commit atomically.
                conn.execute(
                    """UPDATE counters
                       SET inserted_total = inserted_total + ?,
                           duplicate_total = duplicate_total + ?,
                           last_ingest_id = ?
                       WHERE id = 1""",
                    (inserted, duplicates, last_id),
                )
                self._prune(conn, protected_ids)
        # Return only after COMMIT.
        return BatchResult(inserted, duplicates)

    def check_ready(self) -> None:
        """Check the initialized tables without creating a missing database."""
        with self._connection() as conn:
            conn.execute("SELECT source_id, boot_id, seq FROM events LIMIT 1").fetchone()
            counters = conn.execute(
                "SELECT inserted_total, duplicate_total FROM counters WHERE id = 1"
            ).fetchone()
            if counters is None:
                raise sqlite3.DatabaseError("collector counter row is missing")


    def query_events(
        self,
        *,
        limit: int = QUERY_DEFAULT_LIMIT,
        cursor: int = 0,
        source_id: str | None = None,
        kind: str | None = None,
    ) -> EventPage:
        """Return retained events after a collector ID, with optional filters."""
        if type(limit) is not int or not 1 <= limit <= QUERY_MAX_LIMIT:
            raise InvalidQueryError(f"limit must be an integer in 1..{QUERY_MAX_LIMIT}")
        if type(cursor) is not int or not 0 <= cursor < 2**63:
            raise InvalidQueryError("cursor must be a nonnegative integer below 2**63")
        if source_id is not None and (
            not isinstance(source_id, str) or not 1 <= len(source_id) <= 64
            or re.fullmatch(r"[A-Za-z0-9_-]+", source_id) is None
        ):
            raise InvalidQueryError("invalid source_id filter")
        if kind is not None and (not isinstance(kind, str) or kind not in SUPPORTED_EVENT_KINDS):
            raise InvalidQueryError("invalid kind filter")

        conditions = ["ingest_id > ?"]
        parameters: list[Any] = [cursor]
        if source_id is not None:
            conditions.append("source_id = ?")
            parameters.append(source_id)
        if kind is not None:
            conditions.append("kind = ?")
            parameters.append(kind)
        # Fetch one extra row to detect a next page.
        parameters.append(limit + 1)
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT ingest_id, event_json, received_at_utc FROM events WHERE "
                + " AND ".join(conditions) + " ORDER BY ingest_id LIMIT ?",
                parameters,
            ).fetchall()
        page_rows = rows[:limit]
        events = [
            StoredEvent(row[0], validate_event(json.loads(row[1])), row[2])
            for row in page_rows
        ]
        next_cursor = page_rows[-1][0] if len(rows) > limit else None
        # Cursors are stable IDs; pages are live reads and may reflect pruning.
        return EventPage(events, next_cursor)

    def stats(self, *, now: datetime | None = None) -> CollectorStats:
        """Lifetime counters plus a capped sample of recently retained events."""
        now = datetime.now(timezone.utc) if now is None else now
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        now = now.astimezone(timezone.utc)
        end = now.isoformat(timespec="microseconds")
        start = (now - timedelta(seconds=self.stats_window_seconds)).isoformat(timespec="microseconds")
        with self._connection() as conn:
            with self._transaction(conn, write=False):
                counters = conn.execute(
                    "SELECT inserted_total, duplicate_total, pruned_total FROM counters WHERE id = 1"
                ).fetchone()
                if counters is None:
                    raise sqlite3.DatabaseError("collector counter row is missing")
                retained = conn.execute("""
                    SELECT COUNT(*), COALESCE(SUM(length(CAST(event_json AS BLOB))), 0),
                           MAX(received_at_utc) FROM events
                """).fetchone()
                rows = conn.execute(
                    """SELECT kind, event_json FROM events
                       WHERE received_at_utc >= ? AND received_at_utc <= ?
                       ORDER BY received_at_utc DESC, ingest_id DESC LIMIT ?""",
                    (start, end, self.stats_max_events + 1),
                ).fetchall()
        by_kind: dict[str, int] = {}
        durations: list[float] = []
        status_classes: dict[str, int] = {}
        errors = 0
        for kind, content in rows[:self.stats_max_events]:
            by_kind[kind] = by_kind.get(kind, 0) + 1
            if kind == "http.request":
                request = validate_event(json.loads(content)).payload
                durations.append(request.duration_ms)
                label = f"{request.status // 100}xx"
                status_classes[label] = status_classes.get(label, 0) + 1
                errors += int(request.status >= 400)
        durations.sort()
        n = len(durations)
        # Nearest-rank percentiles summarize request spans in the bounded sample.
        def percentile(fraction: float) -> float | None:
            return durations[math.ceil(n * fraction) - 1] if n else None

        # Scale before summing to avoid finite-duration overflow.
        maximum = durations[-1] if n else 0.0
        mean = maximum * (math.fsum(value / maximum for value in durations) / n) if maximum else (0.0 if n else None)
        requests = HttpRequestStats(n, errors, status_classes, mean, percentile(0.5), percentile(0.95), percentile(0.99))
        recent = RecentStats(
            start_at_utc=start,
            end_at_utc=end,
            row_limit=self.stats_max_events,
            sampled_count=min(len(rows), self.stats_max_events),
            capped=len(rows) > self.stats_max_events,
            by_kind=by_kind,
            http_requests=requests,
        )
        return CollectorStats(
            inserted_total=counters[0],
            duplicate_total=counters[1],
            pruned_total=counters[2],
            retained_count=retained[0],
            retained_bytes=retained[1],
            max_retained_events=self.max_retained_events,
            last_received_at_utc=retained[2],
            recent=recent,
        )
