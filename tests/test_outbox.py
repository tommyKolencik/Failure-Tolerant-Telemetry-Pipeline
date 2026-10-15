"""Durable quarantine/capacity behavior; no development databases are used."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
import sqlite3

import pytest

from agent.outbox import Outbox, QuarantineCapacityError
from agent.sources import SyntheticSource
from shared.schema import canonical_event_bytes, event_key


@pytest.fixture
def events():
    return list(SyntheticSource(seed=7).events(8))


@pytest.fixture
def outbox(tmp_path):
    result = Outbox(tmp_path / "outbox.sqlite3")
    result.initialize()
    return result


def test_quarantine_is_atomic_idempotent_and_preserves_exact_content(outbox, events):
    for event in events[:3]:
        assert outbox.publish(event).accepted
    key = event_key(events[0])
    assert outbox.quarantine([key, key], reason="HTTP 409", http_status=409) == 1
    assert outbox.quarantine([key], reason="HTTP 409", http_status=409) == 0
    stats = outbox.stats()
    assert (stats.pending_count, stats.quarantine_count, stats.quarantined_total) == (2, 1, 1)
    archived = Outbox(outbox.path).peek_quarantine()[0]
    assert archived.key == key
    assert archived.event_json.encode() == canonical_event_bytes(events[0])
    assert archived.http_status == 409
    assert archived.reason == "HTTP 409"
    assert stats.quarantine_bytes == len(canonical_event_bytes(events[0]))
    assert [event_key(event) for event in outbox.peek_batch()] == [event_key(event) for event in events[1:3]]


def test_quarantine_row_retention_prunes_oldest_and_persists_counters(outbox, events):
    outbox.max_quarantine_events = 2
    for event in events[:3]:
        outbox.publish(event)
        outbox.quarantine([event_key(event)], reason="rejected")
    assert [record.key for record in outbox.peek_quarantine()] == [event_key(event) for event in reversed(events[1:3])]
    stats = Outbox(outbox.path).stats()
    assert (stats.quarantined_total, stats.quarantine_count, stats.quarantine_pruned_total) == (3, 2, 1)
    assert stats.pending_count == 0


def test_quarantine_byte_retention_prunes_old_content(outbox, events):
    sizes = [len(canonical_event_bytes(event)) for event in events[:2]]
    outbox.max_quarantine_bytes = sum(sizes) - 1
    for event in events[:2]:
        outbox.publish(event)
        outbox.quarantine([event_key(event)], reason="rejected")
    assert [record.key for record in outbox.peek_quarantine()] == [event_key(events[1])]
    assert outbox.stats().quarantine_bytes == sizes[1]
    assert outbox.stats().quarantine_pruned_total == 1


def test_new_quarantine_batch_must_fit_or_stay_pending(outbox, events):
    outbox.max_quarantine_events = 1
    for event in events[:2]:
        outbox.publish(event)
    with pytest.raises(QuarantineCapacityError):
        outbox.quarantine([event_key(event) for event in events[:2]], reason="rejected")
    assert outbox.stats().pending_count == 2
    assert outbox.stats().quarantined_total == 0
    outbox.max_quarantine_bytes = len(canonical_event_bytes(events[0])) - 1
    with pytest.raises(QuarantineCapacityError):
        outbox.quarantine([event_key(events[0])], reason="rejected")
    assert outbox.stats().pending_count == 2


class CommitFailureOutbox(Outbox):
    @contextmanager
    def _connection(self, **kwargs):
        with super()._connection(**kwargs) as conn:
            def authorize(action, argument, unused, database, trigger):
                return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and argument == "COMMIT" else sqlite3.SQLITE_OK
            conn.set_authorizer(authorize)
            yield conn


def test_quarantine_failed_commit_rolls_back_move_pruning_and_counts(outbox, events):
    outbox.max_quarantine_events = 1
    for event in events[:2]:
        outbox.publish(event)
    outbox.quarantine([event_key(events[0])], reason="earlier failure")
    before = outbox.stats()
    archived = outbox.peek_quarantine()
    failing = CommitFailureOutbox(outbox.path, max_quarantine_events=1)
    with pytest.raises(sqlite3.DatabaseError):
        failing.quarantine([event_key(events[1])], reason="new failure")
    assert outbox.stats() == before
    assert outbox.peek_quarantine() == archived
    assert event_key(outbox.peek_batch()[0]) == event_key(events[1])


def test_quarantine_frees_pending_capacity(outbox, events):
    size = len(canonical_event_bytes(events[0]))
    outbox.max_pending_bytes = size
    assert outbox.publish(events[0]).status == "inserted"
    assert outbox.publish(events[1]).status == "capacity"
    outbox.quarantine([event_key(events[0])], reason="rejected")
    assert outbox.publish(events[0]).status == "inserted"
    assert outbox.stats().capacity_rejections == 1


def test_concurrent_moves_keep_quarantine_bounded_and_accounted(outbox, events):
    outbox.max_quarantine_events = 3
    for event in events:
        outbox.publish(event)
    def move(event):
        return outbox.quarantine([event_key(event)], reason="rejected")
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(move, events)) == 8
    stats = outbox.stats()
    assert (stats.pending_count, stats.quarantined_total, stats.quarantine_count, stats.quarantine_pruned_total) == (0, 8, 3, 5)


@pytest.mark.parametrize("reason,status", [("", None), (" ", None), ("a" * 201, None), ("bad", True), ("bad", 999)])
def test_bad_quarantine_metadata_does_not_move_rows(outbox, events, reason, status):
    outbox.publish(events[0])
    with pytest.raises(ValueError):
        outbox.quarantine([event_key(events[0])], reason=reason, http_status=status)
    assert outbox.stats().pending_count == 1
    assert outbox.stats().quarantined_total == 0


def test_bad_later_key_does_not_partially_move_rows(outbox, events):
    outbox.publish(events[0])
    with pytest.raises(ValueError):
        outbox.quarantine([event_key(events[0]), ("source", "boot", True)], reason="rejected")
    assert outbox.stats().pending_count == 1


def test_old_outbox_schema_migrates_without_resetting_data(tmp_path, events):
    path = tmp_path / "previous.sqlite3"
    event = events[0]
    content = canonical_event_bytes(event)
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript("""
            CREATE TABLE pending (
                source_id TEXT NOT NULL, boot_id TEXT NOT NULL, seq INTEGER NOT NULL,
                event_json TEXT NOT NULL, content_hash TEXT NOT NULL,
                event_bytes INTEGER NOT NULL, received_at_utc TEXT NOT NULL,
                PRIMARY KEY (source_id, boot_id, seq)
            );
            CREATE TABLE counters (id INTEGER PRIMARY KEY, capacity_rejections INTEGER NOT NULL);
            INSERT INTO counters VALUES (1, 7);
        """)
        from shared.schema import event_hash
        conn.execute("INSERT INTO pending VALUES (?, ?, ?, ?, ?, ?, ?)", (
            *event_key(event), content.decode(), event_hash(event), len(content), "2026-10-13T00:00:00+00:00",
        ))
        conn.commit()
    migrated = Outbox(path)
    migrated.initialize()
    migrated.initialize()
    assert canonical_event_bytes(migrated.peek_batch()[0]) == content
    assert migrated.stats().capacity_rejections == 7
    assert migrated.stats().quarantined_total == 0


def test_lower_quarantine_caps_apply_on_restart(outbox, events):
    for event in events[:3]:
        outbox.publish(event)
        outbox.quarantine([event_key(event)], reason="rejected")
    lower = Outbox(outbox.path, max_quarantine_events=1)
    lower.initialize()
    lower.initialize()
    assert (lower.stats().quarantine_count, lower.stats().quarantine_pruned_total) == (1, 2)


@pytest.mark.parametrize("settings", [
    {"max_quarantine_events": 0}, {"max_quarantine_events": True},
    {"max_quarantine_bytes": 0}, {"max_quarantine_bytes": True},
])
def test_invalid_quarantine_limits_fail_before_storage(tmp_path, settings):
    with pytest.raises(ValueError):
        Outbox(tmp_path / "unused.sqlite3", **settings)
