"""Collector correctness checks. All database files live in pytest's tmp_path."""
import asyncio
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from threading import Event as ThreadEvent

from fastapi.testclient import TestClient
import httpx
from pydantic import ValidationError
import pytest

from agent.outbox import Outbox
from agent.sources import SyntheticSource
from collector.app import create_app
from collector.store import BatchResult, CollectorStore, ConflictError, InvalidQueryError
from shared.config import HTTP_BODY_MAX_BYTES
from shared.schema import canonical_event_bytes, event_key, validate_event


def event(seq=1):
    return {
        "schema_version": 1,
        "source_id": "synthetic-01",
        "boot_id": "6c2fd9a088364f158ad86285e097da41",
        "seq": seq,
        "kind": "sensor.temperature",
        "source_uptime_us": seq * 1_000,
        "payload": {"temperature_c": 21.875, "sensor_ok": True},
    }


def snapshot(store):
    # Read durable data independently of the store's write methods.
    with closing(sqlite3.connect(store.path)) as conn:
        rows = conn.execute(
            "SELECT source_id, boot_id, seq, event_json FROM events ORDER BY seq"
        ).fetchall()
        counters = conn.execute(
            "SELECT inserted_total, duplicate_total FROM counters WHERE id = 1"
        ).fetchone()
    return rows, counters


@pytest.fixture
def store(tmp_path):
    result = CollectorStore(tmp_path / "collector.sqlite3")
    result.initialize()
    return result


@pytest.fixture
def api(tmp_path):
    application = create_app(tmp_path / "collector.sqlite3")
    # Entering TestClient's context runs FastAPI's startup lifespan.
    with TestClient(application) as client:
        yield application, client


def test_replay_and_reopen_preserve_exact_content(store):
    originals = [event(1), event(2)]
    assert store.insert_batch(originals) == BatchResult(2, 0)
    assert store.insert_batch(originals) == BatchResult(0, 2)
    reopened = CollectorStore(store.path)
    reopened.initialize()
    rows, counters = snapshot(reopened)
    assert [row[3] for row in rows] == [
        canonical_event_bytes(validate_event(raw)).decode() for raw in originals
    ]
    assert counters == (2, 2)


def test_conflict_rolls_back_earlier_inserts_and_duplicate_counts(store):
    store.insert_batch([event(1)])
    before = snapshot(store)
    changed = event(1)
    changed["payload"]["temperature_c"] = 22
    # A duplicate and a new insert occur before the conflict; both roll back.
    with pytest.raises(ConflictError) as caught:
        store.insert_batch([event(1), event(2), changed])
    assert caught.value.key == event_key(validate_event(event(1)))
    assert snapshot(store) == before


def test_repeated_keys_within_one_batch(store):
    assert store.insert_batch([event(1), event(1)]) == BatchResult(1, 1)
    before = snapshot(store)
    changed = event(2)
    changed["source_uptime_us"] += 1
    with pytest.raises(ConflictError):
        store.insert_batch([event(2), changed])
    assert snapshot(store) == before


def test_invalid_later_event_and_mutated_model_do_not_partially_insert(store):
    invalid = event(2)
    invalid["seq"] = True
    with pytest.raises(ValidationError):
        store.insert_batch([event(1), invalid])
    mutated = validate_event(event(2))
    mutated.seq = -1
    with pytest.raises(ValidationError):
        store.insert_batch([event(1), mutated])
    assert snapshot(store) == ([], (0, 0))


@pytest.mark.parametrize("events", [[], [event()] * 101, (event(),)])
def test_direct_store_calls_obey_batch_limits(store, events):
    with pytest.raises(ValueError):
        store.insert_batch(events)
    assert snapshot(store) == ([], (0, 0))


def test_readiness_does_not_create_a_missing_database(tmp_path):
    missing = tmp_path / "missing.sqlite3"
    with pytest.raises(sqlite3.Error):
        CollectorStore(missing).check_ready()
    assert not missing.exists()


class CommitFailureStore(CollectorStore):
    @contextmanager
    def _connection(self, **kwargs):
        with super()._connection(**kwargs) as conn:
            # A real SQLite fault at COMMIT exercises rollback after insertion.
            def authorize(action, argument, unused, database, trigger):
                if action == sqlite3.SQLITE_TRANSACTION and argument == "COMMIT":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK
            conn.set_authorizer(authorize)
            yield conn


def test_failed_commit_returns_503_and_rolls_back(api):
    application, client = api
    original = application.state.store
    application.state.store = CommitFailureStore(original.path)
    response = client.post("/v1/events/batch", json={"events": [event()]})
    assert response.status_code == 503
    assert snapshot(original) == ([], (0, 0))
    application.state.store = original
    assert client.post("/v1/events/batch", json={"events": [event()]}).status_code == 200


def test_storage_lock_is_a_failure_then_later_traffic_recovers(api):
    application, client = api
    application.state.store.busy_timeout_ms = 10
    with closing(sqlite3.connect(application.state.store.path, autocommit=True)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        response = client.post("/v1/events/batch", json={"events": [event()]})
        assert response.status_code == 503
        conn.execute("ROLLBACK")
    assert snapshot(application.state.store) == ([], (0, 0))
    assert client.post("/v1/events/batch", json={"events": [event()]}).status_code == 200


def test_http_acknowledgment_duplicate_and_atomic_conflict(api):
    application, client = api
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}
    first = client.post("/v1/events/batch", json={"events": [event()]})
    assert first.status_code == 200
    assert first.json() == {"inserted": 1, "duplicates": 0}
    assert snapshot(application.state.store)[1] == (1, 0)
    assert client.post("/v1/events/batch", json={"events": [event()]}).json() == {
        "inserted": 0, "duplicates": 1
    }
    before = snapshot(application.state.store)
    changed = event()
    changed["payload"]["temperature_c"] = 22
    response = client.post("/v1/events/batch", json={"events": [event(2), changed]})
    assert response.status_code == 409
    assert snapshot(application.state.store) == before


@pytest.mark.parametrize("body, expected", [
    (b"{", 400),
    (b"\xff", 400),
    (b'{"events":[],"extra":NaN}', 400),
    (b'{"events":[],"extra":Infinity}', 400),
    (b'{"events":[],"extra":-Infinity}', 400),
    (b"[" * 2_000, 400),
    (b'"' + b"a" * (HTTP_BODY_MAX_BYTES - 1) + b'"', 413),
    (b"[]", 422),
    (b'{"events":[]}', 422),
    (json.dumps({"events": [event()] * 101}).encode(), 422),
    (json.dumps({"events": [event()], "extra": 1}).encode(), 422),
    (json.dumps({"events": [event(), {"kind": "unknown"}]}).encode(), 422),
], ids=[
    "malformed-json", "invalid-utf8", "nan", "infinity", "negative-infinity",
    "deep-incomplete-json", "oversized-body", "bare-list", "empty-batch",
    "101-events", "extra-wrapper-field", "invalid-later-event",
])
def test_http_rejects_bad_bodies_without_writes_and_recovers(api, body, expected):
    application, client = api
    response = client.post("/v1/events/batch", content=body, headers={"Content-Type": "application/json"})
    assert response.status_code == expected
    assert snapshot(application.state.store) == ([], (0, 0))
    assert client.post("/v1/events/batch", json={"events": [event()]}).status_code == 200


def test_media_type_and_bounded_validation_diagnostics(api):
    application, client = api
    assert client.post("/v1/events/batch", content=b"{}").status_code == 415
    invalid = event()
    invalid["seq"] = "invalid-secret-payload"
    response = client.post("/v1/events/batch", json={"events": [invalid]})
    assert response.status_code == 422
    assert "invalid-secret-payload" not in response.text
    assert len(response.content) < 200
    accepted = client.post(
        "/v1/events/batch", content=json.dumps({"events": [event()]}),
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    assert accepted.status_code == 200


@pytest.mark.parametrize("declared_length", [None, b"1"])
def test_actual_streamed_bytes_are_capped_even_with_missing_or_false_length(api, declared_length):
    application, client = api
    messages = []
    reads = 0
    headers = [(b"content-type", b"application/json")]
    if declared_length is not None:
        headers.append((b"content-length", declared_length))

    async def receive():
        nonlocal reads
        reads += 1
        if reads == 1:
            return {"type": "http.request", "body": b" " * HTTP_BODY_MAX_BYTES, "more_body": True}
        if reads == 2:
            return {"type": "http.request", "body": b" ", "more_body": True}
        raise AssertionError("handler continued reading after the byte limit")

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": "POST", "scheme": "http", "path": "/v1/events/batch",
        "raw_path": b"/v1/events/batch", "query_string": b"", "root_path": "",
        "headers": headers, "client": ("127.0.0.1", 1), "server": ("test", 80),
    }
    asyncio.run(application(scope, receive, send))
    assert messages[0]["status"] == 413
    assert reads == 2
    assert snapshot(application.state.store) == ([], (0, 0))
    assert client.post("/v1/events/batch", json={"events": [event()]}).status_code == 200


def test_body_exactly_at_byte_limit_is_accepted(api):
    application, client = api
    body = json.dumps({"events": [event()]}).encode()
    body += b" " * (HTTP_BODY_MAX_BYTES - len(body))
    assert client.post("/v1/events/batch", content=body, headers={"Content-Type": "application/json"}).status_code == 200


def test_health_stays_alive_when_storage_is_unavailable(api, tmp_path):
    application, client = api
    application.state.store.path = tmp_path / "missing.sqlite3"
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 503
    assert client.post("/v1/events/batch", json={"events": [event()]}).status_code == 503
    assert not application.state.store.path.exists()


def test_failed_startup_does_not_silently_serve_requests(tmp_path):
    blocked = tmp_path / "blocked"
    blocked.write_text("this path is a file")
    with pytest.raises(FileExistsError):
        with TestClient(create_app(blocked / "collector.sqlite3")):
            pytest.fail("startup unexpectedly succeeded")


def test_slow_storage_does_not_block_health_or_acknowledge_early(api):
    application, client = api
    entered = ThreadEvent()
    release = ThreadEvent()

    class DelayedStore(CollectorStore):
        def insert_batch(self, events):
            entered.set()
            if not release.wait(3):
                raise RuntimeError("test did not release storage")
            return super().insert_batch(events)

    application.state.store = DelayedStore(application.state.store.path)

    async def exercise():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as async_client:
            pending = asyncio.create_task(async_client.post(
                "/v1/events/batch", json={"events": [event()]},
            ))
            try:
                # Poll briefly without blocking the loop that runs the request.
                async def wait_until_entered():
                    while not entered.is_set():
                        await asyncio.sleep(0.005)
                await asyncio.wait_for(wait_until_entered(), timeout=2)
                health = await asyncio.wait_for(async_client.get("/healthz"), timeout=1)
                assert health.status_code == 200
                assert not pending.done()
                assert snapshot(application.state.store) == ([], (0, 0))
            finally:
                release.set()
                response = await pending
            assert response.status_code == 200
            assert snapshot(application.state.store)[1] == (1, 0)

    asyncio.run(exercise())


def test_lost_ack_replays_without_losing_or_duplicating_stored_events(api, tmp_path):
    application, client = api
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    assert outbox.publish(event()).accepted
    pending = outbox.peek_batch()
    body = {"events": [item.model_dump(mode="json") for item in pending]}
    assert client.post("/v1/events/batch", json=body).json() == {"inserted": 1, "duplicates": 0}
    # Simulate losing the successful response: do not acknowledge the local row.
    assert outbox.stats().pending_count == 1
    assert client.post("/v1/events/batch", json=body).json() == {"inserted": 0, "duplicates": 1}
    assert outbox.acknowledge([event_key(item) for item in pending]) == 1
    assert outbox.stats().pending_count == 0
    assert len(snapshot(application.state.store)[0]) == 1


def test_one_thousand_seeded_events_reconcile_exactly_through_outbox_and_api(api, tmp_path):
    application, client = api
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    source = SyntheticSource(seed=7)
    originals = list(source.events(1_000))
    for item in originals:
        assert outbox.publish(item).accepted
    while pending := outbox.peek_batch():
        response = client.post(
            "/v1/events/batch", json={"events": [item.model_dump(mode="json") for item in pending]},
        )
        assert response.status_code == 200
        assert response.json() == {"inserted": len(pending), "duplicates": 0}
        outbox.acknowledge([event_key(item) for item in pending])
    rows, counters = snapshot(application.state.store)
    actual = {(source, boot, seq): content for source, boot, seq, content in rows}
    expected = {
        event_key(validate_event(raw)): canonical_event_bytes(validate_event(raw)).decode()
        for raw in originals
    }
    assert actual == expected
    assert counters == (1_000, 0)
    assert outbox.stats().pending_count == 0


def test_event_queries_filter_and_paginate_by_collector_id(api):
    application, client = api
    originals = [event(seq) for seq in (90, 20, 10, 50, 30)]
    originals[1]["source_id"] = "other-source"
    originals[3]["source_id"] = "other-source"
    application.state.store.insert_batch(originals)
    first = client.get("/v1/events", params={"source_id": "synthetic-01", "limit": 2}).json()
    assert [row["event"]["seq"] for row in first["events"]] == [90, 10]
    assert first["next_cursor"] == 3
    second = client.get("/v1/events", params={
        "source_id": "synthetic-01", "kind": "sensor.temperature",
        "limit": 2, "cursor": first["next_cursor"],
    }).json()
    assert [row["event"]["seq"] for row in second["events"]] == [30]
    assert second["next_cursor"] is None
    assert client.get("/v1/events", params={"kind": "host.metrics"}).json()["events"] == []
    assert client.get("/v1/events", params={"source_id": "missing"}).json()["events"] == []
    assert client.get("/v1/events", params={"cursor": 100}).json()["events"] == []
    assert "received_at_utc" in first["events"][0]


def test_event_query_default_and_maximum_limits(api):
    application, client = api
    application.state.store.insert_batch([event(seq) for seq in range(100)])
    application.state.store.insert_batch([event(100)])
    assert len(client.get("/v1/events").json()["events"]) == 100
    result = client.get("/v1/events", params={"limit": 1000}).json()
    assert len(result["events"]) == 101
    assert result["next_cursor"] is None


@pytest.mark.parametrize("params", [
    {"limit": 0}, {"limit": 1001}, {"limit": "true"},
    {"cursor": -1}, {"cursor": str(2**63)}, {"cursor": "bad"},
    {"source_id": ""}, {"source_id": "a" * 65}, {"source_id": "name\n"},
    {"source_id": "x' OR 1=1 --"}, {"kind": "unknown"},
])
def test_http_query_rejects_invalid_bounds_and_filters(api, params):
    application, client = api
    assert client.get("/v1/events", params=params).status_code == 422
    assert snapshot(application.state.store) == ([], (0, 0))


@pytest.mark.parametrize("params", [
    {"limit": True}, {"limit": 1001}, {"cursor": True},
    {"cursor": 2**63}, {"source_id": "bad\n"}, {"kind": ["sensor.temperature"]},
])
def test_direct_query_calls_repeat_boundary_validation(store, params):
    with pytest.raises(InvalidQueryError):
        store.query_events(**params)


def test_retention_pagination_and_ids_survive_pruning_restart_and_vacuum(tmp_path):
    retained = CollectorStore(tmp_path / "retained.sqlite3", max_retained_events=100)
    retained.initialize()
    retained.insert_batch([event(seq) for seq in range(100)])
    page = retained.query_events(limit=3)
    assert page.next_cursor == 3
    retained.insert_batch([event(seq) for seq in range(100, 105)])
    next_page = retained.query_events(limit=5, cursor=page.next_cursor)
    assert [row.event_id for row in next_page.events] == [6, 7, 8, 9, 10]
    assert [row.event.seq for row in next_page.events] == [5, 6, 7, 8, 9]
    stats = retained.stats()
    assert (stats.inserted_total, stats.retained_count, stats.pruned_total) == (105, 100, 5)
    reopened = CollectorStore(retained.path, max_retained_events=100)
    reopened.initialize()
    with reopened._connection() as conn:
        conn.execute("VACUUM")
    reopened.insert_batch([event(105)])
    newest = reopened.query_events(cursor=105).events
    assert [row.event_id for row in newest] == [106]
    assert reopened.stats().pruned_total == 6


def test_replay_after_pruning_is_explicitly_a_new_insertion(tmp_path):
    retained = CollectorStore(tmp_path / "retained.sqlite3", max_retained_events=100)
    retained.initialize()
    retained.insert_batch([event(seq) for seq in range(100)])
    retained.insert_batch([event(100)])
    assert retained.insert_batch([event(0)]) == BatchResult(1, 0)
    assert retained.query_events(cursor=101).events[0].event.seq == 0
    stats = retained.stats()
    assert stats.dedup_scope == "retained_events"
    assert (stats.inserted_total, stats.retained_count, stats.pruned_total) == (102, 100, 2)


def retention_snapshot(store):
    with closing(sqlite3.connect(store.path)) as conn:
        return (
            conn.execute("SELECT * FROM events ORDER BY ingest_id").fetchall(),
            conn.execute("SELECT * FROM counters").fetchall(),
        )


def test_conflict_and_failed_commit_roll_back_retention_and_id_counters(tmp_path):
    retained = CollectorStore(tmp_path / "retained.sqlite3", max_retained_events=100)
    retained.initialize()
    retained.insert_batch([event(seq) for seq in range(100)])
    before = retention_snapshot(retained)
    changed = event(99)
    changed["payload"]["temperature_c"] += 1
    with pytest.raises(ConflictError):
        retained.insert_batch([event(100), changed])
    assert retention_snapshot(retained) == before
    failing = CommitFailureStore(retained.path, max_retained_events=100)
    with pytest.raises(sqlite3.DatabaseError):
        failing.insert_batch([event(100)])
    assert retention_snapshot(retained) == before
    retained.insert_batch([event(100)])
    assert retained.query_events(cursor=100).events[0].event_id == 101


def test_retention_keeps_all_current_batch_keys_including_old_duplicates(tmp_path):
    retained = CollectorStore(tmp_path / "retained.sqlite3", max_retained_events=100)
    retained.initialize()
    retained.insert_batch([event(seq) for seq in range(100)])
    batch = [event(0)] + [event(seq) for seq in range(100, 199)]
    assert retained.insert_batch(batch) == BatchResult(99, 1)
    stored = {row.event.seq for row in retained.query_events(limit=100).events}
    assert stored == {raw["seq"] for raw in batch}
    assert retained.insert_batch(batch) == BatchResult(0, 100)


def test_recent_stats_boundaries_and_row_cap_are_explicit(store):
    now = datetime(2026, 10, 12, 12, tzinfo=timezone.utc)
    start = now - timedelta(seconds=300)
    store.stats_max_events = 2
    store.insert_batch([event(seq) for seq in range(1, 6)])
    times = [start - timedelta(seconds=1), start, now - timedelta(seconds=1), now, now + timedelta(seconds=1)]
    with store._connection() as conn:
        with store._transaction(conn):
            for seq, received_at in enumerate(times, 1):
                conn.execute(
                    "UPDATE events SET received_at_utc = ? WHERE seq = ?",
                    (received_at.isoformat(timespec="microseconds"), seq),
                )
    result = store.stats(now=now)
    assert (result.retained_count, result.inserted_total) == (5, 5)
    assert result.retained_bytes == sum(len(canonical_event_bytes(validate_event(event(seq)))) for seq in range(1, 6))
    assert result.recent.sampled_count == 2
    assert result.recent.row_limit == 2
    assert result.recent.capped is True
    assert result.recent.by_kind == {"sensor.temperature": 2}
    store.stats_max_events = 10
    result = store.stats(now=now)
    assert result.recent.sampled_count == 3
    assert result.recent.capped is False
    with pytest.raises(ValueError):
        store.stats(now=now.replace(tzinfo=None))


def test_stats_route_and_query_storage_failures(api, tmp_path):
    application, client = api
    empty = client.get("/v1/stats")
    assert empty.status_code == 200
    assert empty.json()["last_received_at_utc"] is None
    application.state.store.insert_batch([event()])
    application.state.store.insert_batch([event()])
    stats = client.get("/v1/stats").json()
    assert stats["inserted_total"] == 1
    assert stats["duplicate_total"] == 1
    assert stats["retained_count"] == 1
    assert stats["recent"]["sampled_count"] == 1
    assert stats["dedup_scope"] == "retained_events"
    application.state.store.path = tmp_path / "missing.sqlite3"
    assert client.get("/v1/events").status_code == 503
    assert client.get("/v1/stats").status_code == 503
    assert not application.state.store.path.exists()


def test_previous_checkpoint_database_migrates_without_losing_content(tmp_path):
    path = tmp_path / "previous.sqlite3"
    content = canonical_event_bytes(validate_event(event()))
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript("""
            CREATE TABLE events (
                source_id TEXT NOT NULL, boot_id TEXT NOT NULL, seq INTEGER NOT NULL,
                kind TEXT NOT NULL, event_json TEXT NOT NULL, content_hash TEXT NOT NULL,
                received_at_utc TEXT NOT NULL, PRIMARY KEY (source_id, boot_id, seq)
            );
            CREATE TABLE counters (
                id INTEGER PRIMARY KEY, inserted_total INTEGER NOT NULL,
                duplicate_total INTEGER NOT NULL
            );
            INSERT INTO counters VALUES (1, 1, 7);
        """)
        from shared.schema import event_hash
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*event_key(validate_event(event())), "sensor.temperature", content.decode(),
             event_hash(validate_event(event())), datetime.now(timezone.utc).isoformat(timespec="microseconds")),
        )
        conn.commit()
    upgraded = CollectorStore(path)
    upgraded.initialize()
    upgraded.initialize()
    assert canonical_event_bytes(upgraded.query_events().events[0].event) == content
    assert upgraded.stats().duplicate_total == 7
    assert upgraded.insert_batch([event()]) == BatchResult(0, 1)
    upgraded.insert_batch([event(2)])
    assert [row.event_id for row in upgraded.query_events().events] == [1, 2]


@pytest.mark.parametrize("settings", [
    {"max_retained_events": 99}, {"max_retained_events": True},
    {"stats_window_seconds": 0}, {"stats_window_seconds": 86401},
    {"stats_max_events": 0}, {"stats_max_events": True},
])
def test_retention_and_stats_configuration_is_validated(tmp_path, settings):
    with pytest.raises(ValueError):
        CollectorStore(tmp_path / "unused.sqlite3", **settings)


def test_lower_retention_cap_is_applied_on_startup(tmp_path):
    original = CollectorStore(tmp_path / "retained.sqlite3")
    original.initialize()
    original.insert_batch([event(seq) for seq in range(100)])
    original.insert_batch([event(100)])
    lower = CollectorStore(original.path, max_retained_events=100)
    lower.initialize()
    lower.initialize()
    result = lower.stats()
    assert (result.retained_count, result.pruned_total, result.inserted_total) == (100, 1, 101)
    assert lower.query_events(limit=1).events[0].event.seq == 1
