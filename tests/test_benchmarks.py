"""Benchmark measurements must follow durability and compare exact content."""
from dataclasses import asdict
import json
import sqlite3

import httpx
import pytest

from agent.outbox import Outbox
from agent.sources import SyntheticSource
from benchmarks.run import Ledger, MeasuredOutbox, MeasuredTransport, MeasuredUploader, Settings, main, percentiles, reconcile, recovery_timing
from collector.store import CollectorStore
from shared.schema import canonical_event_bytes, event_key


def measured(tmp_path, count=2):
    ledger = Ledger()
    outbox = MeasuredOutbox(tmp_path / "outbox.sqlite3", ledger)
    outbox.initialize()
    events = list(SyntheticSource().events(count))
    for event in events:
        assert outbox.publish(event).accepted
    return ledger, outbox, events


def test_lost_ack_has_no_trusted_latency_or_local_ack_until_safe_retry(tmp_path):
    ledger, outbox, events = measured(tmp_path)
    store = CollectorStore(tmp_path / "collector.sqlite3")
    store.initialize()
    bodies = []
    def handler(request):
        bodies.append(request.content)
        result = store.insert_batch(json.loads(request.content)["events"])
        if len(bodies) == 1:
            raise httpx.ReadTimeout("ACK lost after COMMIT", request=request)
        return httpx.Response(200, json=asdict(result))
    transport = MeasuredTransport(httpx.MockTransport(handler))
    with httpx.Client(transport=transport) as client:
        uploader = MeasuredUploader(outbox, "http://collector", ledger, transport, client=client)
        assert uploader.upload_once().status == "retry"
        assert ledger.attempts[0]["http_start_ns"] is not None
        assert ledger.attempts[0]["http_ack_ms"] is None
        assert all(row["local_ack_ns"] is None for row in ledger.events.values())
        assert uploader.upload_once().duplicates == 2
    assert bodies[0] == bodies[1]
    assert ledger.attempts[1]["http_ack_ms"] >= 0
    assert ledger.accepted_count == ledger.acknowledged_count == 2
    for row in ledger.events.values():
        assert row["publish_start_ns"] <= row["publish_return_ns"] <= row["local_ack_ns"]
    actual = {event_key(row.event): canonical_event_bytes(row.event).decode() for row in store.query_events().events}
    assert reconcile(ledger, actual, {}, {})["complete"]


def test_trusted_collector_ack_is_separate_from_failed_local_deletion(tmp_path, monkeypatch):
    ledger, outbox, events = measured(tmp_path)
    transport = MeasuredTransport(httpx.MockTransport(lambda request: httpx.Response(200, json={"inserted": 2, "duplicates": 0})))
    def fail_commit(self, keys):
        raise sqlite3.OperationalError("local commit failed")
    monkeypatch.setattr(Outbox, "acknowledge", fail_commit)
    with httpx.Client(transport=transport) as client:
        result = MeasuredUploader(outbox, "http://collector", ledger, transport, client=client).upload_once()
    assert result.status == "retry"
    assert ledger.attempts[0]["http_ack_ms"] >= 0  # Remote ACK was valid.
    assert ledger.acknowledged_count == 0  # Local deletion did not commit.
    assert all(row["local_ack_ns"] is None for row in ledger.events.values())
    assert outbox.stats().pending_count == 2


def test_untrustworthy_ack_does_not_count_as_latency_sample(tmp_path):
    ledger, outbox, events = measured(tmp_path)
    transport = MeasuredTransport(httpx.MockTransport(lambda request: httpx.Response(200, json={"inserted": 1, "duplicates": 0})))
    with httpx.Client(transport=transport) as client:
        result = MeasuredUploader(outbox, "http://collector", ledger, transport, client=client).upload_once()
    assert result.status == "retry"
    assert ledger.attempts[0]["http_ack_ms"] is None
    assert ledger.acknowledged_count == 0


def test_equal_counts_cannot_hide_missing_unexpected_or_changed_events(tmp_path):
    ledger, outbox, events = measured(tmp_path)
    outbox.acknowledge([event_key(event) for event in events])
    expected = {event_key(event): canonical_event_bytes(event).decode() for event in events}
    replaced = dict(expected)
    del replaced[event_key(events[1])]
    extra = SyntheticSource().next_event()
    replaced[event_key(extra)] = canonical_event_bytes(extra).decode()
    result = reconcile(ledger, replaced, {}, {})
    assert result["accepted"] == result["stored"] == 2
    assert not result["complete"]
    assert result["counts"]["missing_from_collector"] == result["counts"]["unexpected_stored"] == 1
    changed = dict(expected)
    changed[event_key(events[0])] = "different content"
    assert reconcile(ledger, changed, {}, {})["counts"]["conflicting_content"] == 1
    good = reconcile(ledger, expected, {}, {})
    assert good["complete"]
    assert good["accepted_sha256"] == good["stored_sha256"]


def test_capacity_rejections_and_pending_rows_prevent_false_success(tmp_path):
    ledger = Ledger()
    outbox = MeasuredOutbox(tmp_path / "outbox.sqlite3", ledger, max_pending_bytes=0)
    outbox.initialize()
    assert outbox.publish(SyntheticSource().next_event()).status == "capacity"
    result = reconcile(ledger, {}, {}, {})
    assert result["accepted"] == 0 and result["counts"]["rejected"] == 1
    assert not result["complete"]
    ledger, outbox, events = measured(tmp_path / "other")
    pending = {event_key(event): canonical_event_bytes(event).decode() for event in events}
    result = reconcile(ledger, {}, pending, {})
    assert result["counts"]["pending"] == result["counts"]["missing_from_collector"] == 2
    assert result["counts"]["unaccounted"] == 0
    assert not result["complete"]


def test_raw_observations_preserve_event_identity_and_monotonic_timestamps(tmp_path):
    ledger, outbox, events = measured(tmp_path)
    outbox.acknowledge([event_key(event) for event in events])
    ledger.save(tmp_path)
    rows = [json.loads(line) for line in (tmp_path / "events.ndjson").read_text().splitlines()]
    assert [tuple(row["key"]) for row in rows] == [event_key(event) for event in events]
    assert [row["event_json"] for row in rows] == [canonical_event_bytes(event).decode() for event in events]
    assert all(row["accepted"] and row["publish_start_ns"] <= row["local_ack_ns"] for row in rows)


def test_percentiles_are_nearest_rank_and_empty_samples_are_explicit():
    assert percentiles([]) == {"p50_ms": None, "p95_ms": None, "p99_ms": None}
    assert percentiles([100.0, 1.0, 10.0]) == {"p50_ms": 10.0, "p95_ms": 100.0, "p99_ms": 100.0}


def test_recovery_tracks_the_specified_backlog_despite_new_traffic(tmp_path):
    ledger, outbox, events = measured(tmp_path, count=3)
    keys = [event_key(event) for event in events[:2]]
    ledger.events[keys[0]]["local_ack_ns"] = 200_000_000
    assert recovery_timing(ledger, keys, 100_000_000)["readiness_to_drain_seconds"] is None
    ledger.events[keys[1]]["local_ack_ns"] = 400_000_000
    # The third event remains pending; it was not part of the recovery target.
    assert recovery_timing(ledger, keys, 100_000_000)["readiness_to_drain_seconds"] == 0.3
    early = recovery_timing(ledger, keys, 500_000_000)
    assert early["readiness_to_drain_seconds"] == 0
    assert early["drained_before_readiness_probe"]
    assert recovery_timing(ledger, [], 100_000_000)["readiness_to_drain_seconds"] is None


@pytest.mark.parametrize("options", [
    {"duration": float("nan")}, {"rates": (float("inf"),)}, {"rates": (1e308,)},
    {"rates": (1e-300,)}, {"rates": (10**1000,)}, {"duration": 10**1000},
    {"rates": (100, 100)}, {"batch_sizes": (0,)}, {"batch_sizes": (101,)},
    {"repeats": True}, {"rates": (2000,), "duration": 60},
    {"duration": 2, "outage_seconds": 2, "outage_at": 1},
])
def test_invalid_or_unbounded_matrix_is_rejected(options):
    with pytest.raises(ValueError):
        Settings(**options)


def test_dry_run_validates_and_prints_plan_without_creating_artifacts(tmp_path, capsys):
    path = tmp_path / "results"
    assert main(["--duration", "2", "--repeats", "1", "--rates", "100", "200", "--output", str(path), "--dry-run"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["trials"] == 6
    assert plan["production_seconds_total"] == 12
    assert not path.exists()
