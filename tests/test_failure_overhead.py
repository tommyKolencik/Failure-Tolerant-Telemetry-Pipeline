"""Crash boundary isolation, database recovery, and honest overhead comparisons."""
import json
import signal
import subprocess
import sys

import pytest

from agent.sources import HttpRequestSource, SyntheticSource
from benchmarks.crash_checks import FaultingStore
from benchmarks.demo_overhead import AuditedOutbox, Settings, assess_events, main, paired_results, request_plan
from collector.store import CollectorStore
from shared.schema import canonical_event_bytes, event_key


@pytest.mark.parametrize("stage", ["before_commit", "after_commit"])
def test_real_sigkill_preserves_atomic_rows_counters_and_replay(tmp_path, stage):
    database, marker = tmp_path / "collector.sqlite3", tmp_path / "marker.json"
    base = CollectorStore(database)
    base.initialize()
    old = SyntheticSource(source_id="old-01").next_event()
    base.insert_batch([old])
    new = list(SyntheticSource().events(3))
    inputs = tmp_path / "input.json"
    inputs.write_text(json.dumps([event.model_dump() for event in new]))
    code = "import sys,json; from benchmarks.crash_checks import FaultingStore; s=FaultingStore(sys.argv[1],fault_stage=sys.argv[2],marker_path=sys.argv[3]); s.initialize(); s.insert_batch(json.load(open(sys.argv[4])))"
    result = subprocess.run([sys.executable, "-c", code, str(database), stage, str(marker), str(inputs)], capture_output=True, text=True, timeout=10)
    assert result.returncode == -signal.SIGKILL
    witness = json.loads(marker.read_text())
    assert witness["in_transaction"] == (stage == "before_commit")
    assert witness["visible_rows_on_connection"] == 4
    recovered = CollectorStore(database)
    recovered.initialize()
    survived = [row.event for row in recovered.query_events().events]
    expected = [old] + (new if stage == "after_commit" else [])
    assert [canonical_event_bytes(event) for event in survived] == [canonical_event_bytes(event) for event in expected]
    assert recovered.stats().inserted_total == len(expected)
    replay = recovered.insert_batch(new)
    assert replay.inserted == (3 if stage == "before_commit" else 0)
    assert replay.duplicates == (3 if stage == "after_commit" else 0)
    assert recovered.stats().retained_count == 4


def test_fault_store_startup_and_read_transactions_do_not_trigger_crash(tmp_path):
    marker = tmp_path / "marker.json"
    store = FaultingStore(tmp_path / "collector.sqlite3", fault_stage="before_commit", marker_path=marker)
    store.initialize()
    store.check_ready()
    assert store.stats().retained_count == 0
    assert not marker.exists()


def test_invalid_fault_ingest_rolls_back_context_without_triggering_commit(tmp_path):
    marker = tmp_path / "marker.json"
    store = FaultingStore(tmp_path / "collector.sqlite3", fault_stage="after_commit", marker_path=marker)
    store.initialize()
    with pytest.raises(ValueError):
        store.insert_batch([])
    store.initialize()  # Would SIGKILL if the failed call left the gate armed.
    assert not marker.exists()


def test_audit_reconciles_content_acks_keys_and_status_semantics(tmp_path):
    settings = Settings(requests=3, concurrency=1)
    source = HttpRequestSource()
    events = [source.request_event(route=route, method="GET", status=status, duration_ms=1.0) for route, status in request_plan(3)]
    outbox = AuditedOutbox(tmp_path / "outbox.sqlite3", 3)
    outbox.initialize()
    for event in events:
        assert outbox.publish(event).accepted
    assert len(outbox.peek_batch()) == 3
    outbox.acknowledge([event_key(event) for event in events])
    recording = {"source_id": source.source_id, "boot_id": source.boot_id, "accepted": 3,
                 "capacity": 0, "unavailable": 0, "invalid": 0, "conflict": 0, "recording_errors": 0}
    stored = dict(outbox.seen)
    assert assess_events(outbox, stored, recording, settings, True)["complete"]
    changed = events[0].model_dump()
    changed["payload"]["status"] = 500
    from shared.schema import validate_event
    wrong = validate_event(changed)
    # Even matching audit/store content must not conceal a wrong normal status.
    outbox.seen[event_key(wrong)] = canonical_event_bytes(wrong).decode()
    stored = dict(outbox.seen)
    result = assess_events(outbox, stored, recording, settings, True)
    assert not result["complete"]
    assert result["counts"]["wrong_kind_method_or_status"] == 1


def test_disabled_mode_requires_zero_telemetry_even_with_good_http_behavior(tmp_path):
    settings = Settings(requests=3, concurrency=1)
    outbox = AuditedOutbox(tmp_path / "outbox.sqlite3", 3)
    outbox.initialize()
    recording = {"source_id": "demo-http-01", "boot_id": "a" * 32, "accepted": 0,
                 "capacity": 0, "unavailable": 0, "invalid": 0, "conflict": 0, "recording_errors": 0}
    assert assess_events(outbox, {}, recording, settings, False)["complete"]
    event = HttpRequestSource().request_event(route="/normal", method="GET", status=200, duration_ms=0.0)
    assert not assess_events(outbox, {event_key(event): canonical_event_bytes(event).decode()}, recording, settings, False)["complete"]


def test_failed_or_unmatched_workloads_cannot_produce_overhead_delta():
    base = {"repeat": 1, "complete": True, "requests_planned": 3, "concurrency": 1, "slow_delay": 0.1,
            "per_route_client_latency": {route: {"p95_ms": 1.0} for route, _ in request_plan(3)},
            "demo_cpu_ms_per_request": 1.0}
    disabled, enabled = {**base, "enabled": False}, {**base, "enabled": True, "demo_cpu_ms_per_request": 2.0}
    assert paired_results([disabled]) == []
    result = paired_results([disabled, enabled])[0]
    assert result["valid_comparison"] and result["demo_cpu_ms_per_request_delta"] == 1.0
    for bad in ({**enabled, "complete": False}, {**enabled, "concurrency": 2}):
        result = paired_results([disabled, bad])[0]
        assert not result["valid_comparison"] and result["enabled_minus_disabled"] is None


@pytest.mark.parametrize("options", [
    {"requests": True}, {"concurrency": 0}, {"requests": 3, "concurrency": 4},
    {"slow_delay": float("nan")}, {"requests": 100_000, "concurrency": 1, "slow_delay": 5},
])
def test_unbounded_or_invalid_demo_plan_is_rejected(options):
    with pytest.raises(ValueError):
        Settings(**options)


def test_demo_dry_run_preserves_matched_plan_and_writes_nothing(tmp_path, capsys):
    directory = tmp_path / "run"
    assert main(["--requests", "30", "--concurrency", "2", "--repeats", "1", "--output", str(directory), "--dry-run"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["trials"] == 2 and plan["requests_total"] == 60
    assert not directory.exists()
