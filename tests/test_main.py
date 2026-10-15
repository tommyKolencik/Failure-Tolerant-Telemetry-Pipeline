"""Agent lifecycle: independent production, recovery, failure, and real signals."""
from dataclasses import asdict
import json
import signal
import subprocess
import sys
from threading import Event as StopEvent, Thread
import time

import httpx
import pytest

from agent.main import AgentAlreadyRunningError, _exclusive_outbox, run_agent
from agent.outbox import Outbox
from agent.sources import SyntheticSource
from agent.uploader import Uploader
from collector.store import CollectorStore
from shared.schema import canonical_event_bytes, event_key


def ack(request):
    return httpx.Response(200, json={"inserted": len(json.loads(request.content)["events"]), "duplicates": 0})


def test_slow_upload_does_not_stop_production_and_shutdown_joins_worker(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    entered, release = StopEvent(), StopEvent()
    results, errors = [], []
    def slow(request):
        entered.set()
        assert release.wait(5)
        return ack(request)
    with httpx.Client(transport=httpx.MockTransport(slow)) as client:
        with Uploader(outbox, client=client, batch_size=1) as uploader:
            def run():
                try:
                    results.append(run_agent(outbox, SyntheticSource(), uploader, count=20, rate=1_000, drain_timeout=3))
                except Exception as exc:
                    errors.append(exc)
            controller = Thread(target=run)
            controller.start()
            try:
                assert entered.wait(3)
                deadline = time.monotonic() + 3
                while outbox.stats().pending_count < 20 and time.monotonic() < deadline:
                    time.sleep(0.005)
                # No ACK can finish yet, but the source still committed all 20.
                assert outbox.stats().pending_count == 20
            finally:
                release.set()
                controller.join(timeout=5)
            assert not controller.is_alive()
    assert not errors
    assert results[0].accepted == results[0].offered == 20
    assert results[0].exit_code == results[0].outbox.pending_count == 0


def test_restart_recovers_old_keys_and_new_source_session(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    old_source = SyntheticSource(seed=7)
    def offline(request):
        raise httpx.ConnectError("collector offline", request=request)
    with httpx.Client(transport=httpx.MockTransport(offline)) as client:
        with Uploader(outbox, client=client) as uploader:
            first = run_agent(outbox, old_source, uploader, count=5, rate=1_000, drain_timeout=0)
    assert first.accepted == first.outbox.pending_count == 5
    assert first.exit_code == 1
    expected = {event_key(event): canonical_event_bytes(event) for event in outbox.peek_batch()}
    # Reopen the database and make a new source, as a fresh process would.
    reopened = Outbox(outbox.path)
    new_source = SyntheticSource(seed=7)
    expected.update({event_key(event): canonical_event_bytes(event) for event in SyntheticSource(seed=7, boot_id=new_source.boot_id).events(3)})
    store = CollectorStore(tmp_path / "collector.sqlite3")
    store.initialize()
    def collect(request):
        result = store.insert_batch(json.loads(request.content)["events"])
        return httpx.Response(200, json=asdict(result))
    with httpx.Client(transport=httpx.MockTransport(collect)) as client:
        with Uploader(reopened, client=client) as uploader:
            second = run_agent(reopened, new_source, uploader, count=3, rate=1_000, drain_timeout=3)
    assert second.offered == second.accepted == 3
    assert new_source.boot_id != old_source.boot_id
    assert second.exit_code == second.outbox.pending_count == 0
    actual = {event_key(row.event): canonical_event_bytes(row.event) for row in store.query_events().events}
    assert actual == expected


def test_zero_count_drains_backlog_without_producing(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    for event in SyntheticSource().events(4):
        assert outbox.publish(event).accepted
    with httpx.Client(transport=httpx.MockTransport(ack)) as client:
        with Uploader(outbox, client=client) as uploader:
            result = run_agent(outbox, SyntheticSource(), uploader, count=0)
    assert result.offered == result.accepted == 0
    assert result.exit_code == result.outbox.pending_count == 0


def test_capacity_rejections_are_visible_and_fail_run(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3", max_pending_bytes=0)
    with httpx.Client(transport=httpx.MockTransport(ack)) as client:
        with Uploader(outbox, client=client) as uploader:
            result = run_agent(outbox, SyntheticSource(), uploader, count=5, rate=1_000)
    assert result.offered == result.rejections["capacity"] == result.outbox.capacity_rejections == 5
    assert result.accepted == result.outbox.pending_count == 0
    assert result.exit_code == 1


@pytest.mark.parametrize("status", [401, 422])
def test_blocked_upload_or_quarantine_is_a_visible_failure(tmp_path, status):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(status))) as client:
        with Uploader(outbox, client=client, batch_size=1) as uploader:
            result = run_agent(outbox, SyntheticSource(), uploader, count=1, drain_timeout=2)
    assert result.exit_code == 1
    assert result.accepted == 1
    if status == 401:
        assert result.outbox.pending_count == 1
        assert "401" in result.reason
    else:
        assert result.outbox.pending_count == 0
        assert result.outbox.quarantined_total == 1
        assert "quarantined" in result.reason


def test_worker_exception_stops_continuous_producer_and_releases_lock(tmp_path, monkeypatch):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    with httpx.Client(transport=httpx.MockTransport(ack)) as client:
        with Uploader(outbox, client=client) as uploader:
            def crash(stop):
                raise RuntimeError("worker failed")
            monkeypatch.setattr(uploader, "run", crash)
            result = run_agent(outbox, SyntheticSource(), uploader)
    assert result.exit_code == 1
    assert result.reason == "uploader failed: RuntimeError"
    with _exclusive_outbox(outbox):
        pass


def test_second_agent_cannot_take_same_outbox(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    with _exclusive_outbox(outbox):
        with httpx.Client(transport=httpx.MockTransport(ack)) as client:
            with Uploader(outbox, client=client) as uploader:
                with pytest.raises(AgentAlreadyRunningError):
                    run_agent(outbox, SyntheticSource(), uploader, count=1)
        assert not outbox.path.exists()


def test_stop_interrupts_a_long_producer_wait_and_reports_aggregates(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    stop = StopEvent()
    reports = []
    def report(value):
        reports.append(value)
        stop.set()
    with httpx.Client(transport=httpx.MockTransport(ack)) as client:
        with Uploader(outbox, client=client) as uploader:
            result = run_agent(outbox, SyntheticSource(), uploader, rate=0.01, report_interval=0.01,
                               stop_event=stop, report=report, drain_timeout=2)
    assert result.offered == result.accepted == 1
    assert result.exit_code == 0
    assert result.reason == "stopped and drained"
    assert reports
    assert all(value["offered"] == value["accepted"] == 1 for value in reports)
    assert "payload" not in json.dumps(reports)


@pytest.mark.parametrize("kwargs", [
    {"count": -1}, {"count": True}, {"rate": 0}, {"rate": float("nan")},
    {"rate": True}, {"drain_timeout": -1}, {"report_interval": 0},
])
def test_bad_runtime_configuration_does_not_create_database(tmp_path, kwargs):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    with httpx.Client(transport=httpx.MockTransport(ack)) as client:
        with Uploader(outbox, client=client) as uploader:
            with pytest.raises(ValueError):
                run_agent(outbox, SyntheticSource(), uploader, **kwargs)
    assert not outbox.path.exists()


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_cli_real_signal_preserves_exact_accepted_events(tmp_path, signum):
    path = tmp_path / "outbox.sqlite3"
    command = [sys.executable, "-m", "agent.main", "--outbox", str(path), "--rate", "10",
               "--collector-url", "http://127.0.0.1:1", "--drain-timeout", "0"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        starting = json.loads(process.stdout.readline())
        assert starting["type"] == "starting"
        outbox = Outbox(path)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if path.exists():
                try:
                    if outbox.stats().pending_count:
                        break
                except Exception:
                    pass
            time.sleep(0.005)
        else:
            pytest.fail("agent did not persist any events")
        process.send_signal(signum)
        stdout, stderr = process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
    assert not stderr
    finished = [json.loads(line) for line in stdout.splitlines()][-1]
    assert finished["type"] == "finished"
    assert finished["accepted"] > 0
    assert finished["accepted"] == finished["offered"] == finished["outbox"]["pending_count"]
    # Leftovers give exit 1 even when a signal triggered the stop.
    assert process.returncode == finished["exit_code"] == 1
    expected = list(SyntheticSource(boot_id=starting["boot_id"]).events(finished["accepted"]))
    assert [canonical_event_bytes(event) for event in outbox.peek_batch()] == [canonical_event_bytes(event) for event in expected]
