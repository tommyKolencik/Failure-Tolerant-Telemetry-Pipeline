"""Verify CLI delivery and recovery using owned processes and temporary data."""
import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import platform
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import httpx

from agent.outbox import Outbox
from agent.sources import SyntheticSource
from shared.schema import canonical_event_bytes, event_key, validate_event


def stop_owned(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def verify(*, with_metrics=False, outage_seconds=0.0):
    with tempfile.TemporaryDirectory(prefix="telemetry-agent-check-") as directory:
        folder = Path(directory)
        outbox = Outbox(folder / "outbox.sqlite3")
        collector = agent = demo = None
        collector_log = (folder / "collector.log").open("w+")
        with httpx.Client(trust_env=False, timeout=1) as client:
            def start_server(database, module="collector.app", port=0):
                listener = socket.socket()
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", port))
                listener.listen(128)
                chosen = listener.getsockname()[1]
                code = f"import sys,uvicorn; from {module} import create_app; uvicorn.run(create_app(sys.argv[1]),fd=int(sys.argv[2]),log_level='error')"
                try:
                    process = subprocess.Popen(
                        [sys.executable, "-c", code, str(database), str(listener.fileno())],
                        cwd=ROOT, pass_fds=(listener.fileno(),), stdout=collector_log, stderr=collector_log,
                    )
                finally:
                    listener.close()
                endpoint = f"http://127.0.0.1:{chosen}"
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    try:
                        if client.get(endpoint + "/readyz").status_code == 200:
                            return process, endpoint, chosen
                    except httpx.RequestError:
                        pass
                    if process.poll() is not None:
                        break
                    time.sleep(0.02)
                stop_owned(process)
                collector_log.seek(0)
                raise RuntimeError("collector did not become ready: " + collector_log.read()[-1000:])

            def command(*options):
                return [sys.executable, "-m", "agent.main", "--outbox", str(outbox.path),
                        "--collector-url", url, "--seed", "7", *options]

            def run(*options, expected_exit=0):
                result = subprocess.run(command(*options), cwd=ROOT, capture_output=True, text=True, timeout=35)
                assert result.returncode == expected_exit, (result.returncode, result.stdout, result.stderr)
                assert not result.stderr, result.stderr
                return [json.loads(line) for line in result.stdout.splitlines()]

            def fetched():
                events, cursor = {}, None
                while True:
                    params = {"limit": 1000}
                    if cursor is not None:
                        params["cursor"] = cursor
                    response = client.get(url + "/v1/events", params=params)
                    response.raise_for_status()
                    page = response.json()
                    for row in page["events"]:
                        event = validate_event(row["event"])
                        key = event_key(event)
                        assert key not in events
                        events[key] = canonical_event_bytes(event)
                    cursor = page["next_cursor"]
                    if cursor is None:
                        return events

            try:
                collector, url, port = start_server(folder / "collector.sqlite3")
                reports = run("--count", "1000", "--rate", "100")
                assert reports[-1]["accepted"] == reports[-1]["offered"] == 1000
                expected = {event_key(event): canonical_event_bytes(event) for event in
                            SyntheticSource(seed=7, boot_id=reports[0]["boot_id"]).events(1000)}
                assert fetched() == expected

                # Kill owned processes with pending rows to verify recovery and lock release.
                stop_owned(collector)
                collector = None
                outage_started = time.monotonic()
                agent = subprocess.Popen(command("--rate", "100"), cwd=ROOT,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                starting = json.loads(agent.stdout.readline())
                collision = run("--count", "0", expected_exit=1)
                assert "another agent" in collision[-1]["reason"]
                deadline = time.monotonic() + outage_seconds + 8
                while (outbox.stats().pending_count < 50 or time.monotonic() - outage_started < outage_seconds) and time.monotonic() < deadline:
                    assert agent.poll() is None
                    time.sleep(0.01)
                assert outbox.stats().pending_count >= 50
                agent.kill()
                agent.communicate(timeout=5)
                assert agent.returncode == -signal.SIGKILL
                agent = None
                with sqlite3.connect(outbox.path) as connection:
                    pending = [validate_event(json.loads(row[0])) for row in
                               connection.execute("SELECT event_json FROM pending ORDER BY seq")]
                generated = list(SyntheticSource(seed=7, boot_id=starting["boot_id"]).events(len(pending)))
                assert [canonical_event_bytes(event) for event in pending] == [canonical_event_bytes(event) for event in generated]
                expected.update({event_key(event): canonical_event_bytes(event) for event in pending})

                collector, url, _ = start_server(folder / "collector.sqlite3", port=port)
                outage_elapsed = time.monotonic() - outage_started
                recovery_started = time.monotonic()
                recovered = run("--count", "0")
                assert recovered[0]["boot_id"] != starting["boot_id"]
                assert recovered[-1]["offered"] == recovered[-1]["accepted"] == 0
                recovery_elapsed = time.monotonic() - recovery_started
                assert fetched() == expected

                agent = subprocess.Popen(command("--rate", "100"), cwd=ROOT,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                graceful_start = json.loads(agent.stdout.readline())
                time.sleep(0.15)
                agent.terminate()
                stdout, stderr = agent.communicate(timeout=15)
                assert not stderr, stderr
                final = [json.loads(line) for line in stdout.splitlines()][-1]
                assert agent.returncode == final["exit_code"] == 143
                assert final["offered"] == final["accepted"] > 0
                expected.update({event_key(event): canonical_event_bytes(event) for event in
                                SyntheticSource(seed=7, boot_id=graceful_start["boot_id"]).events(final["accepted"])})
                assert fetched() == expected
                metrics = None
                if with_metrics:
                    # Snapshot accepted host/demo content before recovery.
                    demo, demo_url, _ = start_server(outbox.path, module="demo.app")
                    for path, status in [("/normal", 200), ("/slow", 200), ("/error", 500), ("/items/42?secret=excluded", 200)]:
                        assert client.get(demo_url + path).status_code == status
                    # Body receipt can precede telemetry COMMIT; wait for acceptance.
                    deadline = time.monotonic() + 5
                    while client.get(demo_url + "/telemetry").json()["recording"]["accepted"] < 4 and time.monotonic() < deadline:
                        time.sleep(0.01)
                    assert outbox.stats().pending_count == 4
                    stop_owned(collector)
                    collector = None
                    live = run("--source", "host", "--count", "3", "--rate", "2", "--drain-timeout", "0", expected_exit=1)
                    assert live[-1]["offered"] == live[-1]["accepted"] == 3
                    with sqlite3.connect(outbox.path) as connection:
                        measured = [validate_event(json.loads(row[0])) for row in
                                    connection.execute("SELECT event_json FROM pending ORDER BY source_id, boot_id, seq")]
                    assert len(measured) == 7
                    hosts = [event for event in measured if event.kind == "host.metrics"]
                    assert [event.seq for event in hosts] == [0, 1, 2]
                    assert hosts[0].payload.cpu_percent is None
                    assert all(event.payload.cpu_percent is not None for event in hosts[1:])
                    requests = [event for event in measured if event.kind == "http.request"]
                    assert [event.payload.route for event in requests] == ["/normal", "/slow", "/error", "/items/{item_id}"]
                    assert requests[1].payload.duration_ms >= 100
                    assert "secret" not in "".join(canonical_event_bytes(event).decode() for event in requests)
                    expected.update({event_key(event): canonical_event_bytes(event) for event in measured})
                    collector, url, _ = start_server(folder / "collector.sqlite3", port=port)
                    run("--count", "0")
                    assert fetched() == expected
                    recent = client.get(url + "/v1/stats").json()["recent"]["http_requests"]
                    assert recent["sampled_count"] == 4 and recent["error_count"] == 1
                    metrics = {"host_events": 3, "request_events": 4,
                               "slow_request_duration_ms": requests[1].payload.duration_ms,
                               "request_stats": recent, "exact_accepted_content_recovered": True}
                stats = outbox.stats()
                assert stats.pending_count == stats.quarantined_total == stats.capacity_rejections == 0

                files = ["agent/main.py", "agent/outbox.py", "agent/sources.py", "agent/uploader.py",
                         "collector/app.py", "collector/store.py", "shared/config.py", "shared/schema.py",
                         "scripts/verify-agent.py", "demo/app.py"]
                hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in files}
                return {
                    "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                    "host": socket.gethostname(), "python": platform.python_version(),
                    "platform": platform.platform(), "sqlite": sqlite3.sqlite_version,
                    "versions": {name: version(name) for name in ("httpx", "fastapi", "pydantic", "uvicorn")},
                    "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                    "file_sha256": hashes, "seed": 7, "target_offered_rate": 100,
                    "transport": "real localhost HTTP, separate collector and agent CLI processes",
                    "first_reconciliation_events": 1000, "hard_kill_pending_recovered": len(pending),
                    "requested_outage_seconds": outage_seconds, "outage_to_ready_seconds": outage_elapsed,
                    "readiness_to_recovery_seconds": recovery_elapsed,
                    "concurrent_agent_rejected": True, "graceful_sigterm_events": final["accepted"],
                    "graceful_sigterm_exit_code": 143, "final_reconciliation_events": len(expected),
                    "pending_after": stats.pending_count, "quarantined_total": stats.quarantined_total,
                    "metrics": metrics,
                    "limits": "same-host functional check; not a sustained throughput benchmark, collector commit-boundary crash, or Arduino hardware test",
                }
            finally:
                stop_owned(agent)
                stop_owned(demo)
                stop_owned(collector)
                collector_log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--with-metrics", action="store_true", help="also check real host CPU/RAM and separate demo HTTP process")
    parser.add_argument("--outage-seconds", type=float, default=0.0, help="minimum collector outage while producing, 0..60 seconds")
    args = parser.parse_args()
    if not 0 <= args.outage_seconds <= 60:
        parser.error("outage-seconds must be in 0..60")
    evidence = verify(with_metrics=args.with_metrics, outage_seconds=args.outage_seconds)
    content = json.dumps(evidence, indent=2) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content, encoding="utf-8")
    print(content, end="")
