"""Typed live sources, normalized request measurements, and mixed delivery."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from types import SimpleNamespace

from fastapi.testclient import TestClient
import httpx
from pydantic import ValidationError
import pytest

from agent.main import run_agent
from agent.outbox import Outbox
from agent.sources import HostMetricsSource, HttpRequestSource, SyntheticSource
from agent.uploader import Uploader
from collector.app import create_app as collector_app
from collector.store import CollectorStore, ConflictError
from demo.app import RequestRecorder, RequestTelemetryMiddleware, create_app
from shared.schema import BatchRequest, HostMetricsEvent, HttpRequestEvent, canonical_event_bytes, event_key, validate_event


class Clock:
    now = 0
    def __call__(self):
        return self.now


def host(**kwargs):
    return HostMetricsSource(memory_reader=lambda: SimpleNamespace(total=8_000, available=2_000), **kwargs)


def request_event(**changes):
    payload = {"route": "/normal", "method": "GET", "status": 200, "duration_ms": 2.5}
    payload.update(changes)
    return HttpRequestSource().request_event(**payload)


def test_discriminator_selects_correct_payload_and_rejects_mismatches():
    events = [SyntheticSource().next_event(), host(cpu_reader=lambda: 0.0).next_event(), request_event()]
    batch = BatchRequest.model_validate({"events": [event.model_dump() for event in events]})
    assert [event.kind for event in batch.events] == ["sensor.temperature", "host.metrics", "http.request"]
    assert isinstance(batch.events[1], HostMetricsEvent)
    assert isinstance(batch.events[2], HttpRequestEvent)
    raw = events[0].model_dump()
    raw["kind"] = "host.metrics"
    with pytest.raises(ValidationError):
        validate_event(raw)
    raw["kind"] = "button.action"  # Still unfinished, not an arbitrary payload.
    with pytest.raises(ValidationError):
        validate_event(raw)


@pytest.mark.parametrize("changes", [
    {"cpu_percent": True, "cpu_sample_us": 100_000},
    {"cpu_percent": float("nan"), "cpu_sample_us": 100_000},
    {"cpu_percent": 101.0, "cpu_sample_us": 100_000},
    {"cpu_percent": 5.0, "cpu_sample_us": None},
    {"cpu_percent": None, "cpu_sample_us": 100_000},
    {"memory_available_bytes": 9_000}, {"memory_total_bytes": "8000"},
])
def test_bad_host_measurements_cannot_cross_acceptance_boundary(tmp_path, changes):
    raw = host(cpu_reader=lambda: 0.0).next_event().model_dump()
    raw["payload"].update(changes)
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    assert outbox.publish(raw).status == "invalid"
    assert outbox.stats().pending_count == 0


@pytest.mark.parametrize("changes", [
    {"route": "/normal?secret=123"}, {"route": "/normal\n"}, {"method": "get"},
    {"status": True}, {"status": 600}, {"duration_ms": float("inf")}, {"duration_ms": -1.0},
])
def test_bad_request_measurements_are_rejected(changes):
    with pytest.raises(ValidationError):
        request_event(**changes)


def test_cpu_warmup_short_intervals_and_real_uptime_are_explicit():
    clock = Clock()
    reads = iter([99.0, 25.0, 0.0])  # First is ignored, last is a valid idle CPU.
    calls = []
    def cpu():
        calls.append(clock.now)
        return next(reads)
    source = host(clock_ns=clock, cpu_reader=cpu)
    first = source.next_event()
    assert first.payload.cpu_percent is first.payload.cpu_sample_us is None
    clock.now = 50_000_000
    early = source.next_event()
    assert early.payload.cpu_percent is early.payload.cpu_sample_us is None
    assert len(calls) == 1  # Do not reset the baseline for a too-short tick.
    clock.now = 200_000_000
    measured = source.next_event()
    assert measured.payload.cpu_percent == 25.0
    assert measured.payload.cpu_sample_us == measured.source_uptime_us == 200_000
    clock.now = 400_000_000
    idle = source.next_event()
    assert idle.payload.cpu_percent == 0.0
    assert [first.seq, early.seq, measured.seq, idle.seq] == [0, 1, 2, 3]


def test_host_sampler_cannot_move_to_a_thread_with_another_cpu_baseline():
    source = host(cpu_reader=lambda: 0.0)
    source.next_event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(RuntimeError, match="one producer thread"):
            pool.submit(source.next_event).result()


def test_concurrent_request_events_have_unique_keys_and_failed_validation_does_not_advance():
    source = HttpRequestSource()
    with pytest.raises(ValidationError):
        source.request_event(route="bad", method="GET", status=200, duration_ms=0.0)
    def produce(index):
        return source.request_event(route="/items/{item_id}", method="GET", status=200, duration_ms=float(index))
    with ThreadPoolExecutor(max_workers=8) as pool:
        events = list(pool.map(produce, range(100)))
    assert sorted(event.seq for event in events) == list(range(100))
    assert len({event_key(event) for event in events}) == 100
    assert len({event.boot_id for event in events}) == 1


def test_demo_routes_record_actual_duration_normalized_path_and_errors(tmp_path):
    application = create_app(tmp_path / "outbox.sqlite3", slow_delay_seconds=0.025)
    with TestClient(application) as client:
        assert client.get("/normal").json() == {"status": "ok"}
        assert client.get("/slow").status_code == 200
        assert client.get("/error").status_code == 500
        assert client.get("/items/42?secret=redacted").json() == {"item_id": 42}
        assert client.get("/items/99").status_code == 200
        assert client.get("/missing/private/id?secret=x").status_code == 404
        assert client.get("/healthz").status_code == client.get("/readyz").status_code == 200
        status = client.get("/telemetry").json()
        assert status["recording"]["accepted"] == 6
    events = application.state.outbox.peek_batch()
    assert [event.payload.route for event in events] == ["/normal", "/slow", "/error", "/items/{item_id}", "/items/{item_id}", "/__unmatched__"]
    assert events[1].payload.duration_ms >= 25
    assert [event.payload.status for event in events] == [200, 200, 500, 200, 200, 404]
    assert all(event.kind == "http.request" for event in events)
    assert "secret" not in "".join(canonical_event_bytes(event).decode() for event in events)


@pytest.mark.parametrize("enabled", [True, False])
def test_instrumentation_configuration_preserves_app_behavior(tmp_path, enabled):
    path = tmp_path / "outbox.sqlite3"
    application = create_app(path, instrumentation_enabled=enabled, max_pending_bytes=0)
    with TestClient(application) as client:
        assert client.get("/normal").status_code == 200
        assert client.get("/error").status_code == 500
        report = client.get("/telemetry").json()
    assert report["recording"]["accepted"] == 0
    assert report["recording"]["capacity"] == (2 if enabled else 0)
    assert path.exists() == enabled


def test_unavailable_telemetry_does_not_replace_response(tmp_path, monkeypatch):
    application = create_app(tmp_path / "outbox.sqlite3")
    with TestClient(application) as client:
        monkeypatch.setattr(application.state.recorder.source, "request_event", lambda **kwargs: (_ for _ in ()).throw(OSError("storage failure")))
        assert client.get("/normal").status_code == 200
        assert client.get("/telemetry").json()["recording"]["recording_errors"] == 1


def test_timer_includes_streamed_body_but_excludes_background_work(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    clock = Clock()
    recorder = RequestRecorder(outbox, HttpRequestSource())
    async def application(scope, receive, send):
        scope["route"] = SimpleNamespace(path="/stream")
        clock.now = 5_000_000
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"a", "more_body": True})
        clock.now = 30_000_000
        await send({"type": "http.response.body", "body": b"b", "more_body": False})
        clock.now = 1_000_000_000  # Work after the final body is excluded.
    middleware = RequestTelemetryMiddleware(application, recorder, clock_ns=clock)
    async def receive():
        return {"type": "http.request", "body": b""}
    async def send(message):
        pass
    asyncio.run(middleware({"type": "http", "path": "/stream", "method": "GET"}, receive, send))
    assert outbox.peek_batch()[0].payload.duration_ms == 30.0


def test_mixed_events_recover_from_lost_ack_and_models_are_revalidated(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    demo = create_app(outbox.path, slow_delay_seconds=0)
    with TestClient(demo) as client:
        client.get("/normal")
        client.get("/error")
    for event in [SyntheticSource().next_event(), host(cpu_reader=lambda: 0.0).next_event()]:
        assert outbox.publish(event).accepted
    expected = {event_key(event): canonical_event_bytes(event) for event in outbox.peek_batch()}
    application = collector_app(tmp_path / "collector.sqlite3")
    calls = []
    with TestClient(application) as collector:
        def send(request):
            calls.append(request.content)
            response = collector.post("/v1/events/batch", content=request.content, headers={"Content-Type": "application/json"})
            if len(calls) == 1:
                raise httpx.ReadTimeout("ACK lost after mixed batch COMMIT", request=request)
            return httpx.Response(response.status_code, content=response.content, headers=response.headers)
        with httpx.Client(transport=httpx.MockTransport(send)) as client:
            with Uploader(outbox, client=client) as uploader:
                assert uploader.upload_once().status == "retry"
                assert uploader.upload_once().duplicates == 4
        rows = collector.get("/v1/events").json()["events"]
        actual = {event_key(validate_event(row["event"])): canonical_event_bytes(validate_event(row["event"])) for row in rows}
        assert actual == expected
        stats = collector.get("/v1/stats").json()["recent"]["http_requests"]
        assert stats["sampled_count"] == 2
        assert stats["error_count"] == 1
    assert outbox.stats().pending_count == 0
    mutated = host(cpu_reader=lambda: 0.0).next_event()
    mutated.payload.memory_available_bytes = 9_000
    assert outbox.publish(mutated).status == "invalid"
    with pytest.raises(ValidationError):
        application.state.store.insert_batch([SyntheticSource().next_event(), mutated])
    assert application.state.store.stats().retained_count == 4


def test_conflict_detection_includes_kind_and_rolls_back_entire_mixed_batch(tmp_path):
    store = CollectorStore(tmp_path / "collector.sqlite3")
    store.initialize()
    original = request_event()
    store.insert_batch([original])
    changed = host(source_id=original.source_id, boot_id=original.boot_id, cpu_reader=lambda: 0.0).next_event()
    assert event_key(changed) == event_key(original)
    with pytest.raises(ConflictError):
        store.insert_batch([SyntheticSource().next_event(), changed])
    assert store.stats().retained_count == 1
    assert canonical_event_bytes(store.query_events().events[0].event) == canonical_event_bytes(original)


def test_request_stats_use_bounded_sample_and_explicit_empty_values(tmp_path):
    store = CollectorStore(tmp_path / "collector.sqlite3", stats_max_events=2)
    store.initialize()
    assert store.stats().recent.http_requests.p50_duration_ms is None
    source = HttpRequestSource()
    events = [source.request_event(route="/normal", method="GET", status=status, duration_ms=duration)
              for status, duration in [(200, 1.0), (404, 10.0), (500, 20.0)]]
    store.insert_batch(events)
    recent = store.stats().recent
    assert recent.capped
    assert recent.http_requests.sampled_count == recent.http_requests.error_count == 2
    assert recent.http_requests.status_classes == {"4xx": 1, "5xx": 1}
    assert recent.http_requests.mean_duration_ms == 15.0
    assert recent.http_requests.p50_duration_ms == 10.0
    assert recent.http_requests.p95_duration_ms == recent.http_requests.p99_duration_ms == 20.0


def test_host_events_use_agent_runtime_and_upload_independently(tmp_path):
    clock = Clock()
    source = host(clock_ns=clock, cpu_reader=lambda: 25.0)
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    collected = []
    def send(request):
        events = json.loads(request.content)["events"]
        collected.extend(events)
        return httpx.Response(200, json={"inserted": len(events), "duplicates": 0})
    with httpx.Client(transport=httpx.MockTransport(send)) as client:
        with Uploader(outbox, client=client) as uploader:
            result = run_agent(outbox, source, uploader, count=3, rate=1_000, drain_timeout=2)
    assert result.exit_code == result.outbox.pending_count == 0
    assert result.offered == result.accepted == len(collected) == 3
    assert all(validate_event(event).kind == "host.metrics" for event in collected)
