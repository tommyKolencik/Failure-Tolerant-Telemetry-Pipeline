"""Transport failures and durable ACK/quarantine boundaries with HTTPX."""
from contextlib import contextmanager
import json
import random
import sqlite3
from threading import Event as ThreadEvent, Thread

from fastapi.testclient import TestClient
import httpx
import pytest

from agent.outbox import Outbox
from agent.sources import SyntheticSource
from agent.uploader import Uploader, UploaderBlockedError
from collector.app import create_app
from shared.config import HTTP_ACK_MAX_BYTES
from shared.schema import canonical_event_bytes, event_key


@pytest.fixture
def queued(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    source = SyntheticSource(seed=7)
    events = list(source.events(3))
    for event in events:
        assert outbox.publish(event).accepted
    return outbox, source, events


@pytest.fixture
def api(tmp_path):
    application = create_app(tmp_path / "collector.sqlite3")
    with TestClient(application) as client:
        yield application, client


def forwarding_client(api, *, lose_first_ack=False):
    application, collector = api
    requests = []
    def handler(request):
        requests.append(request.content)
        response = collector.post("/v1/events/batch", content=request.content, headers={"Content-Type": "application/json"})
        if lose_first_ack and len(requests) == 1:
            raise httpx.ReadTimeout("ACK lost after collector commit", request=request)
        return httpx.Response(response.status_code, content=response.content, headers=response.headers)
    return httpx.Client(transport=httpx.MockTransport(handler)), requests


def test_valid_ack_drains_only_the_submitted_rows(queued):
    outbox, source, events = queued
    bodies = []
    def handler(request):
        bodies.append(json.loads(request.content))
        assert request.url.path == "/v1/events/batch"
        assert request.headers["accept-encoding"] == "identity"
        assert request.extensions["timeout"]["connect"] == 2.0
        return httpx.Response(200, json={"inserted": 2, "duplicates": 0})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with Uploader(outbox, batch_size=2, client=client) as uploader:
            result = uploader.upload_once()
        assert not client.is_closed
    assert result.status == "acked"
    assert result.acknowledged == result.inserted == 2
    assert [event_key(event) for event in outbox.peek_batch()] == [event_key(events[2])]
    assert len(bodies[0]["events"]) == 2


@pytest.mark.parametrize("ack", [
    {"inserted": True, "duplicates": 2}, {"inserted": "3", "duplicates": 0},
    {"inserted": 3.0, "duplicates": 0}, {"inserted": -1, "duplicates": 4},
    {"inserted": 2, "duplicates": 0}, {"inserted": 4, "duplicates": 0},
    {"inserted": 3}, {"inserted": 3, "duplicates": 0, "extra": 1}, [],
])
def test_untrustworthy_ack_never_deletes_or_quarantines(queued, ack):
    outbox, source, events = queued
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=ack))) as client:
        result = Uploader(outbox, client=client).upload_once()
    assert result.status == "retry"
    assert result.retry_after_seconds > 0
    assert outbox.stats().pending_count == 3
    assert outbox.stats().quarantined_total == 0


@pytest.mark.parametrize("body,headers", [
    (b"{", {"Content-Type": "application/json"}),
    (b"\xff", {"Content-Type": "application/json"}),
    (b"{}", {"Content-Type": "text/html"}),
    (b"{}", {"Content-Type": "application/json", "Content-Length": "-1"}),
    (b"{}", {"Content-Type": "application/json", "Content-Length": str(HTTP_ACK_MAX_BYTES + 1)}),
])
def test_malformed_or_mislabelled_success_keeps_rows(queued, body, headers):
    outbox, source, events = queued
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body, headers=headers))) as client:
        assert Uploader(outbox, client=client).upload_once().status == "retry"
    assert outbox.stats().pending_count == 3


def test_streamed_ack_is_bounded_with_false_content_length(queued):
    outbox, source, events = queued
    chunks_read = []
    class TooLarge(httpx.SyncByteStream):
        def __iter__(self):
            for index in range(3):
                chunks_read.append(index)
                yield b" " * HTTP_ACK_MAX_BYTES
    def handler(request):
        return httpx.Response(200, stream=TooLarge(), headers={"Content-Type": "application/json", "Content-Length": "1"})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = Uploader(outbox, client=client).upload_once()
    assert result.status == "retry"
    assert chunks_read == [0, 1]
    assert outbox.stats().pending_count == 3


def test_compressed_ack_is_rejected_before_body_read(queued):
    outbox, source, events = queued
    class MustNotRead(httpx.SyncByteStream):
        def __iter__(self):
            raise AssertionError("compressed body was read")
            yield b""
    def handler(request):
        return httpx.Response(200, stream=MustNotRead(), headers={"Content-Type": "application/json", "Content-Encoding": "gzip"})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert Uploader(outbox, client=client).upload_once().status == "retry"
    assert outbox.stats().pending_count == 3


@pytest.mark.parametrize("status", [202, 204, 408, 429, 500, 503])
def test_transient_or_non_acknowledging_status_keeps_original_batch(queued, status):
    outbox, source, events = queued
    requests = []
    def handler(request):
        requests.append(request.content)
        if len(requests) == 1:
            return httpx.Response(status)
        return httpx.Response(200, json={"inserted": 3, "duplicates": 0})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        uploader = Uploader(outbox, client=client)
        assert uploader.upload_once().status == "retry"
        assert outbox.publish(source.next_event()).accepted
        assert uploader.upload_once().status == "acked"
    assert requests[0] == requests[1]
    assert outbox.stats().pending_count == 1


@pytest.mark.parametrize("exception", [httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError])
def test_transport_failures_preserve_pending_content(queued, exception):
    outbox, source, events = queued
    def handler(request):
        raise exception("failure", request=request)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert Uploader(outbox, client=client).upload_once().status == "retry"
    assert [canonical_event_bytes(event) for event in outbox.peek_batch()] == [canonical_event_bytes(event) for event in events]


def test_backoff_grows_to_cap_and_resets_after_ack(queued):
    outbox, source, events = queued
    statuses = iter([503] * 6 + [200, 503])
    def handler(request):
        status = next(statuses)
        return httpx.Response(200, json={"inserted": 3, "duplicates": 0}) if status == 200 else httpx.Response(status)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        uploader = Uploader(outbox, client=client, retry_base_seconds=1, retry_cap_seconds=4, rng=random.Random(7))
        for window in (1, 2, 4, 4, 4, 4):
            result = uploader.upload_once()
            assert window / 2 <= result.retry_after_seconds <= window
        assert uploader.upload_once().status == "acked"
        outbox.publish(source.next_event())
        result = uploader.upload_once()
        assert 0.5 <= result.retry_after_seconds <= 1


def test_lost_ack_after_real_collector_commit_replays_duplicates(queued, api):
    outbox, source, events = queued
    client, requests = forwarding_client(api, lose_first_ack=True)
    with client:
        uploader = Uploader(outbox, client=client)
        assert uploader.upload_once().status == "retry"
        assert outbox.stats().pending_count == 3
        assert api[0].state.store.stats().retained_count == 3
        result = uploader.upload_once()
    assert result.duplicates == result.acknowledged == 3
    assert requests[0] == requests[1]
    assert api[0].state.store.stats().retained_count == 3
    assert outbox.stats().pending_count == 0


def test_permanent_batch_conflict_isolates_bad_event_and_delivers_neighbors(queued, api):
    outbox, source, events = queued
    changed = events[1].model_dump(mode="python")
    changed["payload"]["temperature_c"] += 1
    api[0].state.store.insert_batch([changed])
    client, requests = forwarding_client(api)
    with client:
        uploader = Uploader(outbox, client=client)
        assert uploader.upload_once().status == "isolating"
        assert outbox.stats().pending_count == 3
        assert uploader.upload_once().status == "acked"
        assert uploader.upload_once().status == "quarantined"
        assert uploader.upload_once().status == "acked"
        assert uploader.upload_once().status == "idle"
        for event in source.events(2):
            outbox.publish(event)
        assert uploader.upload_once().acknowledged == 2
    assert [len(json.loads(body)["events"]) for body in requests] == [3, 1, 1, 1, 2]
    assert outbox.stats().quarantined_total == 1
    assert outbox.peek_quarantine()[0].key == event_key(events[1])
    assert outbox.peek_quarantine()[0].http_status == 409
    assert api[0].state.store.stats().retained_count == 5


@pytest.mark.parametrize("status", [400, 409, 413, 422])
def test_single_permanent_rejection_is_archived(queued, status):
    outbox, source, events = queued
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(status))) as client:
        result = Uploader(outbox, client=client, batch_size=1).upload_once()
    assert (result.status, result.quarantined) == ("quarantined", 1)
    assert outbox.stats().pending_count == 2
    assert outbox.peek_quarantine()[0].http_status == status


@pytest.mark.parametrize("status", [301, 401, 403, 404, 405, 415])
def test_endpoint_or_auth_problem_blocks_without_quarantine(queued, status):
    outbox, source, events = queued
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(status, headers={"Location": "http://other/v1/events/batch"})), follow_redirects=True) as client:
        uploader = Uploader(outbox, client=client)
        with pytest.raises(UploaderBlockedError):
            uploader.run(ThreadEvent())
    assert outbox.stats().pending_count == 3
    assert outbox.stats().quarantined_total == 0


def test_wrapper_utf8_bytes_and_exact_body_limit_control_batch_size(queued):
    outbox, source, events = queued
    limit = len(json.dumps({"events": [events[0].model_dump(mode="json")]}, separators=(",", ":"), ensure_ascii=False).encode())
    bodies = []
    def handler(request):
        bodies.append(request.content)
        return httpx.Response(200, json={"inserted": 1, "duplicates": 0})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert Uploader(outbox, client=client, max_body_bytes=limit).upload_once().acknowledged == 1
    assert len(bodies[0]) == limit
    assert outbox.stats().pending_count == 2


def test_oversized_event_is_archived_locally_and_later_event_progresses(tmp_path):
    outbox = Outbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    source = SyntheticSource()
    oversized = source.next_event().model_dump(mode="python")
    oversized["payload"] = {"temperature_c": None, "sensor_ok": False, "error_code": "🌡" * 64}
    outbox.publish(oversized)
    small = source.next_event()
    outbox.publish(small)
    calls = []
    def handler(request):
        calls.append(request.content)
        return httpx.Response(200, json={"inserted": 1, "duplicates": 0})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        uploader = Uploader(outbox, client=client, max_body_bytes=350)
        assert uploader.upload_once().quarantined == 1
        assert calls == []
        assert uploader.upload_once().acknowledged == 1
    assert outbox.stats().pending_count == 0
    assert json.loads(calls[0])["events"][0]["seq"] == small.seq


def test_quarantine_capacity_failure_blocks_and_preserves_pending(queued):
    outbox, source, events = queued
    outbox.max_quarantine_bytes = 1
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(422))) as client:
        result = Uploader(outbox, client=client, batch_size=1).upload_once()
    assert result.status == "blocked"
    assert outbox.stats().pending_count == 3
    assert outbox.stats().quarantined_total == 0


def test_failed_ack_deletion_commit_preserves_batch_for_duplicate_retry(queued, api):
    outbox, source, events = queued
    class FailingDelete(Outbox):
        @contextmanager
        def _connection(self, **kwargs):
            with super()._connection(**kwargs) as conn:
                def authorize(action, argument, unused, database, trigger):
                    return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and argument == "COMMIT" else sqlite3.SQLITE_OK
                conn.set_authorizer(authorize)
                yield conn
    client, requests = forwarding_client(api)
    with client:
        uploader = Uploader(FailingDelete(outbox.path), client=client)
        assert uploader.upload_once().status == "retry"
        assert outbox.stats().pending_count == 3
        uploader.outbox = outbox
        assert uploader.upload_once().duplicates == 3
    assert outbox.stats().pending_count == 0
    assert api[0].state.store.stats().retained_count == 3


def test_worker_collects_partial_batch_then_flushes(tmp_path):
    empty = ThreadEvent()
    stop = ThreadEvent()
    errors = []
    class ObservedOutbox(Outbox):
        def peek_batch(self, limit=100):
            result = super().peek_batch(limit)
            if not result:
                empty.set()
            return result
    outbox = ObservedOutbox(tmp_path / "outbox.sqlite3")
    outbox.initialize()
    bodies = []
    def handler(request):
        bodies.append(request.content)
        stop.set()
        count = len(json.loads(request.content)["events"])
        return httpx.Response(200, json={"inserted": count, "duplicates": 0})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        uploader = Uploader(outbox, client=client, batch_size=3, flush_wait_seconds=0.1)
        def run():
            try:
                uploader.run(stop)
            except Exception as exc:
                errors.append(exc)
        worker = Thread(target=run)
        worker.start()
        try:
            assert empty.wait(2)
            for event in SyntheticSource().events(2):
                outbox.publish(event)
            worker.join(2)
            assert not worker.is_alive()
        finally:
            stop.set()
            worker.join(2)
    assert errors == []
    assert len(json.loads(bodies[0])["events"]) == 2
    assert outbox.stats().pending_count == 0


def test_stop_interrupts_long_retry_wait(queued):
    outbox, source, events = queued
    called, stop = ThreadEvent(), ThreadEvent()
    def handler(request):
        called.set()
        return httpx.Response(503)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        uploader = Uploader(outbox, client=client, retry_base_seconds=5, retry_cap_seconds=5)
        worker = Thread(target=uploader.run, args=(stop,))
        worker.start()
        try:
            assert called.wait(2)
            stop.set()
            worker.join(1)
            assert not worker.is_alive()
        finally:
            stop.set()
            worker.join(2)
    assert outbox.stats().pending_count == 3


@pytest.mark.parametrize("settings", [
    {"batch_size": 0}, {"batch_size": 101}, {"batch_size": True},
    {"max_body_bytes": 0}, {"max_body_bytes": 131073},
    {"flush_wait_seconds": 0.3}, {"flush_wait_seconds": 0},
    {"retry_base_seconds": float("nan")}, {"retry_cap_seconds": float("inf")},
    {"retry_base_seconds": 2, "retry_cap_seconds": 1},
])
def test_invalid_uploader_configuration_is_rejected(queued, settings):
    with pytest.raises(ValueError):
        Uploader(queued[0], **settings)


@pytest.mark.parametrize("url", ["relative/path", "ftp://host", "http://host?x=1", "http://host#fragment"])
def test_invalid_collector_base_url_is_rejected(queued, url):
    with pytest.raises(ValueError):
        Uploader(queued[0], url)


def test_one_thousand_events_reconcile_through_actual_uploader(queued, api):
    outbox, source, original = queued
    original += list(source.events(997))
    for event in original[3:]:
        outbox.publish(event)
    client, requests = forwarding_client(api)
    with client:
        uploader = Uploader(outbox, client=client)
        while outbox.stats().pending_count:
            assert uploader.upload_once().status == "acked"
    page = api[0].state.store.query_events(limit=1000)
    actual = {event_key(row.event): canonical_event_bytes(row.event) for row in page.events}
    expected = {event_key(event): canonical_event_bytes(event) for event in original}
    assert actual == expected
    assert len(requests) == 10
    assert outbox.stats().quarantined_total == 0
