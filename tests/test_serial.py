"""Bounded framing, unmodified identities, reconnects, and independent uploads."""
from collections import deque
from dataclasses import asdict
import json
import subprocess
import sys
from threading import Event as StopEvent, Thread
import time

import httpx
import pytest
import serial

from agent.main import _exclusive_outbox, AgentAlreadyRunningError, run_agent
from agent.outbox import Outbox
from agent.serial_source import NDJSONFramer, READ_CHUNK_BYTES, SerialSource, decode_frame, open_port
from agent.sources import SyntheticSource
from agent.uploader import Uploader
from collector.store import CollectorStore
from shared.schema import canonical_event_bytes, event_key, validate_event


def wire(event):
    return canonical_event_bytes(event) + b'\n'


def feed(framer, data):
    frames = []
    for offset in range(0, len(data), READ_CHUNK_BYTES):
        frames.extend(framer.feed(data[offset:offset + READ_CHUNK_BYTES]))
        assert framer.buffered_bytes <= framer.limit - 1
    return frames


class FakePort:
    def __init__(self, *chunks):
        self.chunks = deque(chunks)
        self.read_sizes = []
        self.closed = False

    @property
    def in_waiting(self):
        return len(self.chunks[0]) if self.chunks and isinstance(self.chunks[0], bytes) else 0

    def read(self, size):
        assert 1 <= size <= READ_CHUNK_BYTES
        self.read_sizes.append(size)
        if not self.chunks:
            return b''
        item = self.chunks.popleft()
        if isinstance(item, Exception):
            raise item
        if len(item) > size:
            self.chunks.appendleft(item[size:])
        return item[:size]

    def close(self):
        self.closed = True


def reader_for(port, **options):
    return SerialSource('/dev/test-serial', port_factory=lambda *args: port, **options)


def collect(reader, count):
    events = []
    for _ in range(2000):
        event = reader.poll_event(StopEvent())
        stats = reader.snapshot()
        assert stats['buffered_read_bytes'] <= 256 and stats['buffered_frame_bytes'] <= 1023
        if event is not None:
            events.append(event)
            if len(events) == count:
                return events
    pytest.fail('reader did not produce the requested valid events')


def ack(request):
    return httpx.Response(200, json={'inserted': len(json.loads(request.content)['events']), 'duplicates': 0})


def test_exact_1024_bytes_including_newline_are_accepted_next_byte_is_rejected():
    event = SyntheticSource().next_event()
    content = canonical_event_bytes(event)
    exact = content + b' ' * (1023 - len(content)) + b'\n'
    assert len(exact) == 1024
    framer = NDJSONFramer()
    assert canonical_event_bytes(decode_frame(feed(framer, exact)[0])) == content
    too_long = content + b' ' * (1024 - len(content)) + b'\n'
    assert feed(framer, too_long + wire(event)) == [content]
    assert framer.stats.oversized_lines == 1
    assert framer.stats.oversized_discarded_bytes == 1025


def test_oversized_tail_that_looks_valid_is_discarded_until_newline():
    events = list(SyntheticSource().events(2))
    bad = b'x' * 1024 + wire(events[0])
    framer = NDJSONFramer()
    assert feed(framer, bad + wire(events[1])) == [canonical_event_bytes(events[1])]
    assert framer.stats.oversized_lines == 1
    assert framer.stats.oversized_discarded_bytes == len(bad)


def test_unterminated_long_stream_keeps_no_growing_buffer_and_counts_once():
    framer = NDJSONFramer()
    for _ in range(1000):
        assert list(framer.feed(b'x' * 256)) == []
        assert framer.buffered_bytes <= 1023
    assert framer.buffered_bytes == 0
    assert framer.stats.oversized_lines == 1
    assert framer.stats.oversized_discarded_bytes == 256_000
    assert feed(framer, b'\nnext\n') == [b'next']


def test_utf8_split_crlf_and_multiple_partial_frames_preserve_content():
    raw = SyntheticSource().next_event().model_dump()
    raw['payload'] = {'temperature_c': None, 'sensor_ok': False, 'error_code': 'I2C_échec'}
    event = validate_event(raw)
    content = canonical_event_bytes(event)
    boundary = content.index('é'.encode()) + 1
    framer = NDJSONFramer()
    assert list(framer.feed(content[:boundary])) == []
    assert feed(framer, content[boundary:] + b'\r\n') == [content + b'\r']
    assert canonical_event_bytes(decode_frame(content + b'\r')) == content


@pytest.mark.parametrize('bad', [b'\xff', b'not json', b'NaN', b'Infinity', b'{"seq":0,"seq":1}'])
def test_bad_json_does_not_block_later_valid_frame(bad):
    event = SyntheticSource().next_event()
    reader = reader_for(FakePort(b'\n' + bad + b'\n' + wire(event)))
    assert canonical_event_bytes(collect(reader, 1)[0]) == canonical_event_bytes(event)
    assert reader.snapshot()['invalid_json'] == 1
    reader.close()


@pytest.mark.parametrize('change', [
    {'kind': 'button.action', 'payload': {}}, {'seq': '1'}, {'schema_version': 2},
    {'payload': {'temperature_c': 0, 'sensor_ok': False, 'error_code': 'I2C'}},
])
def test_schema_rejection_does_not_rewrite_ids_or_block_valid_neighbors(change):
    event = SyntheticSource().next_event()
    bad = {**event.model_dump(), **change}
    reader = reader_for(FakePort(b'\n' + json.dumps(bad).encode() + b'\n' + wire(event)))
    assert canonical_event_bytes(collect(reader, 1)[0]) == canonical_event_bytes(event)
    assert reader.snapshot()['invalid_schema'] == 1
    reader.close()


def test_deep_but_bounded_json_is_rejected_without_crashing_reader():
    event = SyntheticSource().next_event()
    nested = b'[' * 500 + b'0' + b']' * 500 + b'\n'
    reader = reader_for(FakePort(b'\n' + nested + wire(event)))
    assert canonical_event_bytes(collect(reader, 1)[0]) == canonical_event_bytes(event)
    assert reader.snapshot()['invalid_schema'] == 1
    reader.close()


def test_first_complete_line_is_explicitly_discarded_to_synchronize():
    first, second = list(SyntheticSource().events(2))
    reader = reader_for(FakePort(wire(first) + wire(second)))
    assert reader.source_id is reader.boot_id is None
    assert event_key(collect(reader, 1)[0]) == event_key(second)
    stats = reader.snapshot()
    assert stats['resync_lines'] == 1 and stats['resync_discarded_bytes'] == len(wire(first))
    assert stats['valid_events'] == 1
    reader.close()


def test_disconnect_discards_partial_and_resynchronizes_new_firmware_boot():
    before = SyntheticSource(boot_id='a' * 32).next_event()
    after = SyntheticSource(boot_id='b' * 32).next_event()
    partial = wire(before)[:30]
    old = FakePort(b'\n' + partial, serial.SerialException('disconnected'))
    new = FakePort(wire(before) + wire(after))  # First line resynchronizes.
    ports = iter([old, new])
    now = [0.0]
    reader = SerialSource('/dev/test-serial', port_factory=lambda *args: next(ports), clock=lambda: now[0])
    assert reader.poll_event(StopEvent()) is None
    assert reader.snapshot()['buffered_frame_bytes'] == len(partial)
    assert reader.poll_event(StopEvent()) is None
    assert old.closed and reader.snapshot()['truncated_frames'] == 1
    now[0] = 1.1
    assert canonical_event_bytes(collect(reader, 1)[0]) == canonical_event_bytes(after)
    assert reader.boot_id == 'b' * 32
    stats = reader.snapshot()
    assert stats['connections'] == 2 and stats['read_errors'] == 1 and stats['resync_lines'] == 2
    reader.close()


def test_open_error_has_bounded_diagnostic_and_retry_can_recover():
    event = SyntheticSource().next_event()
    calls = []
    now = [0.0]
    port = FakePort(b'\n' + wire(event))
    def open_fake(*args):
        calls.append(args)
        if len(calls) == 1:
            raise serial.SerialException('missing ' * 1000)
        return port
    reader = SerialSource('/dev/test-serial', port_factory=open_fake, clock=lambda: now[0])
    assert reader.poll_event(StopEvent()) is None
    assert reader.snapshot()['open_errors'] == 1
    assert len(reader.snapshot()['last_transport_error']) == 160
    now[0] = 0.99
    reader.poll_event(StopEvent())
    assert len(calls) == 1
    now[0] = 1.01
    assert event_key(collect(reader, 1)[0]) == event_key(event)
    assert reader.snapshot()['last_transport_error'] is None
    reader.close()


def test_close_counts_unprocessed_prefetch_and_never_reopens():
    framer = NDJSONFramer()
    feed(framer, b'partial')
    framer.clear_partial()
    assert framer.stats.truncated_frames == 1 and framer.stats.partial_discarded_bytes == 7
    events = list(SyntheticSource().events(2))
    reader = reader_for(FakePort(b'\n' + b''.join(wire(event) for event in events)))
    collect(reader, 1)
    remaining = reader.snapshot()['buffered_read_bytes']
    assert remaining > 0
    reader.close()
    reader.close()
    assert reader.snapshot()['unprocessed_read_bytes'] == remaining
    assert reader.snapshot()['buffered_frame_bytes'] == reader.snapshot()['buffered_read_bytes'] == 0
    with pytest.raises(RuntimeError, match='closed'):
        reader.poll_event(StopEvent())


def test_stop_before_poll_never_opens_device():
    stop = StopEvent()
    stop.set()
    reader = SerialSource('/dev/test-serial', port_factory=lambda *args: pytest.fail('opened after stop'))
    assert reader.poll_event(stop) is None
    reader.close()


@pytest.mark.parametrize('frame', ['text', b'x' * 1024])
def test_decoder_cannot_bypass_byte_bound(frame):
    with pytest.raises(ValueError):
        decode_frame(frame)


def test_unexpected_driver_failure_closes_reader_and_releases_runtime_lock(tmp_path):
    class BadPort(FakePort):
        def read(self, size):
            return b'x' * (size + 1)
    port = BadPort()
    reader = reader_for(port)
    outbox = Outbox(tmp_path / 'outbox.sqlite3')
    with httpx.Client(transport=httpx.MockTransport(ack)) as client:
        with Uploader(outbox, client=client) as uploader:
            with pytest.raises(RuntimeError, match='bounded read'):
                run_agent(outbox, reader, uploader, rate=None, count=1)
    assert port.closed and not reader.snapshot()['connected']
    with _exclusive_outbox(outbox):
        pass


def test_serial_commits_all_frames_while_http_upload_is_stalled(tmp_path):
    events = list(SyntheticSource(source_id='nano-esp32-01', boot_id='c' * 32).events(20))
    port = FakePort(b'\n' + b''.join(wire(event) for event in events))
    reader = reader_for(port)
    outbox = Outbox(tmp_path / 'outbox.sqlite3')
    entered, release = StopEvent(), StopEvent()
    results, errors = [], []
    def stalled(request):
        entered.set()
        assert release.wait(5)
        return ack(request)
    with httpx.Client(transport=httpx.MockTransport(stalled)) as client:
        with Uploader(outbox, client=client, batch_size=1) as uploader:
            def run():
                try:
                    results.append(run_agent(outbox, reader, uploader, rate=None, count=20, drain_timeout=3))
                except Exception as exc:
                    errors.append(exc)
            controller = Thread(target=run)
            controller.start()
            try:
                assert entered.wait(3)
                deadline = time.monotonic() + 3
                while outbox.stats().pending_count < 20 and time.monotonic() < deadline:
                    time.sleep(.005)
                assert outbox.stats().pending_count == 20
                assert {event_key(event): canonical_event_bytes(event) for event in outbox.peek_batch()} == {event_key(event): canonical_event_bytes(event) for event in events}
            finally:
                release.set()
                controller.join(5)
            assert not controller.is_alive()
    assert not errors
    result = results[0]
    assert result.accepted == result.offered == 20 and result.exit_code == 0
    assert result.serial['valid_events'] == 20 and not result.serial['connected'] and port.closed


def test_mixed_firmware_identities_out_of_order_replays_match_collector(tmp_path):
    a = list(SyntheticSource(source_id='nano-01', boot_id='a' * 32).events(3))
    b = list(SyntheticSource(source_id='nano-02', boot_id='b' * 32).events(2))
    events = [a[2], b[0], a[0], a[1], b[1], a[0]]
    reader = reader_for(FakePort(b'\n' + b''.join(wire(event) for event in events)))
    store = CollectorStore(tmp_path / 'collector.sqlite3')
    store.initialize()
    def collect_http(request):
        return httpx.Response(200, json=asdict(store.insert_batch(json.loads(request.content)['events'])))
    outbox = Outbox(tmp_path / 'outbox.sqlite3')
    with httpx.Client(transport=httpx.MockTransport(collect_http)) as client:
        with Uploader(outbox, client=client) as uploader:
            result = run_agent(outbox, reader, uploader, rate=None, count=len(events), drain_timeout=3)
    assert result.offered == result.accepted == 6 and result.exit_code == 0
    assert result.outbox.pending_count == result.outbox.quarantined_total == 0
    assert {event_key(row.event): canonical_event_bytes(row.event) for row in store.query_events().events} == {event_key(event): canonical_event_bytes(event) for event in a + b}


def test_zero_count_drains_without_opening_serial_port(tmp_path):
    outbox = Outbox(tmp_path / 'outbox.sqlite3')
    outbox.initialize()
    assert outbox.publish(SyntheticSource().next_event()).accepted
    reader = SerialSource('/dev/test-serial', port_factory=lambda *args: pytest.fail('opened during drain-only'))
    with httpx.Client(transport=httpx.MockTransport(ack)) as client:
        with Uploader(outbox, client=client) as uploader:
            result = run_agent(outbox, reader, uploader, count=0, rate=None)
    assert result.offered == result.accepted == result.outbox.pending_count == 0
    assert result.serial['connections'] == result.serial['open_errors'] == 0


def test_serial_capacity_rejection_is_visible_and_fails_run(tmp_path):
    events = list(SyntheticSource().events(2))
    reader = reader_for(FakePort(b'\n' + b''.join(wire(event) for event in events)))
    outbox = Outbox(tmp_path / 'outbox.sqlite3', max_pending_bytes=0)
    with httpx.Client(transport=httpx.MockTransport(ack)) as client:
        with Uploader(outbox, client=client) as uploader:
            result = run_agent(outbox, reader, uploader, rate=None, count=2)
    assert result.exit_code == 1 and result.rejections['capacity'] == 2
    assert result.serial['valid_events'] == 2 and result.accepted == 0


def test_reports_continue_during_empty_input_and_stop_without_reopening(tmp_path):
    reader = reader_for(FakePort())
    reports, stop = [], StopEvent()
    def report(value):
        reports.append(value)
        stop.set()
    outbox = Outbox(tmp_path / 'outbox.sqlite3')
    with httpx.Client(transport=httpx.MockTransport(ack)) as client:
        with Uploader(outbox, client=client) as uploader:
            result = run_agent(outbox, reader, uploader, rate=None, report_interval=.01, stop_event=stop, report=report)
    assert result.offered == result.accepted == 0 and result.exit_code == 0
    assert reports[0]['source_id'] is reports[0]['boot_id'] is None
    assert reports[0]['serial']['connected'] and not result.serial['connected']


def test_outbox_owner_refusal_never_opens_device(tmp_path):
    outbox = Outbox(tmp_path / 'outbox.sqlite3')
    reader = SerialSource('/dev/test-serial', port_factory=lambda *args: pytest.fail('opened before lock'))
    with _exclusive_outbox(outbox):
        with httpx.Client(transport=httpx.MockTransport(ack)) as client:
            with Uploader(outbox, client=client) as uploader:
                with pytest.raises(AgentAlreadyRunningError):
                    run_agent(outbox, reader, uploader, rate=None, count=1)
    assert not outbox.path.exists()


def test_native_factory_requests_low_control_lines_before_open_and_closes_failure(monkeypatch):
    calls = []
    class Port:
        def __init__(self, **kwargs):
            calls.append(kwargs)
        def open(self):
            assert self.dtr is self.rts is False
            assert self.port == '/dev/test-serial'
            raise serial.SerialException('failed')
        def close(self):
            calls.append('closed')
    monkeypatch.setattr(serial, 'Serial', Port)
    with pytest.raises(serial.SerialException):
        open_port('/dev/test-serial', 115200, .1)
    assert calls[0]['port'] is None and calls[0]['exclusive'] is True and calls[-1] == 'closed'


@pytest.mark.parametrize('options', [
    {'port': ''}, {'port': 'x\n'}, {'port': 'x' * 513}, {'baudrate': True}, {'baudrate': 0},
    {'timeout': 0}, {'timeout': float('nan')}, {'timeout': 10**1000}, {'reconnect_delay': 0},
])
def test_invalid_serial_configuration_has_no_io(options):
    with pytest.raises(ValueError):
        SerialSource(**{'port': '/dev/test-serial', **options}, port_factory=lambda *args: pytest.fail('opened'))


@pytest.mark.parametrize('limit', [True, 1, 1025])
def test_invalid_line_bound_is_rejected(limit):
    with pytest.raises(ValueError):
        NDJSONFramer(limit)


@pytest.mark.parametrize('options', [
    ['--source', 'serial'], ['--source', 'serial', '--serial-port', '/dev/test', '--rate', '100'],
    ['--source', 'serial', '--serial-port', '/dev/test', '--source-id', 'invented'],
    ['--source', 'serial', '--serial-port', '/dev/test', '--baud', '0'],
    ['--serial-port', '/dev/test'], ['--serial-timeout', '.1'],
])
def test_bad_cli_combinations_create_no_database(tmp_path, options):
    path = tmp_path / 'outbox.sqlite3'
    result = subprocess.run([sys.executable, '-m', 'agent.main', '--outbox', str(path), *options], capture_output=True, text=True, timeout=5)
    assert result.returncode == 2 and not path.exists() and not result.stdout
