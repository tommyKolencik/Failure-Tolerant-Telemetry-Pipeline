"""Verify real serial/CLI/HTTP delivery with owned pseudo-terminals."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import pty
import select
import signal
import sqlite3
import subprocess
import sys
import time
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from importlib.metadata import version
from agent.outbox import Outbox
from agent.sources import SyntheticSource
from benchmarks.run import OwnedCollector, metadata, record_sources, Settings, stored_events, write_json
from shared.schema import canonical_event_bytes, event_key, validate_event


def stop_owned(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(5)


def reports(path: Path) -> list[dict]:
    content = path.read_bytes()
    if len(content) > 512 * 1024:
        raise RuntimeError('verification report log exceeded its bound')
    # Ignore a partially written final log row.
    complete = content[:content.rfind(b'\n') + 1]
    return [json.loads(line) for line in complete.splitlines()]


def wait_for(check, *, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(.02)
    raise AssertionError('verification condition timed out')


def write_wire(master, payload, process):
    # Bound writes by a deadline; small chunks exercise frame fragmentation.
    deadline = time.monotonic() + 15
    position = 0
    while position < len(payload):
        if process.poll() is not None or time.monotonic() >= deadline:
            raise AssertionError('serial writer could not finish its bounded workload')
        if not select.select([], [master], [], .05)[1]:
            continue
        try:
            position += os.write(master, payload[position:position + 71])
        except BlockingIOError:
            pass


def wire(event):
    return canonical_event_bytes(event) + b'\n'


def pending(outbox):
    with sqlite3.connect(outbox.path.as_uri() + '?mode=ro', uri=True) as connection:
        values = [validate_event(json.loads(row[0])) for row in connection.execute('SELECT event_json FROM pending')]
    return {event_key(event): canonical_event_bytes(event).decode() for event in values}


def verify(directory: Path) -> dict:
    collector = OwnedCollector(directory)
    outbox = Outbox(directory / 'outbox.sqlite3')
    process = None
    handles, fds = [], []
    link = directory / 'serial-port'

    def make_terminal():
        master, slave = pty.openpty()
        fds.extend([master, slave])
        os.set_blocking(master, False)
        replacement = directory / ('port-link-' + uuid4().hex)
        replacement.symlink_to(os.ttyname(slave))
        os.replace(replacement, link)  # Only our verifier's own symlink.
        return master

    def start_agent(name, *options):
        log, err = directory / (name + '.ndjson'), directory / (name + '.stderr')
        output, errors = log.open('wb'), err.open('wb')
        handles.extend([output, errors])
        command = [sys.executable, '-m', 'agent.main', '--source', 'serial', '--serial-port', str(link),
                   '--outbox', str(outbox.path), '--collector-url', collector.url,
                   '--report-interval', '.05', '--serial-reconnect-delay', '.2', *options]
        agent = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=errors)
        return agent, log, err

    def latest(log, agent):
        if agent.poll() is not None:
            raise AssertionError('serial agent exited unexpectedly; inspect its logs')
        rows = reports(log)
        return rows[-1] if rows else {}

    def progress(log, agent, condition):
        row = latest(log, agent)
        return row if row.get('type') == 'progress' and condition(row) else None

    try:
        collector.start()
        master = make_terminal()
        process, log, err = start_agent('serial-agent')
        wait_for(lambda: progress(log, process, lambda row: row['serial']['connected']))
        assert reports(log)[0]['source_id'] is reports(log)[0]['boot_id'] is None

        events = list(SyntheticSource(source_id='nano-esp32-01', boot_id='c' * 32).events(1000))
        failed_sensor = events[-1].model_dump()
        failed_sensor['payload'] = {'temperature_c': None, 'sensor_ok': False, 'error_code': 'TEST_échec'}
        events[-1] = validate_event(failed_sensor)
        expected = {event_key(event): canonical_event_bytes(event).decode() for event in events}
        sentinel = events[0].model_dump()
        sentinel.update(seq=2000, source_uptime_us=20_000_000)
        sentinel = validate_event(sentinel)
        unsupported = {**events[0].model_dump(), 'kind': 'button.action', 'payload': {}}
        invalid = {**events[0].model_dump(), 'seq': '1'}
        junk = (b'\n\xff\nnot-json\nNaN\n{"seq":0,"seq":1}\n \t\r\n'
                + json.dumps(unsupported).encode() + b'\n' + json.dumps(invalid).encode() + b'\n'
                + b'X' * 1200 + wire(sentinel))
        first = canonical_event_bytes(events[0])
        exact = first + b' ' * (1023 - len(first)) + b'\n'
        assert len(exact) == 1024
        write_wire(master, junk + exact + b''.join(wire(event) for event in events[1:]), process)
        initial = wait_for(lambda: progress(log, process, lambda row: row['offered'] == row['accepted'] == 1000 and row['outbox']['pending_count'] == 0))
        assert stored_events(collector.url) == expected
        framing = initial['serial']
        assert framing['invalid_json'] == 4 and framing['invalid_schema'] == 2
        assert framing['oversized_lines'] == framing['empty_lines'] == framing['resync_lines'] == 1
        print(json.dumps({'type': 'initial_delivery', 'unique_events': 1000, 'framing': framing}), flush=True)

        # Replay after local ACKs to exercise collector deduplication.
        write_wire(master, b''.join(wire(event) for event in events[:10]), process)
        wait_for(lambda: progress(log, process, lambda row: row['offered'] == row['accepted'] == 1010 and row['outbox']['pending_count'] == 0))
        assert stored_events(collector.url) == expected

        partial = b'{"schema_version":1,"source_id":"nano-'
        write_wire(master, partial, process)
        wait_for(lambda: progress(log, process, lambda row: row['serial']['buffered_frame_bytes'] == len(partial)))
        os.close(master)
        fds.remove(master)
        disconnected = wait_for(lambda: progress(log, process, lambda row: row['serial']['read_errors'] >= 1 and row['serial']['truncated_frames'] == 1))
        assert disconnected['serial']['partial_discarded_bytes'] == len(partial)
        master = make_terminal()
        wait_for(lambda: progress(log, process, lambda row: row['serial']['connections'] == 2 and row['serial']['connected']))
        rebooted = SyntheticSource(source_id='nano-esp32-01', boot_id='d' * 32)
        new = list(rebooted.events(10))
        # Discard the first line after reopen, even if it looks valid.
        ordered = [new[2], new[0], new[1], *new[3:]]
        write_wire(master, wire(sentinel) + b''.join(wire(event) for event in ordered), process)
        expected.update({event_key(event): canonical_event_bytes(event).decode() for event in new})
        reconnected = wait_for(lambda: progress(log, process, lambda row: row['offered'] == row['accepted'] == 1020 and row['outbox']['pending_count'] == 0))
        assert reconnected['boot_id'] == 'd' * 32 and reconnected['serial']['resync_lines'] == 2
        assert stored_events(collector.url) == expected
        print(json.dumps({'type': 'reconnect', 'new_events': 10, 'partial_bytes_discarded': len(partial)}), flush=True)

        collector.stop()
        later = list(rebooted.events(50))
        later_expected = {event_key(event): canonical_event_bytes(event).decode() for event in later}
        expected.update(later_expected)
        write_wire(master, b''.join(wire(event) for event in later), process)
        before_kill = wait_for(lambda: progress(log, process, lambda row: row['offered'] == row['accepted'] == 1070 and row['outbox']['pending_count'] == 50))
        process.kill()
        assert process.wait(5) == -signal.SIGKILL
        assert pending(outbox) == later_expected
        assert not err.read_bytes()
        process = None
        collector.start()
        process, drain_log, drain_err = start_agent('drain-agent', '--count', '0')
        assert process.wait(10) == 0
        drain = reports(drain_log)[-1]
        assert drain['offered'] == drain['accepted'] == 0
        assert drain['serial']['connections'] == drain['serial']['open_errors'] == 0
        assert drain['outbox']['pending_count'] == drain['outbox']['quarantined_total'] == 0
        assert stored_events(collector.url) == expected and not drain_err.read_bytes()
        process = None
        print(json.dumps({'type': 'outage_kill_recovery', 'pending_survived': 50, 'drain_serial_connections': 0}), flush=True)

        process, stop_log, stop_err = start_agent('signal-agent')
        wait_for(lambda: progress(stop_log, process, lambda row: row['serial']['connected']))
        final_events = list(SyntheticSource(source_id='native-stop-01', boot_id='f' * 32).events(3))
        expected.update({event_key(event): canonical_event_bytes(event).decode() for event in final_events})
        write_wire(master, b'\n' + b''.join(wire(event) for event in final_events), process)
        wait_for(lambda: progress(stop_log, process, lambda row: row['offered'] == row['accepted'] == 3 and row['outbox']['pending_count'] == 0))
        process.send_signal(signal.SIGTERM)
        assert process.wait(10) == 143
        finished = reports(stop_log)[-1]
        assert finished['exit_code'] == 143 and not finished['serial']['connected']
        assert not stop_err.read_bytes()
        process = None
        actual = stored_events(collector.url)
        assert actual == expected and len(actual) == 1063
        stats = outbox.stats()
        assert stats.pending_count == stats.quarantined_total == stats.capacity_rejections == 0
        from collector.store import CollectorStore
        counters = CollectorStore(collector.database_path).stats()
        assert counters.retained_count == counters.inserted_total == 1063 and counters.duplicate_total == 10
        for name, values in (('expected.ndjson', expected), ('stored.ndjson', actual)):
            (directory / name).write_text(''.join(values[key] + '\n' for key in sorted(values)))
        return {'complete': True, 'simulated_transport': 'POSIX pseudo-terminal through pySerial, real CLI and localhost HTTP',
                'unique_reconciled_events': 1063, 'valid_frames_offered': 1073, 'late_replay_duplicates': 10,
                'initial_framing': framing, 'after_reconnect': reconnected['serial'],
                'before_hard_kill': before_kill['serial'], 'pending_survived_hard_kill': 50,
                'drain_only_serial_connections': 0, 'signal_exit': 143, 'signal_result': finished,
                'pending_after': 0, 'quarantined_total': 0,
                'limits': ['not a physical USB, sensor, firmware baud/reset, or throughput test',
                           'startup/reconnect discards through first LF; pre-acceptance losses are separate']}
    finally:
        stop_owned(process)
        collector.close()
        for handle in handles:
            handle.close()
        for fd in fds:
            os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    directory = (args.output or ROOT / 'benchmarks/results' / ('serial-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + uuid4().hex[:8])).resolve()
    try:
        directory.mkdir(parents=True, exist_ok=False)
        evidence = metadata(Settings(), extra_sources=('agent/main.py', 'agent/serial_source.py', 'scripts/verify-serial.py', 'requirements.txt'))
        evidence['configuration'] = {'initial_unique_events': 1000, 'replays': 10, 'after_reconnect': 10,
                                     'outage_pending_events': 50, 'signal_events': 3, 'baudrate': 115200,
                                     'read_timeout_seconds': .1, 'reconnect_delay_seconds': .2}
        evidence['versions']['pyserial'] = version('pyserial')
        evidence['limits'] = ['simulated POSIX terminal, no physical USB or sensor measurements', 'owned temporary test processes only']
        write_json(directory / 'metadata.json', evidence)
        record_sources(directory, evidence)
        result = verify(directory)
        write_json(directory / 'summary.json', result)
        print(json.dumps({'type': 'finished', 'complete': True, 'unique_reconciled_events': 1063, 'output': str(directory)}), flush=True)
        return 0
    except (AssertionError, OSError, RuntimeError, ValueError, sqlite3.Error, httpx.HTTPError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'type': 'error', 'reason': str(exc)[:200], 'output': str(directory)}), flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
