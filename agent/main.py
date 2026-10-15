"""Coordinate acquisition, independent uploads, and graceful shutdown."""
import argparse
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import fcntl
import json
import math
from queue import Empty, Queue
import signal
import sqlite3
from threading import Event as StopEvent, Thread
import time

from agent.outbox import Outbox, OutboxStats
from agent.serial_source import SerialSource
from agent.sources import EventSource, HostMetricsSource, SyntheticSource
from agent.uploader import Uploader, UploaderBlockedError
from shared.config import (
    HTTP_BATCH_MAX_EVENTS,
    OUTBOX_MAX_PENDING_BYTES,
    QUARANTINE_MAX_BYTES,
    QUARANTINE_MAX_EVENTS,
)


@dataclass(frozen=True)
class AgentResult:
    offered: int
    accepted: int
    rejections: dict[str, int]
    elapsed_seconds: float
    outbox: OutboxStats
    exit_code: int
    reason: str
    serial: dict | None = None


class AgentAlreadyRunningError(Exception):
    """A cooperating agent process already owns this outbox's uploader."""


@contextmanager
def _exclusive_outbox(outbox: Outbox) -> Iterator[None]:
    # Keep the lock file: unlinking it can let another process lock a different inode.
    # The OS releases this advisory lock on exit, including crashes.
    outbox.path.parent.mkdir(parents=True, exist_ok=True)
    with outbox.path.with_name(outbox.path.name + ".agent.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AgentAlreadyRunningError("another agent owns this outbox") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def run_agent(
    outbox: Outbox,
    source: EventSource | SerialSource,
    uploader: Uploader,
    *,
    count: int | None = None,
    rate: float | None = 100.0,
    drain_timeout: float = 10.0,
    report_interval: float = 5.0,
    stop_event: StopEvent | None = None,
    report: Callable[[dict], None] | None = None,
) -> AgentResult:
    """Acquire on this thread, upload on a worker, and retain pending rows.

    count=None runs until stopped; count=0 drains only. Serial requires
    rate=None. The caller owns the uploader's lifetime."""
    if count is not None and (type(count) is not int or not 0 <= count < 2**63):
        raise ValueError("count must be nonnegative and below 2**63, or None")
    serial_mode = isinstance(source, SerialSource)
    if serial_mode and rate is not None:
        raise ValueError("serial sources require rate=None; firmware controls event arrival")
    numeric_options = [("drain_timeout", drain_timeout), ("report_interval", report_interval)]
    if not serial_mode:
        numeric_options.append(("rate", rate))
    for name, value in numeric_options:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"{name} must be finite and {'nonnegative' if name == 'drain_timeout' else 'positive'}")
        if value < 0 or (value == 0 and name != "drain_timeout"):
            raise ValueError(f"{name} must be finite and {'nonnegative' if name == 'drain_timeout' else 'positive'}")
    if not serial_mode and (1 / rate == 0 or not math.isfinite(1 / rate)):
        raise ValueError("rate has an unusable scheduling interval")
    if uploader.outbox.path != outbox.path:
        raise ValueError("producer and uploader must use the same outbox")

    with _exclusive_outbox(outbox):
        outbox.initialize()
        initial = outbox.stats()
        production_stop = StopEvent() if stop_event is None else stop_event
        upload_stop = StopEvent()
        failures: Queue[Exception] = Queue(maxsize=1)
        worker_done = StopEvent()
        offered = accepted = 0
        # Per-run rejections; the outbox also persists capacity rejections.
        rejections = {name: 0 for name in ("capacity", "unavailable", "invalid", "conflict")}
        started = time.monotonic()
        next_report = started + report_interval

        def emit_progress() -> None:
            nonlocal next_report
            if report is not None:
                try:
                    stats = asdict(outbox.stats())
                except sqlite3.Error:
                    stats = None  # A reporting failure must not discard data.
                upload_result = uploader.last_result
                report({
                    "type": "progress", "source_id": source.source_id,
                    "boot_id": source.boot_id, "offered": offered,
                    "accepted": accepted, "rejections": dict(rejections),
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "outbox": stats, "uploader_status": upload_result.status,
                    "uploader_reason": upload_result.reason,
                    **({"serial": source.snapshot()} if serial_mode else {}),
                })
            next_report = time.monotonic() + report_interval

        def upload_worker() -> None:
            try:
                uploader.run(upload_stop)
                if not upload_stop.is_set():
                    raise RuntimeError("uploader returned before shutdown")
            except Exception as exc:
                # Stop acquisition if the uploader fails.
                failures.put_nowait(exc)
                production_stop.set()
            finally:
                worker_done.set()

        worker = Thread(target=upload_worker, name="telemetry-uploader", daemon=False)
        worker.start()
        try:
            next_publish = started
            interval = None if serial_mode else 1 / rate
            try:
                while not production_stop.is_set() and (count is None or offered < count):
                    now = time.monotonic()
                    if now >= next_report:
                        emit_progress()
                        if production_stop.is_set():
                            break
                    if serial_mode:
                        # Serial preserves firmware identity and cadence.
                        event = source.poll_event(production_stop)
                        if event is None:
                            continue
                        if production_stop.is_set():
                            break
                    else:
                        if now < next_publish:
                            production_stop.wait(min(0.05, next_publish - now, max(0, next_report - now)))
                            continue
                        event = source.next_event()
                    result = outbox.publish(event)
                    offered += 1
                    if result.accepted:
                        accepted += 1
                    else:
                        rejections[result.status] += 1
                    if not serial_mode:
                        # Skip missed ticks instead of producing catch-up bursts.
                        next_publish += interval
                        if next_publish < time.monotonic():
                            next_publish = time.monotonic() + interval
            finally:
                if serial_mode:
                    source.close()  # Stop reception BEFORE draining/closing HTTP.

            # Drain accepted work after acquisition stops. This deadline bounds the
            # wait, not completion of an in-flight HTTP operation.
            deadline = time.monotonic() + drain_timeout
            while not worker_done.is_set():
                try:
                    if outbox.stats().pending_count == 0:
                        break
                except sqlite3.Error:
                    pass  # Leave rows alone and try again until the drain limit.
                now = time.monotonic()
                if now >= deadline:
                    break
                if now >= next_report:
                    emit_progress()
                worker_done.wait(min(0.05, max(0, deadline - now)))
        finally:
            # Join before closing HTTP or releasing the lock: an upload may still commit.
            try:
                if serial_mode:
                    source.close()
            finally:
                upload_stop.set()
                worker.join()

        stats = outbox.stats()
        try:
            failure = failures.get_nowait()
        except Empty:
            failure = None
        if failure is not None:
            reason = str(failure) if isinstance(failure, UploaderBlockedError) else f"uploader failed: {type(failure).__name__}"
        elif stats.pending_count:
            reason = "drain wait ended with pending events; restart with the same outbox"
        elif sum(rejections.values()):
            reason = "some offered events were rejected locally"
        elif stats.quarantined_total > initial.quarantined_total:
            reason = "some events were quarantined instead of delivered"
        else:
            reason = "stopped and drained" if production_stop.is_set() else "completed and drained"
        failed = failure is not None or stats.pending_count > 0 or sum(rejections.values()) > 0 or stats.quarantined_total > initial.quarantined_total
        return AgentResult(offered, accepted, dict(rejections), time.monotonic() - started, stats,
                           int(failed), reason, source.snapshot() if serial_mode else None)


def _print_report(value: dict) -> None:
    print(json.dumps(value, separators=(",", ":"), allow_nan=False), flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run synthetic, host, or serial telemetry through a durable outbox and independent HTTP uploader")
    parser.add_argument("--outbox", default="data/outbox.sqlite3")
    parser.add_argument("--collector-url", default="http://127.0.0.1:8000")
    parser.add_argument("--source", choices=("synthetic", "host", "serial"), default="synthetic")
    parser.add_argument("--source-id", default=None, help="synthetic/host only; serial preserves firmware IDs")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--count", type=int, default=None, help="offered events; omit for continuous production, use 0 to drain only")
    parser.add_argument("--rate", type=float, default=None, help="timed sources only (default: synthetic 100, host 1); serial follows arrivals")
    parser.add_argument("--serial-port", default=None, help="required for serial; explicit device path, no auto-detection")
    parser.add_argument("--baud", type=int, default=None, help="serial baudrate (default: provisional 115200)")
    parser.add_argument("--serial-timeout", type=float, default=None, help="serial read timeout, 0.01..0.5 sec (default: 0.1)")
    parser.add_argument("--serial-reconnect-delay", type=float, default=None, help="serial reopen delay, 0.05..60 sec (default: 1)")
    parser.add_argument("--drain-timeout", type=float, default=10.0, help="seconds to wait for backlog after production stops (default: 10)")
    parser.add_argument("--report-interval", type=float, default=5.0)
    parser.add_argument("--batch-size", type=int, default=HTTP_BATCH_MAX_EVENTS)
    parser.add_argument("--max-pending-bytes", type=int, default=OUTBOX_MAX_PENDING_BYTES)
    parser.add_argument("--max-quarantine-events", type=int, default=QUARANTINE_MAX_EVENTS)
    parser.add_argument("--max-quarantine-bytes", type=int, default=QUARANTINE_MAX_BYTES)
    args = parser.parse_args(argv)

    production_stop = StopEvent()
    received_signal: int | None = None

    def request_stop(signum: int, frame: object) -> None:
        nonlocal received_signal
        received_signal = signum
        # Defer cleanup to the controller.
        production_stop.set()

    previous_handlers = {signum: signal.signal(signum, request_stop) for signum in (signal.SIGINT, signal.SIGTERM)}
    try:
        outbox = Outbox(args.outbox, max_pending_bytes=args.max_pending_bytes,
                        max_quarantine_events=args.max_quarantine_events, max_quarantine_bytes=args.max_quarantine_bytes)
        if args.source == "serial":
            if args.serial_port is None:
                parser.error("--source serial requires --serial-port")
            if args.source_id is not None or args.rate is not None:
                parser.error("serial preserves firmware identity/cadence; omit --source-id and --rate")
            source = SerialSource(args.serial_port, baudrate=115_200 if args.baud is None else args.baud,
                                  timeout=0.1 if args.serial_timeout is None else args.serial_timeout,
                                  reconnect_delay=1.0 if args.serial_reconnect_delay is None else args.serial_reconnect_delay)
        else:
            if any(value is not None for value in (args.serial_port, args.baud, args.serial_timeout, args.serial_reconnect_delay)):
                parser.error("serial options require --source serial")
        if args.source == "host":
            source = HostMetricsSource(source_id="host-01" if args.source_id is None else args.source_id)
        elif args.source == "synthetic":
            source = SyntheticSource(source_id="synthetic-01" if args.source_id is None else args.source_id, seed=args.seed)
        rate = None if args.source == "serial" else (args.rate if args.rate is not None else (1.0 if args.source == "host" else 100.0))
        with Uploader(outbox, args.collector_url, batch_size=args.batch_size) as uploader:
            _print_report({"type": "starting", "source_id": source.source_id, "boot_id": source.boot_id,
                           "outbox_path": str(outbox.path), "count": args.count, "rate": rate, "source": args.source,
                           **({"serial_port": source.port, "baudrate": source.baudrate,
                               "serial_timeout": source.timeout, "serial_reconnect_delay": source.reconnect_delay}
                              if args.source == "serial" else {})})
            result = run_agent(outbox, source, uploader, count=args.count, rate=rate,
                               drain_timeout=args.drain_timeout, report_interval=args.report_interval,
                               stop_event=production_stop, report=_print_report)
        exit_code = result.exit_code or (128 + received_signal if received_signal is not None else 0)
        _print_report({"type": "finished", "source_id": source.source_id, "boot_id": source.boot_id,
                       **asdict(result), "exit_code": exit_code})
        return exit_code
    except ValueError as exc:
        parser.error(str(exc))
    except (AgentAlreadyRunningError, OSError, sqlite3.Error, OverflowError) as exc:
        _print_report({"type": "error", "reason": str(exc)[:200], "exit_code": 1})
        return 1
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
