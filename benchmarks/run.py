"""Isolated synthetic load trials with durable-ACK timing and exact reconciliation."""
import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import csv
import hashlib
from importlib.metadata import version
import json
import math
from pathlib import Path
import platform
import random
import socket
import sqlite3
import subprocess
import sys
from threading import Event as StopEvent, Lock, Thread
import time
from uuid import uuid4

import httpx
import psutil

from agent.outbox import Outbox
from agent.sources import SyntheticSource
from agent.uploader import Uploader
from shared.config import COLLECTOR_MAX_RETAINED_EVENTS, HTTP_BATCH_MAX_EVENTS, OUTBOX_MAX_PENDING_BYTES
from shared.schema import canonical_event_bytes, event_key, validate_event

ROOT = Path(__file__).resolve().parents[1]
MAX_EVENTS_PER_TRIAL = 100_000  # Provisional measurement-ledger memory bound.


@dataclass(frozen=True)
class Settings:
    duration: float = 60.0
    repeats: int = 3
    rates: tuple[float, ...] = (100.0,)
    batch_sizes: tuple[int, ...] = (1, 20, 100)
    seed: int = 7
    drain_timeout: float = 30.0
    sample_interval: float = 0.25
    max_pending_bytes: int = OUTBOX_MAX_PENDING_BYTES
    outage_seconds: float = 0.0
    outage_at: float = 1.0

    def __post_init__(self) -> None:
        for name, minimum, maximum in (("duration", 0.1, 3600), ("drain_timeout", 0, 600),
                                      ("sample_interval", 0.05, 5), ("outage_seconds", 0, 300), ("outage_at", 0, 3600)):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not minimum <= value <= maximum or not math.isfinite(value):
                raise ValueError(f"{name} must be finite and in {minimum}..{maximum}")
        if type(self.repeats) is not int or not 1 <= self.repeats <= 10:
            raise ValueError("repeats must be an integer in 1..10")
        if type(self.seed) is not int:
            raise ValueError("seed must be an integer")
        if type(self.max_pending_bytes) is not int or not 0 <= self.max_pending_bytes <= OUTBOX_MAX_PENDING_BYTES:
            raise ValueError("max_pending_bytes must be an integer in 0..16 MiB")
        if not self.rates or any(isinstance(rate, bool) or not isinstance(rate, (int, float)) or rate <= 0 or rate > MAX_EVENTS_PER_TRIAL / self.duration or not math.isfinite(rate) or not math.isfinite(1e9 / rate) for rate in self.rates):
            raise ValueError("rates must be positive/finite and offer at most 100,000 events per trial")
        if not self.batch_sizes or any(type(size) is not int or not 1 <= size <= HTTP_BATCH_MAX_EVENTS for size in self.batch_sizes):
            raise ValueError("batch_sizes must be integers in 1..100")
        if len(set(self.rates)) != len(self.rates) or len(set(self.batch_sizes)) != len(self.batch_sizes):
            raise ValueError("rates and batch_sizes must not contain duplicates")
        if len(self.rates) * len(self.batch_sizes) * self.repeats > 100:
            raise ValueError("matrix must contain at most 100 trials")
        if self.outage_seconds and self.outage_at + self.outage_seconds >= self.duration:
            raise ValueError("outage must end before the production window ends")


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def percentiles(values: list[float]) -> dict:
    ordered = sorted(values)
    n = len(ordered)
    return {f"p{p}_ms": ordered[math.ceil(n * p / 100) - 1] if n else None for p in (50, 95, 99)}


class Ledger:
    """Bounded per-trial observations; benchmark RAM includes this measurement."""

    def __init__(self) -> None:
        self.lock = Lock()
        self.events: dict[tuple, dict] = {}
        self.attempts: list[dict] = []
        self.samples: list[dict] = []
        self.accepted_count = self.acknowledged_count = 0

    def start_publish(self, event) -> None:
        key = event_key(event)
        content = canonical_event_bytes(event).decode("utf-8")
        with self.lock:
            if len(self.events) >= MAX_EVENTS_PER_TRIAL or key in self.events:
                raise ValueError("benchmark ledger exhausted or source reused an event key")
            # Start before publish: COMMIT may become visible before the caller returns.
            self.events[key] = {"event_json": content, "publish_start_ns": time.monotonic_ns(),
                                "publish_return_ns": None, "accepted": False, "status": None,
                                "local_ack_ns": None}

    def finish_publish(self, event, result) -> None:
        finished = time.monotonic_ns()
        with self.lock:
            self.events[event_key(event)].update(publish_return_ns=finished, accepted=result.accepted, status=result.status)
            self.accepted_count += int(result.accepted)

    def acknowledged(self, keys: list[tuple]) -> None:
        finished = time.monotonic_ns()
        with self.lock:
            for key in keys:
                self.acknowledged_count += int(self.events[key]["local_ack_ns"] is None)
                self.events[key]["local_ack_ns"] = finished

    def save(self, directory: Path) -> None:
        # Preserve raw content and timestamps after workers join.
        with (directory / "events.ndjson").open("w", encoding="utf-8") as output:
            for key, record in self.events.items():
                output.write(json.dumps({"key": key, **record}, allow_nan=False) + "\n")
        with (directory / "attempts.ndjson").open("w", encoding="utf-8") as output:
            for record in self.attempts:
                output.write(json.dumps(record, allow_nan=False) + "\n")
        if self.samples:
            with (directory / "samples.csv").open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=list(self.samples[0]))
                writer.writeheader()
                writer.writerows(self.samples)


class MeasuredOutbox(Outbox):
    """Benchmark-only wrappers; the production acceptance/deletion rules stay."""

    def __init__(self, path: Path, ledger: Ledger, **options) -> None:
        super().__init__(path, **options)
        self.ledger = ledger

    def publish(self, event):
        self.ledger.start_publish(event)
        result = super().publish(event)
        self.ledger.finish_publish(event, result)
        return result

    def acknowledge(self, keys):
        removed = super().acknowledge(keys)
        # Record ACK time only after successful local deletion COMMIT.
        if removed != len(keys):
            raise RuntimeError("benchmark ACK did not remove exactly its selected keys")
        self.ledger.acknowledged(keys)
        return removed


class MeasuredTransport(httpx.BaseTransport):
    def __init__(self, inner: httpx.BaseTransport) -> None:
        self.inner = inner
        self.reset()

    def reset(self) -> None:
        self.keys: list[tuple] = []
        self.started_ns: int | None = None
        self.body_bytes = 0

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        body = request.content  # Uploader already bounds this request to 128 KiB.
        self.keys = [(event["source_id"], event["boot_id"], event["seq"]) for event in json.loads(body)["events"]]
        self.body_bytes = len(body)
        self.started_ns = time.monotonic_ns()
        return self.inner.handle_request(request)

    def close(self) -> None:
        self.inner.close()


class MeasuredUploader(Uploader):
    def __init__(self, outbox, collector_url, ledger: Ledger, transport: MeasuredTransport, **options) -> None:
        super().__init__(outbox, collector_url, **options)
        self.ledger, self.transport = ledger, transport
        self.collector_ack_ns: int | None = None

    def _read_ack(self, response, submitted):
        ack = super()._read_ack(response, submitted)
        # Record validated remote ACK before local deletion on the same host clock.
        self.collector_ack_ns = time.monotonic_ns()
        return ack

    def upload_once(self):
        self.transport.reset()
        self.collector_ack_ns = None
        started = time.monotonic_ns()
        result = super().upload_once()
        finished = time.monotonic_ns()
        http_start = self.transport.started_ns
        if len(self.ledger.attempts) >= 200_000:
            raise RuntimeError("benchmark attempt ledger limit reached")
        self.ledger.attempts.append({
            "started_ns": started, "finished_ns": finished, "keys": self.transport.keys,
            "http_start_ns": http_start, "collector_ack_ns": self.collector_ack_ns,
            "body_bytes": self.transport.body_bytes, "result": asdict(result),
            "http_ack_ms": (self.collector_ack_ns - http_start) / 1_000_000 if self.collector_ack_ns is not None and http_start is not None else None,
        })
        return result


class OwnedCollector:
    """Own a per-trial ASGI server; the collector is the default factory."""

    def __init__(self, directory: Path, *, module: str = "collector.app",
                 factory_options: dict | None = None, database_name: str = "collector.sqlite3",
                 log_name: str = "collector.log") -> None:
        self.directory = directory
        self.module = module
        self.factory_options = ({"max_retained_events": COLLECTOR_MAX_RETAINED_EVENTS}
                                if factory_options is None and module == "collector.app" else (factory_options or {}))
        self.database_path = directory / database_name
        self.process: subprocess.Popen | None = None
        self.port = 0
        self.cpu_previous = 0.0
        self.guard = Lock()
        self.log = (directory / log_name).open("w+")

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> int:
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(("127.0.0.1", self.port))
            listener.listen(128)
            self.port = listener.getsockname()[1]
            # Retain the full workload for reconciliation. Pass factory options as argv data.
            code = "import sys,json,importlib,uvicorn; factory=importlib.import_module(sys.argv[3]).create_app; uvicorn.run(factory(sys.argv[1],**json.loads(sys.argv[4])),fd=int(sys.argv[2]),log_level='error',access_log=False)"
            with self.guard:
                self.process = subprocess.Popen([sys.executable, "-c", code,
                    str(self.database_path), str(listener.fileno()), self.module, json.dumps(self.factory_options)],
                    cwd=ROOT, pass_fds=(listener.fileno(),), stdout=self.log, stderr=self.log)
        finally:
            listener.close()
        deadline = time.monotonic() + 10
        with httpx.Client(trust_env=False, timeout=0.5) as client:
            while time.monotonic() < deadline:
                try:
                    if client.get(self.url + "/readyz").status_code == 200:
                        return time.monotonic_ns()
                except httpx.RequestError:
                    pass
                if self.process.poll() is not None:
                    break
                time.sleep(0.02)
        raise RuntimeError("owned server failed readiness; inspect its trial log")

    def resources(self) -> tuple[int, float]:
        with self.guard:
            if self.process is None or self.process.poll() is not None:
                return 0, self.cpu_previous
            try:
                process = psutil.Process(self.process.pid)
                cpu = process.cpu_times()
                return process.memory_info().rss, self.cpu_previous + cpu.user + cpu.system
            except psutil.NoSuchProcess:
                return 0, self.cpu_previous

    def stop(self) -> None:
        with self.guard:
            process = self.process
            if process is not None and process.poll() is None:
                try:
                    cpu = psutil.Process(process.pid).cpu_times()
                    self.cpu_previous += cpu.user + cpu.system
                except psutil.NoSuchProcess:
                    pass
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            self.process = None

    def close(self) -> None:
        self.stop()
        self.log.close()


def stored_events(url: str) -> dict[tuple, str]:
    events, cursor = {}, None
    with httpx.Client(trust_env=False, timeout=5) as client:
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
                if key in events or len(events) >= COLLECTOR_MAX_RETAINED_EVENTS:
                    raise RuntimeError("collector returned duplicate keys or exceeded reconciliation bound")
                events[key] = canonical_event_bytes(event).decode("utf-8")
            cursor = page["next_cursor"]
            if cursor is None:
                return events


def reconcile(ledger: Ledger, stored: dict, pending: dict, quarantined: dict) -> dict:
    accepted = {key: row["event_json"] for key, row in ledger.events.items() if row["accepted"]}
    rejected = set(ledger.events) - set(accepted)
    acknowledged = {key for key, row in ledger.events.items() if row["local_ack_ns"] is not None}
    conflicts = {key for mapping in (stored, pending, quarantined) for key in set(accepted) & set(mapping) if accepted[key] != mapping[key]}
    sets = {
        "rejected": rejected, "pending": set(pending), "quarantined": set(quarantined),
        "missing_from_collector": set(accepted) - set(stored),
        "unaccounted": set(accepted) - (set(stored) | set(pending) | set(quarantined)),
        "unexpected_stored": set(stored) - set(accepted), "conflicting_content": conflicts,
        "unacknowledged": set(accepted) - acknowledged,
    }
    # Hashes aid auditing; exact content-set comparisons decide correctness.
    def digest(mapping):
        hasher = hashlib.sha256()
        for key in sorted(mapping):
            hasher.update(mapping[key].encode("utf-8") + b"\n")
        return hasher.hexdigest()
    return {"complete": all(not keys for keys in sets.values()), "accepted": len(accepted),
            "stored": len(stored), "counts": {name: len(keys) for name, keys in sets.items()},
            "sets": {name: sorted(keys) for name, keys in sets.items()},
            "accepted_sha256": digest(accepted), "stored_sha256": digest(stored)}


def database_contents(path: Path, table: str) -> dict:
    if table not in ("pending", "quarantine"):
        raise ValueError("unsupported benchmark table")
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
        rows = connection.execute(f"SELECT event_json FROM {table}").fetchall()
    events = [validate_event(json.loads(row[0])) for row in rows]
    return {event_key(event): canonical_event_bytes(event).decode("utf-8") for event in events}


def disk_bytes(path: Path) -> int:
    total = 0
    for file in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            total += file.stat().st_size
        except FileNotFoundError:
            pass  # SQLite can checkpoint/remove a WAL between resource samples.
    return total


def recovery_timing(ledger: Ledger, keys: list[tuple], ready_ns: int | None) -> dict:
    # Track exact backlog keys; new arrivals can keep the live queue nonempty.
    empty = {"drained_ns": None, "readiness_to_drain_seconds": None,
             "drained_before_readiness_probe": False}
    if ready_ns is None or not keys:
        return empty
    acknowledgments = [ledger.events[key]["local_ack_ns"] for key in keys]
    if any(value is None for value in acknowledgments):
        return empty
    drained_ns = max(acknowledgments)
    return {"drained_ns": drained_ns,
            "readiness_to_drain_seconds": max(0, (drained_ns - ready_ns) / 1e9),
            # Retain ACKs that precede the readiness probe; remaining delay is then zero.
            "drained_before_readiness_probe": drained_ns < ready_ns}


def run_trial(directory: Path, settings: Settings, rate: float, batch_size: int, repeat: int) -> dict:
    directory.mkdir()
    ledger = Ledger()
    source = SyntheticSource(seed=settings.seed)
    outbox = MeasuredOutbox(directory / "outbox.sqlite3", ledger, max_pending_bytes=settings.max_pending_bytes)
    outbox.initialize()
    collector = OwnedCollector(directory)
    upload_stop, monitor_stop = StopEvent(), StopEvent()
    failure_lock, failures = Lock(), []
    recovery = {"stopped_ns": None, "ready_ns": None, "backlog_at_ready": None, "backlog_before_restart": None}
    recovery_keys: list[tuple] = []
    threads: list[Thread] = []
    started_ns = production_end_ns = measured_end_ns = 0
    start_cpu = time.process_time()

    def remember_failure(exc):
        with failure_lock:
            failures.append(f"{type(exc).__name__}: {str(exc)[:200]}")
        upload_stop.set()

    try:
        collector.start()
        transport = MeasuredTransport(httpx.HTTPTransport(retries=0, limits=httpx.Limits(max_connections=1, max_keepalive_connections=1)))
        with httpx.Client(transport=transport, trust_env=False) as client:
            uploader = MeasuredUploader(outbox, collector.url, ledger, transport, client=client,
                                        batch_size=batch_size, rng=random.Random(settings.seed + repeat))
            started_ns = time.monotonic_ns()
            start_cpu = time.process_time()
            collector_start_cpu = collector.resources()[1]

            def upload():
                try:
                    uploader.run(upload_stop)
                except Exception as exc:
                    remember_failure(exc)

            def monitor():
                process = psutil.Process()
                try:
                    while not monitor_stop.is_set():
                        stats = outbox.stats()
                        collector_rss, collector_cpu = collector.resources()
                        with ledger.lock:
                            accepted = ledger.accepted_count
                            acked = ledger.acknowledged_count
                        ledger.samples.append({
                            "elapsed_seconds": (time.monotonic_ns() - started_ns) / 1e9,
                            "pending_count": stats.pending_count, "pending_bytes": stats.pending_bytes,
                            "accepted": accepted, "acknowledged": acked,
                            "agent_rss_bytes": process.memory_info().rss, "agent_cpu_seconds": time.process_time() - start_cpu,
                            "collector_rss_bytes": collector_rss, "collector_cpu_seconds": collector_cpu - collector_start_cpu,
                            "outbox_disk_bytes": disk_bytes(outbox.path),
                            "collector_disk_bytes": disk_bytes(directory / "collector.sqlite3"),
                        })
                        monitor_stop.wait(settings.sample_interval)
                except Exception as exc:
                    remember_failure(exc)

            def outage():
                nonlocal recovery_keys
                try:
                    if upload_stop.wait(settings.outage_at):
                        return
                    collector.stop()
                    recovery["stopped_ns"] = time.monotonic_ns()
                    if upload_stop.wait(settings.outage_seconds):
                        return
                    with sqlite3.connect(outbox.path.as_uri() + "?mode=ro", uri=True) as connection:
                        recovery_keys = connection.execute("SELECT source_id, boot_id, seq FROM pending").fetchall()
                    recovery["backlog_before_restart"] = len(recovery_keys)
                    recovery["ready_ns"] = collector.start()
                    recovery["backlog_at_ready"] = outbox.stats().pending_count
                except Exception as exc:
                    remember_failure(exc)

            threads = [Thread(target=upload, name="benchmark-upload"), Thread(target=monitor, name="benchmark-monitor")]
            if settings.outage_seconds:
                threads.append(Thread(target=outage, name="benchmark-outage"))
            try:
                for worker in threads:
                    worker.start()
                deadline = started_ns + int(settings.duration * 1e9)
                next_publish = started_ns
                interval_ns = math.ceil(1e9 / rate)
                while not upload_stop.is_set() and time.monotonic_ns() < deadline:
                    now = time.monotonic_ns()
                    if now < next_publish:
                        upload_stop.wait(min(0.05, (next_publish - now) / 1e9))
                        continue
                    outbox.publish(source.next_event())
                    next_publish += interval_ns
                    if next_publish < time.monotonic_ns():
                        next_publish = time.monotonic_ns() + interval_ns
                production_end_ns = time.monotonic_ns()
                backlog_at_production_end = outbox.stats().pending_count
                drain_deadline = production_end_ns + int(settings.drain_timeout * 1e9)
                while not upload_stop.is_set() and outbox.stats().pending_count and time.monotonic_ns() < drain_deadline:
                    upload_stop.wait(0.02)
            finally:
                upload_stop.set()
                for worker in (threads[0], *threads[2:]):
                    if worker.ident is not None:
                        worker.join()
                monitor_stop.set()
                if threads[1].ident is not None:
                    threads[1].join()
            measured_end_ns = time.monotonic_ns()
            agent_cpu = time.process_time() - start_cpu
            collector_cpu = collector.resources()[1] - collector_start_cpu

        actual = stored_events(collector.url)
        result = reconcile(ledger, actual, database_contents(outbox.path, "pending"), database_contents(outbox.path, "quarantine"))
        result["complete"] = result["complete"] and not failures
        write_json(directory / "reconciliation.json", result)
        if settings.outage_seconds:
            write_json(directory / "recovery-target-keys.json", recovery_keys)
        with (directory / "stored.ndjson").open("w", encoding="utf-8") as output:
            for key in sorted(actual):
                output.write(actual[key] + "\n")
        accepted_rows = [row for row in ledger.events.values() if row["accepted"]]
        acknowledged_rows = [row for row in accepted_rows if row["local_ack_ns"] is not None]
        delivery = [(row["local_ack_ns"] - row["publish_start_ns"]) / 1e6 for row in acknowledged_rows]
        # Preserve ACK-before-return races instead of clamping negative deltas to zero.
        races = sum(row["local_ack_ns"] < row["publish_return_ns"] for row in acknowledged_rows)
        after_return = [(row["local_ack_ns"] - row["publish_return_ns"]) / 1e6 for row in acknowledged_rows if row["local_ack_ns"] >= row["publish_return_ns"]]
        http_acks = [attempt["http_ack_ms"] for attempt in ledger.attempts if attempt["http_ack_ms"] is not None]
        acked_attempts = [attempt for attempt in ledger.attempts if attempt["result"]["status"] == "acked"]
        window = (production_end_ns - started_ns) / 1e9
        elapsed = (measured_end_ns - started_ns) / 1e9
        acknowledgments_in_window = sum(row["local_ack_ns"] <= production_end_ns for row in acknowledged_rows)
        samples = ledger.samples
        summary = {
            "trial": directory.name, "rate_target": rate, "batch_size": batch_size, "repeat": repeat,
            "source_id": source.source_id, "boot_id": source.boot_id, "seed": settings.seed,
            "duration_requested_seconds": settings.duration, "production_seconds": window,
            "production_start_ns": started_ns, "production_end_ns": production_end_ns, "measurement_end_ns": measured_end_ns,
            "offered": len(ledger.events), "accepted": len(accepted_rows), "rejected": len(ledger.events) - len(accepted_rows),
            "stored": len(actual), "acknowledged": len(acknowledged_rows), "complete": result["complete"], "errors": failures,
            "offered_per_second": len(ledger.events) / window, "accepted_per_second": len(accepted_rows) / window,
            "acknowledged_during_production": acknowledgments_in_window, "acknowledged_per_second": acknowledgments_in_window / window,
            "backlog_at_production_end": backlog_at_production_end, "pending_after": outbox.stats().pending_count,
            "peak_sampled_backlog": max((row["pending_count"] for row in samples), default=0),
            "delivery_delay": {"definition": "publish call start to successful local ACK/deletion return", "count": len(delivery), **percentiles(delivery)},
            "after_publish_return_delay": {"count": len(after_return), "timestamp_order_races": races, **percentiles(after_return)},
            "http_ack_latency": {"definition": "transport start to complete schema-validated collector ACK, before local deletion", "count": len(http_acks), **percentiles(http_acks)},
            "http_attempts": sum(attempt["http_start_ns"] is not None for attempt in ledger.attempts),
            "retry_attempts": sum(attempt["result"]["status"] == "retry" for attempt in ledger.attempts),
            "mean_acked_batch_size": sum(attempt["result"]["submitted_events"] for attempt in acked_attempts) / len(acked_attempts) if acked_attempts else None,
            "agent_cpu_percent_one_core": agent_cpu / elapsed * 100, "collector_cpu_percent_one_core": collector_cpu / elapsed * 100,
            "agent_peak_sampled_rss_bytes": max((row["agent_rss_bytes"] for row in samples), default=0),
            "collector_peak_sampled_rss_bytes": max((row["collector_rss_bytes"] for row in samples), default=0),
            "outbox_peak_sampled_disk_bytes": max((row["outbox_disk_bytes"] for row in samples), default=0),
            "collector_peak_sampled_disk_bytes": max((row["collector_disk_bytes"] for row in samples), default=0),
            "recovery": {**recovery, **recovery_timing(ledger, recovery_keys, recovery["ready_ns"])},
            "payload_bytes_mean": sum(len(row["event_json"].encode("utf-8")) for row in ledger.events.values()) / len(ledger.events) if ledger.events else None,
        }
        write_json(directory / "summary.json", summary)
        return summary
    finally:
        upload_stop.set()
        monitor_stop.set()
        for worker in threads:
            if worker.ident is not None:
                worker.join()
        ledger.save(directory)
        collector.close()


def metadata(settings, *, extra_sources: tuple[str, ...] = ()) -> dict:
    names = ["benchmarks/run.py", "agent/uploader.py", "agent/outbox.py", "agent/sources.py",
             "collector/app.py", "collector/store.py", "shared/schema.py", "shared/config.py", *extra_sources]
    return {
        "started_at_utc": datetime.now(timezone.utc).isoformat(), "configuration": asdict(settings),
        "host": socket.gethostname(), "platform": platform.platform(), "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version, "logical_cpus": psutil.cpu_count(), "physical_cpus": psutil.cpu_count(logical=False),
        "memory_total_bytes": psutil.virtual_memory().total,
        "versions": {name: version(name) for name in ("httpx", "fastapi", "pydantic", "uvicorn", "psutil", "plotnine")},
        "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()),
        "file_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in names},
        "limits": ["same-host synthetic load: generator, measurement, and collector contend for CPU/disk",
                   "agent resource totals include generator, timing ledger, polling, and HTTP client",
                   "RSS/backlog/disk peaks are sampled, not continuous maxima",
                   "benchmark parent is reused; Python allocator high-water RSS can carry between trials",
                   "CPU percent is relative to one logical core and can exceed 100%",
                   "existing applications can affect measurements; no warmup is excluded",
                   "no physical-device or instrumentation-on/off overhead result is claimed"],
    }


def record_sources(directory: Path, evidence: dict) -> None:
    # Archive exact bytes: a dirty commit hash cannot reproduce the source.
    for name, expected in evidence["file_sha256"].items():
        content = (ROOT / name).read_bytes()
        if hashlib.sha256(content).hexdigest() != expected:
            raise RuntimeError("source changed while recording benchmark metadata")
        target = directory / "source" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)


def plot_results(directory: Path, summaries: list[dict]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import pandas as pd
    from plotnine import aes, facet_wrap, geom_boxplot, geom_point, ggplot, labs, theme_bw
    rows = [{"rate": str(row["rate_target"]), "batch": str(row["batch_size"]),
             "delivery_p95_ms": row["delivery_delay"]["p95_ms"],
             "accepted_per_second": row["accepted_per_second"]} for row in summaries if row["complete"]]
    if not rows:
        return
    frame = pd.DataFrame(rows)
    frame["batch"] = pd.Categorical(frame["batch"], categories=[str(size) for size in sorted({row["batch_size"] for row in summaries})], ordered=True)
    delay = (ggplot(frame, aes("batch", "delivery_p95_ms")) + geom_boxplot() + geom_point()
             + facet_wrap("~rate") + theme_bw() + labs(x="Maximum batch size", y="Publish-to-ACK p95 (ms)", title="Same-host synthetic delivery delay"))
    delay.save(directory / "delivery-p95.png", width=8, height=4, dpi=160, verbose=False)
    throughput = (ggplot(frame, aes("batch", "accepted_per_second")) + geom_boxplot() + geom_point()
                  + facet_wrap("~rate") + theme_bw() + labs(x="Maximum batch size", y="Accepted events / second", title="Same-host synthetic acceptance rate"))
    throughput.save(directory / "acceptance-rate.png", width=8, height=4, dpi=160, verbose=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--rates", nargs="+", type=float, default=[100])
    parser.add_argument("--batch-sizes", nargs="+", type=int, default=[1, 20, 100])
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--drain-timeout", type=float, default=30)
    parser.add_argument("--sample-interval", type=float, default=0.25)
    parser.add_argument("--max-pending-bytes", type=int, default=OUTBOX_MAX_PENDING_BYTES)
    parser.add_argument("--outage-seconds", type=float, default=0)
    parser.add_argument("--outage-at", type=float, default=1)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--plots", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="print the validated matrix without starting processes")
    args = parser.parse_args(argv)
    try:
        settings = Settings(duration=args.duration, repeats=args.repeats, rates=tuple(args.rates), batch_sizes=tuple(args.batch_sizes),
                            seed=args.seed, drain_timeout=args.drain_timeout, sample_interval=args.sample_interval,
                            max_pending_bytes=args.max_pending_bytes, outage_seconds=args.outage_seconds, outage_at=args.outage_at)
    except ValueError as exc:
        parser.error(str(exc))
    trials = len(settings.rates) * len(settings.batch_sizes) * settings.repeats
    if args.dry_run:
        print(json.dumps({"configuration": asdict(settings), "trials": trials,
                          "production_seconds_total": trials * settings.duration}, indent=2))
        return 0
    directory = args.output or ROOT / "benchmarks" / "results" / (datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ-") + uuid4().hex[:8])
    directory = directory.resolve()
    try:
        directory.mkdir(parents=True, exist_ok=False)
        evidence = metadata(settings)
        write_json(directory / "metadata.json", evidence)
        record_sources(directory, evidence)
        summaries = []
        for rate in settings.rates:
            for repeat in range(1, settings.repeats + 1):
                offset = (repeat - 1) % len(settings.batch_sizes)
                sizes = settings.batch_sizes[offset:] + settings.batch_sizes[:offset]
                for size in sizes:
                    name = f"rate-{rate:g}-batch-{size}-rep-{repeat}"
                    print(json.dumps({"type": "trial_start", "trial": name, "output": str(directory)}), flush=True)
                    summary = run_trial(directory / name, settings, rate, size, repeat)
                    summaries.append(summary)
                    write_json(directory / "summary.json", summaries)
                    print(json.dumps({"type": "trial_finished", "trial": name, "complete": summary["complete"],
                                      "offered": summary["offered"], "stored": summary["stored"],
                                      "delivery_p95_ms": summary["delivery_delay"]["p95_ms"]}), flush=True)
        with (directory / "summary.csv").open("w", newline="", encoding="utf-8") as output:
            fields = ["trial", "rate_target", "batch_size", "repeat", "production_seconds", "offered", "accepted", "rejected", "stored", "complete",
                      "offered_per_second", "accepted_per_second", "acknowledged_per_second", "backlog_at_production_end", "pending_after", "peak_sampled_backlog",
                      "http_attempts", "retry_attempts", "mean_acked_batch_size", "agent_cpu_percent_one_core", "collector_cpu_percent_one_core"]
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            writer.writerows({field: row[field] for field in fields} for row in summaries)
        if args.plots:
            plot_results(directory, summaries)
        passed = all(row["complete"] for row in summaries)
        print(json.dumps({"type": "finished", "complete": passed, "trials": len(summaries), "output": str(directory)}), flush=True)
        return 0 if passed else 1
    except (OSError, ValueError, RuntimeError, httpx.HTTPError, sqlite3.Error) as exc:
        print(json.dumps({"type": "error", "reason": str(exc)[:200], "output": str(directory)}), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
