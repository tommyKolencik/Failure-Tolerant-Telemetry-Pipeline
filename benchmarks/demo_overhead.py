"""Compare matched closed-loop demo workloads with instrumentation off/on."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from statistics import mean
import sqlite3
from threading import Event as StopEvent, Thread
import time
from uuid import uuid4

import httpx
import psutil

from agent.outbox import Outbox
from agent.uploader import Uploader
from benchmarks.run import OwnedCollector, ROOT, disk_bytes, metadata, percentiles, record_sources, stored_events, write_json
from shared.schema import canonical_event_bytes, event_key, validate_event

ROUTES = (("/normal", 200), ("/slow", 200), ("/error", 500))


@dataclass(frozen=True)
class Settings:
    requests: int = 6_000
    concurrency: int = 4
    repeats: int = 3
    slow_delay: float = 0.1
    drain_timeout: float = 30.0
    sample_interval: float = 0.25

    def __post_init__(self):
        for name, low, high in (("requests", 3, 100_000), ("concurrency", 1, 64), ("repeats", 1, 10)):
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{name} must be an integer in {low}..{high}")
        if self.concurrency > self.requests:
            raise ValueError("concurrency cannot exceed requests")
        for name, low, high in (("slow_delay", 0, 5), ("drain_timeout", 0.1, 300), ("sample_interval", 0.05, 5)):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not low <= value <= high or not math.isfinite(value):
                raise ValueError(f"{name} must be finite and in {low}..{high}")
        if self.requests / 3 * self.slow_delay / self.concurrency > 300:
            raise ValueError("estimated slow-route time exceeds 300 seconds per trial")


def expected_body(route: str, delay: float) -> dict:
    return {"/normal": {"status": "ok"}, "/slow": {"status": "slow", "delay_seconds": delay},
            "/error": {"detail": "deterministic demo error"}}[route]


def request_plan(count: int) -> list[tuple[str, int]]:
    return [ROUTES[index % len(ROUTES)] for index in range(count)]


def recording_count(recording: dict) -> int:
    return sum(recording[name] for name in ("accepted", "capacity", "unavailable", "invalid", "conflict", "recording_errors"))


class AuditedOutbox(Outbox):
    """Observe pending content before uploads; no journal writes inside the demo."""

    def __init__(self, path: Path, limit: int):
        super().__init__(path)
        self.limit = limit
        self.seen: dict[tuple, str] = {}
        self.acknowledged: set[tuple] = set()

    def peek_batch(self, limit=100):
        events = super().peek_batch(limit)
        for event in events:
            key = event_key(event)
            if key not in self.seen:
                if len(self.seen) >= self.limit:
                    raise RuntimeError("demo audit exceeded planned event count")
                self.seen[key] = canonical_event_bytes(event).decode("utf-8")
        return events

    def acknowledge(self, keys):
        removed = super().acknowledge(keys)
        if removed != len(keys):
            raise RuntimeError("demo audit deletion did not match its ACK keys")
        self.acknowledged.update(keys)
        return removed


def assess_events(outbox: AuditedOutbox, stored: dict, recording: dict, settings: Settings, enabled: bool) -> dict:
    planned_keys = {(recording["source_id"], recording["boot_id"], seq) for seq in range(settings.requests)} if enabled else set()
    expected_routes = Counter(route for route, _ in request_plan(settings.requests)) if enabled else Counter()
    decoded = {key: validate_event(json.loads(content)) for key, content in stored.items()}
    stored_routes = Counter(event.payload.route for event in decoded.values() if event.kind == "http.request")
    statuses = dict(ROUTES)
    semantic_errors = {key for key, event in decoded.items() if event.kind != "http.request" or
                       event.payload.method != "GET" or event.payload.status != statuses.get(event.payload.route)}
    conflicts = {key for key in set(outbox.seen) & set(stored) if outbox.seen[key] != stored[key]}
    problems = {"missing_stored": planned_keys - set(stored), "unexpected_stored": set(stored) - planned_keys,
                "missing_audited": planned_keys - set(outbox.seen), "unexpected_audited": set(outbox.seen) - planned_keys,
                "missing_local_ack": planned_keys - outbox.acknowledged, "conflicting_content": conflicts,
                "wrong_kind_method_or_status": semantic_errors}
    stats = outbox.stats()
    rejected = recording_count(recording) - recording["accepted"]
    complete = (all(not keys for keys in problems.values()) and stored_routes == expected_routes
                and recording["accepted"] == (settings.requests if enabled else 0) and rejected == 0
                and stats.pending_count == stats.quarantined_total == stats.capacity_rejections == 0)
    return {"complete": complete, "accepted": recording["accepted"], "rejected_or_recording_error": rejected,
            "stored": len(stored), "audited": len(outbox.seen), "local_acks": len(outbox.acknowledged),
            "counts": {name: len(keys) for name, keys in problems.items()},
            "sets": {name: sorted(keys) for name, keys in problems.items()},
            "expected_route_counts": dict(expected_routes), "stored_route_counts": dict(stored_routes),
            "outbox": stats.__dict__}


def load_requests(url: str, settings: Settings, observations: list, stop: StopEvent) -> None:
    def worker(worker_id):
        # Each worker reuses a client and waits for each body before its next request.
        with httpx.Client(trust_env=False, timeout=max(5, settings.slow_delay + 2),
                          limits=httpx.Limits(max_connections=1, max_keepalive_connections=1)) as client:
            for index in range(worker_id, settings.requests, settings.concurrency):
                if stop.is_set():
                    return
                route, expected_status = ROUTES[index % len(ROUTES)]
                started = time.perf_counter_ns()
                status, body_matches, error = None, False, None
                try:
                    response = client.get(url + route)
                    finished = time.perf_counter_ns()  # HTTPX has read the full body.
                    status = response.status_code
                    body_matches = response.json() == expected_body(route, settings.slow_delay)
                except (httpx.HTTPError, ValueError) as exc:
                    finished = time.perf_counter_ns()
                    error = type(exc).__name__
                observations[index] = {"request_id": index, "route": route, "expected_status": expected_status,
                                       "status": status, "body_matches": body_matches, "error": error,
                                       "started_ns": started, "finished_ns": finished,
                                       "duration_ms": (finished - started) / 1e6}
                if error is not None or status != expected_status or not body_matches:
                    stop.set()  # Bound failure time; incomplete work cannot pass.
    pool = ThreadPoolExecutor(max_workers=settings.concurrency)
    try:
        futures = [pool.submit(worker, index) for index in range(settings.concurrency)]
        for future in futures:
            future.result()
    finally:
        stop.set()
        pool.shutdown(wait=True, cancel_futures=True)


def run_trial(directory: Path, settings: Settings, enabled: bool, repeat: int) -> dict:
    directory.mkdir()
    outbox = AuditedOutbox(directory / "outbox.sqlite3", settings.requests)
    # Run identical outbox/uploader/collector infrastructure in both modes.
    outbox.initialize()
    collector = OwnedCollector(directory)
    demo = OwnedCollector(directory, module="demo.app", database_name="outbox.sqlite3", log_name="demo.log",
                          factory_options={"instrumentation_enabled": enabled, "slow_delay_seconds": settings.slow_delay})
    upload_stop, monitor_stop, load_stop = StopEvent(), StopEvent(), StopEvent()
    observations = [None] * settings.requests
    samples, failures, threads = [], [], []
    try:
        collector.start()
        demo.start()
        with httpx.Client(trust_env=False, timeout=5) as diagnostics, httpx.Client(trust_env=False) as upload_client:
            before = diagnostics.get(demo.url + "/telemetry").json()["recording"]
            assert recording_count(before) == 0
            uploader = Uploader(outbox, collector.url, client=upload_client)
            start_cpu = time.process_time()
            demo_start_cpu, collector_start_cpu = demo.resources()[1], collector.resources()[1]
            started_ns = time.monotonic_ns()

            def upload():
                try:
                    uploader.run(upload_stop)
                except Exception as exc:
                    failures.append(f"uploader: {type(exc).__name__}")
                    load_stop.set()

            def monitor():
                process = psutil.Process()
                try:
                    while not monitor_stop.is_set():
                        demo_rss, demo_cpu = demo.resources()
                        collector_rss, collector_cpu = collector.resources()
                        stats = outbox.stats()
                        samples.append({"elapsed_seconds": (time.monotonic_ns() - started_ns) / 1e9,
                                        "pending_count": stats.pending_count, "pending_bytes": stats.pending_bytes,
                                        "parent_rss_bytes": process.memory_info().rss, "demo_rss_bytes": demo_rss,
                                        "collector_rss_bytes": collector_rss, "outbox_disk_bytes": disk_bytes(outbox.path),
                                        "parent_cpu_seconds": time.process_time() - start_cpu,
                                        "demo_cpu_seconds": demo_cpu - demo_start_cpu,
                                        "collector_cpu_seconds": collector_cpu - collector_start_cpu})
                        monitor_stop.wait(settings.sample_interval)
                except Exception as exc:
                    failures.append(f"monitor: {type(exc).__name__}")
                    load_stop.set()

            threads = [Thread(target=upload, name="demo-benchmark-upload"), Thread(target=monitor, name="demo-benchmark-monitor")]
            try:
                for thread in threads:
                    thread.start()
                load_requests(demo.url, settings, observations, load_stop)
                load_end_ns = time.monotonic_ns()
                completed = [row for row in observations if row is not None]
                successful = sum(row["error"] is None and row["status"] == row["expected_status"] and row["body_matches"] for row in completed)
                # Body receipt can precede telemetry COMMIT; wait for counters, then drain.
                deadline = time.monotonic() + settings.drain_timeout
                while True:
                    response = diagnostics.get(demo.url + "/telemetry")
                    response.raise_for_status()
                    recording = response.json()["recording"]
                    settled = not enabled or recording_count(recording) >= successful
                    if settled and outbox.stats().pending_count == 0:
                        break
                    if failures or time.monotonic() >= deadline:
                        break
                    time.sleep(0.02)
            finally:
                upload_stop.set()
                monitor_stop.set()
                load_stop.set()
                for thread in threads:
                    if thread.ident is not None:
                        thread.join()
            end_ns = time.monotonic_ns()
            parent_cpu = time.process_time() - start_cpu
            demo_cpu, collector_cpu = demo.resources()[1] - demo_start_cpu, collector.resources()[1] - collector_start_cpu
        stored = stored_events(collector.url)
        reconciliation = assess_events(outbox, stored, recording, settings, enabled)
        complete = reconciliation["complete"] and successful == settings.requests and not failures
        per_route = {}
        for route, _ in ROUTES:
            values = [row["duration_ms"] for row in completed if row["route"] == route and row["body_matches"] and row["error"] is None and row["status"] == row["expected_status"]]
            per_route[route] = {"count": len(values), "mean_ms": mean(values) if values else None, **percentiles(values)}
        summary = {"trial": directory.name, "enabled": enabled, "repeat": repeat, "complete": complete,
                   "requests_planned": settings.requests, "requests_completed": len(completed), "successful_behavior_checks": successful,
                   "concurrency": settings.concurrency, "slow_delay": settings.slow_delay,
                   "load_seconds": (load_end_ns - started_ns) / 1e9, "load_start_ns": started_ns, "load_end_ns": load_end_ns,
                   "measurement_end_ns": end_ns, "requests_per_second": len(completed) / ((load_end_ns - started_ns) / 1e9),
                   "per_route_client_latency": per_route, "recording": recording, "telemetry_outcome": reconciliation,
                   "parent_cpu_seconds": parent_cpu, "demo_cpu_seconds": demo_cpu, "collector_cpu_seconds": collector_cpu,
                   "demo_cpu_ms_per_request": demo_cpu * 1000 / settings.requests,
                   "parent_peak_sampled_rss_bytes": max((row["parent_rss_bytes"] for row in samples), default=0),
                   "demo_peak_sampled_rss_bytes": max((row["demo_rss_bytes"] for row in samples), default=0),
                   "collector_peak_sampled_rss_bytes": max((row["collector_rss_bytes"] for row in samples), default=0),
                   "peak_sampled_backlog": max((row["pending_count"] for row in samples), default=0), "failures": failures}
        write_json(directory / "summary.json", summary)
        write_json(directory / "reconciliation.json", reconciliation)
        (directory / "stored.ndjson").write_text("".join(stored[key] + "\n" for key in sorted(stored)), encoding="utf-8")
        return summary
    finally:
        upload_stop.set()
        monitor_stop.set()
        for thread in threads:
            if thread.ident is not None:
                thread.join()
        (directory / "requests.ndjson").write_text("".join(json.dumps(row if row is not None else {"request_id": i, "skipped": True}) + "\n" for i, row in enumerate(observations)), encoding="utf-8")
        (directory / "audited-events.ndjson").write_text("".join(json.dumps({"key": key, "event_json": value, "local_ack": key in outbox.acknowledged}) + "\n" for key, value in outbox.seen.items()), encoding="utf-8")
        write_json(directory / "samples.json", samples)
        demo.close()
        collector.close()


def paired_results(summaries: list[dict]) -> list[dict]:
    results = []
    for repeat in sorted({row["repeat"] for row in summaries}):
        pair = {row["enabled"]: row for row in summaries if row["repeat"] == repeat}
        if set(pair) != {False, True}:
            continue
        disabled, enabled = pair[False], pair[True]
        valid = (disabled["complete"] and enabled["complete"] and
                 all(disabled[name] == enabled[name] for name in ("requests_planned", "concurrency", "slow_delay")))
        deltas = {route: {"p95_delta_ms": enabled["per_route_client_latency"][route]["p95_ms"] - disabled["per_route_client_latency"][route]["p95_ms"]}
                  for route, _ in ROUTES} if valid else None
        results.append({"repeat": repeat, "valid_comparison": valid, "enabled_minus_disabled": deltas,
                        "demo_cpu_ms_per_request_delta": enabled["demo_cpu_ms_per_request"] - disabled["demo_cpu_ms_per_request"] if valid else None})
    return results


def plot_results(directory: Path, summaries: list[dict]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import pandas as pd
    from plotnine import aes, facet_wrap, geom_boxplot, geom_point, ggplot, labs, theme_bw
    rows = [{"mode": "enabled" if row["enabled"] else "disabled", "route": route,
             "p95_ms": row["per_route_client_latency"][route]["p95_ms"]}
            for row in summaries if row["complete"] for route, _ in ROUTES]
    if not rows:
        return
    frame = pd.DataFrame(rows)
    frame["mode"] = pd.Categorical(frame["mode"], categories=["disabled", "enabled"], ordered=True)
    chart = (ggplot(frame, aes("mode", "p95_ms")) + geom_boxplot() + geom_point()
             + facet_wrap("~route", scales="free_y") + theme_bw()
             + labs(x="Demo instrumentation", y="Client request-to-body p95 (ms)",
                    title="Matched same-host demo workload: per-trial p95"))
    chart.save(directory / "client-p95.png", width=10, height=4, dpi=160, verbose=False)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=6000)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--slow-delay", type=float, default=0.1)
    parser.add_argument("--drain-timeout", type=float, default=30)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--plots", action="store_true")
    args = parser.parse_args(argv)
    try:
        settings = Settings(args.requests, args.concurrency, args.repeats, args.slow_delay, args.drain_timeout)
    except ValueError as exc:
        parser.error(str(exc))
    if args.dry_run:
        print(json.dumps({"settings": settings.__dict__, "trials": 2 * settings.repeats,
                          "requests_total": 2 * settings.repeats * settings.requests,
                          "minimum_slow_wait_seconds_per_trial": settings.requests / 3 * settings.slow_delay / settings.concurrency}, indent=2))
        return 0
    directory = (args.output or ROOT / "benchmarks/results" / ("demo-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid4().hex[:8])).resolve()
    try:
        directory.mkdir(parents=True, exist_ok=False)
        evidence = metadata(settings, extra_sources=("benchmarks/demo_overhead.py", "demo/app.py"))
        evidence["limits"] = ["fixed closed-loop request count/concurrency, not a target offered-rate test",
                              "same-host load generator, uploader, audit, demo, and collector contend for resources",
                              "client times end at body receipt; recorder acceptance and delivery are checked afterward",
                              "outbox content audit adds parent CPU/serialization work in enabled mode",
                              "both modes run equal collector/uploader infrastructure with an initialized outbox",
                              "CPU totals include diagnostic settling/drain; RSS/backlog peaks are sampled",
                              "fresh server processes per trial; no excluded warmup; order alternates across pairs"]
        write_json(directory / "metadata.json", evidence)
        record_sources(directory, evidence)
        summaries = []
        for repeat in range(1, settings.repeats + 1):
            for enabled in ((False, True) if repeat % 2 else (True, False)):
                name = f"{'enabled' if enabled else 'disabled'}-rep-{repeat}"
                print(json.dumps({"type": "trial_start", "trial": name, "output": str(directory)}), flush=True)
                result = run_trial(directory / name, settings, enabled, repeat)
                summaries.append(result)
                write_json(directory / "summary.json", summaries)
                write_json(directory / "paired.json", paired_results(summaries))
                print(json.dumps({"type": "trial_finished", "trial": name, "complete": result["complete"],
                                  "requests": result["requests_completed"], "requests_per_second": result["requests_per_second"],
                                  "accepted_telemetry": result["recording"]["accepted"]}), flush=True)
        complete = all(row["complete"] for row in summaries)
        if args.plots:
            plot_results(directory, summaries)
        print(json.dumps({"type": "finished", "complete": complete, "trials": len(summaries), "output": str(directory)}), flush=True)
        return 0 if complete else 1
    except (OSError, AssertionError, RuntimeError, ValueError, sqlite3.Error, httpx.HTTPError) as exc:
        print(json.dumps({"type": "error", "reason": str(exc)[:200], "output": str(directory)}), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
