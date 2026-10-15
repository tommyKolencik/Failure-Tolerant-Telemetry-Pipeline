"""Kill owned collectors at COMMIT boundaries and reconcile exact content."""
import argparse
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import importlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import time
from uuid import uuid4

import httpx

from agent.outbox import Outbox
from agent.sources import SyntheticSource
from agent.uploader import Uploader
from benchmarks.run import OwnedCollector, ROOT, metadata, record_sources, stored_events, write_json
from collector.store import CollectorStore
from shared.schema import canonical_event_bytes, event_key

_INGESTING = ContextVar("crash_check_ingesting", default=False)


class CommitGate:
    """Delegate SQL; intercept just the ingest transaction's COMMIT statement."""

    def __init__(self, connection: sqlite3.Connection, stage: str, marker: Path) -> None:
        self.connection, self.stage, self.marker = connection, stage, marker

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def crash(self) -> None:
        # The dying connection sees uncommitted rows; recovery checks use a fresh one.
        data = {"stage": self.stage, "pid": os.getpid(),
                "in_transaction": self.connection.in_transaction,
                "visible_rows_on_connection": self.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]}
        with self.marker.open("w", encoding="utf-8") as output:
            json.dump(data, output)
            output.flush()
            os.fsync(output.fileno())
        os.kill(os.getpid(), signal.SIGKILL)
        raise RuntimeError("SIGKILL unexpectedly returned")

    def execute(self, sql, *args, **kwargs):
        if sql == "COMMIT" and self.stage == "before_commit":
            self.crash()
        result = self.connection.execute(sql, *args, **kwargs)
        if sql == "COMMIT" and self.stage == "after_commit":
            self.crash()
        return result


class FaultingStore(CollectorStore):
    def __init__(self, *args, fault_stage: str, marker_path: str | Path, **kwargs) -> None:
        if fault_stage not in ("before_commit", "after_commit"):
            raise ValueError("unsupported crash stage")
        super().__init__(*args, **kwargs)
        self.fault_stage, self.marker_path = fault_stage, Path(marker_path)

    def insert_batch(self, events):
        # Arm the gate only for this worker's ingest transaction.
        token = _INGESTING.set(True)
        try:
            return super().insert_batch(events)
        finally:
            _INGESTING.reset(token)

    @contextmanager
    def _transaction(self, connection, *, write: bool = True) -> Iterator[None]:
        gated = CommitGate(connection, self.fault_stage, self.marker_path) if write and _INGESTING.get() else connection
        # Delegate transaction control to the production store.
        with super()._transaction(gated, write=write):
            yield


def create_app(database_path, *, fault_stage: str, marker_path: str):
    module = importlib.import_module("collector.app")
    original = module.CollectorStore
    # Build synchronously in the owned child, then restore the module immediately.
    def factory(*args, **kwargs):
        return FaultingStore(*args, fault_stage=fault_stage, marker_path=marker_path, **kwargs)
    module.CollectorStore = factory
    try:
        return module.create_app(database_path)
    finally:
        module.CollectorStore = original


class RecordingTransport(httpx.BaseTransport):
    def __init__(self) -> None:
        self.inner = httpx.HTTPTransport(retries=0)
        self.bodies: list[bytes] = []

    def handle_request(self, request):
        if len(self.bodies) >= 100:
            raise RuntimeError("crash check request observation limit reached")
        self.bodies.append(request.content)
        return self.inner.handle_request(request)

    def close(self):
        self.inner.close()


@dataclass(frozen=True)
class Settings:
    batch_sizes: tuple[int, ...] = (1, 100)
    seed: int = 7

    def __post_init__(self):
        if not self.batch_sizes or len(set(self.batch_sizes)) != len(self.batch_sizes) or any(type(n) is not int or not 1 <= n <= 100 for n in self.batch_sizes):
            raise ValueError("batch sizes must be unique integers in 1..100")


def run_case(directory: Path, stage: str, size: int, seed: int) -> dict:
    directory.mkdir()
    database = directory / "collector.sqlite3"
    store = CollectorStore(database)
    store.initialize()
    baseline = list(SyntheticSource(source_id="preexisting-01", seed=0).events(5))
    store.insert_batch(baseline)
    expected = {event_key(event): canonical_event_bytes(event).decode() for event in baseline}
    source = SyntheticSource(seed=seed)
    first, later = list(source.events(size)), list(source.events(5))
    outbox = Outbox(directory / "outbox.sqlite3")
    outbox.initialize()
    for event in first + later:
        assert outbox.publish(event).accepted
    expected.update({event_key(event): canonical_event_bytes(event).decode() for event in first + later})
    marker = directory / "crash-marker.json"
    server = OwnedCollector(directory, module="benchmarks.crash_checks",
                            factory_options={"fault_stage": stage, "marker_path": str(marker)})
    replacement = None
    transport = RecordingTransport()
    try:
        server.start()
        with httpx.Client(transport=transport, trust_env=False) as client:
            with Uploader(outbox, server.url, batch_size=size, client=client) as uploader:
                failed = uploader.upload_once()
                assert failed.status == "retry"
                assert server.process.wait(timeout=5) == -signal.SIGKILL
                witness = json.loads(marker.read_text())
                assert witness["stage"] == stage
                assert witness["in_transaction"] == (stage == "before_commit")
                assert witness["visible_rows_on_connection"] == 5 + size
                assert outbox.stats().pending_count == size + 5
                # Check surviving content and counters after the process dies.
                recovered_store = CollectorStore(database)
                recovered_store.initialize()
                surviving = {event_key(row.event): canonical_event_bytes(row.event).decode() for row in recovered_store.query_events(limit=1000).events}
                expected_survivors = {event_key(event): canonical_event_bytes(event).decode() for event in baseline + (first if stage == "after_commit" else [])}
                assert surviving == expected_survivors
                counters = recovered_store.stats()
                assert counters.inserted_total == len(expected_survivors)
                assert counters.duplicate_total == 0
                write_json(directory / "surviving-after-kill.json", list(surviving.values()))

                # Restart on the same port/database and retry the cached request.
                server.stop()
                replacement = OwnedCollector(directory, log_name="restarted-collector.log")
                replacement.port = server.port
                replacement.start()
                time.sleep(failed.retry_after_seconds)
                replay = uploader.upload_once()
                assert replay.status == "acked"
                assert replay.inserted == (size if stage == "before_commit" else 0)
                assert replay.duplicates == (size if stage == "after_commit" else 0)
                assert transport.bodies[0] == transport.bodies[1]
                while outbox.stats().pending_count:
                    assert uploader.upload_once().status == "acked"
        actual = stored_events(replacement.url)
        assert actual == expected
        final = recovered_store.stats()
        assert final.inserted_total == len(expected)
        assert final.duplicate_total == (size if stage == "after_commit" else 0)
        assert outbox.stats().quarantined_total == outbox.stats().capacity_rejections == 0
        for filename, values in (("accepted-and-preexisting.ndjson", expected), ("stored.ndjson", actual)):
            (directory / filename).write_text("".join(values[key] + "\n" for key in sorted(values)), encoding="utf-8")
        result = {"case": directory.name, "stage": stage, "batch_size": size, "complete": True,
                  "witness": witness, "collector_rows_after_kill": len(surviving),
                  "pending_after_kill": size + 5, "retry_inserted": replay.inserted,
                  "retry_duplicates": replay.duplicates, "first_and_retry_body_sha256": hashlib.sha256(transport.bodies[0]).hexdigest(),
                  "request_bodies_identical": True, "final_reconciled_events": len(actual),
                  "new_locally_accepted_events": size + 5, "preexisting_events": 5,
                  "pending_after": outbox.stats().pending_count, "quarantined_total": 0}
        write_json(directory / "summary.json", result)
        return result
    finally:
        server.close()
        if replacement is not None:
            replacement.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-sizes", nargs="+", type=int, default=[1, 100])
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        settings = Settings(tuple(args.batch_sizes), args.seed)
    except ValueError as exc:
        parser.error(str(exc))
    directory = (args.output or ROOT / "benchmarks/results" / ("crash-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid4().hex[:8])).resolve()
    try:
        directory.mkdir(parents=True, exist_ok=False)
        evidence = metadata(settings, extra_sources=("benchmarks/crash_checks.py",))
        evidence["limits"] = ["owned collector process SIGKILL checks around COMMIT", "not a power-cut, OS-crash, filesystem-corruption, or hardware test"]
        write_json(directory / "metadata.json", evidence)
        record_sources(directory, evidence)
        cases = []
        for size in settings.batch_sizes:
            for stage in ("before_commit", "after_commit"):
                name = f"{stage}-batch-{size}"
                print(json.dumps({"type": "case_start", "case": name}), flush=True)
                result = run_case(directory / name, stage, size, settings.seed)
                cases.append(result)
                write_json(directory / "summary.json", cases)
                print(json.dumps({"type": "case_finished", **result}), flush=True)
        print(json.dumps({"type": "finished", "complete": True, "cases": len(cases), "output": str(directory)}), flush=True)
        return 0
    except (AssertionError, OSError, RuntimeError, sqlite3.Error, httpx.HTTPError) as exc:
        print(json.dumps({"type": "error", "reason": str(exc)[:200], "output": str(directory)}), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
