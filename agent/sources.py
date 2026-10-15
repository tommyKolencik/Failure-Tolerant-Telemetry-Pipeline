"""Synthetic events, host measurements, and thread-safe request events."""
import argparse
from collections.abc import Callable, Iterator
import random
from threading import Lock, get_ident
import time
from typing import Protocol
from uuid import uuid4

import psutil

from shared.config import SCHEMA_VERSION
from shared.schema import Event, EventEnvelope, canonical_event_bytes, validate_event


class EventSource(Protocol):
    source_id: str
    boot_id: str

    def next_event(self) -> Event: ...


class SyntheticSource:
    """One synthetic source session, owned by one producer worker."""

    def __init__(
        self,
        *,
        source_id: str = "synthetic-01",
        seed: int = 7,
        uptime_step_us: int = 10_000,
        boot_id: str | None = None,
    ) -> None:
        if type(seed) is not int:
            raise ValueError("seed must be an integer")
        if type(uptime_step_us) is not int or not 1 <= uptime_step_us < 2**63:
            raise ValueError("uptime_step_us must be a positive integer below 2**63")

        # Use fresh boot IDs on restart; explicit IDs are for repeatable fixtures.
        session = EventEnvelope.model_validate({
            "schema_version": SCHEMA_VERSION,
            "source_id": source_id,
            "boot_id": uuid4().hex if boot_id is None else boot_id,
            "seq": 0,
            "source_uptime_us": 0,
        })
        self.source_id = session.source_id
        self.boot_id = session.boot_id
        self.uptime_step_us = uptime_step_us
        self._seq = 0

        self._rng = random.Random(seed)

    def next_event(self) -> Event:
        """Create one validated event and advance the source's sequence."""
        uptime = self._seq * self.uptime_step_us
        if self._seq >= 2**63 or uptime >= 2**63:
            raise OverflowError("synthetic source exhausted its sequence/uptime range")

        event = validate_event({
            "schema_version": SCHEMA_VERSION,
            "source_id": self.source_id,
            "boot_id": self.boot_id,
            "seq": self._seq,
            "kind": "sensor.temperature",
            "source_uptime_us": uptime,
            "payload": {
                "temperature_c": round(self._rng.uniform(15, 30), 3),
                "sensor_ok": True,
            },
        })
        self._seq += 1
        return event

    def events(self, count: int) -> Iterator[Event]:
        """Lazily produce count events; repeated calls continue this session."""
        if type(count) is not int or not 0 <= count < 2**63:
            raise ValueError("count must be a nonnegative integer below 2**63")
        # Uptime ticks are simulated; the caller controls pacing and durable acceptance.
        return (self.next_event() for _ in range(count))


def main() -> None:
    """Print synthetic NDJSON for inspection, without accessing storage/network."""
    parser = argparse.ArgumentParser(description="Preview synthetic telemetry events")
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--source-id", default="synthetic-01")
    parser.add_argument("--uptime-step-us", type=int, default=10_000)
    args = parser.parse_args()
    try:
        source = SyntheticSource(
            source_id=args.source_id, seed=args.seed, uptime_step_us=args.uptime_step_us,
        )
        for event in source.events(args.count):
            print(canonical_event_bytes(event).decode("utf-8"))
    except (ValueError, OverflowError) as exc:
        parser.error(str(exc))


class _LiveSession:
    """Common host session for real measurements, including concurrent requests."""

    def __init__(self, source_id: str, boot_id: str | None, clock_ns: Callable[[], int]) -> None:
        session = EventEnvelope.model_validate({
            "schema_version": SCHEMA_VERSION, "source_id": source_id,
            "boot_id": uuid4().hex if boot_id is None else boot_id,
            "seq": 0, "source_uptime_us": 0,
        })
        self.source_id, self.boot_id = session.source_id, session.boot_id
        self._clock_ns = clock_ns
        self._started_ns = clock_ns()
        self._seq = 0
        self._lock = Lock()

    def _event(self, kind: str, payload: dict) -> Event:
        # Allocate identities under the lock; advance only after validation.
        with self._lock:
            uptime = (self._clock_ns() - self._started_ns) // 1_000
            if self._seq >= 2**63 or not 0 <= uptime < 2**63:
                raise OverflowError("live source exhausted its sequence/uptime range")
            event = validate_event({
                "schema_version": SCHEMA_VERSION, "source_id": self.source_id,
                "boot_id": self.boot_id, "seq": self._seq,
                "source_uptime_us": uptime, "kind": kind, "payload": payload,
            })
            self._seq += 1
            return event


class HostMetricsSource(_LiveSession):
    """Nonblocking CPU/RAM sampler, owned by one producer thread."""

    def __init__(self, *, source_id: str = "host-01", boot_id: str | None = None,
                 clock_ns: Callable[[], int] = time.monotonic_ns,
                 cpu_reader: Callable[[], float] | None = None,
                 memory_reader: Callable | None = None) -> None:
        super().__init__(source_id, boot_id, clock_ns)
        self._cpu_reader = cpu_reader if cpu_reader is not None else lambda: psutil.cpu_percent(interval=None)
        self._memory_reader = memory_reader if memory_reader is not None else psutil.virtual_memory
        self._cpu_at_ns: int | None = None
        self._owner_thread: int | None = None

    def next_event(self) -> Event:
        owner = get_ident()
        if self._owner_thread is None:
            self._owner_thread = owner
        elif owner != self._owner_thread:
            raise RuntimeError("host CPU sampling must stay on one producer thread")
        now = self._clock_ns()
        cpu, sample_us = None, None
        if self._cpu_at_ns is None:
            # Prime the thread-local CPU baseline; report null for warmup.
            self._cpu_reader()
            self._cpu_at_ns = now
        elif now - self._cpu_at_ns >= 100_000_000:
            # Keep the CPU baseline across ticks shorter than 100 ms.
            cpu = self._cpu_reader()
            sample_us = (now - self._cpu_at_ns) // 1_000
            self._cpu_at_ns = now
        memory = self._memory_reader()
        return self._event("host.metrics", {
            "cpu_percent": cpu, "cpu_sample_us": sample_us,
            "memory_total_bytes": memory.total,
            "memory_available_bytes": memory.available,
        })


class HttpRequestSource(_LiveSession):
    """Create measured request events with one thread-safe source/session."""

    def __init__(self, *, source_id: str = "demo-http-01", boot_id: str | None = None,
                 clock_ns: Callable[[], int] = time.monotonic_ns) -> None:
        super().__init__(source_id, boot_id, clock_ns)

    def request_event(self, *, route: str, method: str, status: int, duration_ms: float) -> Event:
        return self._event("http.request", {
            "route": route, "method": method, "status": status, "duration_ms": duration_ms,
        })


if __name__ == "__main__":
    main()
