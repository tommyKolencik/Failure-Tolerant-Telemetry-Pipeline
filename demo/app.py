"""Measure demo requests and publish them to the agent's shared outbox."""
import argparse
import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict
import math
from pathlib import Path
import sqlite3
from threading import Lock
import time

from fastapi import FastAPI, HTTPException
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send
import uvicorn

from agent.outbox import Outbox
from agent.sources import HttpRequestSource


class RequestRecorder:
    """Publish measured events; retain bounded aggregate process-local counters."""

    def __init__(self, outbox: Outbox, source: HttpRequestSource) -> None:
        self.outbox, self.source = outbox, source
        self._lock = Lock()
        self._counts = {"accepted": 0, "capacity": 0, "unavailable": 0,
                        "invalid": 0, "conflict": 0, "recording_errors": 0}

    def record(self, route: str, method: str, status: int, duration_ms: float) -> None:
        try:
            event = self.source.request_event(route=route, method=method, status=status, duration_ms=duration_ms)
        except ValueError:
            name = "invalid"
        else:
            result = self.outbox.publish(event)
            name = "accepted" if result.accepted else result.status
        with self._lock:
            self._counts[name] += 1

    def note_error(self) -> None:
        with self._lock:
            self._counts["recording_errors"] += 1

    def snapshot(self) -> dict:
        with self._lock:
            counts = dict(self._counts)
        return {"source_id": self.source.source_id, "boot_id": self.source.boot_id, **counts}


class RequestTelemetryMiddleware:
    def __init__(self, app: ASGIApp, recorder: RequestRecorder,
                 clock_ns: Callable[[], int] = time.perf_counter_ns) -> None:
        self.app, self.recorder = app, recorder
        self._clock_ns = clock_ns

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] in ("/healthz", "/readyz", "/telemetry"):
            await self.app(scope, receive, send)
            return
        started = self._clock_ns()
        status = 500
        finished: int | None = None

        async def measured_send(message: Message) -> None:
            nonlocal status, finished
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                finished = self._clock_ns()

        try:
            await self.app(scope, receive, measured_send)
        except asyncio.CancelledError:
            # 499 is the local convention for cancellation before the final body.
            if finished is None:
                status = 499
            raise
        except Exception:
            if finished is None:
                status = 500
            raise  # Application errors remain application errors.
        finally:
            # Time through final-body send, excluding later publish/background work.
            elapsed_ms = ((finished if finished is not None else self._clock_ns()) - started) / 1_000_000
            route = getattr(scope.get("route"), "path", "/__unmatched__")
            try:
                await run_in_threadpool(self.recorder.record, route, scope["method"], status, elapsed_ms)
            except Exception:
                # Preserve the application response; expose recording failures separately.
                self.recorder.note_error()


def create_app(
    outbox_path: str | Path = "data/outbox.sqlite3",
    *,
    instrumentation_enabled: bool = True,
    source_id: str = "demo-http-01",
    slow_delay_seconds: float = 0.1,
    max_pending_bytes: int | None = None,
) -> FastAPI:
    if type(instrumentation_enabled) is not bool:
        raise ValueError("instrumentation_enabled must be a boolean")
    if isinstance(slow_delay_seconds, bool) or not isinstance(slow_delay_seconds, (int, float)) or not math.isfinite(slow_delay_seconds) or not 0 <= slow_delay_seconds <= 5:
        raise ValueError("slow_delay_seconds must be finite and between 0 and 5")
    options = {} if max_pending_bytes is None else {"max_pending_bytes": max_pending_bytes}
    outbox = Outbox(outbox_path, **options)
    recorder = RequestRecorder(outbox, HttpRequestSource(source_id=source_id))

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        if instrumentation_enabled:
            await run_in_threadpool(outbox.initialize)
        yield

    application = FastAPI(title="Instrumented Telemetry Demo", lifespan=lifespan)
    application.state.outbox = outbox
    application.state.recorder = recorder
    if instrumentation_enabled:
        application.add_middleware(RequestTelemetryMiddleware, recorder=recorder)

    @application.get("/normal")
    async def normal() -> dict:
        return {"status": "ok"}

    @application.get("/slow")
    async def slow() -> dict:
        await asyncio.sleep(slow_delay_seconds)
        return {"status": "slow", "delay_seconds": slow_delay_seconds}

    @application.get("/error")
    async def error() -> JSONResponse:
        return JSONResponse({"detail": "deterministic demo error"}, status_code=500)

    @application.get("/items/{item_id}")
    async def item(item_id: int) -> dict:
        return {"item_id": item_id}

    @application.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @application.get("/readyz")
    async def readyz() -> dict:
        if instrumentation_enabled:
            try:
                await run_in_threadpool(outbox.stats)
            except sqlite3.Error as exc:
                raise HTTPException(503, "demo outbox unavailable") from exc
        return {"status": "ready", "instrumentation_enabled": instrumentation_enabled}

    @application.get("/telemetry")
    async def telemetry() -> dict:
        counts = recorder.snapshot()
        try:
            stats = asdict(await run_in_threadpool(outbox.stats)) if instrumentation_enabled else None
        except sqlite3.Error:
            stats = None
        return {"instrumentation_enabled": instrumentation_enabled, "recording": counts, "outbox": stats}

    return application


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the measured normal/slow/error demo")
    parser.add_argument("--outbox", default="data/outbox.sqlite3")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--no-instrumentation", action="store_true")
    parser.add_argument("--slow-delay", type=float, default=0.1)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be in 1..65535")
    try:
        application = create_app(args.outbox, instrumentation_enabled=not args.no_instrumentation,
                                 slow_delay_seconds=args.slow_delay)
    except ValueError as exc:
        parser.error(str(exc))
    uvicorn.run(application, host="127.0.0.1", port=args.port)


app = create_app()  # Also supports: python3 -m uvicorn demo.app:app --port 8001

if __name__ == "__main__":
    main()
