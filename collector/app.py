"""Bounded HTTP ingest, health, queries, and collector statistics."""
import json
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import Field, ValidationError
from starlette.concurrency import run_in_threadpool
from starlette.requests import ClientDisconnect

from collector.store import (
    CollectorStats, CollectorStore, ConflictError, EventPage, InvalidQueryError,
)
from shared.config import (
    COLLECTOR_MAX_RETAINED_EVENTS,
    HTTP_BODY_MAX_BYTES,
    QUERY_DEFAULT_LIMIT,
    QUERY_MAX_LIMIT,
    STATS_MAX_EVENTS,
    STATS_WINDOW_SECONDS,
)
from shared.schema import BatchRequest, StrictModel


class BatchAcknowledgment(StrictModel):
    inserted: int = Field(ge=0)
    duplicates: int = Field(ge=0)


def _reject_json_constant(value: str) -> None:
    # Reject nonstandard constants before schema validation.
    raise ValueError("nonfinite JSON constants are not supported")


async def _read_limited_body(request: Request) -> bytes:
    """Enforce the byte cap before JSON parsing, including chunked requests."""
    media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if media_type != "application/json":
        raise HTTPException(415, "Content-Type must be application/json")

    declared_length = request.headers.get("content-length")
    if declared_length is not None:
        try:
            if not declared_length.isascii() or not declared_length.isdecimal():
                raise ValueError
            length = int(declared_length)
        except ValueError as exc:
            raise HTTPException(400, "invalid Content-Length") from exc
        if length > HTTP_BODY_MAX_BYTES:
            raise HTTPException(413, "request body exceeds the byte limit")

    body = bytearray()
    try:
        # Enforce actual bytes even when Content-Length is absent or incorrect.
        async for chunk in request.stream():
            if len(body) + len(chunk) > HTTP_BODY_MAX_BYTES:
                raise HTTPException(413, "request body exceeds the byte limit")
            body.extend(chunk)
    except ClientDisconnect as exc:
        raise HTTPException(400, "request body was interrupted") from exc
    return bytes(body)


def create_app(
    database_path: str | Path = "data/collector.sqlite3",
    *,
    busy_timeout_ms: int = 1_000,
    max_retained_events: int = COLLECTOR_MAX_RETAINED_EVENTS,
    stats_window_seconds: int = STATS_WINDOW_SECONDS,
    stats_max_events: int = STATS_MAX_EVENTS,
) -> FastAPI:
    """Build an app with configurable storage, including temporary test paths."""
    store = CollectorStore(
        database_path, busy_timeout_ms=busy_timeout_ms,
        max_retained_events=max_retained_events,
        stats_window_seconds=stats_window_seconds, stats_max_events=stats_max_events,
    )

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        # Keep synchronous SQLite work off the event loop.
        await run_in_threadpool(store.initialize)
        application.state.store = store
        yield

    application = FastAPI(title="Telemetry Collector", lifespan=lifespan)

    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/readyz")
    async def readyz(request: Request) -> dict[str, str]:
        try:
            await run_in_threadpool(request.app.state.store.check_ready)
        except sqlite3.Error as exc:
            raise HTTPException(503, "collector storage unavailable") from exc
        return {"status": "ready"}

    @application.post(
        "/v1/events/batch",
        response_model=BatchAcknowledgment,
        responses={
            400: {"description": "Malformed UTF-8/JSON or interrupted body"},
            409: {"description": "Same event key, different content; entire batch rejected"},
            413: {"description": "Request body exceeds 128 KiB"},
            415: {"description": "Content-Type must be application/json"},
            422: {"description": "Wrapper or event schema validation failed"},
            503: {"description": "Collector storage unavailable"},
        },
    )
    async def ingest_batch(request: Request) -> BatchAcknowledgment:
        """Commit 1..100 events before returning inserted and duplicate counts."""
        body = await _read_limited_body(request)
        try:
            raw = json.loads(
                body.decode("utf-8"),
                parse_constant=_reject_json_constant,
            )
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise HTTPException(400, "body must be valid finite UTF-8 JSON") from exc

        try:
            batch = BatchRequest.model_validate(raw)
        except ValidationError as exc:
            # Avoid echoing invalid source payloads.
            raise HTTPException(
                422,
                {"message": "batch failed schema validation", "error_count": exc.error_count()},
            ) from exc

        try:
            # Wait for COMMIT before constructing the ACK.
            result = await run_in_threadpool(
                request.app.state.store.insert_batch, batch.events
            )
        except ConflictError as exc:
            raise HTTPException(
                409,
                {"message": "event key has different content", "key": list(exc.key)},
            ) from exc
        except sqlite3.Error as exc:
            raise HTTPException(503, "collector storage unavailable") from exc

        return BatchAcknowledgment(inserted=result.inserted, duplicates=result.duplicates)

    @application.get("/v1/events", response_model=EventPage)
    async def events(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=QUERY_MAX_LIMIT)] = QUERY_DEFAULT_LIMIT,
        cursor: Annotated[int, Query(ge=0, lt=2**63)] = 0,
        source_id: Annotated[str | None, Query(min_length=1, max_length=64)] = None,
        kind: Annotated[str | None, Query(min_length=1, max_length=64)] = None,
    ) -> EventPage:
        """Page retained events by collector ID; continue with the same filters.

        next_cursor=null marks the current end. Pages reflect live retention."""
        try:
            return await run_in_threadpool(
                request.app.state.store.query_events,
                limit=limit, cursor=cursor, source_id=source_id, kind=kind,
            )
        except InvalidQueryError as exc:
            raise HTTPException(422, "invalid event query parameters") from exc
        except sqlite3.Error as exc:
            raise HTTPException(503, "collector storage unavailable") from exc

    @application.get("/v1/stats", response_model=CollectorStats)
    async def stats(request: Request) -> CollectorStats:
        """Lifetime counters and a capped recent sample by collector receipt time."""
        try:
            return await run_in_threadpool(request.app.state.store.stats)
        except sqlite3.Error as exc:
            raise HTTPException(503, "collector storage unavailable") from exc

    return application


app = create_app()
