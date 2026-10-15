"""Batch and retry pending events; delete only after a validated durable ACK."""
from dataclasses import dataclass
import math
import random
from threading import Event as StopEvent
import time
from typing import Literal, Self
import sqlite3

import httpx
from pydantic import Field, ValidationError

from agent.outbox import Outbox, QuarantineCapacityError
from shared.config import (
    BATCH_FLUSH_MAX_WAIT_SECONDS,
    HTTP_ACK_MAX_BYTES,
    HTTP_BATCH_MAX_EVENTS,
    HTTP_BODY_MAX_BYTES,
    HTTP_CONNECT_TIMEOUT_SECONDS,
    HTTP_TIMEOUT_SECONDS,
    RETRY_BASE_SECONDS,
    RETRY_CAP_SECONDS,
)
from shared.schema import Event, StrictModel, canonical_event_bytes, event_key


class CollectorAcknowledgment(StrictModel):
    inserted: int = Field(ge=0, le=HTTP_BATCH_MAX_EVENTS)
    duplicates: int = Field(ge=0, le=HTTP_BATCH_MAX_EVENTS)


@dataclass(frozen=True)
class UploadResult:
    status: Literal["idle", "acked", "retry", "isolating", "quarantined", "blocked"]
    submitted_events: int = 0
    acknowledged: int = 0
    quarantined: int = 0
    inserted: int = 0
    duplicates: int = 0
    retry_after_seconds: float = 0.0
    reason: str | None = None


class AcknowledgmentError(Exception):
    """A successful HTTP status did not contain a trustworthy complete ACK."""


class UploaderBlockedError(Exception):
    """Operator action is needed; pending events remain in the outbox."""

    def __init__(self, result: UploadResult) -> None:
        self.result = result
        super().__init__(result.reason)


class Uploader:
    def __init__(
        self,
        outbox: Outbox,
        collector_url: str = "http://127.0.0.1:8000",
        *,
        batch_size: int = HTTP_BATCH_MAX_EVENTS,
        max_body_bytes: int = HTTP_BODY_MAX_BYTES,
        flush_wait_seconds: float = BATCH_FLUSH_MAX_WAIT_SECONDS,
        retry_base_seconds: float = RETRY_BASE_SECONDS,
        retry_cap_seconds: float = RETRY_CAP_SECONDS,
        client: httpx.Client | None = None,
        rng: random.Random | None = None,
    ) -> None:
        if type(batch_size) is not int or not 1 <= batch_size <= HTTP_BATCH_MAX_EVENTS:
            raise ValueError(f"batch_size must be an integer in 1..{HTTP_BATCH_MAX_EVENTS}")
        if type(max_body_bytes) is not int or not 1 <= max_body_bytes <= HTTP_BODY_MAX_BYTES:
            raise ValueError(f"max_body_bytes must be an integer in 1..{HTTP_BODY_MAX_BYTES}")
        for name, value in (
            ("flush_wait_seconds", flush_wait_seconds),
            ("retry_base_seconds", retry_base_seconds),
            ("retry_cap_seconds", retry_cap_seconds),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        if flush_wait_seconds > BATCH_FLUSH_MAX_WAIT_SECONDS or retry_base_seconds > retry_cap_seconds:
            raise ValueError("flush wait exceeds its cap or retry base exceeds retry cap")
        url = httpx.URL(collector_url)
        if url.scheme not in ("http", "https") or not url.host or url.query or url.fragment:
            raise ValueError("collector_url must be an absolute HTTP(S) base URL without query/fragment")
        self._url = url.copy_with(path=url.path.rstrip("/") + "/v1/events/batch")
        self.outbox = outbox
        self.batch_size = batch_size
        self.max_body_bytes = max_body_bytes
        self.flush_wait_seconds = flush_wait_seconds
        self.retry_base_seconds = retry_base_seconds
        self.retry_cap_seconds = retry_cap_seconds
        self._backoff_window = retry_base_seconds
        self._rng = random.Random() if rng is None else rng
        self._isolation_remaining = 0
        self._cached_batch: tuple[list[Event], bytes] | None = None
        self.last_result = UploadResult("idle")
        self._timeout = httpx.Timeout(HTTP_TIMEOUT_SECONDS, connect=HTTP_CONNECT_TIMEOUT_SECONDS)
        # HTTPX timeouts bound individual operations, not total request time.
        self._owns_client = client is None
        self._client = client if client is not None else httpx.Client(
            timeout=self._timeout, follow_redirects=False, trust_env=False,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _retry(self, reason: str, submitted: int = 0) -> UploadResult:
        # Equal jitter uses the upper half of the capped exponential window.
        delay = self._rng.uniform(self._backoff_window / 2, self._backoff_window)
        self._backoff_window = min(self.retry_cap_seconds, self._backoff_window * 2)
        return UploadResult("retry", submitted_events=submitted, retry_after_seconds=delay, reason=reason)

    def _completed(self, count: int) -> None:
        self._backoff_window = self.retry_base_seconds
        self._isolation_remaining = max(0, self._isolation_remaining - count)
        self._cached_batch = None

    def _pack(self, pending: list[Event]) -> tuple[list[Event], bytes]:
        # Budget wrapper, commas, and UTF-8 content together.
        prefix, suffix = b'{"events":[', b']}'
        size = len(prefix) + len(suffix)
        selected: list[Event] = []
        parts: list[bytes] = []
        for event in pending:
            content = canonical_event_bytes(event)
            extra = len(content) + (1 if parts else 0)
            if size + extra > self.max_body_bytes:
                break
            selected.append(event)
            parts.append(content)
            size += extra
        return selected, prefix + b",".join(parts) + suffix

    @staticmethod
    def _read_ack(response: httpx.Response, submitted: int) -> CollectorAcknowledgment:
        media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if media_type != "application/json":
            raise AcknowledgmentError("ACK must use application/json")
        # Reject compression before reading so decompression cannot bypass the cap.
        if response.headers.get("content-encoding", "identity").strip().lower() != "identity":
            raise AcknowledgmentError("compressed ACK is unsupported")
        declared = response.headers.get("content-length")
        if declared is not None:
            if not declared.isascii() or not declared.isdecimal() or len(declared) > 20:
                raise AcknowledgmentError("invalid ACK Content-Length")
            if int(declared) > HTTP_ACK_MAX_BYTES:
                raise AcknowledgmentError("ACK exceeds byte limit")
        body = bytearray()
        for chunk in response.iter_bytes():
            if len(body) + len(chunk) > HTTP_ACK_MAX_BYTES:
                raise AcknowledgmentError("ACK exceeds byte limit")
            body.extend(chunk)
        try:
            ack = CollectorAcknowledgment.model_validate_json(bytes(body))
        except ValidationError as exc:
            raise AcknowledgmentError("ACK failed schema validation") from exc
        if ack.inserted + ack.duplicates != submitted:
            raise AcknowledgmentError("ACK counts do not cover the submitted batch")
        return ack

    def upload_once(self) -> UploadResult:
        """Attempt once now; callers must honor retry_after_seconds or use run()."""
        try:
            result = self._upload_once()
        except sqlite3.Error:
            result = self._retry("outbox storage unavailable")
        except QuarantineCapacityError:
            result = UploadResult("blocked", reason="rejected event cannot fit quarantine; adjust its limits")
        self.last_result = result
        return result

    def _upload_once(self) -> UploadResult:
        limit = 1 if self._isolation_remaining else self.batch_size
        if self._cached_batch is None:
            pending = self.outbox.peek_batch(limit)
            if not pending:
                self._completed(self._isolation_remaining)
                return UploadResult("idle")
            selected, body = self._pack(pending)
            if not selected:
                moved = self.outbox.quarantine(
                    [event_key(pending[0])], reason="event exceeds HTTP request byte limit",
                )
                self._completed(1)
                return UploadResult("quarantined", quarantined=moved, reason="event exceeds HTTP request byte limit")
            self._cached_batch = selected, body
        # Retain the exact request through ambiguous failures.
        selected, body = self._cached_batch

        try:
            with self._client.stream(
                "POST", self._url, content=body,
                headers={"Content-Type": "application/json", "Accept-Encoding": "identity"},
                timeout=self._timeout, follow_redirects=False,
            ) as response:
                status = response.status_code
                ack = self._read_ack(response, len(selected)) if status == 200 else None
        except httpx.RequestError as exc:
            # A timeout may follow COMMIT; retry unchanged content to receive duplicate ACKs.
            return self._retry(f"HTTP transport failure: {type(exc).__name__}", len(selected))
        except AcknowledgmentError as exc:
            return self._retry(str(exc), len(selected))

        if ack is not None:
            # Delete after the complete ACK; failed deletion leaves the batch retryable.
            removed = self.outbox.acknowledge([event_key(event) for event in selected])
            self._completed(len(selected))
            return UploadResult(
                "acked", submitted_events=len(selected), acknowledged=removed,
                inserted=ack.inserted, duplicates=ack.duplicates,
            )
        if status in (408, 429) or 500 <= status <= 599 or 200 <= status <= 299:
            return self._retry(f"HTTP {status}: no durable batch acknowledgment", len(selected))
        if status in (400, 409, 413, 422):
            if len(selected) > 1:
                self._isolation_remaining = len(selected)
                self._cached_batch = None
                return UploadResult("isolating", submitted_events=len(selected), reason=f"HTTP {status}: retry batch members individually")
            moved = self.outbox.quarantine(
                [event_key(selected[0])], reason=f"collector permanently rejected event: HTTP {status}",
                http_status=status,
            )
            self._completed(1)
            return UploadResult("quarantined", submitted_events=1, quarantined=moved, reason=f"HTTP {status}")
        # Configuration errors block without deleting pending events.
        return UploadResult("blocked", submitted_events=len(selected), reason=f"HTTP {status}: check collector endpoint/configuration")

    def run(self, stop_event: StopEvent) -> None:
        """Run on the uploader worker; stop waits are interruptible by shutdown."""
        poll = min(0.05, self.flush_wait_seconds)
        last_empty_at: float | None = None
        flush_deadline: float | None = None
        while not stop_event.is_set():
            now = time.monotonic()
            limit = 1 if self._isolation_remaining else self.batch_size
            try:
                pending = self.outbox.peek_batch(limit)
            except sqlite3.Error:
                self.last_result = self._retry("outbox storage unavailable")
                stop_event.wait(self.last_result.retry_after_seconds)
                continue
            if not pending:
                last_empty_at, flush_deadline = now, None
                self._completed(self._isolation_remaining)
                stop_event.wait(poll)
                continue
            if len(pending) < limit:
                if flush_deadline is None:
                    # Include polling delay in the flush deadline; flush startup backlog immediately.
                    flush_deadline = now if last_empty_at is None else last_empty_at + self.flush_wait_seconds
                remaining = flush_deadline - time.monotonic()
                if remaining > 0:
                    stop_event.wait(min(poll, remaining))
                    continue
            result = self.upload_once()
            flush_deadline, last_empty_at = None, None
            if result.status == "blocked":
                raise UploaderBlockedError(result)
            if result.status == "retry":
                stop_event.wait(result.retry_after_seconds)
