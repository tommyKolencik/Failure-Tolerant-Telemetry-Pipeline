"""Strict event validation, canonical content, and identity."""
import hashlib
import json
import re
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator

from shared.config import HTTP_BATCH_MAX_EVENTS, SCHEMA_VERSION


class StrictModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        allow_inf_nan=False,
    )


class TemperaturePayload(StrictModel):
    temperature_c: float | None = Field(ge=-50, le=150)
    sensor_ok: bool

    error_code: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
    )

    @model_validator(mode="after")
    def check_temp_ok(self) -> Self:
        if self.sensor_ok:
            if self.temperature_c is None:
                raise ValueError("sensor_ok=True requires a temperature reading")
            if self.error_code is not None:
                raise ValueError("sensor_ok=True requires error_code=None")
        else:
            if self.temperature_c is not None:
                raise ValueError("sensor_ok=False requires temperature_c=None")
            if self.error_code is None or not self.error_code.strip():
                raise ValueError("sensor_ok=False requires a nonblank error_code")

        return self


class EventEnvelope(StrictModel):
    schema_version: int
    source_id: str = Field(min_length=1, max_length=64)
    boot_id: str = Field(min_length=32, max_length=32)
    seq: int = Field(ge=0, lt=2**63)
    source_uptime_us: int = Field(ge=0, lt=2**63)

    @field_validator("schema_version")
    @classmethod
    def check_schema_version(cls, value: int) -> int:
        if value != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
        return value

    @field_validator("source_id")
    @classmethod
    def check_source_id(cls, value: str) -> str:
        if re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
            raise ValueError("source_id must use ASCII letters, digits, _ or -")
        return value

    @field_validator("boot_id")
    @classmethod
    def check_boot_id(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{32}", value) is None:
            raise ValueError("boot_id must be 32 lowercase hexadecimal characters")
        return value


class TemperatureEvent(EventEnvelope):
    kind: Literal["sensor.temperature"]
    payload: TemperaturePayload


class HostMetricsPayload(StrictModel):
    # Physical RAM in bytes; memory use is total minus available.
    memory_total_bytes: int = Field(gt=0, lt=2**63)
    memory_available_bytes: int = Field(ge=0, lt=2**63)
    # CPU and its interval are both null during warmup or short intervals.
    cpu_percent: float | None = Field(ge=0, le=100)
    cpu_sample_us: int | None = Field(ge=100_000, lt=2**63)

    @model_validator(mode="after")
    def check_measurements(self) -> Self:
        if self.memory_available_bytes > self.memory_total_bytes:
            raise ValueError("available memory cannot exceed total memory")
        if (self.cpu_percent is None) != (self.cpu_sample_us is None):
            raise ValueError("CPU reading and observation interval must be present together")
        return self


class HostMetricsEvent(EventEnvelope):
    kind: Literal["host.metrics"]
    payload: HostMetricsPayload


class HttpRequestPayload(StrictModel):
    # Use route templates; unmatched paths use /__unmatched__.
    route: str = Field(min_length=1, max_length=256)
    method: str = Field(min_length=1, max_length=16)
    status: int = Field(ge=100, le=599)
    duration_ms: float = Field(ge=0)

    @field_validator("route")
    @classmethod
    def check_route(cls, value: str) -> str:
        if not value.startswith("/") or "?" in value or "#" in value or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError("route must be a path template without query, fragment, or control characters")
        return value

    @field_validator("method")
    @classmethod
    def check_method(cls, value: str) -> str:
        if re.fullmatch(r"[A-Z]+", value) is None:
            raise ValueError("method must contain uppercase ASCII letters")
        return value


class HttpRequestEvent(EventEnvelope):
    kind: Literal["http.request"]
    payload: HttpRequestPayload


# TODO: Confirm firmware payloads for button.action and system.status.
# H1_Starter.md is missing; actions, timing rules, counters, and reset reasons
# cannot be inferred safely.
Event = Annotated[TemperatureEvent | HostMetricsEvent | HttpRequestEvent, Field(discriminator="kind")]
_EVENT_ADAPTER = TypeAdapter(Event)


def validate_event(raw: Any) -> Event:
    """Return a validated event, or raise Pydantic's ValidationError."""
    # Revalidate model instances to catch post-construction mutation.
    if isinstance(raw, EventEnvelope):
        raw = raw.model_dump(mode="python")
    return _EVENT_ADAPTER.validate_python(raw)


class BatchRequest(StrictModel):
    # Enforce the HTTP byte cap before parsing in collector/app.py.
    events: list[Event] = Field(min_length=1, max_length=HTTP_BATCH_MAX_EVENTS)


def event_key(event: Event) -> tuple[str, str, int]:
    """Identity used to detect retransmissions, independent of content."""
    return (event.source_id, event.boot_id, event.seq)


def canonical_event_bytes(event: Event) -> bytes:
    """Serialize validated source content consistently for storage/comparison."""
    # Include defaults so omitted and explicit null values compare equally.
    content = event.model_dump(mode="json")
    return json.dumps(
        content,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def event_hash(event: Event) -> str:
    """SHA-256 fingerprint of the entire normalized event, not just its key."""
    return hashlib.sha256(canonical_event_bytes(event)).hexdigest()
