"""Shared event contract: validate source content 
    before storing or uploading it.
"""
import hashlib
import json
import re
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from shared.config import HTTP_BATCH_MAX_EVENTS, SCHEMA_VERSION

# A BaseModel uses annotated fields to validate input and build a Python object.
# Subclasses inherit this configuration, so we only define the common rules once.
# Nested payload models also inherit StrictModel: nesting alone does not pass
# an outer model's configuration into an unrelated payload model.
# Docs: https://docs.pydantic.dev/latest/concepts/config/
class StrictModel(BaseModel):
    model_config = ConfigDict(
        strict=True,         # For example, reject "42" or True for an int field.
        extra="forbid",      # Reject unknown fields instead of silently ignoring them.
        allow_inf_nan=False, # Reject NaN and positive/negative infinity.
    )


class TemperaturePayload(StrictModel):
    # The field is required, but a failed reading uses None as its value.
    # The project-chosen -50..150 Celsius range is inclusive.
    temperature_c: float | None = Field(ge=-50, le=150)
    sensor_ok: bool

    error_code: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
    )

    @model_validator(mode="after")
    def check_temp_ok(self) -> Self:
        # Pydantic has already checked field types and temperature limits.
        # Check that the reported sensor state agrees with the other fields.
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

        # An after-model validator must return the validated instance.
        return self


# The envelope describes the source and event identity, regardless of payload.
# Fields without defaults are required; incoming events must supply their IDs.
# Field(ge=0, lt=2**63) means 0 <= value < 2**63, as specified by the starter.
class EventEnvelope(StrictModel):
    schema_version: int
    source_id: str = Field(min_length=1, max_length=64)
    boot_id: str = Field(min_length=32, max_length=32)
    seq: int = Field(ge=0, lt=2**63)
    source_uptime_us: int = Field(ge=0, lt=2**63)

    # Field validators check one field. Your model validator above checks the
    # relationship between multiple fields. Both run after normal type checks
    # here, and must return the validated value or model on success.
    # Docs: https://docs.pydantic.dev/latest/concepts/validators/
    @field_validator("schema_version")
    @classmethod
    def check_schema_version(cls, value: int) -> int:
        if value != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
        return value

    @field_validator("source_id")
    @classmethod
    def check_source_id(cls, value: str) -> str:
        # fullmatch checks every character, including a possible final newline.
        if re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
            raise ValueError("source_id must use ASCII letters, digits, _ or -")
        return value

    @field_validator("boot_id")
    @classmethod
    def check_boot_id(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{32}", value) is None:
            raise ValueError("boot_id must be 32 lowercase hexadecimal characters")
        return value

    # This checks an individual event, not a stream's sequence ordering.
    # Database uniqueness and producer sequencing are responsibilities elsewhere.


# Inheritance adds kind and payload to all the envelope fields above.
# Literal allows exactly this kind string. There is no default: it is required.
# Pydantic validates the nested payload and creates a TemperaturePayload object.
class TemperatureEvent(EventEnvelope):
    kind: Literal["sensor.temperature"]
    payload: TemperaturePayload


# TODO: Define the remaining payloads once their contracts are chosen.
# The starter suggests these fields, but does not fully specify their rules:
# - button.action: action, detected_us, applied_us. Choose allowed actions and
#   whether applied_us must be >= detected_us; confirm with the firmware.
# - system.status: choose explicit queue/counter field names and reset reasons.
# - host.metrics: choose memory fields and units (bytes, percent, or both).
# - http.request: choose route/method string limits and allowed method values;
#   specify status bounds and use finite, nonnegative duration_ms.
# Each payload should inherit StrictModel and bound its strings/numeric values.
# We deliberately do not accept arbitrary dictionaries for unfinished payloads.

# Event is currently an alias for the one fully specified event type.
# TODO: After defining the other payloads, add one event subclass per kind and
# replace this alias with Annotated[A | B | ..., Field(discriminator="kind")].
# A discriminator selects the matching model using the kind field; TypeAdapter
# will then let validate_event validate that union rather than a single class.
# Until then, the four other planned kinds are rejected, even though config.py
# lists them. The complete five-kind shared contract is still unfinished.
# Docs: https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions
Event = TemperatureEvent


def validate_event(raw: Any) -> Event:
    """Return a validated event, or raise Pydantic's ValidationError."""
    # Any describes what callers can TRY to submit; it does not disable checks.
    # model_validate checks raw input against Event's fields and validators.
    # Docs: https://docs.pydantic.dev/latest/concepts/models/#validating-data
    return Event.model_validate(raw)


class BatchRequest(StrictModel):
    # HTTP shape: {"events": [event, ...]}. Every item is validated as an Event.
    # Reject empty batches and batches over the existing configured count cap.
    # The HTTP byte cap must be enforced before JSON parsing in collector/app.py.
    events: list[Event] = Field(min_length=1, max_length=HTTP_BATCH_MAX_EVENTS)


def event_key(event: Event) -> tuple[str, str, int]:
    """Identity used to detect retransmissions, independent of content."""
    return (event.source_id, event.boot_id, event.seq)


def canonical_event_bytes(event: Event) -> bytes:
    """Serialize validated source content consistently for storage/comparison."""
    # model_dump converts nested models to dictionaries with JSON-compatible
    # values. Defaults are included: omitted error_code and explicit None both
    # become null. This compares normalized content, not the original JSON text.
    # Docs: https://docs.pydantic.dev/latest/concepts/serialization/
    content = event.model_dump(mode="json")
    # Sorting keys and removing optional whitespace makes input key order
    # irrelevant. UTF-8 is fixed here; allow_nan=False is an extra safeguard.
    return json.dumps(
        content,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def event_hash(event: Event) -> str:
    """SHA-256 fingerprint of the entire normalized event, not just its key."""
    # Same identity with changed payload/uptime changes this fingerprint.
    # Storage must compare content too and decide whether to reject a conflict.
    # Host receipt/commit timestamps stay outside this source event.
    return hashlib.sha256(canonical_event_bytes(event)).hexdigest()
