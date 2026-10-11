"""Step 1 learning guide: define what a valid telemetry event looks like.

Work through the TODOs in order. Event and TemperaturePayload are started below;
the remaining shared-contract helpers still need implementation.
Finish this file before agent/outbox.py, then collector/store.py and app.py.
"""
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from shared.config import SCHEMA_VERSION

# WHY THIS FILE EXISTS
# The agent and collector need the same rules for accepting an event. Keeping
# those rules here prevents a producer from accepting data the collector rejects.
# Read the sample event and field table in S1_Starter.md before starting.
class TemperaturePayload(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        allow_inf_nan=False,
    )  # Reject NaN and positive/negative infinity.

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


# TODO 1A: Gather imports and define a common model configuration.
# Tools to look up: Pydantic BaseModel, ConfigDict, Field, StrictInt, StrictBool,
# model_validator, TypeAdapter; typing Annotated and Literal; json and hashlib.
# Import the existing limits from shared.config rather than copying numbers.
# Use strict=True and extra="forbid" on your base model and nested payloads.
# "Strict" means a sequence of "42" must not quietly become integer 42.
# "Forbid extras" means a misspelled field causes a visible validation error.
# Also disallow NaN/infinity in every numeric payload field.


# TODO 1B: Define the payload models, starting with TemperaturePayload.
# A payload is the kind-specific information inside the outer event envelope.
# Temperature fields: temperature_c (finite number or None), sensor_ok (strict
# boolean), and a bounded optional error code; choose and document its field name.
# Use a model validator to check the relationship between those fields:
# - sensor_ok=True requires a numeric temperature and no failure code.
# - sensor_ok=False requires temperature_c=None and a nonempty failure code.
# Zero degrees is a valid reading when sensor_ok=True; it is not a failure marker.
# Require temperature_c even when its value is None, so a missing field is caught.
# Then define the other four payloads using the starter's suggested fields:
# - button.action: action, detected_us, applied_us.
# - system.status: queue depths, drop/error counters, reset reason.
# - host.metrics: CPU percentage and memory use; document units/field names.
# - http.request: normalized route, method, status, duration_ms.
# The starter leaves some exact shapes open. Write down your choices in each
# model's comments, bound strings/collections, and keep firmware compatibility
# in mind. Do not accept an arbitrary unvalidated payload as a shortcut.


# TODO 1C: Define the common event-envelope fields in a base event model.
# schema_version: required strict integer equal to SCHEMA_VERSION (initially 1).
# Use an explicit equality validator as well as strict integer validation;
# boolean True must not pass merely because it compares equal to integer 1.
# source_id: 1..64 ASCII letters, digits, underscores, or hyphens.
# boot_id: exactly 32 lowercase hexadecimal characters.
# Hint: re.fullmatch checks the entire ID, including any trailing newline.
# seq and source_uptime_us: strict integers in [0, 2**63); reject booleans too.
# Require every envelope field. Do not silently create an ID for incoming data.
# Sequence ordering is a producer rule; this stateless validator cannot prove
# uniqueness or order across events. Storage handles uniqueness; arrival may vary.


# TODO 1D: Connect each kind to its matching payload model.
# A suggested design is one event subclass per kind, inheriting common fields.
# Each subclass has a Literal kind value and its specific payload type.
# Combine the five event types into an Annotated union with a "kind"
# discriminator. A discriminator selects the model from the event's kind field.
# This checks both the outer envelope and the correct nested payload together.
# Finish all supported kinds before calling the shared contract complete.


# TODO 1E: Write validate_event(raw) and the batch-request model.
# validate_event(raw) should return a validated event or raise ValidationError.
# A TypeAdapter for the event union can be created once and reused here.
# The HTTP request shape is {"events": [...]}: define a strict BatchRequest
# model whose events field uses your event union and has 1..100 items initially.
# Put the maximum in shared.config; reject unexpected wrapper fields too.
# The HTTP byte cap is a separate check in collector/app.py, before parsing.


# TODO 1F: Write the three shared identity/serialization helpers.
# event_key(event) -> tuple: return (source_id, boot_id, seq), in that order.
# canonical_event_bytes(event) -> bytes:
#   Dump the validated model in JSON mode, including all envelope/payload fields.
#   Use sorted keys, compact separators, one fixed ensure_ascii setting, and
#   allow_nan=False; encode the result as UTF-8. Use the same helper on both ends.
#   Normalization makes reordered JSON object keys compare consistently. Keep
#   array order and every meaningful event field; do not hash just the payload.
# event_hash(event) -> str: SHA-256 of those canonical bytes, as hexadecimal.
# Host receipt/commit times do not belong in source content or its hash.
# Validation should not rewrite source_id, boot_id, seq, or source_uptime_us.


# CHECKPOINT BEFORE STEP 2
# Try the starter's sample dictionary: it should validate and have the right key.
# Change seq to -1, True, and "42"; all three should fail.
# Try an unsupported version/kind, bad IDs, extra fields, NaN, and mismatched
# temperature status/value. Confirm each fails for a useful reason.
# Reordering input object keys should preserve canonical bytes and the hash.
# Changing payload or uptime at the same key should change canonical content.
# A valid 100-event BatchRequest should pass; 101 should fail.
# These remaining checks cover the full contract, which is still in progress.

# References for the APIs mentioned above:
# https://docs.pydantic.dev/latest/concepts/strict_mode/
# https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions
