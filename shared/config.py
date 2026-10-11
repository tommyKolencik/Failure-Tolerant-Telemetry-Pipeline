"""Initial limits from S1_Starter.md; enforcement belongs in future components.

Collector retention, stats windows, retry caps, and quarantine retention still
need explicit choices during implementation. These constants do not enforce
limits by themselves.
"""

SCHEMA_VERSION = 1
SUPPORTED_EVENT_KINDS = frozenset(
    {
        "sensor.temperature",
        "button.action",
        "system.status",
        "host.metrics",
        "http.request",
    }
)
SERIAL_LINE_MAX_BYTES = 1_024  # Includes the trailing newline.
HTTP_BATCH_MAX_EVENTS = 100
HTTP_BODY_MAX_BYTES = 128 * 1_024
OUTBOX_MAX_PENDING_BYTES = 16 * 1_024 * 1_024  # Serialized pending data.
BATCH_FLUSH_MAX_WAIT_SECONDS = 0.250
QUERY_DEFAULT_LIMIT = 100
QUERY_MAX_LIMIT = 1_000
