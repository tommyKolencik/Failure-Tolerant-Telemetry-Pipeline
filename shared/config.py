"""Default schema, storage, transport, and query limits."""

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
COLLECTOR_MAX_RETAINED_EVENTS = 100_000  # Keep newest accepted events by arrival.
STATS_WINDOW_SECONDS = 300  # Collector host-clock receipt window: five minutes.
STATS_MAX_EVENTS = 10_000  # Summarize at most this many recent retained events.
QUARANTINE_MAX_EVENTS = 1_000
QUARANTINE_MAX_BYTES = 1 * 1_024 * 1_024  # Canonical event bytes; metadata also row-bounded.
QUARANTINE_REASON_MAX_CHARS = 200
HTTP_ACK_MAX_BYTES = 4 * 1_024
HTTP_TIMEOUT_SECONDS = 5.0  # Per HTTPX network operation, not total elapsed time.
HTTP_CONNECT_TIMEOUT_SECONDS = 2.0
RETRY_BASE_SECONDS = 0.250
RETRY_CAP_SECONDS = 10.0
