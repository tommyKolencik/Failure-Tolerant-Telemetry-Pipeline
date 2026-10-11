"""Scaffold: send pending batches and handle delivery failures.

M1/M2 implementation:
- Use an HTTPX client with explicit timeouts.
- Flush at 100 events or 250 ms; also respect the serialized HTTP body limit.
- Retain rows until a collector acknowledgment follows its durable commit.
- Retry transient errors with capped exponential backoff and jitter.
- Quarantine permanent validation/conflict failures visibly and let later
  traffic progress; count quarantined events.

No HTTP transport or retry loop is implemented yet.
"""
