"""Scaffold: validate events and define their canonical identity and content.

M0/M1 implementation:
- Define strict Pydantic models for the envelope and each supported payload.
- Validate IDs, integer bounds, supported kinds, and finite numeric values.
- Use (source_id, boot_id, seq) as the event key.
- Serialize content deterministically and compute its hash for retry comparison.
- Keep host receipt/commit metadata separate from source event content.

No event validation or hashing is implemented yet.
"""
