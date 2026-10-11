"""Scaffold: atomic collector persistence, duplicates, conflicts, and retention.

M1/M2 implementation:
- Use one SQLite file with WAL, synchronous=FULL, and a bounded busy timeout.
- Give each worker its own connection and keep transactions short.
- Uniquely store (source_id, boot_id, seq), canonical content, and its hash.
- Count identical retries as duplicates; reject different content at the same key.
- Roll back the entire batch on invalid data or a conflict.
- Record host receipt/commit times separately and add finite retention.

No storage operations are implemented yet.
"""
