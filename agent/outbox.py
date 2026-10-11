"""Scaffold: durable local acceptance, pending batches, and acknowledgments.

M1/M2 implementation:
- Expose publish(event): validate and commit, or return an explicit rejection.
- Use SQLite WAL, synchronous=FULL, and a bounded busy timeout.
- Enforce the 16 MiB pending-data cap atomically with insertion.
- Preserve event keys/content across retries and agent restarts.
- Delete acknowledged rows only in a separate committed transaction.
- Bound quarantine records and expose capacity/quarantine rejection counters.

No database or publish function is implemented yet.
"""
