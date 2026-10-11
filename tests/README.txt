Correctness test plan
====================

Status: planned. No automated tests exist yet.
Add tests alongside each implemented behavior; do not count placeholders as
passing coverage. Use temporary database paths rather than development data/.

M0/M1
- Validate envelopes, payloads, supported versions, IDs, integer bounds, and
  finite numbers. Preserve canonical event identity/content.
- Persist accepted rows and enforce capacity atomically.
- Deliver 1,000 seeded synthetic events and reconcile exact keys and payloads.

M2
- Repeat an event: one stored row and an increasing duplicate counter.
- Reuse its key with different content: explicit conflict, original unchanged.
- Invalid/conflicting batch: no partial inserts.
- Collector unavailable: accepted rows remain pending, then drain after recovery.
- Commit followed by a lost acknowledgment: safe retry without an extra row.
- Restart with pending rows; kill collector before and after commit.
- Full outbox and permanent failures: visible rejection/quarantine counters,
  bounded resources, and later valid traffic can progress.

M3/M4
- Verify deterministic demo routes and actual request timing.
- Bound HTTP bodies before parsing and paginate queries.
- Handle partial/malformed/oversized serial frames and reconnection; valid
  traffic after a rejected frame still works and firmware IDs stay unchanged.

Run from the project root once real tests are added:
  python -m pytest tests/ -q

Today pytest would report no tests collected; that is not a passing test suite.
See docs/experiments.txt for process-failure and measurement procedures.
