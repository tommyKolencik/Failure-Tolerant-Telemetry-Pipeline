"""Step 3A learning guide: durably receive a complete batch or reject it.

Implement storage here first, then connect it to HTTP in collector/app.py.
All guidance below is comments; you provide the implementation.
"""

# WHY THIS FILE EXISTS
# This is the collector's permanent event store, distinct from the agent outbox.
# The outbox may resend after a lost response. This store makes identical retries
# safe while exposing a reused event key containing different data.

# TODO 3A-1: Define a small synchronous CollectorStore interface.
# Suggested operations: initialize(), insert_batch(events), check_ready().
# Configure a path; data/collector.sqlite3 is the initial local default.
# Keep this module independent of FastAPI: return results or raise domain errors.
# Suggested ConflictError carries the offending event key, not a whole payload.
# A batch result contains inserted and duplicate counts.
# Later operations can provide bounded queries, stats, and retention.


# TODO 3A-2: Initialize storage with the same connection policy as the outbox.
# One collector process, one file; open/use/close each connection on its worker.
# Configure WAL, synchronous=FULL, and a bounded busy timeout before writes.
# Reuse the explicit transaction strategy you chose in agent/outbox.py. If you
# chose autocommit=True, use SQL COMMIT/ROLLBACK rather than connection methods.
# Suggested events table columns:
#   source_id TEXT, boot_id TEXT, seq INTEGER: NOT NULL composite PRIMARY KEY.
#   kind TEXT: for later filtering.
#   event_json TEXT, content_hash TEXT: exact canonical source content/hash.
#   received_at_utc TEXT: collector's host-clock receipt time.
# Add indexes later to support bounded source/kind queries without full scans.
# Durable inserted/duplicate counters can live in a separate metadata table.
# Do not confuse a pre-commit timestamp stored in a row with the exact moment
# COMMIT completed. Capture completion separately when adding measurements.


# TODO 3A-3: Prepare the ENTIRE batch before opening its write transaction.
# Validate every event with shared.schema, then compute each key/content/hash.
# Reject an invalid member before inserting anything. Enforce the count limit
# here too, so direct calls cannot bypass the HTTP wrapper's event cap.
# Keep host receipt metadata separate; retries must compare source content only.
# Decide/document whether an empty batch is rejected (the schema guide suggests
# rejecting it). Precompute outside the write lock to keep transactions short.


# TODO 3A-4: Insert atomically and classify duplicates/conflicts.
# BEGIN IMMEDIATE; start local inserted=0 and duplicates=0 counters.
# For each prepared event, SELECT by (source_id, boot_id, seq):
# - Missing key: INSERT it; increment inserted.
# - Same canonical content and hash: increment duplicates, do not insert again.
# - Same key, different content: raise ConflictError and ROLLBACK the WHOLE batch.
# Your reads must see earlier inserts in this same transaction, so identical
# repeated keys within one batch count as duplicates and differing ones conflict.
# Use parameterized SQL. Avoid INSERT OR REPLACE: it overwrites original events.
# Avoid plain INSERT OR IGNORE: it would silently hide content conflicts.
# Update persistent counters in the same transaction, then COMMIT once.
# Return counts only AFTER COMMIT succeeds. If commit fails, report an error.
# Roll back any active transaction on exceptions, close the connection in finally,
# and never let an error handler accidentally return a successful batch result.


# TODO 3A-5: Implement check_ready() and plan finite retention.
# Check access to the initialized events table with a tiny bounded read, using
# the configured database; don't silently create a new blank DB during a check.
# Storage errors must reach the API so /readyz can return a non-success status.
# After the first correctness checkpoint, add a configurable retention limit
# for events and diagnostics. Document how pruning affects historical queries
# and deduplication; retain events throughout your reconciliation experiments.
# Future query methods should paginate and obey shared.config's row limits.


# CHECKPOINT BEFORE THE HTTP APP
# Insert [A, B]: result inserted=2, duplicates=0. Insert again: 0 and 2.
# Insert [new C, changed A]: conflict; C must be absent and original A unchanged.
# In one batch, [D, D] should store one row; [new E, changed E] must store neither.
# Close and reopen the store, then compare exact keys and payloads.
# Trigger a storage error: it should raise, not report accepted counts.
# These are cases for you to check; no storage code or tests exist yet.

# References:
# https://www.sqlite.org/lang_transaction.html
# https://docs.python.org/3.12/library/sqlite3.html#transaction-control
