"""Step 2 learning guide: keep accepted events until delivery is acknowledged.

Write your code below the TODOs after completing shared/schema.py.
This module should use SQLite and the shared contract; it needs no network code.
"""
import sqlite3


# WHY THIS FILE EXISTS
# A Python list disappears when the agent exits. The outbox is the durable
# waiting room: an event is accepted only after its insertion commits to disk.
# The eventual uploader reads here and asks for deletion only after an ACK.

# TODO 2A: Define the interface before writing SQL.
# Suggested Outbox methods: initialize(), publish(raw_event), peek_batch(limit),
# acknowledge(keys), and stats(). Constructor input: database path and limits.
# Keep the database path configurable; use data/outbox.sqlite3 for local work.
# Store a path on the object rather than one connection shared across threads.
# Decide the publish result fields: accepted/status/reason are useful. Distinguish
# a new insert, identical pending duplicate, invalid event, conflict, capacity
# rejection, and unavailable storage. A failed commit must never mean accepted.
database = initialize()


# TODO 2B: Write a connection helper and initialize the tables.
# Tools: sqlite3, pathlib.Path, and the shared schema/config helpers.
# Create the parent directory when initializing. Open/close a connection within
# each operation; its entire use stays on the worker thread that opened it.
# Set WAL mode, synchronous=FULL, and an explicit finite busy timeout.
# For explicit transactions on Python 3.12, you can use autocommit=True and
# issue SQL BEGIN IMMEDIATE / COMMIT / ROLLBACK yourself. With that flag,
# connection.commit()/rollback() do nothing; do not mix the two approaches.
# Run PRAGMAs before beginning a transaction. Check setup errors visibly.
# Suggested pending table columns:
#   source_id TEXT, boot_id TEXT, seq INTEGER: composite PRIMARY KEY, NOT NULL.
#   event_json TEXT, content_hash TEXT: canonical event and hash, NOT NULL.
#   event_bytes INTEGER: UTF-8 canonical byte length, NOT NULL.
#   received_at_utc TEXT: host receipt timestamp, independent of source uptime.
# Add durable counters for capacity rejection and later quarantine outcomes.
# Use SQL placeholders for values rather than inserting values with f-strings.


# TODO 2C: Implement publish(raw_event), with an atomic capacity check.
# 1. Validate, canonicalize, and hash using shared.schema before the write lock.
# 2. Open a connection and BEGIN IMMEDIATE, which serializes competing writers.
# 3. Look up the composite key. Compare hash AND canonical content if present.
#    Identical pending event: return a duplicate result, without adding bytes.
#    Different content: explicit conflict, preserving the original row.
# 4. Read the current pending byte total inside this transaction. Starting with
#    SUM(event_bytes) is simple; optimize with a transactional counter later.
# 5. If total + new event bytes exceeds OUTBOX_MAX_PENDING_BYTES, reject newest.
#    Persist a rejection counter without inserting the event; commit that counter
#    before reporting the capacity rejection. A rollback would lose the counter.
# 6. Otherwise insert, COMMIT, and only then return accepted.
# On database/commit errors, roll back any active transaction and close the
# connection. Report failure. Keep validation/serialization outside the lock.
# Never check capacity before BEGIN: two producers could both see free space.


# TODO 2D: Implement peek_batch(limit), without deleting anything.
# Return at most HTTP_BATCH_MAX_EVENTS pending events and their exact keys.
# Choose a deterministic order, for example receipt time followed by the key.
# Decode the saved canonical event; do not give it a new boot ID or sequence.
# Peeking must leave every row available after a process restart.
# The future uploader must also cap the full serialized HTTP wrapper at 128 KiB;
# event count alone does not guarantee that a request fits. An event too large
# for any request needs a visible permanent rejection/quarantine policy later.


# TODO 2E: Implement acknowledge(keys) as its own write transaction.
# Delete only explicitly acknowledged keys, using parameterized SQL.
# COMMIT the deletion; if a transaction fails, retain pending rows for a retry.
# Acknowledging the same keys twice should be harmless.
# Do not acknowledge when merely selecting a batch, starting HTTP, or timing out.
# Crash after collector commit but before deletion: resend the original events.
# The collector's duplicate handling is what makes that resend safe.


# TODO 2F: Implement bounded diagnostics and stats.
# At first report pending count/bytes and durable capacity rejection count.
# Distinguish the 16 MiB logical serialized backlog from physical DB/WAL size.
# Before uploader integration, add bounded quarantine storage and counters so a
# permanently invalid batch can leave the pending queue without vanishing.
# Choose/document finite diagnostic/quarantine retention; avoid logging payloads
# into an unlimited file. Never print an acceptance message before COMMIT.


# CHECKPOINT BEFORE STEP 3
# Publish a few valid events into a temporary DB, close it, and reopen it.
# Verify the exact saved keys/content remain. Peek twice: nothing should vanish.
# Publish a duplicate: pending count/bytes stay unchanged. Change its payload:
# reject as a conflict and preserve the original content.
# Use a small configurable capacity in a check to reach full quickly; verify
# newest is rejected and the earlier accepted rows are still recoverable.
# Acknowledge one key: only that row disappears. Repeat: no error or extra delete.
# No database/network implementation or tests have been added for you.

# References:
# https://docs.python.org/3.12/library/sqlite3.html#transaction-control
# https://www.sqlite.org/lang_transaction.html
