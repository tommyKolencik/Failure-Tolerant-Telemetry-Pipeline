"""Step 3B learning guide: expose the collector store through a small HTTP API.

Complete collector/store.py first. Write the application beneath these TODOs.
This file contains no FastAPI instance or routes yet.
"""

# WHY THIS FILE EXISTS
# An HTTP response is the agent's signal that a batch reached durable storage.
# The API validates the incoming request and calls the store. It must not report
# success while the database operation is still running or after it has failed.

# TODO 3B-1: Create the FastAPI application and its storage lifecycle.
# Tools to look up: FastAPI, Request, HTTPException, Pydantic ValidationError,
# contextlib.asynccontextmanager, and starlette.concurrency.run_in_threadpool.
# Import BatchRequest from shared.schema and CollectorStore/ConflictError from
# collector.store. Read byte/count limits from shared.config.
# Define an app factory that accepts a database path; expose app for Uvicorn.
# The default may use data/collector.sqlite3. A factory makes temporary DBs easy
# to use later in tests without touching your development data.
# Initialize the store during FastAPI lifespan startup and fail visibly if that
# fails. Run synchronous store work through the thread pool, including startup.
# Each store operation must open/use/close its own connection on that worker.
# Save the configured store on application state for the request handlers.


# TODO 3B-2: Add GET /healthz and GET /readyz.
# /healthz: a lightweight process-health response; no large queries.
# /readyz: await the synchronous store.check_ready() in the thread pool.
# Return 200 on successful access and 503 if storage is unavailable.
# These answer different questions: alive versus able to access its event store.


# TODO 3B-3: Add POST /v1/events/batch with a bounded raw-body reader.
# Use an async route receiving Request directly so you can limit the raw body
# BEFORE JSON parsing/model validation. Automatic body-model parsing happens
# too early for this particular byte-limit requirement.
# Check that the media type is application/json (allow its charset parameter).
# A Content-Length above the cap can be rejected early, but never trust it as
# the only check: streamed/chunked requests may omit it or declare it wrongly.
# Iterate request.stream(), count bytes, and reject with 413 immediately when
# the total exceeds HTTP_BODY_MAX_BYTES. Append only within-limit chunks.
# Do not call request.body()/json() first or continue accumulating after the cap.
# Streaming limits what this handler retains; the server controls chunk sizes.


# TODO 3B-4: Decode and validate the complete bounded request.
# Require UTF-8 JSON, reject malformed input with 400, and validate the wrapper
# using BatchRequest. Translate schema failures to 422 and the event cap is 100.
# JSON parsers can permit NaN/infinity extensions: reject them when decoding
# (json.loads has a parse_constant hook), as well as in payload validation.
# The body must be an object with an events list, not a bare list.
# Reject an invalid member before calling insert_batch; no partial acceptance.
# Return bounded diagnostics (for example a count and the first few errors),
# without echoing whole payloads. A raw Request bypasses automatic validation,
# so implement these checks and document the request/response shape yourself.


# TODO 3B-5: Await storage, then return acknowledgment or an explicit error.
# Call store.insert_batch(validated_events) through run_in_threadpool and await
# completion. Only then return 200 with {"inserted": n, "duplicates": m}.
# For success, inserted + duplicates should equal the number of submitted events.
# Map ConflictError to 409; map transient database availability failures to 503.
# Malformed/invalid/oversized input is a permanent rejection, not a retry signal.
# No background insertion after returning success: a crash could lose that data.
# Keep unexpected failures visible as failures; log concise bounded diagnostics.
# No database or serial call should block this async handler's event-loop thread.


# TODO 3B-6: Try the API locally once your app is implemented.
# From the project root, run one collector worker:
#   python3 -m uvicorn collector.app:app --host 127.0.0.1 --port 8000
# Create a JSON request with {"events": [the starter's sample event]}.
# POST it using curl or HTTPX: first response 1 inserted, then 1 duplicate.
# Change its payload at the same key: expect 409 and unchanged stored content.
# Try [new event, conflicting event]: expect 409 with neither partial insert.
# Try malformed JSON, 101 events, and a body above 128 KiB: each should fail;
# a later valid request should still work. Restart and resend: still a duplicate.
# These checks exercise step 3 directly; uploader/source code comes afterward.


# AFTER THIS CHECKPOINT
# Add /v1/events with pagination (default 100, max 1,000) and source/kind filters.
# Add /v1/stats with a documented recent window and row cap. Collector counts
# describe stored/received data; agent outbox backlog belongs to agent diagnostics
# unless explicitly reported to the collector. Keep all responses bounded.
# Then move to synthetic sources/uploader and the 1,000-event reconciliation.

# References:
# https://fastapi.tiangolo.com/advanced/using-request-directly/
# https://fastapi.tiangolo.com/advanced/events/
# https://www.starlette.dev/requests/
# https://www.starlette.dev/threadpool/
