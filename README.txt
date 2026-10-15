Failure-Tolerant Telemetry Pipeline
==================================

Building a Python telemetry pipeline with strict Pydantic event validation,
a persistent SQLite outbox, an independent HTTP uploader, and a SQLite HTTP
collector. The local implementation supports temperature, host metrics, and
HTTP request events. It includes canonical serialization and event identity,
capacity enforcement, duplicate/conflict handling, batch reads, retries, and
acknowledgment-based deletion.

Verification on 2026-10-15 (macOS arm64, Python 3.12.10): all 294 automated
tests passed. A real localhost HTTP experiment recovered all 2,897 queued
events after a 30-second collector outage and agent SIGKILL, reconciling exact
accepted keys and content with zero pending or quarantined events. Recovery
from collector readiness took 0.390 seconds, including drain-agent startup,
in one observation. See docs/verification-2026-10-15.md for reproduction and
scope. Physical sensor integration and Dell retesting remain pending.

Sources -> outbox COMMIT -> HTTP batch -> collector COMMIT -> ACK -> local deletion

Implemented: strict event schemas, bounded storage/parsing, retries, quarantine,
queries/stats, host sampling, request instrumentation, serial reconnection, and
repeatable failure/performance checks. Physical Arduino integration is pending.

Setup
-----

Use Python 3.12+ from the project root (macOS/Linux):

  python3 -m venv .venv
  source .venv/bin/activate
  python -m pip install -r requirements.txt
  python -m pip check
  python -m pytest tests/ -q

Direct dependencies are pinned in requirements.txt. Environment records:
docs/environment.txt. A portable transitive lock remains to add.

Run each service in a separate terminal:

  python -m uvicorn collector.app:app --host 127.0.0.1 --port 8000
  python -m agent.main --source host
  python -m demo.app --port 8001

The host agent uploads its own samples and the demo's shared outbox. Stop the
demo before stopping the agent so acquisition ends before the final drain.

  curl http://127.0.0.1:8001/normal
  curl http://127.0.0.1:8001/slow
  curl http://127.0.0.1:8001/error
  curl 'http://127.0.0.1:8000/v1/events?kind=http.request&limit=10'
  curl http://127.0.0.1:8000/v1/stats
  curl http://127.0.0.1:8001/telemetry

Routes return 200, 200 after 100 ms, and 500 respectively.
/items/{item_id} demonstrates normalized route labels. Health/diagnostic routes
are excluded from recording. --no-instrumentation disables telemetry writes.

Other agent modes
-----------------

Finite synthetic run; omit --count for continuous production:

  python -m agent.main --count 1000 --rate 100 --seed 7

Drain accepted work after a restart, using the SAME outbox path:

  python -m agent.main --count 0 --drain-timeout 30

Preview synthetic NDJSON without storage or networking:

  python -m agent.sources --count 5 --seed 7

Defaults: data/outbox.sqlite3, collector http://127.0.0.1:8000, synthetic 100/sec,
host 1/sec, drain wait 10 s, aggregate NDJSON reports every 5 s.
--count counts offered events, including local rejections; --count 0 drains only.
Timed sources skip missed ticks. Synthetic uptime advances simulated 10,000-us
ticks; host uptime uses a monotonic clock. Run --help for configuration options.

One agent owns each outbox via an advisory process lock. Leave its .agent.lock
file in place; the OS releases the lock on exit. Uploads run on a separate
worker. Slow local storage can still delay acquisition.

Exit codes: 0 = drained finite run; 1 = rejection, new quarantine, pending work,
or runtime failure; 2 = invalid configuration; 130/143 = drained SIGINT/SIGTERM.
Drain timeout bounds the wait, not the final in-flight HTTP operation.

Serial input
------------

After confirming compatible firmware and baud, use a separate outbox alongside
the host/demo agent:

  python -m agent.main --source serial --serial-port /path/to/device --baud 115200 --outbox data/serial-outbox.sqlite3

Firmware owns source_id, boot_id, seq, and uptime. Serial rejects --source-id
and --rate. Each read is at most 256 bytes; lines are at most 1,024 bytes
including LF. Oversized lines discard through the next LF. Every port open
discards through its first LF for alignment, which may drop one unaccepted
valid event. Inspect serial discard/error counters even when exit is zero.

Read timeout defaults to 0.1 s; reconnect delay to 1 s. Reception closes before
draining. Recover without opening serial:

  python -m agent.main --outbox data/serial-outbox.sqlite3 --count 0 --drain-timeout 30

The Dell's USB path and sync commands are in docs/deployment.txt. Baud 115200
is provisional. Physical firmware/sensor/reset behavior remains unverified.
The nonoperational servo is excluded. See docs/serial-walkthrough.md.

Contracts and limits
--------------------

Schemas accept sensor.temperature, host.metrics, and http.request. Strict types,
finite numbers, bounded fields, and cross-field checks reject malformed events.
Button/status payloads await the firmware contract in shared/schema.py.

Identity is (source_id, boot_id, seq). Identical retained content is a duplicate;
changed content at the same key is a conflict. Batches validate and commit
atomically. Local acceptance requires outbox COMMIT; deletion requires a
validated collector ACK. This supports at-least-once transmission within
available capacity/storage. Firmware/USB loss before acceptance is outside it.

Defaults in shared/config.py:
- Pending outbox: 16 MiB canonical content; reject newest when full.
- HTTP batch: 100 events / 128 KiB; partial flush wait at most 250 ms.
- ACK: 4 KiB, HTTP 200, strict counts covering the complete batch.
- Retry: equal-jitter exponential backoff, initially 125..250 ms, capped at 10 s.
- HTTP timeout: connect 2 s; read/write/pool 5 s per operation.
- Quarantine: 1,000 records / 1 MiB; old failures may be evicted and counted.
- Collector retention: 100,000 rows; pruning forgets content AND dedup identity.
- Stats: latest 5-minute receipt window, capped at 10,000 retained events.

Logical limits do not cap physical DB/WAL size. Permanent 400/409/413/422
rejections are isolated into singleton attempts and quarantined. Endpoint/auth
errors and redirects block while preserving pending rows. Automatic requeue
and Retry-After handling remain unimplemented.

Collector API
-------------

GET /healthz, /readyz       Process health; database readiness.
POST /v1/events/batch      {"events": [...]} -> {"inserted": n, "duplicates": m}.
GET /v1/events             Filters: source_id, kind; limit 100, maximum 1,000.
                          Continue with next_cursor and the same filters.
GET /v1/stats              Lifetime counters, retention, capped recent sample.
GET /docs                 Interactive API documentation.

Batch errors: 400 JSON, 409 conflict, 413 size, 415 media type, 422 schema,
503 storage. Query pages are live reads. Collector stats exclude agent backlog.
Request percentiles summarize ASGI response spans through final-body send;
they exclude the later publish and client TCP receipt. Host CPU is null during
warmup/short intervals; memory fields are total/available physical bytes.

Verification and results
------------------------

  python scripts/verify-agent.py --with-metrics --outage-seconds 30
  python scripts/verify-serial.py
  python -m benchmarks.crash_checks
  python -m benchmarks.run --dry-run
  python -m benchmarks.run --duration 60 --repeats 3 --rates 100 --plots
  python -m benchmarks.demo_overhead --requests 6000 --concurrency 4 --repeats 3 --plots

Helpers use isolated data and owned processes. Benchmark outputs preserve raw
events/timing, databases, configuration, and measured source copies under ignored
benchmarks/results/. Existing output directories are refused.

Results and reproduction commands: docs/experiments.txt. Mac tests pass;
serial checks use simulated pseudo-terminals. Measured reports describe their
archived versions, not every later edit. Dell sync/retest and physical firmware
integration remain. The 500/sec synthetic target was not achieved.

File guide
----------

shared/       Schemas and default limits.
agent/        Sources, outbox, uploader, lifecycle, serial framing.
collector/    HTTP API and atomic storage.
demo/         Instrumented application.
tests/        Correctness checks; coverage in tests/README.txt.
benchmarks/   Load, crash, overhead experiments and generated artifacts.
scripts/      Dell sync and native functional checks.
docs/         Deployment, concise reading guides, reports, curated evidence.
S1_Starter.md Design reference.

Learning guides: docs/metrics-walkthrough.md, docs/serial-walkthrough.md,
docs/benchmark-walkthrough.md, docs/failure-overhead-walkthrough.md.
Isolated live demonstration: docs/demo-walkthrough.md.
