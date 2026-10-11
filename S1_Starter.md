# S1 Starter: Failure-Tolerant Telemetry Pipeline

Status: planning / not implemented. Owner: Tommy Kolencik. Created: 2026-10-11.

## Goal and first milestone

Build a small observability pipeline that collects application, host, and ESP32 telemetry, recovers from collector outages, and produces reproducible performance and correctness results.

**Start with a deterministic synthetic producer, a persistent host outbox, and one collector endpoint.** Hardware integration and a dashboard follow after that path works.

The resume value will come from batching, durability boundaries, deduplication, bounded resources, failure recovery, and measured tradeoffs.

## Scope and stack

Use Python, FastAPI, Pydantic, SQLite, pytest, psutil, and pyserial. Use your Dell Linux node as the eventual integration host; development can begin locally. Record and pin versions after checking the available environment.

| Build this week | Defer |
| --- | --- |
| Synthetic events, host metrics, a small demo API, and H1 serial ingestion | Kubernetes, Kafka, multiple storage services |
| Durable outbox, HTTP batching, retry/backoff, collector deduplication | Replication, clustering, universal exactly-once claims |
| Health/stats/query endpoints and a small visual demo if time permits | A polished React dashboard, authentication product, elaborate alerts |

Use one collector process and one database file initially. Keep serial reception independent of network uploads. A blocked HTTP request must not stop the serial reader.

## Architecture and boundaries

~~~mermaid
flowchart TD
  P["App and host producers"] --> O["Host SQLite outbox"]
  H["H1 serial adapter"] --> O
  O --> U["Batch uploader"]
  U --> C["Collector"]
  C --> D["Collector SQLite events"]
  D --> Q["Stats and queries"]
~~~

1. A source event becomes **accepted locally** only after its outbox transaction commits.
2. The uploader retains its batch until the collector acknowledges it.
3. The collector responds successfully only after committing every event in the batch.
4. The agent deletes acknowledged rows in a separate outbox transaction.
5. If either process crashes between steps, the agent may resend. A database uniqueness constraint and content comparison make those retries safe.

The tested guarantee is at-least-once transmission with one stored row per event key for locally accepted events, assuming the disks remain available and the finite outbox has capacity. Firmware/USB loss before local acceptance is outside that guarantee and must be reported separately.

Use WAL mode and synchronous=FULL on both databases, with short transactions and bounded busy timeouts. Give each thread/process its own connection. Avoid blocking serial or database calls directly inside async request handlers.

## Shared S1/H1 event contract

Use UTF-8 newline-delimited JSON (NDJSON) on the device/host link. Each line contains one event:

~~~json
{
  "schema_version": 1,
  "source_id": "nano-esp32-01",
  "boot_id": "6c2fd9a088364f158ad86285e097da41",
  "seq": 42,
  "kind": "sensor.temperature",
  "source_uptime_us": 10500000,
  "payload": {
    "temperature_c": 21.875,
    "sensor_ok": true
  }
}
~~~

| Field | Contract |
| --- | --- |
| schema_version | Integer; initially 1. Reject unsupported versions. |
| source_id | Stable source name, 1-64 ASCII letters/digits/underscore/hyphen. |
| boot_id | Fresh 32-character lowercase hexadecimal session ID on every source restart. |
| seq | Nonnegative integer, unique and increasing within a source/session; below 2^63. |
| kind | One supported event kind; initially sensor.temperature, button.action, system.status, host.metrics, or http.request. |
| source_uptime_us | Nonnegative elapsed microseconds on the source; below 2^63. |
| payload | Object validated according to kind; no NaN or infinity. |

The deduplication key is **(source_id, boot_id, seq)**. Retries preserve the entire event and key. Out-of-order arrival is allowed. Reuse of a key with different content is an explicit conflict, not a silently accepted duplicate.

The serial line limit is **1,024 bytes including its newline**. On an oversized line, discard through the next newline and count the rejection. HTTP batches contain at most 100 events and 128 KiB of request body.

For sensor failures, emit sensor.temperature with temperature_c=null, sensor_ok=false, and a short error code. A failed read must never appear as a valid 0-degree measurement.

Suggested payloads: button.action carries action plus detected_us and applied_us; system.status carries queue depths, drop/error counters, and reset reason; host.metrics carries CPU percentage and memory use; http.request carries a normalized route, method, status, and duration_ms.

Device uptime and host wall time are different clocks. Record host receipt/commit timestamps separately. Do not subtract a device uptime from a host timestamp to claim latency.

## Proposed interfaces and data rules

| Interface | Required behavior |
| --- | --- |
| publish(event) | Validate, then commit to outbox or explicitly reject with a reason. |
| POST /v1/events/batch | Body: {"events":[...]}. Validate the complete batch before insertion. Return inserted and duplicate counts after commit. |
| GET /healthz | Lightweight process health. |
| GET /readyz | Check database access; return a non-success status if storage is unavailable. |
| GET /v1/stats | Counts, backlog, last event times, and selected application latency/error summaries. |
| GET /v1/events | Bounded query by source/kind, with pagination; default 100 and maximum 1,000 rows. |

Use the event tuple as a UNIQUE or PRIMARY KEY in both stores. Store a normalized content hash to distinguish an identical retry from conflicting content. An invalid batch or a conflicting key returns an error without partially inserting that batch.

Enforce the HTTP byte limit while reading the body, before JSON parsing. Limit stats to a documented recent window/row cap. Keep parsing buffers, diagnostic logs, and query results bounded as well as the outbox.

Start with batches of 100 events and a maximum flush wait of 250 ms. Retry connection failures, timeouts, and transient server errors with capped exponential backoff and jitter. A permanent validation/conflict error must produce a visible diagnostic; isolate or quarantine the rejected batch so later events can progress. Count quarantined events explicitly.

Limit pending serialized event data to 16 MiB initially. Enforce the limit atomically with insertion, reject newest arrivals when full, and expose the rejection counter. This is a logical backlog cap: separately measure database/WAL disk use and process RAM. Add finite retention for collector data and diagnostic/quarantine records; record the chosen limits.

## Milestones

- [ ] M0: Record Python/OS versions and agree on the event contract and limits.
- [ ] M1: Send 1,000 synthetic events through outbox -> uploader -> collector; compare exact event keys and payloads.
- [ ] M2: Implement commit-before-ACK, duplicate/conflict handling, retry, and restart recovery.
- [ ] M3: Add host CPU/RAM metrics and actual request timing from a tiny demo API. Give error/slow endpoints deterministic behavior.
- [ ] M4: Parse H1 NDJSON without changing firmware event identities; handle partial lines, reconnects, and malformed frames.
- [ ] M5: Run failure experiments, rate sweeps, and batching comparisons.
- [ ] M6: Write a reproducible README, benchmark report, and 60-90 second demo.

Keep the first implementation synchronous where practical. Introduce concurrency deliberately when independent acquisition and uploading require it. A shared publish() library can let the demo API write its local outbox without inventing another network service.

## Tests that matter

| Experiment | Evidence required |
| --- | --- |
| Send the same event repeatedly | One stored row; duplicate counter increases. |
| Same key, different payload | Explicit conflict; original data unchanged. |
| Collector down for 30 seconds | Local backlog grows; accepted events arrive after restart and drain. |
| Collector commits but its ACK is lost | Retry produces no duplicate stored row. |
| Kill/restart agent with pending rows | Pending accepted keys survive and upload resumes. |
| Kill collector before/after commit | Replayed keys reconcile correctly in both cases. |
| Full outbox | Memory stays controlled; rejection is visible; accepted rows remain recoverable. |
| Invalid batch / oversized body / long serial line | Bounded parsing and explicit rejection; later valid traffic still works. |

After each run, reconcile sets of event keys and payloads. Row counts alone can hide a missing event replaced by a duplicate.

## Measurement plan

Separate a short physical-device run from synthetic load testing. Do not describe synthetic events as real customers or devices.

| Measurement | Method |
| --- | --- |
| Sustained throughput | Start at 100 offered events/sec, then increase. Run each rate for 60 seconds, repeat three times, and inspect backlog growth as well as achieved rate. |
| Delivery delay | For the same-host synthetic benchmark, correlate publish and durable-ACK monotonic timestamps by event key. Report p50/p95/p99. |
| HTTP latency | Measure batch request-to-ACK separately from time waiting in the outbox. |
| Recovery | Time from collector readiness to complete draining of a specified backlog. |
| Completeness | Accepted, rejected/quarantined, stored, missing, and conflicting key sets after drain. |
| Overhead | CPU, RSS, disk use, and demo-API latency with instrumentation enabled versus disabled. |
| Batching tradeoff | Compare batch sizes 1, 20, and 100 on the same machine/workload. |

Save raw run data plus configuration, seed, event count/payload size, versions, hardware, and commit ID. Label single-host results and account for load-generator contention. Use plotnine if creating plots.

## Suggested project files

| Path | Responsibility |
| --- | --- |
| shared/schema.py | Contract validation and canonical event identity/content |
| agent/outbox.py | Durable enqueue, selection, acknowledgment, and capacity |
| agent/uploader.py | HTTP transport and retry policy |
| agent/sources.py | Synthetic, host, and serial adapters |
| collector/app.py | Ingest, health, stats, and bounded queries |
| collector/store.py | Atomic insertion, deduplication, conflicts, retention |
| demo/app.py | Small instrumented application |
| tests/ | Correctness and crash-boundary experiments |
| benchmarks/ | Repeatable load generator and raw results |
| docs/ | Design, experiments, limitations, and demo instructions |

These are proposed paths; this starter file does not imply that the code exists.

## Shared seven-day sequence

| Day | Focus |
| --- | --- |
| 1 | Shared contract; S1 synthetic path; capture H1 hardware/firmware baseline |
| 2 | S1 durability, duplicates, conflicts, and retries |
| 3 | S1 application/host metrics and first benchmark |
| 4 | H1 task separation and protected I2C access |
| 5 | H1 bounded telemetry, failures, timing, and watchdog test |
| 6 | Connect H1 to S1; run combined failure experiments |
| 7 | Repeat measurements, fix remaining failures, document, record demo |

## Completion and continuation

Finished means clean-start reproduction, passing failure experiments, measured results, and an honest limitations section. Performance thresholds are to be established from results.

When continuing: implement M0/M1 first and explain each component. Use the existing branch or main; do not create/push branches automatically. Preserve existing projection-mapping firmware as a baseline. The two finalized resumes stay unchanged until measured project results justify an update.

Possible future resume evidence: "Built a telemetry pipeline processing [measured rate] events/sec; recovered [measured backlog] accepted events after [specified outage], with [measured delay/resource use]." Replace brackets only with verified results.

## Primary references

- [Datadog Summer SWE internship](https://careers.datadoghq.com/detail/8052118/)
- [Datadog engineering: data completeness](https://www.datadoghq.com/blog/engineering/data-pipeline-completeness/)
- [SQLite synchronization and durability](https://www.sqlite.org/pragma.html#pragma_synchronous)
- [FastAPI concurrency guidance](https://fastapi.tiangolo.com/async/)

Companion plan: H1_Starter.md.

