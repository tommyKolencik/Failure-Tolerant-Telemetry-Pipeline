# Failure-Tolerant Telemetry Pipeline design

Owner: Tommy Kolencik. Original plan: 2026-10-11.
Current commands/status: [README](README.txt). Evidence: [experiments](docs/experiments.txt).

Collect application, host, and ESP32 telemetry with bounded resources,
outage recovery, and reproducible correctness/performance results.
Stack: Python, FastAPI, Pydantic, SQLite, psutil, pyserial, pytest, plotnine.
Use one collector worker; keep acquisition independent of uploading.

## Durability

1. Validate and commit source content to the local outbox.
2. Upload unchanged events; retain rows until a complete collector ACK.
3. Commit the entire collector batch before replying.
4. Delete acknowledged outbox rows in a separate transaction.

WAL and synchronous=FULL apply to both databases. Use per-operation connections,
explicit transactions, and bounded busy timeouts. Keep blocking work off async
event loops. At-least-once transmission covers accepted events within available
storage/capacity; firmware/USB loss before acceptance is outside it. Retention
forgets pruned keys, so deduplication protects retained content only.

## Event contract

UTF-8 NDJSON on serial; one event per line:

```json
{
  "schema_version": 1,
  "source_id": "nano-esp32-01",
  "boot_id": "6c2fd9a088364f158ad86285e097da41",
  "seq": 42,
  "kind": "sensor.temperature",
  "source_uptime_us": 10500000,
  "payload": {"temperature_c": 21.875, "sensor_ok": true}
}
```

Identity: `(source_id, boot_id, seq)`. Source IDs use 1..64 ASCII
letters/digits/underscore/hyphen; boot IDs use 32 lowercase hex characters.
Sequence and uptime are nonnegative integers below 2^63. Fresh source sessions
start new boot IDs. Arrival may be out of order; same-key changed content is a
conflict. Retry preserves all content. Device uptime is separate from host time.

Payloads live in [shared/schema.py](shared/schema.py). Failed temperature reads
use null temperature, sensor_ok=false, and a nonblank error code. Host CPU uses
an explicit sample interval; request events use normalized routes.
Button/status fields await the firmware contract; H1_Starter.md is missing.

Limits live in [shared/config.py](shared/config.py): serial 1,024 bytes including
LF, HTTP 100 events/128 KiB, pending 16 MiB, partial flush wait 250 ms.
Enforce limits atomically or before parsing. Capacity rejects newest arrivals;
permanent failures are quarantined and counted. Logical limits require separate
RAM/physical DB/WAL measurements.

## Verification and measurements

Compare complete event keys and canonical content after delivery, replay,
lost ACKs, outages, agent restart, collector kills around COMMIT, capacity
rejections, and malformed/oversized input. Atomic batch failures preserve prior
state. Keep reconciliation workloads within retention.

Synthetic comparisons use 60-second trials, three repeats, and batch caps
1/20/100. Report achieved rates, queue growth, durable publish-to-ACK delay,
HTTP ACK latency, exact-backlog recovery, CPU/RSS/disk, and matched demo
instrumentation overhead. Save raw data, configuration, versions, and exact
measured source. Label same-host contention and simulated input.

Software milestones are implemented: synthetic delivery, durability/recovery,
host/demo metrics, bounded serial parsing, failure experiments, and Mac rate
comparisons. Physical firmware/USB integration and Dell reproduction remain.
Commands, limits, and results are linked from the README.

## Scope and continuation

Preserve projection-mapping firmware as a baseline. The servo is nonoperational
and excluded. Do not create/push branches automatically. Keep finalized resumes
unchanged until measured results justify an update.

Defer clustering, replication, Kafka/Kubernetes, polished dashboards, and
authentication product work. Completion requires clean-start reproduction,
failure evidence, measured results, and explicit remaining limits.

References: [SQLite synchronization](https://www.sqlite.org/pragma.html#pragma_synchronous),
[FastAPI concurrency](https://fastapi.tiangolo.com/async/).

