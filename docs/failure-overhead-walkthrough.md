# Crash and overhead checks

Read [crash_checks.py](../benchmarks/crash_checks.py):

- `CommitGate` intercepts the real COMMIT, records a witness, and sends SIGKILL
  immediately before or after it. Production applications do not import it.
- `FaultingStore` delegates transaction control to the production store.
  A `ContextVar` arms only the ingest worker and resets on success/failure.
- `run_case` checks recovered rows through a fresh connection after death.
  The dying connection's view of uncommitted rows is only a boundary witness.
- The uploader retries its cached bytes against the normal restarted collector.
  Before-COMMIT crashes require inserts; after-COMMIT crashes require duplicate
  ACKs. Both still require local deletion COMMIT.

References: [ContextVar](https://docs.python.org/3.12/library/contextvars.html),
[SQLite WAL](https://www.sqlite.org/wal.html).
Results: [crash report](commit-crash-report.md). Process kills do not test power loss.

Read [demo_overhead.py](../benchmarks/demo_overhead.py):

- `request_plan`/`load_requests` match count, route mix, and concurrency.
  Each client waits for the complete body before its next request: a closed-loop
  workload with measured achieved throughput.
- Client timing ends at body receipt; demo timing ends at ASGI final-body send.
  Both can precede telemetry COMMIT, so the runner waits for counters and drain.
- `AuditedOutbox`/`assess_events` compare canonical content, identities, local
  ACKs, route/method/status semantics, and disabled-mode zero telemetry.
- `paired_results` subtracts disabled from enabled only for complete matched
  trials. CPU includes settling/drain; parent auditing and same-host contention
  limit interpretation.

Reference: [HTTPX clients](https://www.python-httpx.org/advanced/clients/).
Results: [overhead report](demo-overhead-report.md).
Commands: [experiments.txt](experiments.txt).
