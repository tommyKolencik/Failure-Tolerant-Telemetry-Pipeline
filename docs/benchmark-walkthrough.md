# Benchmark measurements

Read `Settings` → `Ledger` → measurement wrappers → `OwnedCollector` →
`run_trial` → `reconcile` in [benchmarks/run.py](../benchmarks/run.py).

Each trial owns fresh databases, a source session, and a collector/localhost
port. Producer, uploader, and resource monitor run independently. Defaults are
100 offered events/sec, 60 seconds, three repeats, batch caps 1/20/100.
The ledger is bounded to 100,000 offered events/trial and 100 trials/matrix;
these experiment limits are separate from outbox capacity.

| Measurement | Boundary |
| --- | --- |
| Publish-to-ACK delay | Before publish → successful local deletion COMMIT |
| HTTP ACK latency | Transport start → complete validated collector ACK |
| Post-return delay | Publish return → local ACK; ordering races remain explicit |
| Throughput | Actual offers/acceptances/local ACKs inside the production window |
| Recovery | Collector readiness → last ACK for the exact captured backlog keys |
| CPU | Process CPU time; one core = 100%, including drain |
| RSS/disk/backlog peaks | Sampled every 250 ms by default |

All event timing uses one parent host monotonic clock. A failed deletion is
not a local ACK; an invalid/lost response is not a successful HTTP latency
sample. COMMIT can become visible before publish returns. If recovery ACKs
precede the readiness probe, the remaining recovery delay is zero.

The producer skips missed ticks. Compare requested and achieved rates, then
accepted and acknowledged rates plus backlog curves. Eventual reconciliation
does not establish sustained throughput. The batch cap does not guarantee full
batches. Summary p95 comparisons use medians of per-trial percentiles, not pooled
events; rates use means across trials.

Reconciliation compares full canonical content for every accepted key, local
ACKs, collector rows, pending rows, and quarantine. Rejection, leftover rows,
missing/extra/changed content, or worker failure prevents success.

Runs save ledgers, attempts, samples, stored content, outcome sets, databases,
logs, metadata, and exact source copies. See [experiment commands](experiments.txt).
Measured reports: [100 target](benchmark-report.md), [200/500 targets](benchmark-rate-sweep-report.md).

Limits: same-host contention, uncontrolled background activity, no excluded
warmup, sampled peaks, and reused-parent allocator/cache effects. Parent
resources include generation, observation, polling, and uploading; they do not
measure standalone production-agent overhead. Rates run in configured order;
batch order rotates within repeats. Synthetic Mac results do not establish
Dell/Arduino performance or indefinite queue stability.

References: [monotonic time](https://docs.python.org/3.12/library/time.html#time.monotonic_ns),
[process resources](https://psutil.io/api/#psutil.Process.memory_info).
