Correctness checks
==================

  python3 -m pytest tests/ -q

All databases are temporary; tests do not modify data/ or open physical USB.
The suite covers:

test_collector.py        Atomic batches, replay/conflicts, failed commits,
                         bounded HTTP bodies, pagination, retention, migrations,
                         stats, and exact 1,000-event reconciliation.
test_outbox.py           Capacity, quarantine moves/eviction, durable counters,
                         competing writers, migrations, and commit rollback.
test_uploader.py         Exact retries, lost/invalid/oversized ACKs, body packing,
                         isolation, blocked configuration, backoff, and flush.
test_sources.py          Seeded generation, identities, bounds, lazy events.
test_main.py             Independent uploads, restart/drain, ownership, reports,
                         worker failures, and SIGINT/SIGTERM.
test_metrics.py          Strict unions, host CPU semantics, concurrent request
                         identities, ASGI timing, mixed delivery, and stats.
test_benchmarks.py       Durable timing, exact outcome sets, recovery targets,
                         bounded configuration, and side-effect-free dry runs.
test_failure_overhead.py Real child SIGKILL at COMMIT, paired workload semantics,
                         content/ACK audits, and disabled-mode zero telemetry.
test_serial.py           Framing/memory bounds, invalid input, reconnect/discards,
                         firmware identity, cleanup, and stalled-HTTP independence.

Native HTTP/CLI/pseudo-terminal checks are separate:
  python3 scripts/verify-agent.py --with-metrics --outage-seconds 30
  python3 scripts/verify-serial.py
  python3 -m benchmarks.crash_checks

Historical evidence and performance commands: ../docs/experiments.txt.
Dell retest and physical firmware/USB/sensor checks remain pending.
