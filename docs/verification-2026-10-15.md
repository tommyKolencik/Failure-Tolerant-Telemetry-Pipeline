# Current-source verification — 2026-10-15

All **294 automated tests passed**. A separate real-HTTP functional experiment
recovered **all 2,897 queued events** after a **30-second collector outage and
agent SIGKILL**. Complete accepted keys and canonical event content reconciled;
the final collector contained 3,920 expected events, with zero pending or
quarantined events.

Recovery from collector readiness took **0.390 seconds**, including drain-agent
startup. This is one functional-run observation, not a general latency guarantee.
The check also verified normal delivery, competing-agent refusal, graceful
SIGTERM draining, host metrics, and instrumented HTTP-request events.

## Reproduce

From the project root, install `requirements.txt`, then run:

```sh
python3 -m pytest tests/ -q
python3 scripts/verify-agent.py --with-metrics --outage-seconds 30 --output /tmp/agent-verification.json
```

The suite passed in 6.44 seconds with one upstream TestClient dependency
deprecation warning. Test databases are temporary. The functional helper owns
its agent, demo, and collector processes, uses isolated temporary databases, and
records exact source fingerprints. It requires permission to bind localhost
HTTP listeners. Stop neither unrelated services nor physical serial devices.

## Environment and provenance

- macOS 26.6.2, arm64; Python 3.12.10; SQLite 3.49.1.
- FastAPI 0.143.0, Pydantic 2.14.0, HTTPX 0.28.1, Uvicorn 0.53.0.
- Separate local processes communicating over real localhost HTTP.
- [Raw result and measured source fingerprints](agent-verification-2026-10-15.json).
  These fingerprints were checked against the source immediately before commit.
- The result's base commit is the pre-publication checkout; the per-file
  SHA-256 values identify the measured implementation, including uncommitted
  changes present during the run.

## Scope

This establishes delivery and agent-process restart behavior for the observed
same-host workload. It does not establish power-cut/storage-fault durability,
physical USB/sensor behavior, sustained throughput, or capacity on another host.
The separate collector commit-boundary crash tests are included in the automated
suite; earlier load, commit-crash, serial pseudo-terminal, and overhead results
remain archived experiments in [the experiment index](experiments.txt).

Physical sensor integration and a current Dell retest remain pending. Neither
is required to reproduce this software-only check.
