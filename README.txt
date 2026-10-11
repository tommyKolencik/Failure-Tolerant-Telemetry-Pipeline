Failure-Tolerant Telemetry Pipeline
==================================

Collect application, host, and ESP32 telemetry, preserve locally accepted
events during collector outages, and measure recovery and performance.

Status: directory scaffold. Python modules contain implementation notes, except
shared/config.py, which defines initial limits. No agent, collector, demo API,
tests, or benchmarks run yet. S1_Starter.md is the detailed project plan.

Planned flow
------------
Synthetic / host / serial sources -> host SQLite outbox -> HTTP batch uploader
                                 -> collector -> collector SQLite -> queries

An event is accepted locally only when its outbox transaction commits. The
collector acknowledges a batch only after committing every event. The agent
then deletes acknowledged rows in a separate transaction. Replays use the same
(source_id, boot_id, seq) key and unchanged content: identical retries count as
duplicates; different content at the same key is an explicit conflict.

This aims for at-least-once transmission with one stored row per event key for
locally accepted events, assuming available disks and outbox capacity. It does
not cover firmware/USB loss before acceptance. Correctness is not measured yet.

Set up the development environment
---------------------------------
Target Python 3.12; the inspected Mac has Python 3.12.10. See
docs/environment.txt for the baseline. Run these commands from this directory
on macOS/Linux:

  python3 -m venv .venv
  source .venv/bin/activate
  python -m pip install -r requirements.txt
  python -m pip check

requirements.txt includes FastAPI, Uvicorn, Pydantic, HTTPX, psutil, pyserial,
pytest, and plotnine. SQLite is included with Python and needs no pip package.
Direct versions are pinned; a complete transitive lock is still a later step.
Using a virtual environment keeps the project dependencies together.
The local pip dry run could not resolve the first package; installation is
unverified. See docs/environment.txt for the attempted check and its result.

The setup commands prepare dependencies only. Runtime commands will be added
when agent/main.py, collector/app.py, and demo/app.py are implemented. See
tests/README.txt for the future test command and planned coverage.

Directory and file guide
-----------------------
Every new file is described below. S1_Starter.md already existed.

.
|-- S1_Starter.md             Original design, contract, and milestones.
|-- README.txt               Setup, project status, file guide, and next steps.
|-- requirements.txt         Pinned runtime, testing, and plotting dependencies.
|-- .gitignore               Excludes environments, caches, databases, and logs.
|-- shared/
|   |-- __init__.py          Marks the shared Python package.
|   |-- config.py            Central initial event, batch, and capacity limits.
|   `-- schema.py            Placeholder for validation, event keys, and hashes.
|-- agent/
|   |-- __init__.py          Marks the host-agent Python package.
|   |-- main.py              Placeholder for agent startup and worker lifecycle.
|   |-- outbox.py            Placeholder for durable local buffering and ACKs.
|   |-- uploader.py          Placeholder for HTTP batching, retry, and quarantine.
|   `-- sources.py           Placeholder for synthetic, host, and serial inputs.
|-- collector/
|   |-- __init__.py          Marks the collector Python package.
|   |-- app.py               Placeholder for ingest, health, stats, and queries.
|   `-- store.py             Placeholder for atomic storage, dedup, and retention.
|-- demo/
|   |-- __init__.py          Marks the demo Python package.
|   `-- app.py               Placeholder for timed normal, slow, and error routes.
|-- tests/
|   `-- README.txt           Prioritized correctness tests and future invocation.
|-- benchmarks/
|   |-- run.py               Placeholder for repeatable load and recovery runs.
|   `-- results/
|       `-- .gitkeep          Keeps the directory for generated run artifacts.
|-- data/
|   `-- .gitkeep              Keeps the directory for local runtime databases.
`-- docs/
    |-- environment.txt      Recorded Python/OS/SQLite baseline and package pins.
    `-- experiments.txt      Failure procedures, metrics, and remaining choices.

Use data/outbox.sqlite3 and data/collector.sqlite3 as the initial local database
paths when implementing storage. data/ contents are runtime artifacts.
benchmarks/results/ will hold raw run data and plots. Generated contents are
ignored by default; deliberately retain selected evidence for final reporting.

Next steps, in order
-------------------
1. M0: finish the event/payload contract and remaining finite limits. The local
   environment baseline is recorded; check the Dell Linux node before testing
   there. Install dependencies and capture resolved versions for reproducibility.

2. M1: implement shared/schema.py, agent/outbox.py, collector/store.py,
   collector/app.py, agent/uploader.py, and the synthetic path in
   agent/sources.py plus agent/main.py. Send 1,000 seeded events and compare
   exact keys and payloads after the outbox drains.

3. M2: prove commit-before-ACK, atomic batch rejection, duplicate/conflict
   handling, retries, restart recovery, and capacity enforcement. Run the outage
   and lost-ACK cases in docs/experiments.txt before expanding the inputs.

4. M3/M4: add host CPU/RAM and actual demo request timing, then H1 serial
   ingestion. Keep reception independent of uploading; preserve firmware event
   identities. H1_Starter.md is referenced by the original plan but was not
   present in this directory when the scaffold was created.

5. M5: run failure experiments and rate sweeps; compare batch sizes 1, 20, and
   100. Save raw evidence, resource use, and exact completeness results.

6. M6: add clean-start runtime commands, a benchmark report, measured
   limitations, and a 60-90 second demo. Consider a small visual demo after
   correctness is established.

Keep one collector process and one collector database initially. Use SQLite WAL
and synchronous=FULL on both stores. Bound HTTP reads, serial buffers, queries,
logs, quarantine, and retained collector data as well as pending events.

Initial limits from the starter
-------------------------------
- Schema version 1; supported kinds are sensor.temperature, button.action,
  system.status, host.metrics, and http.request.
- Serial NDJSON: at most 1,024 bytes per line, including newline.
- HTTP: at most 100 events and 128 KiB per request body.
- Batch flush: at most 250 ms wait.
- Outbox: at most 16 MiB of pending serialized event data; reject newest when full.
- Event queries: default 100 rows, maximum 1,000.

These values exist in shared/config.py but their enforcement is not implemented.
Collector retention, diagnostics/quarantine retention, stats caps, retry policy,
and busy timeouts still need explicit choices. Record them before experiments.
