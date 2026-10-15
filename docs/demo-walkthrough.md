# Isolated local demo

Use four terminals in the project root with the environment from
[README](../README.txt). Ports 8010/8011 must be free. This demonstrates
functional buffering/recovery; measured performance is in the reports.

Terminal 1 creates an isolated data folder and starts one collector worker:

```bash
export TELEMETRY_DEMO_DIR="$(mktemp -d "$PWD/data/demo-XXXXXX")"
printf '%s\n' "$TELEMETRY_DEMO_DIR"
python3 -c 'import os; from pathlib import Path; import uvicorn; from collector.app import create_app; uvicorn.run(create_app(Path(os.environ["TELEMETRY_DEMO_DIR"]) / "collector.sqlite3"), host="127.0.0.1", port=8010)'
```

In terminals 2 and 3, substitute the printed path:

```bash
export TELEMETRY_DEMO_DIR='/absolute/path/printed/by/terminal-1'
```

Terminal 2:

```bash
python3 -m agent.main --source host --outbox "$TELEMETRY_DEMO_DIR/outbox.sqlite3" --collector-url http://127.0.0.1:8010 --report-interval 1
```

Terminal 3:

```bash
python3 -m demo.app --port 8011 --outbox "$TELEMETRY_DEMO_DIR/outbox.sqlite3"
```

Terminal 4:

```bash
curl -fsS http://127.0.0.1:8010/readyz
curl -fsS http://127.0.0.1:8011/readyz
curl -sS -i http://127.0.0.1:8011/normal
curl -sS -i http://127.0.0.1:8011/slow
curl -sS -i http://127.0.0.1:8011/error
curl -fsS 'http://127.0.0.1:8010/v1/events?kind=http.request&limit=10'
curl -fsS 'http://127.0.0.1:8010/v1/events?kind=host.metrics&limit=5'
```

Expect 200/200/500; slow waits 100 ms. Allow recording/upload before querying.
Request duration ends at final-body send; the first CPU reading is null.

Stop only the collector with Ctrl+C in terminal 1 for 30 seconds. Keep
acquisition running, call /normal again, and inspect the growing backlog:

```bash
curl -sS http://127.0.0.1:8011/normal
curl -fsS http://127.0.0.1:8011/telemetry
```

Restart the collector with the same `python3 -c` command and existing directory.
Watch pending rows drain; fresh host events can keep a small count in flight.
Acceptance requires outbox COMMIT, collector ACK requires COMMIT, and retries
preserve identities/content. Live row counts alone do not prove completeness.

Stop demo first, then agent, then collector. Check the agent's final report for
zero pending/new quarantine/rejections; drained Ctrl+C exits 130. Keep the data
folder for inspection.

Automated exact-content counterpart:
`python3 scripts/verify-agent.py --with-metrics --outage-seconds 30`.
Hardware integration remains pending; the servo is excluded.
