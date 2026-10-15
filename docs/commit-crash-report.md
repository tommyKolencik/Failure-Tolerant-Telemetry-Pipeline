# Collector crashes around COMMIT

All four native HTTP/process cases passed. **222 newly accepted outbox events
plus 20 setup rows** reconciled to **242 exact final rows**, with unchanged
request replay and zero pending/quarantine.

| Kill boundary | Batch | Rows after reopening | Retry inserts | Retry duplicates | Final rows |
| --- | --- | --- | --- | --- | --- |
| Before COMMIT | 1 | 5 | 1 | 0 | 11 |
| After COMMIT | 1 | 6 | 0 | 1 | 11 |
| Before COMMIT | 100 | 5 | 100 | 0 | 110 |
| After COMMIT | 100 | 105 | 0 | 100 | 110 |

Each isolated case starts with five collector rows, then publishes its test
batch and five later events. Without an ACK, all remain pending. Before COMMIT,
the batch disappears on reopen; after COMMIT, it survives and retries as
duplicates. Later events also deliver. Checks compare exact keys/content,
counters, and request bytes.

The faulting child intercepts the real COMMIT and self-SIGKILLs at the boundary;
startup/read operations are excluded. Its witness can see uncommitted rows.
Recovery evidence comes from a fresh connection after death. Restart uses the
normal collector on the same database/port. Implementation:
[crash walkthrough](failure-overhead-walkthrough.md).

```bash
python3 -m benchmarks.crash_checks
```

[Curated evidence](commit-crash-verification-mac.json) and
[raw artifacts/source](../benchmarks/results/commit-crashes-mac/) preserve the
historical measured version. Raw generated files are ignored by Git.
SQLite WAL/FULL process recovery on this Mac does not establish power-cut,
OS/storage fault, or Dell behavior.
