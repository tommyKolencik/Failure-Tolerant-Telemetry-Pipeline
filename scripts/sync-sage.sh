#!/usr/bin/env bash
# Sync local source, including uncommitted edits, to the Dell checkout.
set -euo pipefail

task_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
task_remote_dir='/home/scaredbucket/Failure-Tolerant-Telemetry-Pipeline'
task_ssh_options=(
  -o BatchMode=yes
  -o ConnectTimeout=10
  -o StrictHostKeyChecking=yes
  -o UpdateHostKeys=no
)

# Require matching repository origins.
task_local_origin="$(git -C "$task_root" remote get-url origin)"
task_remote_origin="$(ssh "${task_ssh_options[@]}" sage \
  "git -C '$task_remote_dir' remote get-url origin")"
if [[ "$task_local_origin" != "$task_remote_origin" ]]; then
  printf '%s\n' 'Git origins differ; check the target checkout before syncing.' >&2
  exit 1
fi

# Preserve remote Git, environments, credentials, and runtime data; never delete.
# Pass through rsync options such as --dry-run.
rsync -azv --itemize-changes \
  -e 'ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes -o UpdateHostKeys=no' \
  --exclude='.git/' \
  --exclude='.venv/' \
  --exclude='venv/' \
  --exclude='.codex/' \
  --exclude='.agents/' \
  --exclude='.aws/' \
  --exclude='__pycache__/' \
  --exclude='.pytest_cache/' \
  --include='.env.example' \
  --exclude='.env' \
  --exclude='.env.*' \
  --exclude='*.sqlite*' \
  --exclude='*.db*' \
  --exclude='*.log' \
  --exclude='*.agent.lock' \
  --exclude='.DS_Store' \
  --exclude='.coverage' \
  --exclude='htmlcov/' \
  --exclude='data/*' \
  --exclude='benchmarks/results/*' \
  "$@" "$task_root/" "sage:$task_remote_dir/"
