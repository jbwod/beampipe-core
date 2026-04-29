#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${SLURM_SSH_KNOWN_HOSTS_SYNC_SRC:-${HOME}/.ssh/known_hosts}"
DST="${ROOT}/deploy/ssh/known_hosts"
if [[ ! -f "$SRC" ]]; then
  echo "Missing source file: $SRC" >&2
  echo "Set SLURM_SSH_KNOWN_HOSTS_SYNC_SRC or create ~/.ssh/known_hosts (e.g. ssh setonix once)." >&2
  exit 1
fi
mkdir -p "$(dirname "$DST")"
cp "$SRC" "$DST"
chmod 644 "$DST"
echo "Synced $SRC -> $DST (644)"
