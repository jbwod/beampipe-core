#!/usr/bin/env bash
set -euo pipefail

: "${SSH_AUTH_SOCK:?Mac ssh-agent not running (set SSH_AUTH_SOCK)}"

FORWARDED="${BEAMPIPE_SSH_VM_FORWARDED_SOCK:-/tmp/beampipe-ssh-agent.sock}"
RELAY="${BEAMPIPE_SSH_VM_BRIDGE_SOCK:-/tmp/beampipe-agent-relay.sock}"

exec podman machine ssh -- \
  -R "${FORWARDED}:${SSH_AUTH_SOCK}" \
  "rm -f ${RELAY} && exec /usr/bin/socat UNIX-LISTEN:${RELAY},mode=666,fork UNIX-CONNECT:${FORWARDED}"
