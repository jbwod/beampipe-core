#!/bin/bash
# Setup Podman engine-id for Dozzle

set -e

echo "Podman engine-id thing"

MACHINE_NAME="${PODMAN_MACHINE_NAME:-podman-machine-default}"

echo "machine: ${MACHINE_NAME}"
podman machine ssh "${MACHINE_NAME}" -- sh -lc '
  set -e
  if command -v uuidgen >/dev/null 2>&1; then
    UUID="$(uuidgen)"
  else
    UUID="$(cat /proc/sys/kernel/random/uuid)"
  fi
  sudo mkdir -p /var/lib/docker
  echo "${UUID}" | sudo tee /var/lib/docker/engine-id >/dev/null
  echo "engine-id set to: ${UUID}"
  echo "engine-id file: $(sudo cat /var/lib/docker/engine-id)"
'
