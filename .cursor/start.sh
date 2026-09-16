#!/usr/bin/env bash
# Cloud Agent start script for DataHub.
#
# Runs on every boot. This VM has no systemd, so the Docker daemon must be
# started manually before `scripts/dev/datahub-dev.sh start` (docker compose)
# can work. It also applies a nested-Docker networking fix.
set -euo pipefail

# br_netfilter exposes the bridge-nf-call sysctls; load it before we try to
# tune them (it may not be loaded until the first bridge is created).
sudo modprobe br_netfilter 2>/dev/null || true

# Start dockerd if it isn't already running.
if ! sudo docker info >/dev/null 2>&1; then
  sudo bash -c 'nohup dockerd --storage-driver=overlay2 >/var/log/dockerd.log 2>&1 &'
  for _ in $(seq 1 60); do
    if sudo docker info >/dev/null 2>&1; then break; fi
    sleep 1
  done
fi

# Allow non-root `docker` (and datahub-dev.sh) to use the socket this boot.
sudo chmod 666 /var/run/docker.sock 2>/dev/null || true

# Nested-Docker fix: in this VM, bridged container-to-container packets are sent
# through iptables (conflicting legacy + nft tables) and get dropped, so
# services on the DataHub compose network can't reach each other (e.g. the
# system-update job fails waiting on opensearch:9200). Disabling bridge-nf lets
# same-bridge traffic flow at L2 while leaving egress/NAT (via the FORWARD
# chain) intact.
sudo sysctl -w net.bridge.bridge-nf-call-iptables=0 2>/dev/null || true
sudo sysctl -w net.bridge.bridge-nf-call-ip6tables=0 2>/dev/null || true

if sudo docker version >/dev/null 2>&1; then
  echo "Docker daemon is ready."
else
  echo "ERROR: Docker daemon failed to start (see /var/log/dockerd.log)." >&2
  exit 1
fi
