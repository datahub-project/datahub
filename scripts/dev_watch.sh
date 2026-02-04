#!/usr/bin/env bash
set -euo pipefail

DATAHUB_ROOT="/Users/jchiu/workspace/datahub"
WEB_DIR="${DATAHUB_ROOT}/datahub-web-react"
LOG_DIR="/tmp"

NODE18_BIN="/opt/homebrew/opt/node@18/bin"

PORT_FORWARD_LOG="${LOG_DIR}/portforward_datahub_9002_pod.log"
VITE_LOG="${LOG_DIR}/vite_3000.log"

cleanup() {
  if [[ -n "${PF_PID:-}" ]]; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${VITE_PID:-}" ]]; then
    kill "${VITE_PID}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT INT TERM

start_port_forward() {
  while true; do
    kubectl -n datapipeline port-forward pod/datahub-7b6548946b-j588n 9002:9002 >> "${PORT_FORWARD_LOG}" 2>&1
    sleep 2
  done
}

start_vite() {
  while true; do
    (cd "${WEB_DIR}" && PATH="${NODE18_BIN}:${PATH}" yarn start) >> "${VITE_LOG}" 2>&1
    sleep 2
  done
}

start_port_forward &
PF_PID=$!

start_vite &
VITE_PID=$!

wait "${PF_PID}" "${VITE_PID}"
