#!/usr/bin/env bash
# Dependency waits for DataHub container entrypoints (replaces dockerize).
# shellcheck disable=SC1091
# Usage: source this file, call datahub_wait_begin once, then datahub_wait_* as needed.

: "${DATAHUB_WAIT_TIMEOUT_SECONDS:=240}"

datahub_wait_begin() {
  export DATAHUB_WAIT_DEADLINE=$(( $(date +%s) + DATAHUB_WAIT_TIMEOUT_SECONDS ))
}

_datahub_wait_expired() {
  (( $(date +%s) >= DATAHUB_WAIT_DEADLINE ))
}

datahub_wait_tcp() {
  local raw="$1"
  local spec="${raw#tcp://}"
  local host="${spec%%:*}"
  local port="${spec##*:}"
  echo "[datahub-wait] TCP ${host}:${port}"
  while ! _datahub_wait_expired; do
    if timeout 1 bash -c "echo >/dev/tcp/${host}/${port}" 2>/dev/null; then
      echo "[datahub-wait] TCP ${host}:${port} ready"
      return 0
    fi
    sleep 1
  done
  echo "[datahub-wait] Timeout waiting for TCP ${host}:${port}" >&2
  exit 1
}

# Optional second argument: custom Authorization or other -H value (single header).
datahub_wait_http() {
  local url="$1"
  local hdr="${2-}"
  echo "[datahub-wait] HTTP GET ${url}"
  while ! _datahub_wait_expired; do
    if [[ -n "$hdr" ]]; then
      if curl -sf -m 5 -H "$hdr" "$url" >/dev/null; then
        echo "[datahub-wait] HTTP ready"
        return 0
      fi
    else
      if curl -sf -m 5 "$url" >/dev/null; then
        echo "[datahub-wait] HTTP ready"
        return 0
      fi
    fi
    sleep 1
  done
  echo "[datahub-wait] Timeout waiting for HTTP ${url}" >&2
  exit 1
}

# Neo4j / Schema Registry: http(s) URL uses GET; bare host:port uses TCP.
datahub_wait_endpoint() {
  local ep="$1"
  if [[ "$ep" == http://* || "$ep" == https://* ]]; then
    datahub_wait_http "$ep" ""
    return
  fi
  local t="${ep#tcp://}"
  datahub_wait_tcp "tcp://${t}"
}
