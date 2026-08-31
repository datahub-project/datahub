#!/usr/bin/env bash
# Capture logs from Docker containers for CI debugging.
# Optional: set COMPOSE_PROJECT_NAME to only capture containers in that project.
# Optional: set TARGET_DIR (default docker_logs); logs written as TARGET_DIR/<sanitized_name>.log

set -e

TARGET_DIR="${TARGET_DIR:=docker_logs}"
TEST_STRATEGY="${TEST_STRATEGY:=}"

# Sanitize a container name for use as a filename (replace invalid chars with _)
sanitize() {
  echo "$1" | sed 's/[^a-zA-Z0-9_.-]/_/g'
}

mkdir -p "$TARGET_DIR"

if [ -n "${COMPOSE_PROJECT_NAME:-}" ]; then
  # Only containers whose name starts with the project prefix (e.g. datahub_)
  prefix="${COMPOSE_PROJECT_NAME}_"
  names=$(docker ps -a --format '{{.Names}}' | grep -E "^${prefix}" || true)
else
  names=$(docker ps -a --format '{{.Names}}')
fi

for name in $names; do
  [ -z "$name" ] && continue
  safe=$(sanitize "$name")
  docker logs "$name" >& "${TARGET_DIR}/${safe}${TEST_STRATEGY}.log" || true
done
