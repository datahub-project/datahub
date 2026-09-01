#!/usr/bin/env bash
# Regression tests for docker_logs.sh project-name filtering.
# Run: .github/scripts/test_docker_logs.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNDER_TEST="${SCRIPT_DIR}/docker_logs.sh"

STUB_DIR="$(mktemp -d)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${STUB_DIR}" "${WORK_DIR}"' EXIT

PASS=0
FAIL=0

assert_file() {
  local path="$1"
  if [[ -f "$path" ]]; then
    PASS=$((PASS + 1))
  else
    echo "FAIL: expected file missing: $path"
    FAIL=$((FAIL + 1))
  fi
}

assert_no_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    PASS=$((PASS + 1))
  else
    echo "FAIL: unexpected file present: $path"
    FAIL=$((FAIL + 1))
  fi
}

# Stub docker: ps lists fixed names; logs writes the container name as content.
cat >"${STUB_DIR}/docker" <<'STUB'
#!/usr/bin/env bash
if [[ "$1" == "ps" ]]; then
  cat <<'EOF'
datahub-system-update-quickstart-1
datahub-mysql-1
datahub_legacy_underscore_1
otherproject-mysql-1
EOF
  exit 0
fi
if [[ "$1" == "logs" ]]; then
  echo "logs-for-$2"
  exit 0
fi
echo "unexpected docker invocation: $*" >&2
exit 1
STUB
chmod +x "${STUB_DIR}/docker"
export PATH="${STUB_DIR}:${PATH}"

echo "== hyphenated Compose v2 names are captured =="
rm -rf "${WORK_DIR}/hyphen"
TARGET_DIR="${WORK_DIR}/hyphen" COMPOSE_PROJECT_NAME=datahub bash "${UNDER_TEST}"
assert_file "${WORK_DIR}/hyphen/datahub-system-update-quickstart-1.log"
assert_file "${WORK_DIR}/hyphen/datahub-mysql-1.log"
assert_file "${WORK_DIR}/hyphen/datahub_legacy_underscore_1.log"
assert_no_file "${WORK_DIR}/hyphen/otherproject-mysql-1.log"

echo "== without COMPOSE_PROJECT_NAME, all containers are captured =="
rm -rf "${WORK_DIR}/all"
TARGET_DIR="${WORK_DIR}/all" bash "${UNDER_TEST}"
assert_file "${WORK_DIR}/all/datahub-mysql-1.log"
assert_file "${WORK_DIR}/all/otherproject-mysql-1.log"

echo
echo "Passed: ${PASS}  Failed: ${FAIL}"
[[ "${FAIL}" -eq 0 ]]
