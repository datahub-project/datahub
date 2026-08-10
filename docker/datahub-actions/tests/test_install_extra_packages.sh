#!/bin/bash

# Tests for docker/datahub-actions/install_extra_packages.sh.
#
# These run without Docker: uv is stubbed on PATH so the script's decision can be
# observed directly. Run with:
#
#     bash docker/datahub-actions/tests/test_install_extra_packages.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNDER_TEST="${SCRIPT_DIR}/../install_extra_packages.sh"

failures=0

pass() { echo "ok   - $1"; }
fail() {
  echo "FAIL - $1"
  echo "       $2"
  failures=$((failures + 1))
}

# A PATH holding only a uv stub that records its arguments.
make_stub_path() {
  local dir="$1"
  mkdir -p "$dir/bin"
  cat >"$dir/bin/uv" <<EOF
#!/bin/bash
echo "\$@" >>"$dir/uv-calls"
EOF
  chmod +x "$dir/bin/uv"
  echo "$dir/bin:/usr/bin:/bin"
}

# A PATH with no uv at all, standing in for the locked image. Deliberately holds
# nothing else either, so the refusal cannot depend on an external command.
make_bare_path() {
  local dir="$1"
  mkdir -p "$dir/bin"
  echo "$dir/bin"
}

# Absolute path to this shell, so tests can run with a PATH that has no bash on it.
BASH_BIN="${BASH:-/bin/bash}"

test_unset_is_a_no_op() {
  local tmp
  tmp="$(mktemp -d)"
  local path
  path="$(make_stub_path "$tmp")"

  if ! env -u ACTIONS_EXTRA_PACKAGES PATH="$path" bash "$UNDER_TEST" >/dev/null 2>&1; then
    fail "unset ACTIONS_EXTRA_PACKAGES exits 0" "script exited non-zero"
    return
  fi
  if [ -f "$tmp/uv-calls" ]; then
    fail "unset ACTIONS_EXTRA_PACKAGES installs nothing" "uv was called: $(cat "$tmp/uv-calls")"
    return
  fi
  pass "unset ACTIONS_EXTRA_PACKAGES is a no-op"
}

test_empty_is_a_no_op() {
  local tmp
  tmp="$(mktemp -d)"
  local path
  path="$(make_stub_path "$tmp")"

  if ! ACTIONS_EXTRA_PACKAGES="" PATH="$path" bash "$UNDER_TEST" >/dev/null 2>&1; then
    fail "empty ACTIONS_EXTRA_PACKAGES exits 0" "script exited non-zero"
    return
  fi
  if [ -f "$tmp/uv-calls" ]; then
    fail "empty ACTIONS_EXTRA_PACKAGES installs nothing" "uv was called: $(cat "$tmp/uv-calls")"
    return
  fi
  pass "empty ACTIONS_EXTRA_PACKAGES is a no-op"
}

test_packages_are_installed() {
  local tmp
  tmp="$(mktemp -d)"
  local path
  path="$(make_stub_path "$tmp")"

  if ! ACTIONS_EXTRA_PACKAGES="acryl-datahub-actions[slack] my-action==1.2.3" \
    PATH="$path" bash "$UNDER_TEST" >/dev/null 2>&1; then
    fail "packages are installed" "script exited non-zero"
    return
  fi
  if [ ! -f "$tmp/uv-calls" ]; then
    fail "packages are installed" "uv was never called"
    return
  fi

  local call
  call="$(cat "$tmp/uv-calls")"
  local expected="pip install acryl-datahub-actions[slack] my-action==1.2.3"
  if [ "$call" != "$expected" ]; then
    fail "every requested package reaches uv" "expected [$expected], got [$call]"
    return
  fi
  pass "every requested package reaches uv, in order"
}

test_no_package_manager_fails_loudly() {
  local tmp
  tmp="$(mktemp -d)"
  local path
  path="$(make_bare_path "$tmp")"

  local output
  output="$(ACTIONS_EXTRA_PACKAGES="my-action==1.2.3" PATH="$path" \
    "$BASH_BIN" "$UNDER_TEST" 2>&1)"
  local status=$?

  if [ "$status" -eq 0 ]; then
    fail "a locked image refuses rather than ignores" "script exited 0"
    return
  fi
  case "$output" in
  *ACTIONS_EXTRA_PACKAGES*) ;;
  *)
    fail "the error names the variable" "stderr was: $output"
    return
    ;;
  esac
  pass "no package manager: refuses with an explanation instead of ignoring"
}

test_install_failure_is_not_swallowed() {
  local tmp
  tmp="$(mktemp -d)"
  mkdir -p "$tmp/bin"
  printf '#!/bin/bash\nexit 3\n' >"$tmp/bin/uv"
  chmod +x "$tmp/bin/uv"

  if ACTIONS_EXTRA_PACKAGES="does-not-exist" PATH="$tmp/bin:/usr/bin:/bin" \
    bash "$UNDER_TEST" >/dev/null 2>&1; then
    fail "a failed install exits non-zero" "script exited 0"
    return
  fi
  pass "a failed install exits non-zero"
}

# The original defect was not in this script, it was that nothing on the runtime
# startup path ran anything like it. Assert the wiring, not just the behaviour.
test_startup_script_runs_the_installer() {
  local start_sh="${SCRIPT_DIR}/../start.sh"

  if ! grep -q "install_extra_packages" "$start_sh"; then
    fail "start.sh runs the installer" \
      "docker/profiles/docker-compose.actions.yml passes ACTIONS_EXTRA_PACKAGES to the
       actions services, but start.sh never reaches install_extra_packages.sh, so the
       variable would silently do nothing"
    return
  fi
  pass "start.sh runs the installer"
}

test_startup_script_runs_the_installer
test_unset_is_a_no_op
test_empty_is_a_no_op
test_packages_are_installed
test_no_package_manager_fails_loudly
test_install_failure_is_not_swallowed

if [ "$failures" -ne 0 ]; then
  echo "${failures} test(s) failed"
  exit 1
fi
echo "all tests passed"
