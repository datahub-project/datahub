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

# One scratch root for the whole suite, removed however the script ends.
#
# Each case used to call `mktemp -d` directly and never clean up. That was survivable while
# the suite was run by hand, but it is now wired into `check`, so every CI invocation would
# leave nine directories behind in /tmp. Allocating under a single root means one trap
# collects all of them, including on an early exit or an interrupt.
TEST_TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEST_TMP_ROOT"' EXIT INT TERM

new_tmp() {
  mktemp -d "${TEST_TMP_ROOT}/case.XXXXXX"
}

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
  tmp="$(new_tmp)"
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
  tmp="$(new_tmp)"
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
  tmp="$(new_tmp)"
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
  tmp="$(new_tmp)"
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
  tmp="$(new_tmp)"
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
# startup path ran anything like it. Assert the wiring by executing it.
#
# An earlier version of this test grepped start.sh for the string "install_extra_packages".
# That token also appears in the fallback default path assignment, so the assertion passed
# whether or not the invocation still existed: it could not fail for the regression it was
# written to guard.
#
# This runs start.sh for real with a stubbed installer and a GMS endpoint that is guaranteed
# to be unreachable, with a one second timeout. start.sh installs, then waits for GMS, then
# exits non-zero. So the marker file existing proves two things at once: the installer was
# actually invoked, and it was invoked before the GMS wait. Move the block after the wait and
# the marker disappears, because the script exits at the wait.
test_startup_script_runs_the_installer() {
  local start_sh="${SCRIPT_DIR}/../start.sh"
  local tmp
  tmp="$(new_tmp)"
  local marker="$tmp/installer-ran"

  printf '#!/bin/bash\ntouch "%s"\nexit 0\n' "$marker" >"$tmp/stub-installer.sh"
  chmod +x "$tmp/stub-installer.sh"

  # Port 1 on the loopback address refuses immediately, so the wait fails fast rather than
  # depending on DNS or a network timeout.
  DATAHUB_ACTIONS_INSTALL_EXTRA_PACKAGES_PATH="$tmp/stub-installer.sh" \
    DATAHUB_GMS_HOST="127.0.0.1" \
    DATAHUB_GMS_PORT="1" \
    DATAHUB_GMS_STARTUP_TIMEOUT_SEC="1" \
    ACTIONS_EXTRA_PACKAGES="my-action==1.2.3" \
    bash "$start_sh" >/dev/null 2>&1
  local status=$?

  if [ ! -f "$marker" ]; then
    fail "start.sh actually invokes the installer, before waiting for GMS" \
      "docker/profiles/docker-compose.actions.yml passes ACTIONS_EXTRA_PACKAGES to the
       actions services, but running start.sh never executed the installer, so the
       variable would silently do nothing"
    return
  fi
  if [ "$status" -eq 0 ]; then
    fail "start.sh actually invokes the installer, before waiting for GMS" \
      "start.sh exited 0 with an unreachable GMS, so this test is not exercising the path
       it claims to"
    return
  fi
  pass "start.sh invokes the installer, and does so before the GMS wait"
}

# start.sh must refuse rather than continue when the installer itself fails. Otherwise a
# missing dependency surfaces much later as a confusing action failure.
test_startup_script_aborts_when_the_installer_fails() {
  local start_sh="${SCRIPT_DIR}/../start.sh"
  local tmp
  tmp="$(new_tmp)"

  printf '#!/bin/bash\nexit 7\n' >"$tmp/failing-installer.sh"
  chmod +x "$tmp/failing-installer.sh"

  local output
  output="$(DATAHUB_ACTIONS_INSTALL_EXTRA_PACKAGES_PATH="$tmp/failing-installer.sh" \
    DATAHUB_GMS_HOST="127.0.0.1" \
    DATAHUB_GMS_PORT="1" \
    DATAHUB_GMS_STARTUP_TIMEOUT_SEC="1" \
    ACTIONS_EXTRA_PACKAGES="my-action==1.2.3" \
    bash "$start_sh" 2>&1)"

  case "$output" in
  *"Waiting for GMS"*)
    fail "a failing installer stops startup" \
      "start.sh continued to the GMS wait after the installer exited non-zero"
    return
    ;;
  esac
  pass "a failing installer stops startup instead of continuing"
}

# Guards the set -f in the installer. Both "pkg==1.*" and "pkg[extra]" are ordinary pip
# syntax and also glob patterns, so an unquoted expansion would silently swap the
# requirement for a matching filename in the working directory.
test_requirements_are_not_glob_expanded() {
  local tmp
  tmp="$(new_tmp)"
  local path
  path="$(make_stub_path "$tmp")"

  local workdir="$tmp/work"
  mkdir -p "$workdir"
  # These exist only to be tempting glob matches for the requirements below.
  touch "$workdir/my-actionb" "$workdir/my-action==1.0"

  if ! (cd "$workdir" && ACTIONS_EXTRA_PACKAGES="my-action[abc] my-action==1.*" \
    PATH="$path" bash "$UNDER_TEST" >/dev/null 2>&1); then
    fail "requirements survive glob-looking characters" "script exited non-zero"
    return
  fi

  local call
  call="$(cat "$tmp/uv-calls")"
  local expected="pip install my-action[abc] my-action==1.*"
  if [ "$call" != "$expected" ]; then
    fail "requirements survive glob-looking characters" \
      "a filename in the working directory replaced a requirement: expected [$expected], got [$call]"
    return
  fi
  pass "requirements containing * ? and [...] reach uv unchanged"
}

# The value can carry index credentials, for example
# "--extra-index-url https://user:token@host/simple pkg". It must not reach the logs.
test_package_list_is_not_logged() {
  local tmp
  tmp="$(new_tmp)"
  local path
  path="$(make_stub_path "$tmp")"
  local secret="https://user:s3cr3t-token@example.invalid/simple"

  local output
  output="$(ACTIONS_EXTRA_PACKAGES="--extra-index-url $secret my-action==1.2.3" \
    PATH="$path" bash "$UNDER_TEST" 2>&1)"

  case "$output" in
  *s3cr3t-token*)
    fail "the package list is not echoed" "a credential in the value reached stdout: $output"
    return
    ;;
  esac

  # And the same on the refusal path, where an operator is most likely to be reading closely.
  local bare
  bare="$(make_bare_path "$tmp")"
  output="$(ACTIONS_EXTRA_PACKAGES="--extra-index-url $secret my-action==1.2.3" \
    PATH="$bare" "$BASH_BIN" "$UNDER_TEST" 2>&1)"

  case "$output" in
  *s3cr3t-token*)
    fail "the package list is not echoed" "a credential reached the refusal message: $output"
    return
    ;;
  esac
  pass "the package list is never echoed, on either path"
}

test_startup_script_runs_the_installer
test_startup_script_aborts_when_the_installer_fails
test_unset_is_a_no_op
test_empty_is_a_no_op
test_packages_are_installed
test_no_package_manager_fails_loudly
test_install_failure_is_not_swallowed
test_requirements_are_not_glob_expanded
test_package_list_is_not_logged

if [ "$failures" -ne 0 ]; then
  echo "${failures} test(s) failed"
  exit 1
fi
echo "all tests passed"
