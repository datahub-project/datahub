#!/usr/bin/env bash
# Tests for next-version.sh — pure-bash, no external dependencies beyond git.
#
# Strategy: feed a sandbox git repo with controlled tag state, run the script,
# assert the output. Each test case isolates its tag set so cases don't interact.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$SCRIPT_DIR/../scripts/next-version.sh"
PASS=0
FAIL=0

red()    { printf '\033[31m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }

assert_eq() {
    local name="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        green "  ✓ $name"
        PASS=$((PASS + 1))
    else
        red   "  ✗ $name"
        red   "      expected: $expected"
        red   "      actual:   $actual"
        FAIL=$((FAIL + 1))
    fi
}

# Set up a sandbox git repo for each test. Sets the global SANDBOX_DIR and cd's
# into it. Don't use `$(sandbox)` — that runs the function in a subshell and the
# `cd` won't persist to the caller.
SANDBOX_DIR=""
sandbox() {
    SANDBOX_DIR=$(mktemp -d)
    cd "$SANDBOX_DIR"
    git init --quiet -b master
    git config user.email "test@example.com"
    git config user.name "Test"
    git commit --allow-empty -m "init" --quiet
    # Add a fake origin so safe-fetch-tags doesn't try to talk to github.
    git remote add origin "$SANDBOX_DIR"
}

cleanup_sandbox() {
    [ -n "$SANDBOX_DIR" ] && rm -rf "$SANDBOX_DIR"
    cd "$SCRIPT_DIR"
    SANDBOX_DIR=""
}

# Run script with stderr suppressed (we only care about stdout — the next version).
# REPO_ROOT is set by the caller to point at the sandbox so the script doesn't
# fall back to the real datahub-acryl repo.
run_next() {
    REPO_ROOT="$PWD" "$SCRIPT" "$@" 2>/dev/null
}

echo "=== next-version.sh tests ==="

# Case 1: latest is RC → bump RC number
sandbox
git tag v1.5.0.12rc1
git tag v1.5.0.13rc2
assert_eq "RC bumps RC number" "v1.5.0.13rc3" "$(run_next)"
cleanup_sandbox

# Case 2: latest is stable → start new RC cycle (fourth-segment bump default)
sandbox
git tag v1.5.0.12
assert_eq "stable starts new RC cycle (fourth bump)" "v1.5.0.13rc1" "$(run_next)"
cleanup_sandbox

# Case 3: explicit `rc patch` override
sandbox
git tag v1.5.0.12
assert_eq "explicit patch bump" "v1.5.1.0rc1" "$(run_next rc patch)"
cleanup_sandbox

# Case 4: explicit `rc minor` override
sandbox
git tag v1.5.0.12
assert_eq "explicit minor bump" "v1.6.0.0rc1" "$(run_next rc minor)"
cleanup_sandbox

# Case 5: explicit `stable` from RC → strips rcN
sandbox
git tag v1.5.0.13rc3
assert_eq "stable strips rcN" "v1.5.0.13" "$(run_next stable)"
cleanup_sandbox

# Note: the "no tags at all" case is intentionally NOT tested. The script falls
# back to `gh release list --repo acryldata/datahub` when local has no tags,
# which can't be isolated without mocking the gh binary. The fallback is
# defensive (only triggers on a brand-new clone with no tags fetched) and not
# worth the test infrastructure cost.

echo ""
echo "Results: $PASS passed, $FAIL failed"
exit "$FAIL"
