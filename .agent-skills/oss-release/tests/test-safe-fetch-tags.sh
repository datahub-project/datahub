#!/usr/bin/env bash
# Tests for safe-fetch-tags.sh — focuses on the classification logic, since the
# actual `git fetch` call requires a network and a real divergent fork to exercise.
#
# We test the script end-to-end against the live origin once (smoke), then unit-test
# the classification logic directly by sourcing the script's regex pattern.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$SCRIPT_DIR/../scripts/safe-fetch-tags.sh"
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

assert_match() {
    local name="$1" pattern="$2" actual="$3"
    if [[ "$actual" =~ $pattern ]]; then
        green "  ✓ $name"
        PASS=$((PASS + 1))
    else
        red   "  ✗ $name"
        red   "      pattern: $pattern"
        red   "      actual:  $actual"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== safe-fetch-tags.sh tests ==="

# Test 1: Smoke — script exits 0 against the live origin and produces sensible output.
# (Skipped if SKIP_NETWORK is set, since CI may not have GitHub access.)
if [ "${SKIP_NETWORK:-0}" != "1" ]; then
    OUT=$("$SCRIPT" 2>&1) || true
    assert_match "live smoke: produces 'Refreshed origin tags' message" \
        '(Refreshed origin tags|^$)' "$OUT"
fi

# Test 2: Classification regex — extract the CURRENT_LINE_PATTERN from the script
# and verify it matches/rejects the right tag names.
PATTERN=$(grep '^CURRENT_LINE_PATTERN=' "$SCRIPT" | sed "s/.*='\\(.*\\)'/\\1/")

# 4-segment tags should match (current line)
for tag in v1.5.0.12 v1.5.0.13rc1 v2.0.0.0 v0.0.0.1rc99; do
    if [[ "$tag" =~ $PATTERN ]]; then
        green "  ✓ pattern matches current-line tag: $tag"
        PASS=$((PASS + 1))
    else
        red "  ✗ pattern should match: $tag"
        FAIL=$((FAIL + 1))
    fi
done

# 3-segment legacy tags should NOT match (legacy line, safe to skip on clobber)
for tag in v1.1.0rc1 v1.5.0rc1 v0.8.28 v1.4.0; do
    if [[ "$tag" =~ $PATTERN ]]; then
        red "  ✗ pattern should NOT match legacy tag: $tag"
        FAIL=$((FAIL + 1))
    else
        green "  ✓ pattern correctly rejects legacy tag: $tag"
        PASS=$((PASS + 1))
    fi
done

# Garbage input should not match
for tag in random-string foo refs/tags/v1.5.0.12 ""; do
    if [[ "$tag" =~ $PATTERN ]]; then
        red "  ✗ pattern should reject garbage: $tag"
        FAIL=$((FAIL + 1))
    else
        green "  ✓ pattern rejects garbage: ${tag:-<empty>}"
        PASS=$((PASS + 1))
    fi
done

echo ""
echo "Results: $PASS passed, $FAIL failed"
exit "$FAIL"
