#!/usr/bin/env bash
# Collect the p0 smoke tier three ways and check that the local lane drops the
# tests marked requires(...) for what the Docker-free stack lacks, and that the
# Docker canary picks up every one of them. See smoke-test/utilities/backends.py.
#
# Run from smoke-test/ with its venv installed. Writes a summary to
# $GITHUB_STEP_SUMMARY when set (stdout otherwise).
set -euo pipefail

out=$(mktemp -d)
summary=${GITHUB_STEP_SUMMARY:-/dev/stdout}

collect() {
  venv/bin/python -m pytest -o addopts="" -p no:cacheprovider --collect-only -q \
    -m "p0 and not release_tests and not perf" 2>/dev/null | grep '::' | sort || true
}

SMOKE_BACKEND=docker SMOKE_BACKEND_MODE=select collect > "$out/docker.txt"
SMOKE_BACKEND=local SMOKE_BACKEND_MODE=select collect > "$out/local.txt"
SMOKE_BACKEND=docker SMOKE_BACKEND_MODE=canary collect > "$out/canary.txt"
comm -23 "$out/docker.txt" "$out/local.txt" > "$out/deselected.txt"
missing=$(comm -23 "$out/deselected.txt" "$out/canary.txt")

count() { wc -l < "$1" | tr -d ' '; }
{
  echo "### Local lane selection (p0 tier)"
  echo ""
  echo "| Lane | Tests |"
  echo "| --- | ---: |"
  echo "| docker | $(count "$out/docker.txt") |"
  echo "| local | $(count "$out/local.txt") |"
  echo "| docker canary | $(count "$out/canary.txt") |"
  echo ""
  echo "<details><summary>Deselected on the local lane</summary>"
  echo ""
  while read -r node; do echo "- \`$node\`"; done < "$out/deselected.txt"
  echo ""
  echo "</details>"
} >> "$summary"

if [[ ! -s "$out/docker.txt" || ! -s "$out/deselected.txt" ]]; then
  echo "::error::The local lane deselected nothing; backend selection is not wired in"
  exit 1
fi
if [[ -n "$missing" ]]; then
  echo "::error::Deselected on the local lane but missing from the Docker canary: $missing"
  exit 1
fi
