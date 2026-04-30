#!/usr/bin/env bash
# validate-rc-for-promotion.sh <RC_TAG> <EXPECTED_SHA>
#
# Runs the two Step 3.5 gates on an RC that finish is about to promote:
#
#   3.5a — Changelist-identity guard. The RC tag must still point at the SHA
#          that finish-preflight.sh resolved it to. Detects the narrow race
#          where the RC tag was force-moved between preflight and promotion.
#
#   3.5b — Connector tests. Prints the full RC chain (so the operator sees
#          which sibling RCs exist and which one is being gated on), then
#          invokes check-connector-tests.sh. Exit code of this script is
#          the exit code of check-connector-tests.sh (0=pass, 2=no run,
#          3=running, 4=failed). See workflows/finish.md for handling.
#
# Consolidated from Step 3.5's compound blocks so Claude Code can classify
# the whole gate as a single command.
#
# Usage:
#   .agent-skills/oss-release/scripts/validate-rc-for-promotion.sh v1.5.0.13rc2 1b6eeadf35a0a3d0ab26a833f77796cf321ea5ac

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

RC_TAG="${1:-}"
EXPECTED_SHA="${2:-}"

if [ -z "$RC_TAG" ] || [ -z "$EXPECTED_SHA" ]; then
    echo "Usage: $0 <rc-tag> <expected-sha>" >&2
    exit 1
fi

# 3.5a — RC tag must still resolve to the SHA we snapshot in finish-preflight.
#
# Use `^{commit}` to dereference annotated tags to their target commit. Plain
# `git rev-parse <tag>` on an annotated tag returns the tag-object SHA, which
# is NOT the same as the commit SHA. cut-release.sh creates annotated tags
# (`git tag -a`), so without the deref this guard would false-positive on
# every RC cut by the skill — finish-preflight.sh uses `git rev-list -n 1`
# (always returns a commit SHA), so the two would never agree.
CURRENT_SHA=$(git rev-parse "${RC_TAG}^{commit}" 2>/dev/null || echo "")
if [ -z "$CURRENT_SHA" ]; then
    echo "ERROR: $RC_TAG no longer resolves to any commit (was it deleted?)." >&2
    exit 1
fi
if [ "$CURRENT_SHA" != "$EXPECTED_SHA" ]; then
    echo "ERROR: $RC_TAG was force-moved." >&2
    echo "       now      : ${CURRENT_SHA:0:10}" >&2
    echo "       expected : ${EXPECTED_SHA:0:10}" >&2
    echo "       Abort and investigate before promoting." >&2
    exit 1
fi

# 3.5b — RC chain + connector tests.
STABLE_PREFIX="${RC_TAG%rc*}"
echo "=== RC chain for ${STABLE_PREFIX} ==="
git tag -l "${STABLE_PREFIX}rc*" | sort -V | sed 's/^/  /'
echo "Gating on: ${RC_TAG}"
echo "  (earlier RCs do not satisfy this gate — each RC is a different SHA)"
echo ""

"$SCRIPT_DIR/check-connector-tests.sh" "$RC_TAG"
exit $?
