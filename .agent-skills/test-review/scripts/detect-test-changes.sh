#!/usr/bin/env bash
# Detect added/modified test files in a PR or local diff.
# Classifies files as smoke test (Python) or integration test (Cypress).
#
# Scope:
#   - smoke-test/**/*.py (excluding tests/cypress/) -> smoke
#   - smoke-test/tests/cypress/**/*.py              -> integration (launcher)
#   - smoke-test/tests/cypress/**/*.js              -> integration (spec)
#
# Out of scope (excluded):
#   - metadata-ingestion/tests/integration/ (connector tests, covered by datahub-connector-pr-review)
#   - metadata-ingestion/tests/unit/
#
# Usage:
#   ./detect-test-changes.sh <PR_NUMBER>   # PR mode (uses gh CLI)
#   ./detect-test-changes.sh local         # Local mode (uses git diff)
#   ./detect-test-changes.sh local <base>  # Local mode against specific base branch
#
# Exit codes:
#   0 = test changes found
#   1 = no test changes detected
#   2 = error
#
# Output format (one per line):
#   smoke:<filepath>
#   integration:<filepath>

set -euo pipefail

# Navigate to repo root (this script lives inside the repo at .agent-skills/test-review/scripts/)
REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$REPO_ROOT"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <PR_NUMBER|local> [base_branch]" >&2
    exit 2
fi

MODE="$1"
BASE_BRANCH="${2:-main}"

# Get list of changed files
if [[ "$MODE" == "local" ]]; then
    CHANGED_FILES=$(git diff --name-only --diff-filter=AMR "origin/${BASE_BRANCH}...HEAD" 2>/dev/null || \
                    git diff --name-only --diff-filter=AMR "${BASE_BRANCH}...HEAD" 2>/dev/null || \
                    git diff --name-only --diff-filter=AMR HEAD~1 2>/dev/null)
else
    PR_NUMBER="$MODE"
    CHANGED_FILES=$(gh pr diff "$PR_NUMBER" --name-only 2>/dev/null)
    if [[ $? -ne 0 ]]; then
        echo "Error: Failed to get PR diff for #${PR_NUMBER}" >&2
        exit 2
    fi
fi

if [[ -z "$CHANGED_FILES" ]]; then
    echo "No changed files detected." >&2
    exit 1
fi

# Filter to test-relevant files (Python and JavaScript test files within smoke-test/)
RELEVANT_FILES=$(echo "$CHANGED_FILES" | grep -E '^smoke-test/' || true)

if [[ -z "$RELEVANT_FILES" ]]; then
    echo "No smoke-test/ changes detected." >&2
    exit 1
fi

FOUND=0

while IFS= read -r filepath; do
    [[ -z "$filepath" ]] && continue

    # Integration tests: Cypress specs (.js) and launcher (.py) under smoke-test/tests/cypress/
    if [[ "$filepath" == smoke-test/tests/cypress/* ]]; then
        # Include .js spec files and .py launcher/helper files
        if [[ "$filepath" == *.js || "$filepath" == *.py || "$filepath" == *.json ]]; then
            echo "integration:${filepath}"
            FOUND=1
        fi
        continue
    fi

    # Smoke tests: Python files under smoke-test/ (excluding cypress)
    if [[ "$filepath" == *.py ]]; then
        echo "smoke:${filepath}"
        FOUND=1
        continue
    fi

    # Config files (pyproject.toml, etc.) are relevant context but not classified
    if [[ "$filepath" == smoke-test/pyproject.toml || "$filepath" == smoke-test/conftest.py ]]; then
        echo "smoke:${filepath}"
        FOUND=1
        continue
    fi

done <<< "$RELEVANT_FILES"

if [[ "$FOUND" -eq 0 ]]; then
    echo "No in-scope test changes detected." >&2
    exit 1
fi
