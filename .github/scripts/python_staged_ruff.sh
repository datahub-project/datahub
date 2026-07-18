#!/usr/bin/env bash
# Run a Python module's pinned ruff on the staged files pre-commit passes in.
#
# pre-commit entry form:
#   bash .github/scripts/python_staged_ruff.sh <module-path> <ruff-args...>
# pre-commit appends the matching staged file paths after <ruff-args...>.
#
# Uses the ruff installed in the module's Gradle-managed venv
# (<module>/venv/bin/ruff), so each module runs its own pinned ruff version.
# Ruff resolves its config by walking up from each staged file to the nearest
# pyproject.toml, so the module's [tool.ruff] config applies automatically.
set -euo pipefail

module="$1"; shift
ruff_bin="${module}/venv/bin/ruff"

if [ ! -x "$ruff_bin" ]; then
  task=":$(printf '%s' "$module" | tr '/' ':'):installDev"
  echo "error: ${ruff_bin} not found." >&2
  echo "       Build the module's Python env first:  ./gradlew ${task}" >&2
  echo "       (that task installs ruff into ${module}/venv)" >&2
  exit 1
fi

exec "$ruff_bin" "$@"
