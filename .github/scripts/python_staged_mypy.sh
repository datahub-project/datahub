#!/usr/bin/env bash
# Run a Python module's pinned mypy (from its Gradle-managed venv) on the staged
# files pre-commit passes in, at the pre-push stage (mypy is slow, so it is kept
# out of the pre-commit path; see PFP-5002).
#
# pre-commit entry form:
#   bash .github/scripts/python_staged_mypy.sh <module-path>
# pre-commit appends the matching staged file paths.
#
# Uses <module>/venv/bin/mypy and runs from the module dir so mypy discovers the
# module's mypy config (setup.cfg [mypy] / pyproject [tool.mypy]). Staged file
# paths are converted from repo-root-relative to module-relative.
set -euo pipefail

module="$1"; shift
root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
mypy_bin="${root}/${module}/venv/bin/mypy"

if [ ! -x "$mypy_bin" ]; then
  task=":$(printf '%s' "$module" | tr '/' ':'):installDev"
  echo "error: ${mypy_bin} not found." >&2
  echo "       Build the module's Python env first:  ./gradlew ${task}" >&2
  exit 1
fi

# Convert repo-root-relative staged paths to module-relative.
rel=()
for f in "$@"; do
  case "$f" in
    "${module}/"*) rel+=("${f#"${module}/"}");;
    *) rel+=("$f");;
  esac
done

# pre-commit only invokes this hook when at least one file matches `files`, but
# guard against an empty list anyway (mypy with no args errors out).
if [ "${#rel[@]}" -eq 0 ]; then
  exit 0
fi

cd "${root}/${module}"
# --follow-imports=silent: only report errors in the staged files themselves;
# mypy still follows imports to resolve types but does not report errors in the
# silently-followed modules. CI runs the full module-wide mypy via
# `./gradlew :<module>:lint`.
exec "$mypy_bin" --show-traceback --show-error-codes --follow-imports=silent "${rel[@]}"
