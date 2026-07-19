#!/usr/bin/env bash
# Run each Python module's pinned mypy (from its Gradle-managed venv) on the
# staged files pre-commit passes in, at the pre-push stage (mypy is slow, so it
# is kept out of the pre-commit path; see PFP-5002).
#
# pre-commit entry form:
#   bash .github/scripts/python_staged_mypy.sh --modules <comma-list>
# pre-commit appends the matching staged file paths.
#
# Groups the staged files by module, then runs each module's
# <module>/venv/bin/mypy from the module dir (so mypy discovers the module's
# mypy config — setup.cfg [mypy] / pyproject [tool.mypy]) on that module's
# module-relative staged paths. Fails (non-zero) if any module's mypy fails or
# any referenced module's venv is missing. CI runs the full module-wide mypy via
# `./gradlew :<module>:lint`.
set -euo pipefail

modules=()
while [ $# -gt 0 ]; do
  case "$1" in
    --modules) IFS=',' read -r -a modules <<<"$2"; shift 2;;
    --) shift; break;;
    *) break;;  # remaining args are the staged files
  esac
done
files=("$@")

root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

# pre-commit only invokes this hook when at least one file matches `files`, but
# guard against an empty list anyway (mypy with no args errors out).
if [ "${#files[@]}" -eq 0 ]; then
  exit 0
fi

rc=0
for m in "${modules[@]}"; do
  # Collect this module's staged files, converted to module-relative paths.
  rel=()
  for f in "${files[@]}"; do
    case "$f" in
      "${m}/"*) rel+=("${f#"${m}/"}");;
    esac
  done
  if [ "${#rel[@]}" -eq 0 ]; then
    continue
  fi

  mypy_bin="${root}/${m}/venv/bin/mypy"
  if [ ! -x "$mypy_bin" ]; then
    task=":$(printf '%s' "$m" | tr '/' ':'):installDev"
    echo "error: ${mypy_bin} not found." >&2
    echo "       Build the module's Python env first:  ./gradlew ${task}" >&2
    rc=1
    continue
  fi

  echo "mypy: ${m}"
  # --follow-imports=silent: only report errors in the staged files themselves;
  # mypy still follows imports to resolve types but does not report errors in
  # the silently-followed modules.
  ( cd "${root}/${m}" && "$mypy_bin" --show-traceback --show-error-codes --follow-imports=silent "${rel[@]}" ) || rc=$?
done

exit $rc
