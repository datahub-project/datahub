#!/usr/bin/env bash
# Guard against accidental massive pushes (>MAX_FILES files). Consolidated
# from the former .githooks/pre-push-oss standalone hook into a pre-commit
# `pre-push`-stage local hook so it runs alongside the other pre-push hooks
# installed via `pre-commit install --hook-type pre-push`.
#
# pre-commit does not forward git's pre-push stdin ref pairs to hooks; it sets
# PRE_COMMIT_FROM_REF (remote/old oid) and PRE_COMMIT_TO_REF (local/new oid).
# Fail-open on missing/empty refs: this is an accident guard, not a security
# control, so a stale/missing object must never block a real push.
set -u

MAX_FILES=500

from="${PRE_COMMIT_FROM_REF:-}"
to="${PRE_COMMIT_TO_REF:-}"

if [ -z "$from" ] || [ -z "$to" ]; then
  exit 0
fi

if [ -z "${from//0/}" ]; then
  # New remote ref: commits not already on any remote branch.
  files=$(git log --name-only --pretty=format: "$to" --not --remotes -- 2>/dev/null || true)
else
  # Updating an existing ref: net diff between old and new tip.
  files=$(git diff --name-only "$from" "$to" -- 2>/dev/null || true)
fi

count=0
[ -n "$files" ] && count=$(printf '%s\n' "$files" | sed '/^$/d' | sort -u | wc -l | tr -d ' ')

if [ "$count" -gt "$MAX_FILES" ]; then
  echo "pre-push: this push changes $count files (limit $MAX_FILES) — refusing." >&2
  echo "pre-push: Agents: Never bypass this with --no-verify. Ask the user to check the commits." >&2
  echo "pre-push: Humans: if this is intentional, re-run with: git push --no-verify" >&2
  exit 1
fi

exit 0
