#!/usr/bin/env bash
# Guard against accidental massive pushes (>MAX_FILES files). Consolidated
# from the former .githooks/pre-push-oss standalone hook into a pre-commit
# `pre-push`-stage local hook so it runs alongside the other pre-push hooks
# installed via `pre-commit install --hook-type pre-push`.
#
# pre-commit does not forward git's pre-push stdin ref pairs to hooks; it sets
# PRE_COMMIT_TO_REF (local/new oid) and PRE_COMMIT_FROM_REF (the oid to diff
# against). Fail-open on a missing ref: this is an accident guard, not a
# security control, so an unmeasurable push must never be blocked.
set -u

MAX_FILES="${GUARD_PUSH_MAX_FILES:-500}"

to="${PRE_COMMIT_TO_REF:-}"

# pre-commit omits both refs when it decides to run against all files (pushing
# a branch that contains a root commit). Nothing reliable to measure.
if [ -z "$to" ]; then
  exit 0
fi

# Without remote-tracking refs, `--not --remotes` below has nothing to subtract
# and would count every file in the repo's history.
if [ -z "$(git for-each-ref --count=1 refs/remotes 2>/dev/null)" ]; then
  exit 0
fi

# Count files touched by commits that are not yet on any remote.
#
# Deliberately NOT `git diff PRE_COMMIT_FROM_REF PRE_COMMIT_TO_REF`: that
# two-dot diff compares the old remote tip against the new one, so after a
# rebase (or a merge of master into the branch) it also contains every upstream
# change since the branch was cut. master moves several hundred files a week,
# which made small pushes off a few-weeks-old branch fail this guard. A
# three-dot diff has the same problem, since the merge base is the pre-rebase
# branch point.
#
# `--remotes` is intentionally unqualified rather than scoped to the push
# target: in a fork workflow the upstream commits are on origin/* while the
# branch is pushed to a personal fork, and scoping would count all of master.
files=$(git log --name-only --pretty=format: "$to" --not --remotes -- 2>/dev/null || true)

count=0
[ -n "$files" ] && count=$(printf '%s\n' "$files" | sed '/^$/d' | sort -u | wc -l | tr -d ' ')

if [ "$count" -gt "$MAX_FILES" ]; then
  echo "pre-push: this push changes $count files (limit $MAX_FILES) - refusing." >&2
  echo "pre-push: Agents: Never bypass this. Ask the user to check the commits." >&2
  echo "pre-push: Humans: if this is intentional, re-run with either of:" >&2
  echo "pre-push:   SKIP=guard-push-file-count git push   # skip only this guard" >&2
  echo "pre-push:   GUARD_PUSH_MAX_FILES=<n> git push     # raise the limit" >&2
  exit 1
fi

exit 0
