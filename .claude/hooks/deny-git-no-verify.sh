#!/usr/bin/env bash
# Claude Code PreToolUse (Bash) hook: block git's --no-verify so agents can't
# skip pre-commit / pre-push checks. See AGENTS.md "Never bypass git hook failures".
# Exit 2 => the tool call is blocked and this stderr is shown to the agent.

payload="$(cat)"

# Block only when the command both invokes git and passes --no-verify. The two
# tokens are matched independently (not spanned) so an embedded quote in a -m
# message can't hide a trailing --no-verify. `git` and `--no-verify` only appear there via the command.
has_git=$(printf '%s' "$payload" | grep -Eqi '(^|[^[:alnum:]])git([^[:alnum:]]|$)' && echo 1)
has_no_verify=$(printf '%s' "$payload" | grep -Eq '(^|[[:space:]])--no-verify([[:space:]"]|$)' && echo 1)

if [ "$has_git" = 1 ] && [ "$has_no_verify" = 1 ]; then
  echo "Blocked: --no-verify bypasses git hooks, which is disallowed (see AGENTS.md)." >&2
  echo "A failing hook is a signal to stop and fix, not skip. Report it to the user and ask how to proceed." >&2
  exit 2
fi

exit 0
