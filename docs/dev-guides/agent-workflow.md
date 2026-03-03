# Agent Development Workflow

This document is the canonical reference for using `datahub-dev.sh` to build, test, and iterate
on DataHub. It is agent-agnostic and applies to Claude Code, Cursor, Codex CLI, Devin, and any
other coding agent.

## `datahub-dev` CLI Tool

A stdlib-only Python CLI for agent-driven development. No venv needed — runs with system `python3`.

**Always use the shell wrapper as the entry point:**

```bash
scripts/dev/datahub-dev.sh <command>
```

Run `scripts/dev/datahub-dev.sh --help` to see all available subcommands (`start`, `status`, `wait`,
`rebuild`, `test`, `flag list/get`, `env`, `sync-flags`, `reset`, `nuke`).

## End-to-End Workflow

1. **Start**: `scripts/dev/datahub-dev.sh start`
2. **Code**: Make changes to Java/Python/frontend code
3. **Rebuild**: `scripts/dev/datahub-dev.sh rebuild --wait`
4. **Test**: `scripts/dev/datahub-dev.sh test <test-path>`
5. **Iterate**: Repeat steps 2–4

**Worktree note:** All Gradle commands inside the tool already pass `-x generateGitPropertiesGlobal`
to avoid git-related failures in worktrees.

## Module-to-Container Mapping

| Source directory                  | Container                                     |
| --------------------------------- | --------------------------------------------- |
| `metadata-service/`               | `datahub-gms`                                 |
| `datahub-graphql-core/`           | `datahub-gms`                                 |
| `metadata-io/`                    | `datahub-gms`                                 |
| `datahub-frontend/`               | `datahub-frontend-react`                      |
| `metadata-jobs/mce-consumer-job/` | `datahub-mce-consumer`                        |
| `metadata-jobs/mae-consumer-job/` | `datahub-mae-consumer`                        |
| `metadata-models/`                | All (triggers full rebuild + code generation) |

## Feature Flag Lifecycle

**All flag changes require a container restart.** Use `env set` + `env restart`:

```bash
scripts/dev/datahub-dev.sh env set SHOW_BROWSE_V2=true
scripts/dev/datahub-dev.sh env restart
```

`flag list` and `flag get` are read-only inspection tools — they show the current live values from
the running server but do not change anything.

The flag manifest at `scripts/generated/flag-classification.json` is **auto-generated**
(gitignored). Run `scripts/dev/datahub-dev.sh sync-flags` after adding fields to `FeatureFlags.java`
or after a fresh clone.

## Recovery Escalation

**When to use each:**

- `reset`: GMS returns 503 and doesn't recover, frontend shows "Unable to connect", tests fail
  with connection errors
- `nuke --keep-data`: Containers in restart loops, port conflicts, `reset` didn't fix it
- `nuke`: ES index corruption, MySQL schema issues after model changes, PDL model changes needing
  clean slate, `nuke --keep-data` didn't fix it

## Structured Test Output

Set `AGENT_MODE=1` to get machine-readable JSON test reports at `smoke-test/build/test-report.json`:

```bash
AGENT_MODE=1 scripts/dev/datahub-dev.sh test tests/test_system_info.py
```
