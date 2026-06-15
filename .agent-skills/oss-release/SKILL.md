---
name: oss-release
description: Cut release candidates and stable releases of the acryl-datahub OSS CLI from the acryldata/datahub fork. Use when running /oss-release rc, /oss-release stable, /oss-release status, /oss-release mark-linear (also accepts legacy aliases /oss-release prep and /oss-release finish); when cutting a new RC; promoting an RC to stable; previewing a release in dry-run; checking CI on a release candidate; or running an optional manual audit of the automated Linear release tracking. Handles preflight, upstream diff, structured safety assessment, version bump, changelog generation, stale-notes detection, tag/release publishing, auto-dispatch of the acryldata/connector-tests nightly workflow for the new RC, and gating stable promotion on that run's success (verified by querying GitHub directly — no local state) plus RC-identity verification.
---

# OSS CLI Release Skill

You are the release engineer for the acryldata/datahub OSS CLI. Your job is to safely prepare
and cut versioned releases of the `acryl-datahub` Python package and related plugins.

This SKILL.md is a **router**. The full workflows live in `workflows/`, with extracted
shared content in `references/` and `templates/`. When invoked, read the appropriate
workflow file end-to-end, then return here only for cross-cutting policies (dry-run
gates, prerequisites).

---

## Quick Start

```
/oss-release rc --dry-run       # safest first invocation: preview a release end-to-end
/oss-release rc                 # cut the next RC (5 min)
/oss-release status             # check CI + connector tests on the latest RC, no action
/oss-release stable             # promote latest RC to stable (after CI is green)
```

Legacy aliases: `prep` ≡ `rc`; `finish` ≡ `stable`. Both work identically.

---

## Multi-Agent Compatibility

This skill works across Claude Code, Cursor, Copilot, Gemini CLI, Windsurf, and other agents.

**What works everywhere:**

- All scripts in `.agent-skills/oss-release/scripts/` (pure bash + `gh` CLI + `git`)
- All checklists and judgment criteria in this document and `references/`

**Claude Code-specific features** (other agents can safely ignore):

- `/oss-release` slash command auto-loads this skill
- `finish` CI monitoring uses `/loop` + `ScheduleWakeup` for async polling
- Sub-agents can be dispatched for parallel diff analysis

**Scripts location:** Run scripts from the repo root exactly as shown — the scripts resolve their
own paths internally via `$(dirname "$0")`.

---

## Subcommands

| Subcommand    | Aliases  | Purpose                                                                                                                                                                                      | Time    | Workflow file                      |
| ------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------------------------- |
| `rc`          | `prep`   | Diff analysis → safety check → changelog → cut RC pre-release → dispatch connector tests                                                                                                     | ~5 min  | `workflows/prep.md`                |
| `stable`      | `finish` | Check CI + connector tests on latest RC → confirm → cut stable release                                                                                                                       | async   | `workflows/finish.md`              |
| `status`      | —        | Show CI + connector-tests status for the latest RC, no action taken                                                                                                                          | instant | `workflows/finish.md` (bottom)     |
| `mark-linear` | —        | **Optional manual audit** of the automated Linear release tracking — finds/labels tickets the `linear-release-tracking.yml` pipeline may have missed (needs Linear MCP). Not a release step. | varies  | `workflows/mark-linear-tickets.md` |

**Dispatch rule:** When the user invokes a subcommand, **read the corresponding workflow
file end-to-end before doing anything else**. The workflow files contain the actual
step-by-step instructions; SKILL.md only handles cross-cutting concerns below.

**Alias resolution:** `rc` and `prep` are interchangeable; `stable` and `finish` are
interchangeable. Accept either form silently — do not ask the user to "use the correct
name." The new names (`rc`, `stable`) are preferred in new prose; the legacy names
(`prep`, `finish`) remain supported indefinitely for muscle memory and existing
documentation.

**Dry-run mode:** Append `--dry-run` to any invocation of `rc`/`prep` or `stable`/`finish` (e.g. `/oss-release rc --dry-run`).
In dry-run mode the workflow runs end-to-end — preflight, diff, version calculation, changelog —
but `cut-release.sh` is invoked with `--dry-run` so no tag is created and no release is published.
Use this to test the skill or preview a release.

**No version numbers required.** `rc` determines the next version automatically.
`stable` and `status` auto-detect the most recent RC tag.

**RC releases are first-class deliverables.** `rc` alone is a complete workflow — use it
when cutting a release candidate for customer testing, staged rollout, or connector
validation. `stable` is called separately, potentially days or weeks later.

---

## Confirmation gates: dry-run vs real-run

Two classes of confirmation: **policy gates** (operator intent — "are you sure
you want to cut this version?") and **state blockers** (something is wrong with
the tree — "HEAD is already tagged," "range is empty"). Both honor dry-run vs
real-run, but phrased slightly differently.

**Policy gates** — next-version, notes approval, promote-to-stable:

| Mode         | Behavior                                                                                                                                         |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Real run** | Stop and wait for the user's explicit "yes" / "confirm" / equivalent. Silence is NOT consent.                                                    |
| **Dry-run**  | Announce each gate ("In a real run I would pause here for confirmation of NEXT_VERSION"), but proceed. Nothing the dry-run does is irreversible. |

**State blockers** — HEAD already tagged, empty range, failed fetch, dirty tree
(covered in detail in `workflows/prep.md` Step 0):

| Mode         | Behavior                                                                                                                                                                                                                                                                              |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Real run** | Stop. Show the blocker and require explicit "republish" / "override" for degenerate-but-intentional cases (HEAD-tagged, empty range). Hard-fail for working-tree-broken cases (dirty tree, HEAD ≠ origin/master, fetch failures) — those can't be overridden without fixing the tree. |
| **Dry-run**  | Announce the blocker and proceed for the degenerate cases. Hard-fail for working-tree-broken cases even in dry-run, since they indicate the tool is running in the wrong state.                                                                                                       |

A user-supplied override always wins: if they explicitly say "stop and ask me each time"
during a dry-run, honor that for the rest of the run.

---

## Prerequisites (verify before any subcommand)

```bash
for cmd in git gh python3; do command -v "$cmd" || echo "MISSING: $cmd"; done
gh auth token &>/dev/null || echo "NOT AUTHENTICATED: run gh auth login"
git remote get-url origin   # must contain acryldata/datahub
git status                  # must be clean
git fetch origin --tags --quiet
```

---

## Repo layout

| Path                               | Purpose                                                                                                                                                                                                                  |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SKILL.md`                         | This file — router and cross-cutting policies                                                                                                                                                                            |
| `workflows/prep.md`                | Full `prep` workflow (Steps 0–5)                                                                                                                                                                                         |
| `workflows/finish.md`              | Full `finish` workflow (Steps 0–6 with 3.5 sub-step) plus `status` subcommand                                                                                                                                            |
| `workflows/mark-linear-tickets.md` | **Optional manual audit** of the automated `linear-release-tracking.yml` pipeline — cross-checks PR↔ticket links (attachment/comment aware) and labels any tickets the automation missed. Not part of the release flow. |
| `references/safety-assessment.md`  | 5-category structured checklist used in `prep` Step 2b                                                                                                                                                                   |
| `references/cut-release-render.md` | Markdown rendering template for `cut-release.sh` output (dry-run + real-run)                                                                                                                                             |
| `templates/release-info-header.md` | Required prepend for every release-notes file                                                                                                                                                                            |
| `scripts/*.sh`                     | Helper scripts (next-version, cut-release, compare-upstream, check-ci, safe-fetch-tags, dispatch-connector-tests, check-connector-tests)                                                                                 |
| `notes/`                           | Generated release notes (gitignored, persists between runs). No runtime state — GitHub is the source of truth for connector-test run status.                                                                             |
| `known-flaky-tests.md`             | CI failures that don't block releases                                                                                                                                                                                    |
| `README.md`                        | Human-facing docs and validation reference                                                                                                                                                                               |
