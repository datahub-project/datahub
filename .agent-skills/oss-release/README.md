# OSS CLI Release Workflow

Tooling for cutting releases of the `acryl-datahub` Python package and related plugins
from the `acryldata/datahub` fork.

---

## Prerequisites

```bash
brew install gh          # GitHub CLI
gh auth login            # authenticate
command -v python3
command -v git
```

You must be inside the `datahub-acryl` repo on a clean `master` branch.

### Optional plugin: `connectors-accelerator`

If the `connectors-accelerator` plugin is installed, the agent invokes its
`generating-datahub-changelog` skill natively to produce categorized release notes.

If the plugin is **not** installed, the agent will ask you whether to:
- **Skip** — attach a minimal placeholder note to the release, or
- **Provide a file** — point to release notes you've drafted manually

Either way, the release can still be cut. The skill is kept in the plugin (not vendored here)
because it's also used for weekly team updates and external announcements.

---

## Quick start

The release workflow is driven by the `/oss-release` slash command in Claude Code:

```
/oss-release rc         # cut the next RC (most common) — alias: prep
/oss-release stable     # promote latest RC to stable        — alias: finish
/oss-release status     # check CI + connector tests on latest RC, no action
```

`rc` and `stable` name what they produce. `prep` and `finish` are the old names, kept
as aliases. Either spelling works; pick whichever reads clearer in your head.

If you prefer to run the bash scripts directly (no Claude required):

```bash
# See what the next version would be
.agent-skills/oss-release/scripts/next-version.sh

# Check CI status for a commit
.agent-skills/oss-release/scripts/check-ci.sh <sha>

# Check how far the fork is behind OSS upstream
.agent-skills/oss-release/scripts/compare-upstream.sh
```

---

## Testing without creating a real release

Every script except `cut-release.sh` and `dispatch-connector-tests.sh` is read-only.
Run any of them whenever you want; they don't touch remote state:

```bash
.agent-skills/oss-release/scripts/next-version.sh                       # prints the next version
.agent-skills/oss-release/scripts/compare-upstream.sh                   # prints upstream diff
.agent-skills/oss-release/scripts/check-ci.sh <sha>                     # prints release-CI status
.agent-skills/oss-release/scripts/check-connector-tests.sh <rc-tag>     # prints connector-tests status
```

`dispatch-connector-tests.sh` actually fires a workflow in
`acryldata/connector-tests` and burns real CI minutes. Use `--dry-run` to see what it
would do without spending them:

```bash
.agent-skills/oss-release/scripts/dispatch-connector-tests.sh v1.5.0.13rc1 --dry-run
```

To test the full workflow end-to-end without creating a tag or publishing a release, use dry-run:

```
/oss-release rc --dry-run
```

Dry-run runs every step for real: preflight, diff, safety check, version calc, changelog,
the lot. The only thing it doesn't do is actually tag and publish. `cut-release.sh` prints
the `git tag` and `gh release create` commands it would run and exits. Use this freely.

You can also invoke `cut-release.sh` directly in dry-run mode with hand-crafted inputs:

```bash
echo "Test notes" > /tmp/test-notes.md
.agent-skills/oss-release/scripts/cut-release.sh \
    1.5.0.99rc1 \
    "$(git rev-parse HEAD)" \
    /tmp/test-notes.md \
    --prerelease \
    --dry-run
```

---

## Typical release cycle

### 1. Cut an RC

```
/oss-release rc
```

The agent walks through the stages below. Each one can stop the workflow if something
looks off; [Validations performed](#validations-performed) lists what each gate is
actually checking and why.

| # | Stage | What happens |
|---|---|---|
| 0 | **Preflight** | Fetch origin, verify clean tree, verify `HEAD == origin/master`, check whether HEAD is already tagged. |
| 1 | **Upstream diff** (`compare-upstream.sh`) | Show how the fork compares to `datahub-project/datahub`. Posture context only — does **not** describe the release content. |
| 2a | **Release-range diff** | Show the actual commits and files changed between the latest stable tag and HEAD. This is the input to safety judgment. |
| 2b | **Safety assessment** | Agent classifies dep changes, breaking-API risk, and OSS scope into ✅ SAFE / ⚠️ REVIEW NEEDED / ❌ BLOCKED. |
| 3 | **Next version** (`next-version.sh`) | Auto-compute next RC tag (e.g. `v1.5.0.12` → `v1.5.0.13rc1`). **Asks you to confirm.** Empty-range republishes are flagged. |
| 3.5 | **Stale-notes check + generate changelog** | Detect a stale `release-notes-<version>.md` from a prior run, then generate fresh notes via `generating-datahub-changelog` (or fall back to skip / user-provided file). **Shown to you for approval.** |
| 4 | **Cut the RC** (`cut-release.sh`) | Create annotated tag, push, publish GitHub pre-release. Re-rendered as a readable Markdown summary in the chat. |
| 5 | **Dispatch connector tests** (`dispatch-connector-tests.sh`) | Fire the `Nightly Connector Tests` workflow on `acryldata/connector-tests` with `version=<new-rc>`. Stateless — run URL printed to chat; `finish` later queries GitHub for its conclusion. Skipped in dry-run. |

RCs are a real deliverable on their own. Ship one to a customer for testing and leave
it as an RC forever if you want; nothing forces you to promote.

### 2. Check CI + connector tests

After `prep`, GitHub Actions runs against the new tag and the connector-tests
workflow dispatched by Step 5 kicks off in `acryldata/connector-tests`.
Check progress with:

```
/oss-release status                                                       # release-triggered CI on acryldata/datahub
.agent-skills/oss-release/scripts/check-connector-tests.sh <rc-tag> # connector tests on acryldata/connector-tests
```

Known flaky tests that don't block releases are listed in [`known-flaky-tests.md`](known-flaky-tests.md).

### 3. Promote to stable

Once CI and connector tests pass and the team is ready:

```
/oss-release stable
```

The agent will:
1. Auto-detect the latest RC tag (highest `rcN` for the version line — rc15 beats rc1)
2. Warn if `master` has moved ahead (new commits not in the RC)
3. Show CI status — unexpected failures block the release
4. **RC-identity guard** — assert the RC tag still points at the same SHA that was resolved at Step 1 (catches a force-moved tag)
5. **Connector-tests gate** — query GitHub directly for the most recent `workflow_dispatch` run on `acryldata/connector-tests/nightly_tests.yaml` whose `set-version` job logged `Testing version: <latest-rc>` exactly; require `conclusion=success` before proceeding. See [Connector-tests integration](#connector-tests-integration) for the decision tree when no matching run exists
6. Print the RC chain (`v1.5.0.13rc1`, `rc5`, …, `rc15`) so you can audit-visibly see which rung we're gating on
7. **Require you to type "yes" before tagging**
8. Create the stable tag (e.g. `v1.5.0.13`) at the RC's exact SHA and publish the GitHub release

---

## Example session

Four worked examples covering the common flows. Real output is a bit more verbose
than shown; I've trimmed some of the chatter.

### Example A — Cutting an RC from clean master

```
you  > /oss-release rc
```

```
agent (Step 0 — Preflight)
  === Preflight summary ===
    HEAD              : b4673c663b841a3e2758d549c8262105b1dd22cc
    origin/master     : b4673c663b841a3e2758d549c8262105b1dd22cc
    latest stable tag : v1.5.0.12 @ 1b6eeadf35
    commits in range  : 2 non-merge commits since v1.5.0.12
    tags at HEAD      : (none)

agent (Step 1 — Upstream diff)
  Gap: OSS is 0 commit(s) ahead | Fork is 1298 commit(s) ahead
  (fork fully synced with datahub-project/master)

agent (Step 2 — Safety assessment)
  | Category          | Verdict | Justification                                      |
  |-------------------|---------|----------------------------------------------------|
  | Dependencies      | ✅ SAFE  | Patch bump cryptography 46.0.5 → 46.0.7            |
  | Breaking / API    | ✅ N/A   | No .py source changes                              |
  | OSS scope         | ✅ N/A   | No acryl_/cloud/internal paths touched             |
  | Build / packaging | ✅ N/A   | No MANIFEST.in or publish workflow changes         |
  | Schema / migration| ✅ N/A   | No metadata-models/ or migration SQL               |
  Overall: ✅ SAFE — proceed to Step 3.

agent (Step 3 — Next version)
  NEXT_VERSION = v1.5.0.13rc1
  Range: 2 non-merge commits since v1.5.0.12 (non-empty, OK).
  Confirm cutting v1.5.0.13rc1? [yes/no]

you  > yes
```

```
agent (Step 3.5 — Release notes)
  [generates notes via generating-datahub-changelog skill, prepends Release Info header]
  Notes written to: .agent-skills/oss-release/notes/release-notes-v1.5.0.13rc1.md
  [shows notes body]
  Proceed? [yes/edit/no]

you  > yes
```

```
agent (Step 4 — Cut the RC)
  [1/3] Creating annotated tag v1.5.0.13rc1 at b4673c663b ...
  [2/3] Creating GitHub release ...
        Release URL: https://github.com/acryldata/datahub/releases/tag/v1.5.0.13rc1
  [3/3] Done.

agent (Step 5 — Dispatch connector tests)
  === Dispatching connector tests for v1.5.0.13rc1 ===
  Run: https://github.com/acryldata/connector-tests/actions/runs/24670000000

  Check status (in ~15-18 min):
    .agent-skills/oss-release/scripts/check-connector-tests.sh v1.5.0.13rc1
```

RC is now a published pre-release. Customers can `pip install acryl-datahub==1.5.0.13rc1`.

### Example B — Promoting the RC to stable

```
you  > /oss-release status
```

```
agent  → check-ci.sh b4673c663b
         SUCCESS (27): build, lint, test-suite, ...
         FAILED (0)
       → check-connector-tests.sh v1.5.0.13rc1
         ✓ Match at pos 1: run #24670000000
         status=completed conclusion=success
         ✓ Connector tests passed for v1.5.0.13rc1

       Safe to promote to stable.
```

```
you  > /oss-release stable
```

```
agent (Steps 0-3 — preflight, detect LATEST_RC, check CI)
  Latest RC: v1.5.0.13rc1  (b4673c663b)
  CI on acryldata/datahub: all required workflows green.

agent (Step 3.5 — RC identity + connector-tests gate)
  === RC chain for v1.5.0.13 ===
    v1.5.0.13rc1
  Gating on: v1.5.0.13rc1  (earlier RCs do not satisfy this gate — each RC is a different SHA)

  === Checking connector tests for v1.5.0.13rc1 ===
    Source       : acryldata/connector-tests / nightly_tests.yaml
    Scan depth   : 30 most-recent workflow_dispatch runs
    [1/30] scanning run 24670000000 ...
  ✓ Match: https://github.com/acryldata/connector-tests/actions/runs/24670000000
    status=completed conclusion=success position=1/30
  ✓ Connector tests passed for v1.5.0.13rc1

agent (Step 5 — Confirm)
  Promote v1.5.0.13rc1 → v1.5.0.13 at SHA b4673c663b? Type "yes" to proceed.

you  > yes
```

```
agent (Step 6 — Cut stable)
  [1/3] Creating annotated tag v1.5.0.13 at b4673c663b ...
  [2/3] gh release create v1.5.0.13 ...
        Release URL: https://github.com/acryldata/datahub/releases/tag/v1.5.0.13
```

### Example C — `stable` blocks because connector tests haven't run

Typical case: `rc` Step 5 didn't actually dispatch — either you did a dry-run, or the
skill version predates Step 5, or the dispatch itself failed quietly.

```
you  > /oss-release stable
```

```
agent (Step 3.5)
  === Checking connector tests for v1.5.0.13rc1 ===
    [scans 30 runs, finds no match]

  ✗ No run in the last 30 workflow_dispatch runs matches 'Testing version: v1.5.0.13rc1'

  I have three options:

    1. Dispatch one now — fires the workflow, takes ~15-18 min.
       Would you like me to auto-poll with /loop and alert when it's done?
    2. Widen the scan — CONNECTOR_TESTS_SCAN_LIMIT=60 (GitHub retains logs ~90 days).
    3. Explicit override — type "override connector tests" to proceed anyway.

  Which would you like?

you  > 1
```

```
agent  → dispatch-connector-tests.sh v1.5.0.13rc1
         Run: https://github.com/acryldata/connector-tests/actions/runs/24690000000
       Stopping stable promotion. Re-run /oss-release stable once CI completes.
       Starting /loop to auto-poll every 3 min ...
```

### Example D — Preview a release without touching anything

```
you  > /oss-release rc --dry-run
```

Walks through Steps 0-5 end-to-end:
- Preflight runs real checks
- Safety assessment is real
- Release notes are really generated
- `cut-release.sh` prints the exact `git tag` + `gh release create` commands it
  *would* run, then exits 0 without tagging
- `dispatch-connector-tests.sh` prints the `gh workflow run` command it
  *would* fire, then exits without dispatching

No tags, no dispatches, no side effects. Run it whenever you want a preview.

---

## Validations performed

Every gate here exists because some past release broke in a specific way and we didn't
want to get bitten again. Inventory:

### Preflight (Stage 0)

| Check | What it verifies | Why it exists |
|---|---|---|
| Clean working tree | `git diff-index --quiet HEAD --` passes | Prevents tagging a release that includes uncommitted local changes you forgot about. |
| `HEAD == origin/master` | Local tip matches the remote branch tip exactly | Stops you from tagging an old SHA after a teammate merged something, or tagging an unpushed local commit nobody else can see. The release tag must point at the canonical, public tip. |
| Tag-already-at-HEAD warning | `git tag --points-at HEAD` is empty (or warns) | Catches the "I just `prep`'d, why am I doing it again?" case where running prep on a stale branch would tag the same SHA under a different name. |
| Range-count summary | Counts non-merge commits in `<latest stable>..HEAD` | Surfaces the empty-range republish case (zero new commits) so you can abort instead of cutting a duplicate-content release. |

### Release-range visibility (Stage 2a)

| Check | What it verifies | Why it exists |
|---|---|---|
| Commit list since last stable | Prints `git log --no-merges <stable>..HEAD` | Gives the agent the *actual* release content — not the fork-vs-upstream divergence (which can be empty even when the release contains significant ingestion changes via upstream sync). |
| Full file-stats diff | `git diff --stat <stable>..HEAD` | Shows total lines/files changed, useful for sanity-checking the magnitude of the release. |
| Safety-relevant paths grep | Filters changed files to `pyproject.toml`, `setup.py`, `uv.lock`, `requirements*.txt`, `metadata-ingestion/`, `datahub-actions/`, `metadata-ingestion-modules/` | Pre-filters the diff to the things that most often warrant a closer safety look. Empty output means no dep/ingestion-code changes. |

### Safety assessment (Stage 2b)

A per-category checklist fed by the Stage 2a output. Every category has to get a
verdict. "N/A" is fine when no files in that category changed, but "nothing to assess"
isn't acceptable — the point is a paper trail showing you looked. Result is a 5-row
table you can scan in a few seconds.

| Category | Files the agent inspects |
|---|---|
| **Dependencies** | `pyproject.toml`, `setup.py`, `setup.cfg`, `uv.lock`, `requirements*.txt` |
| **Breaking / API** | `*.py` removals, validator signatures, CLI command files, config schema files |
| **OSS scope** | `acryl_*`, `*cloud*`, `internal/`, or paths flagged as proprietary |
| **Build / packaging** | `MANIFEST.in`, build scripts, GitHub Actions publish workflows |
| **Schema / migration** | `metadata-models/` PDL, `*.avsc`, migration SQL |

For dependency changes specifically, the agent runs `git show <sha> -- <file>` to inspect
the actual diff (not just the commit title) and classifies each bump:

| Bump type | Verdict |
|---|---|
| Patch bump (`46.0.5 → 46.0.7`), upper bound preserved | ✅ SAFE |
| Minor bump (`46.0.x → 46.1.x`), upper bound preserved | ⚠️ REVIEW NEEDED |
| Major bump (`46.x → 47.x`) or removed upper bound | ⚠️ REVIEW NEEDED, default toward ❌ |
| Package added or removed entirely | ⚠️ REVIEW NEEDED |
| Lockfile-only change with no manifest delta | ✅ SAFE |

**Overall verdict** rolls up the per-category verdicts:

- All ✅ or N/A → ✅ **SAFE** — proceed.
- Any ⚠️ → ⚠️ **REVIEW NEEDED** — present the ⚠️ rows to you; ask how to proceed.
- Any ❌ → ❌ **BLOCKED** — list what must be fixed; stop the workflow.

### Stale-notes detector (Stage 3.5)

| Check | What it verifies | Why it exists |
|---|---|---|
| SHA mismatch in existing notes | If `release-notes-<version>.md` exists, grep its body for any 10–40 char hex SHA and compare against current `HEAD`. | The most insidious failure mode of `prep`: a notes file from a prior `prep` run still sits in the working tree, the branch fast-forwarded since, and reusing the file silently publishes incorrect content (wrong commit SHA, missing changes, false "quiet release" claims). This is a quiet data-integrity bug — there's no error, just wrong content on the GitHub release page. |

If the detector fires, the agent defaults to regenerating the notes. Reuse only happens if you explicitly acknowledge the SHA mismatch is intentional.

### Confirmation gates (Stages 3 and 6/finish)

| Gate | When | What you confirm |
|---|---|---|
| Next-version confirmation | After Stage 3 | The proposed tag (e.g. `v1.5.0.13rc1`) and the non-empty range. Required for `prep`. |
| Notes approval | After Stage 3.5 | The contents of `release-notes-<version>.md` look correct for the release. |
| Promote-to-stable confirmation | `finish` Stage 5 | The RC being promoted, the stable version to be created, and the SHA. Requires explicit "yes"/"confirm". |

### CI status check (`finish` only)

| Check | What it verifies | Why it exists |
|---|---|---|
| Required workflows green | `check-ci.sh <rc-sha>` reports all required CI checks passed | Prevents promoting a broken RC to stable. Failures listed in [`known-flaky-tests.md`](known-flaky-tests.md) are the only ones that may be ignored. |

### RC-identity + connector-tests (`finish` Step 3.5)

| Check | What it verifies | Why it exists |
|---|---|---|
| RC tag still points at `$RC_SHA` | `git rev-parse $LATEST_RC == $RC_SHA` | The existing Step 2 check catches "master moved ahead of RC"; this narrower check catches the case where the RC tag itself was force-moved to a different SHA after Step 1 resolved it. |
| Connector tests concluded successfully for `$LATEST_RC` | `check-connector-tests.sh $LATEST_RC` returns exit 0 — i.e. GitHub has a recent `workflow_dispatch` run on `nightly_tests.yaml` whose `set-version` job logged `Testing version: $LATEST_RC` exactly, with `conclusion=success` | Stable means `finish` promotes the RC's exact SHA — so the tested bytes must equal the shipped bytes. Anchoring on the highest-numbered RC (rc15 > rc1) prevents using an earlier RC's signal as proof for a later RC (different commits). |

---

## Connector-tests integration

`prep` and `finish` integrate with the `acryldata/connector-tests` repo's
[`nightly_tests.yaml`](https://github.com/acryldata/connector-tests/actions/workflows/nightly_tests.yaml)
workflow. The workflow runs connector-by-connector validation against a pinned
`acryl-datahub` wheel version.

One important design choice: no local state. GitHub Actions itself is the answer to
"did this RC pass connector tests?" Nothing is cached between runs; every check queries
`gh run list` and pulls job logs live. That's why you can `rc` on your laptop and
`stable` on a CI runner (or a teammate's machine) and it just works, no sync step
needed.

### How it works

```
prep Step 5    →  dispatch-connector-tests.sh v1.5.0.13rc1
                  │
                  └─ gh workflow run nightly_tests.yaml -f version=v1.5.0.13rc1
                     prints run URL, no state written
                                │
                                ▼
                       (Nightly Connector Tests runs ~15-18 min)
                                │
                                ▼
finish Step 3.5 →  check-connector-tests.sh v1.5.0.13rc1
                  │
                  └─ scan last N workflow_dispatch runs
                     fetch set-version job log for each
                     match "Testing version: v1.5.0.13rc1" (exact)
                     return exit code by status/conclusion
```

### `check-connector-tests.sh` exit codes

| Exit | Meaning | `finish` behavior |
|---|---|---|
| 0 | Most recent matching run concluded with `conclusion=success` | Proceed to Step 4 |
| 2 | No matching run in the last N dispatches (default 30) | Offer three options: dispatch now, widen scan, or override |
| 3 | Matching run exists but still running | Stop; tell user to re-run `finish` after CI finishes (offer `/loop` auto-poll) |
| 4 | Matching run exists but failed or was cancelled | Stop; show run URL; suggest cutting a new RC |

### When `check-connector-tests.sh` returns exit 2, the agent asks you

1. **Dispatch one now** — fire the workflow against the latest RC and
   either wait (`/loop` auto-poll) or re-run `/oss-release stable` later
2. **Widen the scan** — `CONNECTOR_TESTS_SCAN_LIMIT=60 check-connector-tests.sh <rc>`
   (GitHub retains job logs ~90 days, so anything older than that is
   unmatchable regardless of scan depth)
3. **Explicit override** — type "override connector tests" to proceed
   anyway; a prominent warning will be added to the release notes

### Performance

| Case | Approx. cost |
|---|---|
| Run is near the top of recent dispatches (common — `prep` just fired it) | ~2-7 seconds, 1-2 API calls |
| No matching run (full 30-run scan) | ~60-90 seconds, ~60 API calls |
| Match at position 15 of 30 | ~30 seconds |

The scan walks runs newest-first and stops on the first match, so the common case is
quick. No-match is slow because it has to pull every run's log before giving up, but
`finish` only runs once a week, so it's fine.

---

## Version scheme

```
v1.5.0.13rc2
  │ │ │  │ │
  │ │ │  │ └── RC number (dropped for stable)
  │ │ │  └──── fourth component (most common bump)
  │ │ └─────── patch
  │ └───────── minor
  └─────────── major
```

| Bump | From | To (RC) | To (stable) |
|------|------|---------|-------------|
| `fourth` (default) | `v1.5.0.12` | `v1.5.0.13rc1` | `v1.5.0.13` |
| `patch` | `v1.5.0.12` | `v1.5.1.0rc1` | `v1.5.1.0` |
| `minor` | `v1.5.0.12` | `v1.6.0.0rc1` | `v1.6.0.0` |
| `major` | `v1.5.0.12` | `v2.0.0rc1` | `v2.0.0` |

Override the default bump:

```
/oss-release rc patch        # v1.5.0.12 → v1.5.1.0rc1
/oss-release rc minor        # v1.5.0.12 → v1.6.0.0rc1
/oss-release rc major        # v1.5.0.12 → v2.0.0rc1
```

Or directly:

```bash
.agent-skills/oss-release/scripts/next-version.sh rc minor
```

---

## Editing release notes before publishing

The agent writes the generated notes to `.agent-skills/oss-release/notes/release-notes-<version>.md`
and shows them to you for approval before invoking `cut-release.sh`. To edit before
publishing, just say:

> "Pause before cutting — I want to edit the notes first"

The file already exists on disk; edit it directly, then tell the agent to proceed. The
notes directory is gitignored, so your edits won't show up in `git status`.

To inspect notes from a past release, the file persists between runs:

```bash
ls .agent-skills/oss-release/notes/
```

---

## Files in this directory

| Path | Purpose |
|------|---------|
| `SKILL.md` | Router for `/oss-release` — handles cross-cutting policies (dry-run gates, prerequisites, dispatch table); reads workflow files for actual steps |
| `README.md` | This file — human-facing docs and validation reference |
| `workflows/prep.md` | Full `prep` workflow (Steps 0–5) |
| `workflows/finish.md` | Full `finish` workflow (Steps 0–6, with 3.5 sub-step for RC identity + connector-tests) plus `status` subcommand at the bottom |
| `references/safety-assessment.md` | 5-category structured checklist used in `prep` Step 2b |
| `references/cut-release-render.md` | Markdown rendering template for `cut-release.sh` output (dry-run + real-run) |
| `templates/release-info-header.md` | Required prepend for every release-notes file |
| `known-flaky-tests.md` | CI failures that are pre-existing and don't block releases |
| `.gitignore` | Local ignore — keeps `notes/` out of git |
| `notes/` | Generated release notes (one per release; never committed). Created automatically on the first run of `prep` or `finish`. **No runtime state** — the skill does not persist connector-test run records; GitHub is the source of truth. |
| `scripts/next-version.sh` | Calculate next version from git tags |
| `scripts/cut-release.sh` | Create annotated tag + push + `gh release create` |
| `scripts/compare-upstream.sh` | Show how far the fork is behind `datahub-project/datahub` |
| `scripts/check-ci.sh` | Poll GitHub Actions CI status for a commit SHA (release-triggered runs on `acryldata/datahub`) |
| `scripts/dispatch-connector-tests.sh` | Fire `acryldata/connector-tests/nightly_tests.yaml` with `version=<rc-tag>`. Stateless; prints the run URL. Called from `prep` Step 5. |
| `scripts/check-connector-tests.sh` | Query GitHub for the most recent `workflow_dispatch` run matching `Testing version: <rc-tag>`; return exit 0/2/3/4 by conclusion. Called from `finish` Step 3.5. |
| `scripts/safe-fetch-tags.sh` | Wrap `git fetch origin --tags`; suppress benign legacy-tag clobber warnings; loud only on real failures |
| `tests/*.sh` | Shell tests for helper scripts (run via `tests/run-all.sh`) |

---

## Environment variable overrides

Defaults come from the git remote; override only if you need to.

| Variable | Default | Description |
|----------|---------|-------------|
| `GITHUB_REPO_OWNER` | `acryldata` | Owner of the release repo |
| `GITHUB_REPO_NAME` | `datahub` | Name of the release repo |
| `CONNECTOR_TESTS_REPO` | `acryldata/connector-tests` | Repo hosting the connector-tests workflow |
| `CONNECTOR_TESTS_WORKFLOW` | `nightly_tests.yaml` | Workflow filename to dispatch + scan |
| `CONNECTOR_TESTS_REF` | `main` | Branch/ref of connector-tests to dispatch from |
| `CONNECTOR_TESTS_SCAN_LIMIT` | `30` | How many recent `workflow_dispatch` runs `check-connector-tests.sh` scans before giving up. Raise this if you need to verify an older RC. |

