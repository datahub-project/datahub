# report_aspect_changes.py

A CLI tool that produces a markdown breaking-change report classifying every `@Aspect` PDL change between two git refs. Both `--base` and `--head` accept any ref type — tag, branch, commit SHA, or ref-spec (e.g. `HEAD~30`) — so the tool works for release-window audits, hotfix-vs-trunk comparisons, single-PR previews, and ad-hoc spot checks alike. Companion to [`bump_schema_versions.py`](bump_schema_versions.md): where that script _writes_ `schemaVersion` bumps in your working tree, this script _audits_ them across a release window — surfacing missed bumps, spurious bumps, and breaking schema changes.

## Why this exists

The auto-bumper in `bump_schema_versions.py` runs in pre-commit on every PR. It does its job, but its decisions are invisible at release time: the only thing you see in `git diff v1.0.x..acryl-main` for a given aspect is a `schemaVersion N → N+1` line. You can't tell from the line alone whether that bump was:

- **legitimate** — the aspect (or a record it depends on) actually changed shape, OR
- **spurious** — a sibling change in the same PR triggered the bumper as a side-effect with no real schema impact (e.g. PR #9579, which auto-bumped 9 unrelated aspects with no schema changes), OR
- **missing** — the aspect changed shape but the bumper didn't fire (e.g. a manual edit that should have been bumped but wasn't).

This tool walks the same dependency graph the auto-bumper uses, classifies every aspect in the window, and emits a markdown report you can paste into a release ticket or render in `$GITHUB_STEP_SUMMARY`. It's read-only: no files are written under `metadata-models/`.

## Usage

Run from the repository root:

```
python3 .github/scripts/report_aspect_changes.py [OPTIONS]
```

### Options

| Flag            | Default                                                                                                                                                                                                                                                                                                                                                   | Description                                                                                                                                                                                                                                                                                           |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--base REF`    | latest stable `-cloud` release tag in the repo (global version-sorted lookup via `git tag --list 'v*-cloud' --sort=-v:refname`, with rc tags filtered out). Falls back to the latest `-cloud` tag (rc accepted) if no stable release has shipped yet. Independent of `--head` ancestry — stable releases on parallel hotfix branches are correctly found. | Baseline git ref. Accepts any tag, branch, commit SHA, or ref-spec.                                                                                                                                                                                                                                   |
| `--head REF`    | `acryl-main`                                                                                                                                                                                                                                                                                                                                              | Head git ref. Accepts any tag, branch, commit SHA, or ref-spec.                                                                                                                                                                                                                                       |
| `--output FILE` | stdout                                                                                                                                                                                                                                                                                                                                                    | Write the markdown report to a file (use `"$GITHUB_STEP_SUMMARY"` in CI).                                                                                                                                                                                                                             |
| `--per-pr`      | off                                                                                                                                                                                                                                                                                                                                                       | Run the classifier on each PDL-touching commit in `base..head` _in isolation_ (`parent..commit` slice) and aggregate the verdicts. Catches the per-PR `bump_spurious` anti-pattern that wide cumulative windows mask. Slower (~0.5s per PR slice) but enforces the v→v+1-per-justifying-PR invariant. |
| `-h` / `--help` | —                                                                                                                                                                                                                                                                                                                                                         | Show help and exit.                                                                                                                                                                                                                                                                                   |

### Environment variables

None. PDL roots are pinned to `metadata-models/src/main/pegasus/` (matching the workflow's scope) because the report tool delegates to `bump_schema_versions.py` for the dependency graph, which honors `PDL_ROOTS` if you've exported it.

### Examples

```bash
# Default: compares acryl-main vs. the latest stable `-cloud` release tag,
# prints to stdout. No flags needed — defaults self-maintain across releases.
python3 .github/scripts/report_aspect_changes.py

# Pin both refs explicitly (canonical v1.1.0 baseline → branch)
python3 .github/scripts/report_aspect_changes.py \
  --base v1.0.0rc1-cloud \
  --head acryl-main

# Tag → tag (audit a single release cut)
python3 .github/scripts/report_aspect_changes.py \
  --base v1.0.0-cloud --head v1.0.1-cloud

# Branch → branch (compare a hotfix branch to trunk)
python3 .github/scripts/report_aspect_changes.py \
  --base origin/releases/v1.0.0 --head origin/acryl-main

# Commit → commit (slice an exact SHA range)
python3 .github/scripts/report_aspect_changes.py \
  --base 30f791d121 --head 8a0456e560

# Ref-spec (preview the last 30 commits)
python3 .github/scripts/report_aspect_changes.py --base HEAD~30 --head HEAD

# Per-PR audit mode — classify every PDL-touching PR in isolation, aggregate
# (use this to enforce the v→v+1-per-justifying-PR invariant; catches PR
# #9579-style spurious bumps that wide cumulative windows mask)
python3 .github/scripts/report_aspect_changes.py \
  --base v1.0.0rc1-cloud --head HEAD --per-pr

# Save the report to a file for sharing
python3 .github/scripts/report_aspect_changes.py --output report.md

# Run via the GitHub Actions workflow (renders into $GITHUB_STEP_SUMMARY)
# Actions → "PDL Aspect Change Report" → Run workflow, leave inputs blank
```

## How it works

### Step 1 — Resolve baseline

If `--base` is omitted, the tool first tries:

```
git tag --list 'v*-cloud' --sort=-v:refname
```

then filters out any tag whose name contains `rc`, and returns the first remaining tag (highest version stable `-cloud` release). If every matching tag is an rc tag (e.g., early in a new release cycle before any stable has shipped), it falls back to the same lookup **without** the rc filter, returning the latest `-cloud` tag of any kind.

**Why a global tag enumeration rather than `git describe`?** Stable `-cloud` releases in this repo are typically cut on parallel hotfix branches (e.g. `origin/hotfixes/v1.0.0`) that are **not ancestors of `acryl-main`**. An ancestry-based lookup via `git describe` would never see them. The global `git tag --list` lookup finds the latest shipped release regardless of branch topology.

Customer-suffix tags (e.g. `-crucible-trustpilot`, `-raleigh`) are excluded by construction via the `v*-cloud` match pattern. The defaults self-adjust as new releases ship — no code change at release-cut.

### Step 2 — List changed PDL files

```
git diff --name-only <base>..<head> -- metadata-models/src/main/pegasus
```

filtered to `*.pdl`. Files that exist on only one side (added or deleted) are still emitted.

### Step 3 — Classify each changed file

For every changed file the classifier compares base vs. head content and emits findings against **five breaking-change criteria** plus **two `schemaVersion` anomalies**:

| #   | Criterion                                                                 | Bucket   |
| --- | ------------------------------------------------------------------------- | -------- |
| 1   | Removed fields                                                            | breaking |
| 2   | Renamed record without `@renamedFrom` annotation                          | breaking |
| 3   | `optional → required` flip                                                | breaking |
| 4   | Enum value removal                                                        | breaking |
| 5   | Field type change                                                         | breaking |
|     | Added required field                                                      | breaking |
|     | **Structural change without `schemaVersion` bump**                        | breaking |
|     | Added optional field                                                      | additive |
|     | Added enum value                                                          | additive |
|     | `required → optional` flip                                                | noisy    |
|     | Renamed record **with** `@renamedFrom` annotation                         | noisy    |
|     | **`schemaVersion` bump without structural change** (the PR #9579 pattern) | noisy    |

### Step 4 — Walk the dependency graph (transitive impact)

For each changed **non-aspect** record, the tool delegates to `bump_schema_versions.py`'s reverse-include graph + BFS to find every aspect that depends on the record via PDL `includes` OR field-type references. Those aspects are surfaced even if they weren't directly edited.

### Step 5 — Reclassify directly-edited aspects

Aspects that were both directly edited AND reached by the BFS get their `bump_status` re-evaluated with transitive context — so an aspect bumped _because_ a sibling non-aspect changed in the same PR is correctly classified as `bump_done` (legitimate), not `bump_spurious`.

### Step 6 — Per-aspect bump-status classification

Each aspect ends with one of four bump statuses:

| Status          | Trigger                                                                                  |
| --------------- | ---------------------------------------------------------------------------------------- |
| `bump_done`     | `head schemaVersion > base AND a real change exists` (direct or transitive). Legitimate. |
| `bump_needed`   | change exists but version NOT bumped — silent migration hazard.                          |
| `bump_spurious` | version bumped with NO schema change at all (auto-bumper side-effect).                   |
| `not_sure`      | version regressed (head < base) — manual review.                                         |

`schemaVersion` defaults to 1 when absent, matching [`bump_schema_versions.md`](bump_schema_versions.md) semantics.

## Output

Markdown only. Sections always emitted (or replaced with a "no aspect changes" message if the diff is empty):

1. **Header** — base ref, head ref + short SHA, generation timestamp (UTC).
2. **Summary** — single line with file/aspect/transitive/BREAKING/additive/noisy counts.
3. **Bump status** — single line with `bump_done` · `bump_needed` · `bump_spurious` · `not_sure` counts.
4. **Bump status legend** — collapsible `<details>` block explaining each bucket.
5. **Bump status breakdown** — one markdown table per status (`bump_done`, `bump_needed`, `bump_spurious`, `not_sure`), each headed with the count and rendered as `PDL file | PR(s) | Owner | Date | Why` rows. Owner is the name of the last commit author who touched the file in the window; transitively-affected aspects (file not directly edited) show `(no-PR)` and `(no owner — file wasn't directly touched)` in those cells. A file touched by multiple PRs in the window shows them comma-separated in the PR column. Empty buckets render as `_(none)_`.

Then per-bucket sections (each omitted when empty), in fixed order:

| #   | Section                            | What lands here                                                                       | Bullet style          |
| --- | ---------------------------------- | ------------------------------------------------------------------------------------- | --------------------- |
| 1   | `## ⚠️ BREAKING`                   | Hits any of the 5 breaking criteria + structural-change-without-bump.                 | Full per-finding      |
| 2   | `## Transitively affected aspects` | Aspects pulled in via include / field-type from a changed non-aspect record.          | Full per-finding      |
| 3   | `## Additive`                      | New optional fields, new enum values, new files.                                      | Full per-finding      |
| 4   | `## Noisy`                         | `required→optional` flips · `renamedFrom` renames · `schemaVersion` bumps w/o change. | Full per-finding      |
| 5   | `## No logical change`             | File in git diff but no aspect-relevant difference (comment/whitespace).              | Collapsed (path only) |

Per-finding bullet format:

```markdown
- [ASPECT] `metadata-models/.../DatasetProperties.pdl` (abc1234 commit subject)
  - aspect name: `datasetProperties`
  - affected via: ChangedRecord (only for transitively-affected)
  - PR: #9579, #9000 (every PR touching this file in window)
  - bump: bump_done | bump_needed | bump_spurious | not_sure
  - `⚠ BREAKING`: removed field: deprecated
  - `+ additive`: added optional field: lastModifiedSource: optional string
  - `· noisy`: schemaVersion 2→3 bumped with NO structural change
```

`[ASPECT]` vs `[nested]` tag distinguishes aspect files from plain records (the latter are only shown when transitively pulled in, or as additive new files).

### Example output (excerpt — canonical v1.1.0 baseline)

```
# @Aspect PDL change report

**Base:** `v1.0.0rc1-cloud`
**Head:** `HEAD` (sha: `59a7c8b2d2`)
**Generated:** 2026-05-18T12:39:57Z

**Summary:** 79 PDL files changed · 5 aspects directly changed · 22 aspects transitively affected · **28 BREAKING** · 33 additive · 0 noisy

**Bump status:** 14 bump_done · **5 bump_needed** · **0 bump_spurious** · 0 not_sure

**Bump status breakdown:**

### `bump_done` (14)

| Aspect                       | PR(s)               | Owner                 |
| ---------------------------- | ------------------- | --------------------- |
| ActionRequestInfo.pdl        | #9555, #9579        | Nick Adams            |
| AssertionInfo.pdl            | #9555, #9579        | Nick Adams            |
| DataHubAiConversationInfo.pdl| #9585               | Nick Adams            |
| DataHubIngestionSourceInfo.pdl| #9509              | Sergio Gómez Villamor |
| ... (rows trimmed for brevity)                                  |

### `bump_needed` (5)

| Aspect                     | PR(s)   | Owner                                       |
| -------------------------- | ------- | ------------------------------------------- |
| DataHubOAuthClientInfo.pdl | #9311   | Nick Adams                                  |
| DataHubTaskInfo.pdl        | #9242   | John Joyce                                  |
| GlobalSettingsInfo.pdl     | #9272   | Chris Collins                               |
| MonitorSuiteInfo.pdl       | (no-PR) | _(no owner — file wasn't directly touched)_ |
| RecommendationModule.pdl   | (no-PR) | _(no owner — file wasn't directly touched)_ |

### `bump_spurious` (0)

_(none)_

### `not_sure` (0)

_(none)_
```

Notes on this run:

- `bump_needed: 5` is the actionable signal — 5 aspects whose schema changed between `v1.0.0rc1-cloud` and HEAD but were never bumped (silent migration hazards).
- `bump_spurious: 0` here is **expected** at this window width — the per-PR spurious bumps from PR #9579 (visible in narrower windows like `v1.0.1-crucible-trustpilot..acryl-main`) are absorbed by other legitimate changes in the wider window. See [Choosing the window](#choosing-the-window-cumulative-diff-trade-offs) for the full nuance.

## Choosing the window: cumulative-diff trade-offs

The tool runs against **one cumulative diff** (`base..head`), and the classification is computed on that single diff — not per-PR. This is fast and matches how a human reviewer would scan a release window, but it means **window width changes what you see**.

### Narrow window — catches per-PR sins accurately

A window that spans only one (or a few) PRs gives faithful per-PR classification. Example: `v1.0.1-crucible-trustpilot..acryl-main` contains only PR #9579 (9 schemaVersion bumps with no schema changes) and PR #9585 (a real field addition + a justified bump).

In this window the report correctly labels:

- 9 aspects from PR #9579 as **`bump_spurious`** (lone `schemaVersion 2→3` edits, nothing else)
- 1 aspect from PR #9585 as **`bump_done`** (transitively justified by a sibling non-aspect change in the same PR)

Use a narrow window for the **release-window monitor** — this is the canonical use case.

### Wide window — masks per-PR sins

A window that spans many releases bundles every PR's effects into one cumulative diff. Legitimate structural changes from one PR can absorb spurious bumps from another PR, so the bumper anti-pattern disappears from the report.

Example: in `v0.3.15rc1-fis..HEAD` (~4,600 commits, ~2 years), the same 9 aspects from PR #9579 are reported as **`bump_done`** instead of `bump_spurious`. The reason:

- `AssertionInfo.pdl` references `AssertionType` and `AssertionNote` as field types.
- Both of those non-aspect records changed at some point in the 2-year window (different PRs).
- The transitive BFS sees the aspect's deps as having changed, so `transitively_affected = True`.
- The aspect's `schemaVersion` did move (`1 → 3` over the window), so it classifies as `bump_done`.

The wide-window view is telling you "across this 2-year window, AssertionInfo's bump is justified by _some_ PR in the window" — even if PR #9579 specifically wasn't one of them.

This is correct cumulative-diff behavior. It is **not** a refutation of the narrow-window finding: the spurious bumps in PR #9579 are still spurious — they just don't stand out at this scale.

### When to pick which window

| Use case                                                        | Window                                            |
| --------------------------------------------------------------- | ------------------------------------------------- |
| Release-window monitor (e.g. v1.1.0 audit)                      | `--base <previous-release-tag> --head acryl-main` |
| Single-PR audit (preview a change before merge)                 | `--base <sha-before-PR> --head <sha-after-PR>`    |
| Hotfix-vs-trunk comparison                                      | `--base <hotfix-branch> --head acryl-main`        |
| "What does our schema look like across the whole product line?" | `--base v0.3.<oldest> --head acryl-main`          |

For the multi-release retrospective case, **expect `bump_spurious` to be undercounted** — the cumulative diff cannot tell you which PR introduced each spurious bump, and most spurious bumps will be re-classified as `bump_done` by some later legitimate change to the same aspect or one of its deps.

### Getting true per-PR truth at scale — `--per-pr` mode

To accurately label every PR across a long window, use the built-in `--per-pr` flag. It enumerates every first-parent commit in `base..head` that touched a PDL, runs the full classifier on each `parent..commit` slice in isolation, then aggregates the verdicts per aspect.

```bash
python3 .github/scripts/report_aspect_changes.py \
  --base v1.0.0rc1-cloud --head HEAD --per-pr
```

This catches the **v→v+1-per-justifying-PR** invariant: an aspect lands in the "unjustified bumps" bucket whenever any single PR-slice classified its bump as `bump_spurious`, even if a different PR in the same window contributed a legitimate change.

The per-PR report has a different shape from the cumulative report:

- **Header** — base/head/sha/timestamp.
- **Audit line** — `N PR-slice(s) examined · M aspects with bump-relevant verdicts · X with unjustified bumps · Y with missing bumps · Z fully justified`.
- **`⚠️ Aspects with at least one unjustified bump (bump_spurious)`** — per-aspect H3 section, each with a table of every PR-slice that touched it (`PR | sha | bump | author | commit`).
- **`🟡 Aspects with at least one missing bump (bump_needed)`** — same shape, for silent migration hazards.
- **`✅ Aspects with all bumps justified`** — compact summary table (`Aspect | Bumps | Justifying PR(s)`).

Cost: O(N_PDL_touching_commits) git invocations and re-runs of the classifier. For a typical 30-day release window (~20 PRs touching PDLs), runtime is ~10s. A multi-year window (~75 PR-touching commits) is ~60s. Skip it for fast pre-merge previews; use it for release-window audits and retrospectives.

## Per-PR catch-up reclassification (schemaVersion debt model)

Per-PR verdicts only make sense in **chronological context** — a bump-without-change PR is legitimate catch-up when there's a prior unbumped real change to pay for, and truly spurious when there isn't. Without this reconciliation, every late-bumping PR would be reported as `bump_spurious` even when it's correctly fixing earlier missed bumps.

The tool applies this reconciliation to every per-PR audit (both `--per-pr` mode and the per-PR override that enriches the default cumulative report). For each PDL file, slices are walked in commit-date order (oldest first), maintaining a `pending_prs` list — the PRs that contributed a real schema change without their own bump. A single subsequent bump-without-change clears the **entire** pending list (one catch-up bump pays any amount of accumulated debt). Once debt is cleared, further bumps without a change stay `bump_spurious`.

### Reclassification rules

| Slice verdict in isolation                                           | Effect on `pending_prs` | Reclassified verdict                                                                          |
| -------------------------------------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------- |
| `bump_needed` (change, no bump)                                      | append the PR           | stays `bump_needed` — preserves the missing-bump hygiene signal for the change-author         |
| `bump_spurious` (bump, no change) **with** non-empty `pending_prs`   | reset to `[]`           | **`bump_done`** (catch-up); records the cleared PRs in a `catch_up_for_prs` annotation        |
| `bump_spurious` (bump, no change) **with** empty `pending_prs`       | unchanged               | stays `bump_spurious` — truly unjustified                                                     |
| `bump_done` (own change + own bump) **with** non-empty `pending_prs` | reset to `[]`           | stays `bump_done`; the bump implicitly clears prior debt too (annotation records cleared PRs) |
| `bump_done` (own change + own bump) **with** empty `pending_prs`     | unchanged               | stays `bump_done`                                                                             |

### Worked example — `Status.pdl` in `v1.0.0rc1-cloud..HEAD`

Three PRs touched `Status.pdl`:

| PR    | Date       | Slice in isolation              | `pending_prs` after | Reclassified                         |
| ----- | ---------- | ------------------------------- | ------------------- | ------------------------------------ |
| #9192 | 2026-05-13 | added `lifecycleState`, no bump | `[#9192]`           | `bump_needed`                        |
| #9534 | 2026-05-14 | bumped 1→2, no change           | `[]`                | **`bump_done`** (catch-up for #9192) |
| #9579 | 2026-05-15 | bumped 2→3, no change           | `[]` (no debt)      | `bump_spurious`                      |

Without reclassification, both #9534 and #9579 would be `bump_spurious`. With it, only #9579 — the genuinely anomalous PR — is flagged.

### Effect on the file's cumulative bucket

The reclassified slice verdicts feed the per-PR aggregator that drives each file's bucket in the cumulative report. The aggregator honors catch-up reconciliation: a `bump_needed` slice whose PR appears in any later slice's `catch_up_for_prs` is no longer treated as an unpaid debt and does not push the file into `bump_needed`. Priority order (highest wins): `bump_spurious > bump_needed > bump_done > bump_not_needed`.

**Per-PR truth drives the bucket** — when an unreconciled spurious slice exists in the window, the file lands in `bump_spurious` even if cumulative-diff math would call the window `bump_done`. Reviewers see the per-PR breakdown table for the file to identify which specific PR was at fault.

For `Status.pdl`:

- Per-PR aggregator returns `bump_spurious` (because #9579 remains an unreconciled spurious bump after catch-up — the debt was already cleared by #9534)
- Cumulative classifier returns `bump_done` (real change + bump exist in the cumulative window)
- **Outcome:** per-PR truth wins → the file is reported as `bump_spurious`. The bucket row carries the bucket's plain Why text; reviewers consult the file's per-PR breakdown table to see which PR did the unreconciled spurious bump (#9579).

For a file whose only `bump_needed` slices were all caught up (e.g. `DynamicFormAssignment.pdl` where #9465 was caught up by #9560):

- Per-PR aggregator returns `bump_done` (no unreconciled NEEDED, no unreconciled SPURIOUS, at least one DONE slice)
- File lands in `bump_done` with the plain bucket Why text. The catch-up linkage is visible in the file's per-PR breakdown table.

## Exit codes

| Code | Meaning                                                                                                    |
| ---- | ---------------------------------------------------------------------------------------------------------- |
| `0`  | Success (including "no aspect changes in this window").                                                    |
| `1`  | Auto-baseline resolution failed — no `v1.0*` non-rc tag reachable from `--head`. Pass `--base` explicitly. |
| `2`  | Git error (e.g. bad `--base` ref, shallow clone missing tags). Stderr carries the original git message.    |

There is **no exit-code signal for breaking changes** — the report is informational. Treat the rendered markdown as the source of truth and gate on `0 BREAKING` and `0 bump_needed` in the summary line.

## Release Pipeline integration

The companion workflow [`.github/workflows/pdl-change-report.yml`](../workflows/pdl-change-report.yml) is a `workflow_dispatch` runner that:

- Optionally accepts `base` and `head` ref inputs (blank → auto-defaults)
- Optionally accepts a `per_pr` boolean toggle — when checked, runs the per-PR audit mode (see [Getting true per-PR truth at scale](#getting-true-per-pr-truth-at-scale---per-pr-mode))
- Runs the script with full git history and tags fetched
- Writes the markdown directly to `$GITHUB_STEP_SUMMARY` so it renders on the Actions run page

To trigger: **Actions → "PDL Aspect Change Report" → Run workflow**. Tick the **`per_pr`** checkbox to switch to per-PR audit mode for the chosen window.

If you want to keep the report alongside release notes, redirect `--output` to a file under `docs/managed-datahub/release-notes/` or upload it as a workflow artifact (one extra `actions/upload-artifact` step).

## Scope and limitations

- **Heuristic regex-based parsing.** The classifier does not use a full PDL grammar. Edge cases that aren't valid PDL (which would fail codegen anyway) are silently skipped — false negatives are not expected on syntactically-valid PDLs, but a future PDL syntax extension may require updates.
- **PDL root pinned to `metadata-models/src/main/pegasus/`.** Other PDL sources (e.g. plugin modules) aren't scanned. `bump_schema_versions.py` supports `PDL_ROOTS`; if you set it, that override flows through.
- **Transitive walk reads the working tree.** `find_transitively_affected_aspects` (in `bump_schema_versions.py`) walks files on disk, not at arbitrary git refs. The workflow always checks out the head ref, so the default invocation is safe; if you ever invoke with `--head <some-other-ref>` while the working tree is checked out elsewhere, the transitive closure reflects the checkout rather than `head`.
- **Single-hop "affected via" labels.** When a transitively-affected aspect's content references the changed non-aspect directly, the trail names that record. For multi-hop chains (Aspect → Intermediate → ChangedRecord), the label falls back to a generic "transitive dependency" — the BFS still surfaces the aspect correctly, but the proximate-hop name isn't displayed.
- **Cumulative-diff classification.** Findings reflect the cumulative `base..head` diff, not per-PR slices. Wide windows can therefore mask per-PR `bump_spurious` cases when legitimate changes from other PRs in the window justify the same version bump. See [Choosing the window](#choosing-the-window-cumulative-diff-trade-offs) above for the full nuance and the recipe for true per-PR analysis.
- **Read-only.** This script never writes under `metadata-models/`. Compare with `bump_schema_versions.py`, which rewrites `schemaVersion` annotations in place.
- **Tests.** Coverage is in `.github/scripts/test/test_report_aspect_changes.py` (59 unit + smoke tests).
