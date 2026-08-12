# Playwright Shard Rebalancing: Analysis & Options

**Date**: 2026-08-12
**Context**: [PR #19081](https://github.com/datahub-project/datahub/pull/19081) enabled
`workers_per_shard=2`. Follow-up from Slack (platform-reviews, 2026-08-11) flagged that shards
6-8 consistently run ~2x longer than shards 1-5.
**Data source**: `docker-unified.yml` run
[31564824138](https://github.com/datahub-project/datahub/actions/runs/31564824138) (2026-08-12,
master, `success`), 8 shards, `playwright-junit-*` artifacts.

---

## Summary

Playwright's built-in `--shard=N/M` balances shards by **test count only** — it has no notion of
duration. Each shard here gets ~51 tests regardless of how long they take. The result:

| Shard | Wall clock | Sum of test durations | Tests | Files |
| ----- | ---------- | --------------------- | ----- | ----- |
| 1     | 7m46s      | 292.3s                | 51    | 20    |
| 2     | 8m35s      | 354.1s                | 51    | 16    |
| 3     | 8m51s      | 332.4s                | 55    | 9     |
| 4     | 8m04s      | 228.9s                | 47    | 1     |
| **5** | **11m28s** | **613.2s**            | 51    | 15    |
| **6** | **10m25s** | **485.4s**            | 51    | 12    |
| **7** | **10m32s** | **420.2s**            | 51    | 14    |
| **8** | **10m05s** | **416.0s**            | 51    | 11    |

Total test-time across all 94 spec files: 3142.4s → an even split would put each shard at ~393s
(~6.5m) of test-time. Shards 5-8 are 1.35x-2.1x over that ideal; shards 1-4 are 0.6x-0.9x under it.

### Why the imbalance is concentrated, not spread out

Playwright discovers spec files in (roughly) alphabetical path order and slices that ordered list
into N contiguous chunks of equal test count. It never looks at how long a test took last time.
So a shard runs slow specifically when several **duration-heavy directories happen to sit next to
each other alphabetically** and land in the same chunk:

- **Shard 5** (613s, worst shard): `documents/document-management.spec.ts` (255.7s / 11 tests =
  23.2s/test), `glossary/v2-glossary.spec.ts` (102.4s / 6), `glossary/v2-glossary-navigation.spec.ts`
  (60.0s / 3). `documents/` and `glossary/` are alphabetically adjacent — both heavy dirs end up in
  the same shard.
- **Shard 6** (485s): `lineage-v3/v3-lineage-impact-analysis.spec.ts` (102.2s / 9),
  `lineage-v3/v3-lineage-graph.spec.ts` (83.3s / 12), plus `ingestion-v2/sources.spec.ts` (57.6s / 5),
  `ingestion-v3/sources.spec.ts` (53.1s / 5), and both `secrets-tab.spec.ts` files (45.0s + 43.3s).
  `ingestion-*` → `lineage-v3` is another alphabetically-adjacent heavy run.
- **Shard 7/8**: `search/*.spec.ts` (query-and-filter-search, search, search-filters, searchv2 —
  four separate heavy search specs split across 7 and 8), plus `structured-properties/*` and
  `stats-v2/*`.
- By contrast, **shard 4** is a single 47-test file (`change-history/change-history.spec.ts`) at a
  cheap 4.9s/test average, and **shard 1**'s 51 tests average 5.7s/test — light content filling the
  same test-count quota.

Full per-shard top files are in the Appendix.

### What this means

This is not "a few slow tests scattered around" — it's "duration-heavy spec directories cluster
alphabetically, and count-based sharding has no way to see that." Any fix has to either (a) change
_where_ those heavy directories land in the discovery order, or (b) stop relying on contiguous
count-based slicing altogether.

---

## Option A — Rename/reorder specs to interleave heavy and light directories

Prepend numeric/lettered prefixes to test directories (or files) so that, in alphabetical
discovery order, heavy-duration directories are spread out rather than clustered. E.g. move
`documents/` and `glossary/` apart, and interleave `ingestion-v2`/`ingestion-v3`/`lineage-v3` with
lighter directories.

**Pros**

- Smallest possible diff — renames only, no workflow/script changes.
- No new infrastructure to maintain; matches what was already being prototyped locally
  (per Slack: "checking the highest runtime tests for division and file name rename approach").
- Playwright's existing sharding logic is untouched — behavior stays fully standard/predictable.

**Cons**

- Fragile: it's tuned to _today's_ mix of tests. A new heavy spec file added later in the "wrong"
  alphabetical slot silently re-creates the imbalance, with no signal that it happened.
- Renaming directories touches import paths, CI artifact names in historical logs, and possibly
  test-report groupings/dashboards that key off directory names.
- Balancing is approximate — since Playwright still slices by test _count_ per shard, not
  duration, you're manually reasoning about where count-boundaries fall; any change in test count
  per file shifts the boundaries and can undo the interleaving.
- No mechanism to detect drift; needs a human to periodically re-check shard timings and re-shuffle.

---

## Option B — `PWTEST_SHARD_WEIGHTS` environment variable

Playwright's internal `filterForShard` (`node_modules/playwright/lib/runner/testGroups.js`)
accepts weights per shard via `PWTEST_SHARD_WEIGHTS` (comma-separated, one per shard, must match
`shard_count`). Weights change how many tests each shard gets — e.g. give shard 5 a weight of 0.7
(fewer tests) and shard 1 a weight of 1.3 (more tests) so the lighter-per-test shards absorb more
volume.

```yaml
# resusable-playwright-tests.yml, illustrative — needs real calibration
env:
  PWTEST_SHARD_WEIGHTS: '1.3,1.15,1.1,1.35,0.7,0.85,0.9,0.9'
```

**Pros**

- Zero file renames — a pure workflow/env change.
- Reversible instantly (delete the env var to go back to even count-splitting).
- Weights can be derived directly from the `sum of test durations` column above (target ~393s/shard).

**Cons**

- `PWTEST_SHARD_WEIGHTS` is an **undocumented, internal** Playwright env var (found by reading
  `testGroups.js` directly, not in public docs) — no compatibility guarantee across Playwright
  version bumps; could silently stop working or change semantics on an upgrade.
- Still count-based under the hood, just with uneven targets — it doesn't fix the _root cause_
  (no duration awareness), it only compensates for the current snapshot. Same drift problem as
  Option A: add a new heavy file anywhere and the weights need recalibrating.
- The mapping from "weight" to "wall-clock seconds saved" isn't exact — weights control test
  _count_ per shard, and per-test duration varies within a shard too, so calibration is iterative
  (run, measure, adjust) rather than solvable in closed form from this data alone.
- Because it depends on file-discovery order too (same underlying algorithm), the calibrated
  weights are just as sensitive to future test additions as Option A — arguably more opaque, since
  a future reader has no idea why the weights are those specific numbers without this doc.

---

## Option C — Explicit weight-based grouping (LPT bin-packing), like the existing test-weight system

This repo already solves an equivalent problem for smoke/gradle/ingestion suites: a nightly
`update-test-weights.yml` workflow downloads recent CI artifacts, runs
`.github/scripts/generate_test_weights.py` to produce `*_test_weights.json` files, and opens a PR
when weights drift >5%. Those weight files feed batch-balancing elsewhere in CI. Playwright is
notably **not** part of this system today.

Proposed shape:

1. Extend `generate_test_weights.py` (or add a Playwright-specific variant) to parse
   `playwright-junit-*` artifacts and emit `e2e-test/ui/playwright/playwright_test_weights.json`
   keyed by spec file path → duration (the same shape as the numbers in this doc's Appendix).
2. Add a small script (e.g. `scripts/generate_playwright_shard_groups.py`) that reads the weights
   file and greedily bin-packs spec files into `shard_count` balanced groups using
   Longest-Processing-Time-first (LPT) — sort files by duration descending, always add the next
   file to the currently-lightest group. LPT is a good fit here: few, unevenly-sized "items"
   (spec files with widely varying duration) rather than many uniform ones.
3. Change the CI workflow step (`resusable-playwright-tests.yml` line ~181) from
   `--shard="$SHARD/$SHARD_COUNT"` to passing the explicit list of spec files for that shard index
   (Playwright accepts explicit file path args in place of `--shard`), reading the group from a
   generated `shard-groups.json` artifact produced once in the `setup` job and fanned out to each
   shard job.
4. Wire the new weights file into the existing `update-test-weights.yml` cron so it self-heals
   weekly as tests are added/changed, same as smoke/gradle/ingestion do today.

**Pros**

- Fixes the actual root cause: shard composition becomes duration-aware, not count-and-order-aware.
- Self-healing — the existing weekly cron infra already exists; this plugs into a proven pattern
  rather than inventing a new one.
- Robust to new tests: a new heavy spec file gets bin-packed correctly next week automatically,
  no manual re-shuffling required.
- No dependency on Playwright's undocumented internals (Option B) or manual alphabetical reasoning
  (Option A).

**Cons**

- Largest change of the three: new script, new artifact hand-off between `setup` and per-shard
  jobs in `resusable-playwright-tests.yml`/`playwright-e2e-tests.yml`/`docker-unified.yml`, plus
  the weight-generation addition to `update-test-weights.yml`.
- Passing explicit file lists instead of `--shard` changes how Playwright reports "shard N of M" in
  its own output/HTML report — the `reusable-playwright-report.yml` merge step may need adjustment
  to keep report grouping sensible.
- Needs a bootstrap weights file checked in before the cron has run once (or a first manual
  `generate_test_weights.py` run against this analysis's artifacts).
- More moving parts to review/maintain long-term, though arguably that's the cost of the only
  option that doesn't need periodic manual re-tuning.

---

## Recommendation shape (for whoever decides)

- If the goal is "land something today with minimal risk": **A or B**.
- If the goal is "stop revisiting this every few months as the suite grows": **C**, following the
  precedent already established for smoke/gradle/ingestion test weights.
- A or B could also be a stopgap now, with C tracked as a follow-up ticket — consistent with how
  the `no-wait-for-timeout` lint rollout is being staged (enable + ignore-list now, cleanup ticket
  later).

---

## Appendix — full top-duration files per shard (this run)

```
shard 1: 292.3s test-time, 51 tests, 20 files
   49.6s    2t  documents/document-tree-expand.spec.ts
   42.6s   11t  home/homepage-modules.spec.ts
   25.6s    3t  application/application-management.spec.ts
   22.9s    2t  analytics/analytics.spec.ts
   22.7s    6t  ml-entities-v2/v2-model-mlflow.spec.ts
   20.3s    2t  entity-pages/summary-tab/v2-summary-tab-about-section.spec.ts

shard 2: 354.1s test-time, 51 tests, 16 files
   75.6s    5t  settings-v2/v2-managing-groups.spec.ts
   58.4s    2t  settings-v2/v2-manage-policies.spec.ts
   48.9s    2t  settings-v2/v2-home-page-posts.spec.ts
   42.1s    5t  application/application-sidebar.spec.ts
   41.4s    4t  upload-files/upload-files.spec.ts
   20.6s    5t  navbar/navbar.spec.ts

shard 3: 332.4s test-time, 55 tests, 9 files
  184.7s   26t  onboarding/welcome-modal.spec.ts
   78.0s   10t  browse-v2/browse-v2.spec.ts
   21.5s    2t  autocomplete-v2/autocomplete.spec.ts
   19.0s    6t  auth/login.spec.ts
    9.0s    2t  home/homepage-collection-view-all.spec.ts
    8.1s    1t  structured-properties/structured-props-drawer-font.spec.ts

shard 4: 228.9s test-time, 47 tests, 1 file
  228.9s   47t  change-history/change-history.spec.ts

shard 5: 613.2s test-time, 51 tests, 15 files   <-- worst
  255.7s   11t  documents/document-management.spec.ts
  102.4s    6t  glossary/v2-glossary.spec.ts
   60.0s    3t  glossary/v2-glossary-navigation.spec.ts
   33.0s    5t  domains-v2/v2-domains-advanced.spec.ts
   30.0s    5t  incidents-v2/v2-incidents.spec.ts
   28.0s    4t  domains-v2/v2-domains-core.spec.ts

shard 6: 485.4s test-time, 51 tests, 12 files
  102.2s    9t  lineage-v3/v3-lineage-impact-analysis.spec.ts
   83.3s   12t  lineage-v3/v3-lineage-graph.spec.ts
   57.6s    5t  ingestion-v2/sources.spec.ts
   53.1s    5t  ingestion-v3/sources.spec.ts
   45.0s    4t  ingestion-v2/secrets-tab.spec.ts
   43.3s    4t  ingestion-v3/secrets-tab.spec.ts

shard 7: 420.2s test-time, 51 tests, 14 files
   88.5s   10t  search/query-and-filter-search.spec.ts
   59.6s    7t  search/search.spec.ts
   56.5s    7t  mutations-v2/v2-edit-documentation.spec.ts
   55.6s    4t  mutations-v2/v2-domains.spec.ts
   41.8s    5t  search/search-filters.spec.ts
   26.9s    5t  search/searchv2.spec.ts

shard 8: 416.0s test-time, 51 tests, 11 files
  106.0s   10t  search/searchv2.spec.ts
   57.2s    3t  structured-properties/schema-field.spec.ts
   53.8s    7t  stats-v2/v2-change-history.spec.ts
   50.4s    4t  structured-properties/entity-level.spec.ts
   46.3s   10t  stats-v2/v2-charts.spec.ts
   31.2s    6t  siblings-v2/siblings.spec.ts
```

Raw per-file totals across all shards (top 30 by summed duration, some files span 2 shards when a
suite's tests straddle a shard boundary):

```
255.7s  ( 11 tests)  shard(s) [5]     documents/document-management.spec.ts
251.9s  ( 50 tests)  shard(s) [4, 5]  change-history/change-history.spec.ts
184.7s  ( 26 tests)  shard(s) [3]     onboarding/welcome-modal.spec.ts
132.9s  ( 15 tests)  shard(s) [7, 8]  search/searchv2.spec.ts
102.4s  (  6 tests)  shard(s) [5]     glossary/v2-glossary.spec.ts
102.2s  (  9 tests)  shard(s) [6]     lineage-v3/v3-lineage-impact-analysis.spec.ts
 88.5s  ( 10 tests)  shard(s) [7]     search/query-and-filter-search.spec.ts
 83.3s  ( 12 tests)  shard(s) [6]     lineage-v3/v3-lineage-graph.spec.ts
 78.0s  ( 10 tests)  shard(s) [3]     browse-v2/browse-v2.spec.ts
 75.6s  (  5 tests)  shard(s) [2]     settings-v2/v2-managing-groups.spec.ts
 60.0s  (  3 tests)  shard(s) [5]     glossary/v2-glossary-navigation.spec.ts
 59.6s  (  7 tests)  shard(s) [7]     search/search.spec.ts
 58.4s  (  2 tests)  shard(s) [2]     settings-v2/v2-manage-policies.spec.ts
 57.6s  (  5 tests)  shard(s) [6]     ingestion-v2/sources.spec.ts
 57.2s  (  3 tests)  shard(s) [8]     structured-properties/schema-field.spec.ts
 56.5s  (  7 tests)  shard(s) [7]     mutations-v2/v2-edit-documentation.spec.ts
 55.6s  (  4 tests)  shard(s) [7]     mutations-v2/v2-domains.spec.ts
 53.8s  (  7 tests)  shard(s) [8]     stats-v2/v2-change-history.spec.ts
 53.1s  (  5 tests)  shard(s) [6]     ingestion-v3/sources.spec.ts
 50.4s  (  4 tests)  shard(s) [8]     structured-properties/entity-level.spec.ts
 49.6s  (  2 tests)  shard(s) [1]     documents/document-tree-expand.spec.ts
 48.9s  (  2 tests)  shard(s) [2]     settings-v2/v2-home-page-posts.spec.ts
 46.3s  ( 10 tests)  shard(s) [8]     stats-v2/v2-charts.spec.ts
 45.0s  (  4 tests)  shard(s) [6]     ingestion-v2/secrets-tab.spec.ts
 43.3s  (  4 tests)  shard(s) [6]     ingestion-v3/secrets-tab.spec.ts
 42.6s  ( 11 tests)  shard(s) [1]     home/homepage-modules.spec.ts
 42.1s  (  5 tests)  shard(s) [2]     application/application-sidebar.spec.ts
 41.8s  (  5 tests)  shard(s) [7]     search/search-filters.spec.ts
 41.4s  (  4 tests)  shard(s) [2]     upload-files/upload-files.spec.ts
 33.0s  (  5 tests)  shard(s) [5]     domains-v2/v2-domains-advanced.spec.ts
```
