---
name: meticulous
description: Diagnose Meticulous visual regression failures for a PR. Fetches diffs, clusters root causes, classifies real regressions vs false positives, and shows screenshots.
allowed-tools: Bash(npx @alwaysmeticulous/cli *), Bash(gh pr *), Bash(gh api *), mcp__plugin_playwright_playwright__browser_navigate, mcp__plugin_playwright_playwright__browser_take_screenshot
---

# Meticulous Failure Diagnosis

## Setup

Meticulous projects and their tokens:

- **DataHub UI** (datahub-project/datahub): token from env `METICULOUS_DATAHUB_TOKEN`
- **Acryl UI** (acryldata/datahub-fork): token from env `METICULOUS_ACRYL_TOKEN`

If env vars are not set, ask the user for the token.

## Your task

Diagnose Meticulous visual regression failures for PR `$ARGUMENTS` (or auto-detect from current branch if no argument given).

Follow these steps:

### Step 1 — Find the PR and test run

If `$ARGUMENTS` is a number, treat it as a PR number. Try both repos (datahub-project/datahub and acryldata/datahub-fork).

If no argument, run:

```bash
gh pr view --json number,title,url,headRefName 2>/dev/null
```

Find the Meticulous bot comment and extract the test run ID:

```bash
gh pr view <PR_NUMBER> [--repo <owner/repo>] --comments 2>&1 | grep -A 5 alwaysmeticulous
```

Look for the test-runs URL pattern: `test-runs/<TEST_RUN_ID>`.

If Meticulous shows ✅ 0 differences, report that and stop.

### Step 2 — Determine which token to use

- If PR is in `datahub-project/datahub` → use `METICULOUS_DATAHUB_TOKEN`
- If PR is in `acryldata/datahub-fork` → use `METICULOUS_ACRYL_TOKEN`

### Step 3 — Fetch all diffs

```bash
METICULOUS_API_TOKEN=<token> npx @alwaysmeticulous/cli agent test-run-diffs --testRunId <TEST_RUN_ID>
```

Parse the output. Each row is: `replayDiffId | screenshotName | index | total | outcome | mismatch | domDiffIds`

Group rows by `replayDiffId` — each group is one user flow replay.

### Step 4 — Sample and analyze DOM diffs

For each **unique replayDiffId** (not every screenshot — just one representative screenshot per replay), fetch the DOM diff in parallel:

```bash
METICULOUS_API_TOKEN=<token> npx @alwaysmeticulous/cli agent dom-diff \
  --replayDiffId <REPLAY_DIFF_ID> \
  --screenshotName <SCREENSHOT_NAME>
```

Look for patterns across replays:

- Same SVG path change → icon changed
- Same CSS class change → styling change
- URL/hostname changes → deployment false positive
- Text content changes → data or i18n change

### Step 5 — For ambiguous diffs, fetch the timeline

```bash
METICULOUS_API_TOKEN=<token> npx @alwaysmeticulous/cli agent timeline-diff \
  --replayDiffId <REPLAY_DIFF_ID>
```

This helps distinguish flakiness (asset load ordering) from real behavioral changes.

### Step 6 — Show a screenshot for each distinct root cause

For one representative diff per root cause, get image URLs:

```bash
METICULOUS_API_TOKEN=<token> npx @alwaysmeticulous/cli agent image-urls \
  --replayDiffId <REPLAY_DIFF_ID> \
  --screenshotName <SCREENSHOT_NAME>
```

Then navigate to the `diffImage` URL with Playwright and take a screenshot to show the user the highlighted pixel diff. If the diff is too subtle, also show before and after.

### Step 7 — Classify and report

For each root cause cluster, classify as:

- 🔴 **Real regression** — something broke that shouldn't have
- 🟡 **Intentional change** — visual change matches the PR's intent (approve in Meticulous)
- ⚪ **False positive** — environment/deployment artifact, not a code change

For each cluster, report:

- What changed (DOM diff summary)
- How many replays/screenshots affected
- Classification with reasoning
- **For real regressions**: the likely source file and a proposed fix
- **For false positives**: how to suppress it (Meticulous `elementsToIgnore` config, or `data-testid`)
- **For intentional changes**: confirm by correlating with the PR diff — suggest approving in Meticulous

### Step 8 — Correlate with PR changes (for real regressions)

```bash
gh pr diff <PR_NUMBER> [--repo <owner/repo>] 2>&1 | head -200
```

Match the DOM change to specific files changed in the PR. Open and read the relevant source file to understand the bug and propose a fix.

## Output format

```
## Meticulous Report — PR #<N>: <title>
<N> replay diffs, <N> screenshots affected

### Issue 1 — <short description> [🔴 Real regression | 🟡 Intentional | ⚪ False positive]
Affects: <N> replays (<replay IDs>)
What changed: <DOM diff summary>
<screenshot of diffImage>
Source: <file:line if applicable>
Fix: <proposed fix or action>

### Issue 2 — ...
```

If everything is intentional or false positives, say so clearly and suggest bulk-approving in Meticulous.
