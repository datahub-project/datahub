# OSS CLI Release Skill

You are the release engineer for the acryldata/datahub OSS CLI. Your job is to safely prepare
and cut versioned releases of the `acryl-datahub` Python package and related plugins.

---

## Multi-Agent Compatibility

This skill works across Claude Code, Cursor, Copilot, Gemini CLI, Windsurf, and other agents.

**What works everywhere:**
- All scripts in `scripts/` (pure bash + `gh` CLI + `git`)
- All checklists and judgment criteria in this document

**Claude Code-specific features** (other agents can safely ignore):
- `/oss-release` slash command auto-loads this skill
- `finish` CI monitoring uses `/loop` + `ScheduleWakeup` for async polling
- Sub-agents can be dispatched for parallel diff analysis

**Scripts location:** All paths below are relative to `.agent-skills/oss-release/`.

---

## Subcommands

| Subcommand | Purpose | Time |
|------------|---------|------|
| `prep` | Diff analysis → safety check → cut RC pre-release | ~5 min |
| `finish` | Check CI on latest RC → confirm → cut final release | async |
| `status` | Show CI status for the latest RC, no action taken | instant |

**No version numbers required.** `prep` determines the next version automatically from existing
tags. `finish` and `status` auto-detect the most recent RC tag.

**RC releases are first-class deliverables.** `prep` alone is a complete workflow — use it
when cutting an RC for customer testing, staged rollout, or connector validation. `finish`
is called separately, potentially days or weeks later, when the team decides to promote.

---

## `prep` — Cut a Release Candidate

### Step 1 — Upstream Diff Analysis

Run:
```bash
.agent-skills/oss-release/scripts/compare-upstream.sh
```

This fetches `datahub-project/datahub` and `origin/master`, finds the merge base, and outputs:
- OSS commits not yet merged into the fork
- Fork-only (acryl-specific) non-merge commits since last sync
- File-level stats for `metadata-ingestion/` and `datahub-actions/`

**Report to user:**
- Merge base SHA
- Gap size (OSS ahead / fork ahead)
- Ingestion-relevant commits from each side (new connectors, fixes, dep changes)

If the fork is fully synced with OSS (zero gap), say so and continue.

### Step 2 — Safety Assessment

Assess the diff (judgment layer — no script needed):

**Dependency check**
- Did `metadata-ingestion/pyproject.toml`, `setup.py`, or `uv.lock` change?
- If yes: which packages, and are the changes pins, bumps, or removals?

**Breaking / API changes**
- Any validator signature changes (e.g. `pydantic_removed_field`) requiring call-site updates?
- Removed CLI commands, renamed config fields, changed defaults?

**Connector scope**
- New source connectors added? Any connectors removed or golden files deleted?

**OSS scope check**
- Are all changes consistent with an OSS sync + bug-fix release?
- Any acryl-proprietary code (cloud endpoints, internal tooling) that should not be in an
  OSS release?

**Produce a verdict:**

| Verdict | Meaning | Action |
|---------|---------|--------|
| ✅ SAFE | No concerns | Proceed to Step 3 |
| ⚠️ REVIEW NEEDED | Specific items need human review | Present items, ask how to proceed |
| ❌ BLOCKED | Do not release | List what must be fixed first, stop |

### Step 3 — Determine Version

Inspect existing tags to find the next version automatically:

```bash
git fetch origin --tags --quiet
git tag -l "v*" | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(rc[0-9]+)?$' | sort -V | tail -10
```

Version scheme: `v<major>.<minor>.<patch>.<seq>rcN`

- `major.minor.patch` tracks the OSS DataHub release train
- `seq` is an acryldata patch sequence (0, 1, 2, ...)
- `rcN` starts at `rc0`; increment if `rc0` already exists on this version

Logic:
1. Find the latest non-RC tag (e.g. `v1.5.0.7`) — that's the last release
2. The next RC is `v1.5.0.8rc0` (increment `seq`)
3. If `v1.5.0.8rc0` already exists, use `v1.5.0.8rc1`

Show the user the determined version before tagging: "Next RC: `v1.5.0.8rc0` — proceeding."

### Step 4 — Generate Release Notes

Structure (derive content from the Phase 1/2 diff — adapt sections to what's actually present):

```markdown
## v<version>rcN — Release Candidate

### Bug Fixes
- **<Component>**: <what changed and why it matters> ([#<PR>](<link>))

### Improvements
- **<Component>**: <description> ([#<PR>](<link>))

### Documentation
- <description> ([#<PR>](<link>))

### Full Changelog
https://github.com/acryldata/datahub/compare/v<prev>...v<version>rcN
```

**Include:** CLI fixes, connector behavior changes, new/removed connectors, dep bumps affecting
compatibility, deprecation warning improvements.

**Omit:** Pure frontend UI changes, backend-only GMS/ZDU changes with no CLI surface, CI
infrastructure changes, internal refactors with no behavior change.

### Step 5 — Tag and Create Pre-release

```bash
git tag v<version>rcN HEAD
git push origin v<version>rcN
gh release create v<version>rcN \
  --prerelease \
  --title "v<version>rcN" \
  --notes "<generated notes>"
```

Confirm the release URL. The RC is now a complete deliverable — CI workflows will trigger
automatically. The developer can ship this to a customer environment immediately.

**Done.** Do not watch CI or proceed to `finish` unless the user asks.

---

## `finish` — Promote an RC to Official Release

Called when the team decides an RC is ready to become the official release. The RC may have
been cut minutes ago or weeks ago.

### Step 1 — Resolve the RC

Auto-detect the latest RC tag:

```bash
git fetch origin --tags --quiet
git tag -l "v*rc*" | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+rc[0-9]+$' | sort -V | tail -5
```

Pick the most recent one. Resolve its commit SHA:

```bash
git rev-parse <latest-rc-tag>^{}
```

Show the user: "Latest RC: `v1.5.0.8rc0` (`<sha>`) — proceeding."

If something looks wrong (e.g. multiple recent RCs across different versions), ask the user
to confirm which one to promote before continuing.

### Step 2 — Check for commits since the RC

Before doing anything else, check whether `origin/master` has moved ahead of the RC commit:

```bash
git fetch origin master --quiet
RC_SHA=$(git rev-parse v<version>rcN^{})
NEW_COMMITS=$(git log --oneline ${RC_SHA}..origin/master --no-merges)
```

If `NEW_COMMITS` is non-empty, **stop and report** before proceeding:

```
⚠️  New commits on origin/master since <rc-tag>:

  <sha> <subject>
  <sha> <subject>
  ...

These commits will NOT be included in v<version> if you promote this RC.

Options:
  a) Promote this RC as-is — new commits ship in the next release
  b) Include these commits — run `/oss-release prep` to cut a new RC first,
     then come back and run `/oss-release finish <new-rc-tag>`
```

**If the user chooses (b): stop here.** Redirect them to `prep`. Do NOT proceed to CI
checking or release cutting. A new RC must be tagged and tested before any final release
that includes those commits. There is no shortcut from "include new commits" to a final
release — an RC is always required first.

If the user chooses (a), note which commits are being deferred and continue to Step 3.

If `NEW_COMMITS` is empty, note "No new commits since <rc-tag> — branch is clean" and proceed.

### Step 3 — Check CI on the RC

Run:
```bash
.agent-skills/oss-release/scripts/check-ci.sh <sha>
```

**Claude Code:** use `/loop` + `ScheduleWakeup` at 240s intervals if workflows are still running.
**Other agents:** poll with `sleep 60` between calls until `check-ci.sh` outputs `ALL DONE`.

If the RC is old (e.g. cut days ago), release workflows may have already completed — check
history directly:
```bash
gh run list --repo acryldata/datahub --limit 60 \
  --json name,status,conclusion,headSha,event \
  | python3 -c "
import json,sys
runs = json.load(sys.stdin)
sha = '<sha prefix>'
print([r for r in runs if r['headSha'].startswith(sha) and r['event']=='release'])
"
```

### Step 4 — Classify Failures

For each failed workflow, check `known-flaky-tests.md`.

**Pre-existing/flaky — safe to proceed — if ALL of:**
1. Listed in `known-flaky-tests.md`, OR same workflow passed on the same commit in another run
2. None of the changed files (from `compare-upstream.sh`) overlap the failing workflow's scope

**Requires investigation — if ANY of:**
- Not in `known-flaky-tests.md`
- No passing run exists on the same commit for that workflow
- Changed files overlap the failing workflow's scope

If a new flaky pattern is found, add it to `known-flaky-tests.md` (note it for a follow-up commit).

### Step 5 — Confirmation Gate (MANDATORY — never skip)

**STOP. Do not cut the final tag without explicit user confirmation.**

Present a summary:

```
CI Results for <rc-tag> (<sha>)

✓ Passed  (N): pypi-release metadata-ingestion, build & test, ...
✗ Failed  (N): spark smoke test [known flaky], test_nifi_ingest_cluster [known flaky]
… Running (N): (wait if any)

Key workflows:
  pypi-release metadata-ingestion : ✓ / ✗
  pypi-release datahub-actions    : ✓ / ✗
  Metadata Ingestion              : ✓ / ✗
  build & test                    : ✓ / ✗

Verdict: READY TO RELEASE / INVESTIGATE FIRST

Ready for me to cut v<version>?
```

Do not proceed until the user explicitly says yes.

### Step 6 — Generate Final Release Notes

Same structure as the RC notes but without "Release Candidate" framing:

```markdown
## v<version>

### Bug Fixes
...

### Full Changelog
https://github.com/acryldata/datahub/compare/v<prev>...v<version>
```

Write to a temp file:
```bash
cat > /tmp/release-notes-<version>.md << 'EOF'
<notes>
EOF
```

### Step 7 — Cut the Release

```bash
.agent-skills/oss-release/scripts/cut-release.sh <version> <sha> /tmp/release-notes-<version>.md
```

Confirm the final release URL to the user.

---

## `status` — Check CI Without Acting

Auto-detect the latest RC (same logic as `finish` Step 1) and show its CI status.
No tagging, no releasing.

```bash
git fetch origin --tags --quiet
LATEST_RC=$(git tag -l "v*rc*" | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+rc[0-9]+$' | sort -V | tail -1)
SHA=$(git rev-parse "${LATEST_RC}^{}")
.agent-skills/oss-release/scripts/check-ci.sh "$SHA"
```

Useful for checking whether the latest RC is passing before deciding to promote it.

---

## Permissions Required

Add to your `~/.claude/projects/<repo>/settings.local.json` (or approve interactively):

```json
{
  "permissions": {
    "allow": [
      "Bash(git tag:*)",
      "Bash(git push origin:*)",
      "Bash(gh release:*)",
      "Bash(gh run:*)"
    ]
  }
}
```

These are intentionally **not** in the shared `settings.json` — release operations require
deliberate opt-in per machine.

---

## Remember

1. **RC releases are complete deliverables.** `prep` alone ships a usable artifact.
2. **An RC is always required before a final release.** If new commits need to go in,
   cut a new RC first — there is no path from "include new commits" directly to a final tag.
3. **Never skip the confirmation gate in `finish`.** Even with all-green CI.
4. **The RC and final tag point to the same commit.** No new commits between them.
5. **`pypi-release metadata-ingestion` is the critical workflow.** Non-flaky failure here
   blocks the release regardless of everything else.
6. **Update `known-flaky-tests.md` when you find new flakes.** Future releases benefit.
7. **OSS-only scope.** No acryl-proprietary code in tags without `-acryl` suffix.
