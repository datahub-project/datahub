# `prep` — Cut a Release Candidate

End-to-end workflow for cutting an RC from the current `origin/master`. Read SKILL.md
first for prerequisites, dry-run gate behavior, and the subcommand overview.

## Step 0 — Repo preflight

Run this block before anything else. It ensures local state is fresh so version detection
and the changelog are based on the real current state, not stale local caches.

```bash
# If the user's invocation included --dry-run, thread it through via env var so
# Step 4 (cut-release) and any sub-scripts pick it up consistently.
# Example: user runs "/oss-release prep --dry-run" → export this now.
# export OSS_RELEASE_DRY_RUN=true

# Ensure working tree is clean
if ! git diff-index --quiet HEAD --; then
    echo "ERROR: uncommitted changes detected. Commit or stash first." && exit 1
fi

# Fetch origin: tags AND master (so next-version.sh and compare-upstream see real state).
# Tag fetch goes through safe-fetch-tags.sh which distinguishes benign "would clobber"
# warnings on legacy 3-segment tags (irrelevant to current 4-segment release line) from
# real failures. Quiet success when only legacy tags are skipped.
git fetch origin master --quiet || { echo "ERROR: fetch origin master failed" >&2; exit 1; }
.agent-skills/oss-release/scripts/safe-fetch-tags.sh

# The release tag must point at origin/master exactly, not at unpushed local commits
# or a stale position behind remote. If either side differs, abort.
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/master)" ]; then
    echo "ERROR: HEAD does not match origin/master."
    echo "  HEAD         : $(git rev-parse HEAD)"
    echo "  origin/master: $(git rev-parse origin/master)"
    echo "Checkout master and run 'git pull origin master' first."
    exit 1
fi

# Detect the "already tagged" degenerate case: HEAD is pointed at by an existing
# release tag. Cutting another release here would tag the same SHA twice, which
# is almost always a mistake (you ran prep on a stale branch, or the last merge
# was itself the release commit). Warn loudly and require explicit confirmation.
AT_HEAD_TAGS=$(git tag --points-at HEAD 2>/dev/null | grep -E '^v[0-9]' || true)
if [ -n "$AT_HEAD_TAGS" ]; then
    echo "⚠️  WARNING: HEAD is already tagged as:"
    echo "$AT_HEAD_TAGS" | sed 's/^/      /'
    echo "   A new release here will tag the SAME commit under a different name."
    echo "   This is usually not what you want. Confirm explicitly before continuing."
fi
```

**Print a structured preflight summary** (a single block the user can scan) before
moving on. This replaces hunting through individual `rev-parse` calls later:

```bash
LATEST_STABLE=$(git tag -l 'v*' | grep -v rc | sort -V | tail -1)
RANGE_COUNT=$(git rev-list --count --no-merges "${LATEST_STABLE}..HEAD" 2>/dev/null || echo "?")
cat <<EOF
=== Preflight summary ===
  HEAD              : $(git rev-parse HEAD)
  origin/master     : $(git rev-parse origin/master)
  latest stable tag : ${LATEST_STABLE} @ $(git rev-parse "$LATEST_STABLE" 2>/dev/null | cut -c1-10)
  commits in range  : ${RANGE_COUNT} non-merge commits since ${LATEST_STABLE}
  tags at HEAD      : ${AT_HEAD_TAGS:-(none)}
EOF
```

**Blocker handling.** Two common degenerate states surface at this step (and often
co-occur — HEAD being at the latest tag implies an empty range by definition):

| Blocker                                                            | Real run                                                                                                                                                                                              | Dry-run                                                                           |
| ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| HEAD is already tagged as `v*` (`AT_HEAD_TAGS` non-empty)          | **Stop.** Show the preflight summary, name the tag(s), and wait for the user to either abort or explicitly type "republish" to acknowledge they want to tag the same commit under another name.       | Announce the block and proceed — the dry-run is inspectable without side effects. |
| Empty range (`RANGE_COUNT = 0` since `LATEST_STABLE`)              | **Stop** unless the user already acknowledged via the HEAD-tagged path above (same root cause). If the empty range is independent of an at-HEAD tag, still require explicit "republish" confirmation. | Announce and proceed, same as above.                                              |
| Any other failure (fetch failed, HEAD ≠ origin/master, dirty tree) | **Stop hard** regardless of mode — those are working-tree-broken states, not policy blocks.                                                                                                           | Same — hard stop.                                                                 |

If the two warnings co-occur (HEAD-tagged AND empty-range), present them as one
consolidated "nothing to release" block rather than two separate warnings.

## Step 1 — Upstream Diff Analysis

```bash
.agent-skills/oss-release/scripts/compare-upstream.sh
```

**What this shows**: divergence between the fork (`origin/master`) and OSS upstream
(`datahub-project/master`). It computes the merge base and lists what's in each side
that's not in the other. Useful for posture: "are we caught up with OSS? what
acryl-only work has accumulated?"

**What this does NOT show**: it does NOT tell you what's in the _current release range_.
Commits that came into our fork via upstream sync (cryptography bumps, security patches,
bug fixes from the OSS team) appear in the release range but NOT in the fork-only set,
so the "File stats" section of `compare-upstream.sh` can be empty even when the release
contains significant ingestion changes. Use Step 2 below to see release content.

Report to user:

- Merge base SHA
- Gap size (OSS ahead / fork ahead)
- Ingestion-relevant commits from each side

If fully synced (zero OSS gap), note it and continue.

## Step 2 — Safety Assessment

**Step 2a — Release-range diff (this is the input to safety judgment)**

The safety call must be based on what's in `<LATEST_STABLE>..HEAD`, not on
`compare-upstream.sh` output. Run these explicitly:

```bash
LATEST_STABLE=$(git tag -l 'v*' | grep -v rc | sort -V | tail -1)
echo "=== Commits in release range ($LATEST_STABLE..HEAD) ==="
git log --oneline --no-merges "$LATEST_STABLE..HEAD"

echo ""
echo "=== Files changed (full) ==="
git diff --stat "$LATEST_STABLE..HEAD"

echo ""
echo "=== Safety-relevant paths touched ==="
git diff --name-only "$LATEST_STABLE..HEAD" | \
    grep -E '(pyproject\.toml|setup\.py|setup\.cfg|uv\.lock|requirements.*\.txt|metadata-ingestion|datahub-actions|metadata-ingestion-modules)' \
    || echo "(no safety-relevant paths touched)"
```

**Empty-range rendering.** If `$LATEST_STABLE..HEAD` contains zero commits,
all three `git log` / `git diff` commands above produce no output. Don't render
empty tables — they're confusing. Instead, print a single consolidated line and
skip ahead:

```
=== Release range ($LATEST_STABLE..HEAD) ===
  (empty — HEAD is at $LATEST_STABLE, nothing to release)
```

When the range is empty, Step 2b can be abbreviated: render the 5-row safety
table with all categories marked `✅ N/A — empty range` and overall verdict
`✅ SAFE (but nothing to ship)`, then continue to Step 3 where the empty-range
guard will gate the actual decision.

**Step 2b — Apply the safety-assessment checklist**

Read `.agent-skills/oss-release/references/safety-assessment.md` and follow it
exactly. It defines the 5 categories you must classify, the dependency-bump verdict
mapping, and the overall verdict rollup.

Render the result as the 5-row table specified there. If the overall verdict is:

- ✅ SAFE → proceed to Step 3.
- ⚠️ REVIEW NEEDED → present the ⚠️ rows to the user, ask how to proceed.
- ❌ BLOCKED → list what must be fixed; stop the workflow.

## Step 3 — Determine Next Version

**Parse the user's bump intent from `$ARGUMENTS`.** The subcommand form the user
invoked may carry a bump hint:

| User typed                      | BUMP     | Command to run              |
| ------------------------------- | -------- | --------------------------- |
| `/oss-release rc` _(or `prep`)_ | (auto)   | `next-version.sh`           |
| `/oss-release rc patch`         | `patch`  | `next-version.sh rc patch`  |
| `/oss-release rc minor`         | `minor`  | `next-version.sh rc minor`  |
| `/oss-release rc major`         | `major`  | `next-version.sh rc major`  |
| `/oss-release rc fourth`        | `fourth` | `next-version.sh rc fourth` |

Token recognition rules:

- If any of `patch`, `minor`, `major`, `fourth` appears in `$ARGUMENTS`, use it as the
  bump type and invoke `next-version.sh rc <bump>`.
- `--dry-run` is consumed by Step 0 and does not participate in bump-type parsing.
- Everything else → default invocation (no args).

```bash
# Default — auto bump
.agent-skills/oss-release/scripts/next-version.sh

# Explicit bump (agent chooses this branch when BUMP was parsed from $ARGUMENTS)
.agent-skills/oss-release/scripts/next-version.sh rc "$BUMP"
```

**Auto-detection behavior when no bump is passed:**

- Latest tag is RC → increments RC number (e.g. `v1.5.0.7rc3` → `v1.5.0.7rc4`)
- Latest tag is stable → starts new RC cycle with fourth bump (e.g. `v1.5.0.12` → `v1.5.0.13rc1`)

Capture the output as `NEXT_VERSION`.

**Empty-range guard** — re-use `RANGE_COUNT` from the preflight summary:

```bash
if [ "${RANGE_COUNT:-0}" = "0" ]; then
    echo "⚠️  No non-merge commits between ${LATEST_STABLE} and HEAD."
    echo "   ${NEXT_VERSION} would tag the same commit as ${LATEST_STABLE}."
    echo "   Do NOT proceed unless this is an intentional republish."
fi
```

Show `NEXT_VERSION` plus the range-count to the user. **Honor the dry-run vs real-run
gate behavior documented in SKILL.md.** If the range is empty, require the user to
acknowledge the no-op republish in their own words — don't treat silence as consent
even in dry-run.

## Step 3.5 — Release Notes

Every release needs a notes file passed to `cut-release.sh`.

```bash
# Notes live next to the skill (gitignored via .agent-skills/oss-release/.gitignore)
# so they survive between runs but don't clutter `git status` at the repo root.
NOTES_DIR=".agent-skills/oss-release/notes"
mkdir -p "$NOTES_DIR"
NOTES_FILE="${NOTES_DIR}/release-notes-${NEXT_VERSION}.md"
```

**Stale-notes detector** — run this BEFORE generating or reusing notes. A
`release-notes-<version>.md` may already exist from a prior `prep` run on the same
version (e.g. you `prep`'d, walked away, the branch fast-forwarded, and you came back).
Reusing stale notes silently publishes wrong content — this is one of the few failure
modes of `prep` that produces a quiet data-integrity bug rather than a loud error.

```bash
if [ -f "$NOTES_FILE" ]; then
    # Look for a 10+ char hex SHA in the notes file (the "Commit:" header writes one).
    STALE_SHA=$(grep -oE '\b[0-9a-f]{10,40}\b' "$NOTES_FILE" | head -1 || true)
    CURRENT_SHA=$(git rev-parse HEAD)
    if [ -n "$STALE_SHA" ] && [[ "$CURRENT_SHA" != "$STALE_SHA"* ]]; then
        echo "⚠️  STALE NOTES DETECTED"
        echo "   File:    $NOTES_FILE"
        echo "   Notes reference SHA: $STALE_SHA"
        echo "   Current HEAD:        $CURRENT_SHA"
        echo "   The existing notes were written for a different commit and almost"
        echo "   certainly mis-describe the release. Regenerate, or explicitly"
        echo "   confirm reuse before continuing."
    fi
fi
```

If the detector fires, **default to regenerating** — only reuse if the user explicitly
acknowledges the SHA mismatch is intentional (rare; e.g. they manually patched the notes
to be accurate for the new HEAD).

**Default path — generate via the changelog skill:**

Check if the `generating-datahub-changelog` skill is available in your runtime's skill registry
(in Claude Code, look for `connectors-accelerator:generating-datahub-changelog` in the
available-skills list).

If available, invoke it via your native skill tool (e.g. Claude Code's `Skill` tool). **Your
first action inside the invoked skill MUST be to `Read` the template file that matches the
audience mode** (`changelog-external.md` for public release notes, `changelog-internal.md`
otherwise) — do not produce output from the surrounding prose alone. Template fidelity is
non-negotiable: mismatches between the promised format and the delivered notes create work
downstream (release page edits, customer comms regeneration).

After the changelog skill produces output, you MUST prepend the standard **Release Info
header**. Read `.agent-skills/oss-release/templates/release-info-header.md` for the
exact template, substitution table, and trailing installation block. Then use your `Write`
tool to save the assembled output to `$NOTES_FILE`.

Do not attempt to spawn a `claude` subprocess — a Claude Code agent cannot invoke
`claude` CLI recursively.

**If the changelog skill is NOT available** (e.g. `connectors-accelerator` plugin not installed):

Tell the user:

> "The `generating-datahub-changelog` skill is not available. I can either:
>
> - **Skip changelog generation** — use a minimal placeholder release note, or
> - **Use notes you provide** — give me a path to a file you've drafted."

Then based on their choice:

```bash
# If they choose skip — minimal placeholder (still prepend the Release Info header
# from templates/release-info-header.md so the stale-notes detector works on re-runs)
cat > "$NOTES_FILE" <<EOF
Release ${NEXT_VERSION}

No detailed changelog for this release. See git log for the full list of commits.
EOF

# If they provide a file
cp "$USER_PROVIDED_PATH" "$NOTES_FILE"
```

**Verify before continuing:**

```bash
[ -s "$NOTES_FILE" ] || { echo "ERROR: notes file is empty"; exit 1; }
```

Show the contents of `$NOTES_FILE` to the user and ask them to confirm or edit before
proceeding (honor the dry-run gate policy).

## Step 4 — Cut the RC

Once version and release notes are approved:

```bash
SHA=$(git rev-parse HEAD)
VERSION="${NEXT_VERSION#v}"   # strip leading 'v'

# Pick up the dry-run flag set in Step 0 when the user invoked with --dry-run.
# If OSS_RELEASE_DRY_RUN was NOT exported at the top of the workflow, this
# resolves to a real release — so double-check before running in a fresh shell.
DRY_RUN_FLAG=""
[ "${OSS_RELEASE_DRY_RUN:-}" = "true" ] && DRY_RUN_FLAG="--dry-run"

.agent-skills/oss-release/scripts/cut-release.sh \
    "$VERSION" \
    "$SHA" \
    "$NOTES_FILE" \
    --prerelease \
    $DRY_RUN_FLAG
```

`cut-release.sh` in dry-run prints the commit range (`PRIOR_TAG..SHA`) before the
notes preview — if the range is zero, it shows a loud warning. Use that as the last
chance to abort before any real tag is created.

**After the script returns**, re-render its output as a readable Markdown block per
`.agent-skills/oss-release/references/cut-release-render.md`. Always render —
both real and dry-run. Use the dry-run column of the placeholder table for dry-run
output, the real-run column for live output.

## Step 5 — Dispatch connector tests

Once the RC exists on GitHub, kick off the `Nightly Connector Tests` workflow
in `acryldata/connector-tests` so a signal is available by the time `finish`
runs. The dispatch is stateless — the resulting run is queryable from GitHub
via `check-connector-tests.sh` as soon as its `set-version` job starts.

```bash
DRY_RUN_FLAG=""
[ "${OSS_RELEASE_DRY_RUN:-}" = "true" ] && DRY_RUN_FLAG="--dry-run"

.agent-skills/oss-release/scripts/dispatch-connector-tests.sh \
    "$NEXT_VERSION" \
    $DRY_RUN_FLAG
```

**Dry-run:** the script prints the `gh workflow run` command it would
execute and exits.

**Real run:** the script fires the dispatch and prints the resulting run
URL (parsed from `gh workflow run`'s stdout). If cut-release in Step 4
failed or was skipped, **do not** run this step — there's nothing to test.

Surface the run URL to the user as the final artifact of `prep`.
