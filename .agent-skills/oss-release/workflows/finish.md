# `finish` — Promote RC to Stable

End-to-end workflow for promoting a release candidate to a stable release. Read SKILL.md
first for prerequisites and dry-run gate behavior. The `status` subcommand is documented
at the end of this file (it's a strict subset of `finish`'s CI check).

## Step 0 — Repo preflight

```bash
# If the user's invocation included --dry-run, export it now so Step 6 picks it up.
# export OSS_RELEASE_DRY_RUN=true

if ! git diff-index --quiet HEAD --; then
    echo "ERROR: uncommitted changes detected." && exit 1
fi
git fetch origin master --quiet || { echo "ERROR: fetch origin master failed" >&2; exit 1; }
.agent-skills/oss-release/scripts/safe-fetch-tags.sh
```

## Step 1 — Auto-detect latest RC

```bash
LATEST_RC=$(git tag -l 'v*rc*' | sort -V | tail -1)
RC_SHA=$(git rev-list -n 1 "$LATEST_RC")
echo "Latest RC: $LATEST_RC  (${RC_SHA:0:10})"
```

## Step 2 — Check for new commits since RC

```bash
MASTER_SHA=$(git rev-parse origin/master)
if [ "$RC_SHA" != "$MASTER_SHA" ]; then
    AHEAD=$(git rev-list "$RC_SHA..origin/master" --count)
    echo "WARNING: origin/master is $AHEAD commit(s) ahead of ${LATEST_RC}."
    echo "To include new commits, run 'prep' again to cut a new RC first."
fi
```

Ask user to confirm which SHA to release from (honor the dry-run gate policy).

## Step 3 — Check CI

```bash
.agent-skills/oss-release/scripts/check-ci.sh "$RC_SHA"
```

Review the output against `.agent-skills/oss-release/known-flaky-tests.md`.
Only failures listed there may be ignored. Any unexpected failure blocks the release.

**If CI is still running**, use `/loop` to poll:
```
/loop check CI every 5 minutes: .agent-skills/oss-release/scripts/check-ci.sh <sha>
```

## Step 3.5 — Validate RC identity + connector tests

Two guards that run before version is computed. Both block promotion unless
the user explicitly overrides.

**3.5a — Changelist-identity guard.** Confirm the RC tag still points at the
SHA we're about to promote. The existing Step 2 check catches "master moved
ahead of RC"; this catches the narrower case where the RC tag itself was
force-moved to a different SHA after Step 1 resolved it.

```bash
RC_TAG_SHA=$(git rev-parse "$LATEST_RC")
if [ "$RC_TAG_SHA" != "$RC_SHA" ]; then
    echo "ERROR: ${LATEST_RC} points at ${RC_TAG_SHA:0:10} but we resolved it to ${RC_SHA:0:10}"
    echo "       The RC tag was force-moved. Abort and investigate." >&2
    exit 1
fi
```

**3.5b — Connector tests must be green for this RC.** The stable release ships
the same commit that the RC was tested against — so `finish` requires a
run on `acryldata/connector-tests/nightly_tests.yaml` to have been dispatched
with `version=$LATEST_RC` and concluded with `conclusion=success`. GitHub
itself is the source of truth — `check-connector-tests.sh` scans recent
`workflow_dispatch` runs and matches the `set-version` job log.

First, print the RC chain for the version being promoted so the operator sees
exactly which RC we're gating on (the highest-numbered one) and that earlier
RCs in the chain do NOT count as proof of a green test:

```bash
STABLE_PREFIX="${LATEST_RC%rc*}"   # e.g. v1.5.0.13rc15 → v1.5.0.13
echo "=== RC chain for ${STABLE_PREFIX} ==="
git tag -l "${STABLE_PREFIX}rc*" | sort -V | sed 's/^/  /'
echo "Gating on: ${LATEST_RC}  (earlier RCs do not satisfy this gate — each RC is a different SHA)"
```

Then run the checker:

```bash
set +e
.agent-skills/oss-release/scripts/check-connector-tests.sh "$LATEST_RC"
CONNECTOR_RC=$?
set -e

case "$CONNECTOR_RC" in
    0)   echo "Connector tests OK — proceeding." ;;
    2)   echo "⚠️  No connector-tests entry for ${LATEST_RC}." ;;
    3)   echo "⏳ Connector tests still running. Wait for them to finish, then re-run finish." ;;
    4)   echo "❌ Connector tests did not pass for ${LATEST_RC}. Investigate before promoting." ;;
    *)   echo "Unexpected exit code $CONNECTOR_RC from check-connector-tests.sh" ;;
esac
```

**Handling each non-zero exit code in a real run** (dry-run: announce and
proceed without blocking, per SKILL.md):

- **Exit 2 — no matching run found.** Offer and wait for the user:
  1. **Dispatch one now** — run
     `.agent-skills/oss-release/scripts/dispatch-connector-tests.sh ${LATEST_RC}`,
     surface the run URL, then **stop finish** and tell the user to re-run
     `/oss-release finish` once the ~15-18 min CI completes. Offer to set up
     a `/loop` that auto-polls `check-connector-tests.sh ${LATEST_RC}` every
     3 min and alerts when the conclusion is final.
  2. **Widen the scan** — if the operator believes an older dispatch exists
     (e.g. from more than 30 runs back), re-run with
     `CONNECTOR_TESTS_SCAN_LIMIT=60 check-connector-tests.sh ${LATEST_RC}`.
     Note: GitHub retains job logs ~90 days, so runs older than that cannot
     be matched.
  3. **Explicit override** — user types something unambiguous like
     "override connector tests" (not just "yes"). Continue to Step 4 but
     print a prominent warning in the Step 6 release-notes preview.

- **Exit 3 — still running.** Do NOT prompt to dispatch (one is already in
  flight). Stop finish; tell the user to re-run when CI completes. Offer
  the same `/loop` auto-poll option as Exit 2 option 1.

- **Exit 4 — failed.** Do NOT offer to re-dispatch (same SHA will fail the
  same way). Show the run URL, suggest cutting a new RC with a fix, and stop.

## Step 4 — Determine stable version

Strip the `rcN` suffix from the RC tag:
- `v1.5.0.8rc3` → `v1.5.0.8`

Or run:
```bash
.agent-skills/oss-release/scripts/next-version.sh stable
```

## Step 5 — Mandatory confirmation gate

Present to user:
- RC being promoted: `$LATEST_RC`
- Stable version to create: `$STABLE_VERSION`
- Commit SHA: `$RC_SHA`

**Real run: do not proceed until the user explicitly types "yes" or "confirm".**
Dry-run: announce the gate ("In a real run I would block here for explicit
confirmation"), then proceed without blocking.

## Step 6 — Cut the stable release

**Reuse the RC's release notes** — the stable release tags the same commit as the RC,
so the changelog is identical. Fetch them from the existing GitHub RC release:

```bash
# Mirror the prep convention: notes live in .agent-skills/oss-release/notes/
# (gitignored). Persisting them — instead of using /tmp — makes it possible to
# inspect what was published for any release without going back to the GitHub UI.
NOTES_DIR=".agent-skills/oss-release/notes"
mkdir -p "$NOTES_DIR"
NOTES_FILE="${NOTES_DIR}/release-notes-${STABLE_VERSION}.md"
gh release view "$LATEST_RC" --repo acryldata/datahub --json body --jq '.body' > "$NOTES_FILE"

if [ ! -s "$NOTES_FILE" ]; then
    echo "ERROR: could not fetch RC release notes" && exit 1
fi
```

Show `$NOTES_FILE` to the user and ask whether to reuse as-is or regenerate.

If regenerate, follow the same flow as `prep` Step 3.5: invoke
`connectors-accelerator:generating-datahub-changelog` via your native skill tool if
available, or fall back to skip/provide-file if it isn't. Apply the standard Release
Info header per `.agent-skills/oss-release/templates/release-info-header.md`.

```bash
[ -s "$NOTES_FILE" ] || { echo "ERROR: notes file is empty"; exit 1; }
```

Then cut the stable release:

```bash
STABLE_VERSION="${STABLE_VERSION#v}"   # strip leading 'v'

DRY_RUN_FLAG=""
[ "${OSS_RELEASE_DRY_RUN:-}" = "true" ] && DRY_RUN_FLAG="--dry-run"

.agent-skills/oss-release/scripts/cut-release.sh \
    "$STABLE_VERSION" \
    "$RC_SHA" \
    "$NOTES_FILE" \
    $DRY_RUN_FLAG
```

Note: no `--prerelease` flag for stable releases.

**After the script returns**, re-render its output per
`.agent-skills/oss-release/references/cut-release-render.md`. For stable releases:
omit `--prerelease` in the rendered commands code block; set `Pre-release` row in the
parameters table to `false`. Everything else identical to the prep render.

---

# `status` — Check CI for Latest RC

Read-only snapshot of CI status for the most recent RC. No tags created, nothing
published, no confirmation gates. Use this as a quick "is the RC ready to promote yet?"
check between `prep` and `finish`.

```bash
.agent-skills/oss-release/scripts/safe-fetch-tags.sh
LATEST_RC=$(git tag -l 'v*rc*' | sort -V | tail -1)
RC_SHA=$(git rev-list -n 1 "$LATEST_RC")
echo "Latest RC: $LATEST_RC  (${RC_SHA:0:10})"

.agent-skills/oss-release/scripts/check-ci.sh "$RC_SHA"
```

Report status to user. No action taken.
