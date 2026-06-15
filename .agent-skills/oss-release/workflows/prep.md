# `prep` — Cut a Release Candidate

End-to-end workflow for cutting an RC from the current `origin/master`. Read SKILL.md
first for prerequisites, dry-run gate behavior, and the subcommand overview.

## Step 0 — Repo preflight

Run `preflight.sh` before anything else. It performs the clean-tree check, fetches
`origin/master` + tags, verifies `HEAD == origin/master`, detects the "already tagged"
degenerate case, computes the commit range since the latest stable tag, and emits a
one-block summary. Single script call — no compound bash statements to fight Claude
Code's permission classifier.

```bash
# If the user's invocation included --dry-run, thread it through via env var so
# Step 4 (cut-release) and any sub-scripts pick it up consistently.
# Example: user runs "/oss-release prep --dry-run" → export this now.
# export OSS_RELEASE_DRY_RUN=true

.agent-skills/oss-release/scripts/preflight.sh
```

**Interpret the exit code:**

| Exit | Meaning                                                                                       |
| ---- | --------------------------------------------------------------------------------------------- |
| `0`  | Preflight clean. Summary block emitted to stdout. Warnings (if any) printed but non-blocking. |
| `1`  | Hard blocker: dirty tree, fetch failure, or `HEAD != origin/master`. Stop hard, both modes.   |

The script does NOT apply the dry-run vs real-run policy gate for the degenerate "already
tagged" / "empty range" warnings — it just emits the warning lines. Read those out of the
script's output and apply the table below.

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
`compare-upstream.sh` output:

```bash
.agent-skills/oss-release/scripts/release-range-diff.sh
```

The script prints three sections (commits, file stats, safety-relevant paths)
against the latest stable tag it discovers. For an **empty range** (HEAD == latest
stable), it collapses to a single "nothing to release" line — in that case Step 2b
can be abbreviated to the 5-row safety table with all categories marked `✅ N/A —
empty range`, overall verdict `✅ SAFE (but nothing to ship)`, then continue to
Step 3 where the empty-range guard gates the actual decision.

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
.agent-skills/oss-release/scripts/prepare-notes.sh "$NEXT_VERSION"
```

The script creates `.agent-skills/oss-release/notes/` if needed, runs the
stale-notes detector, and prints the notes file path on stdout. Capture that
path — you'll pass it to `cut-release.sh` in Step 4 and use it as the `Write`
target when the changelog skill produces content.

**Interpret the exit code:**

| Exit | Meaning                                                                                                                                                                                                                                                                                                         |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `0`  | Fresh or no existing notes file. Proceed to generate.                                                                                                                                                                                                                                                           |
| `2`  | Stale notes detected (existing file references a SHA that doesn't match HEAD). **Warning on stderr names the mismatch.** Default to regenerating — only reuse if the user explicitly acknowledges the SHA mismatch is intentional (rare; e.g. they manually patched the notes to be accurate for the new HEAD). |
| `1`  | Usage error (missing `<version>` arg). Fix and retry.                                                                                                                                                                                                                                                           |

**Default path — generate via the changelog skill:**

Use the **repo-canonical** changelog skill at
`.agent-skills/generating-datahub-changelog/SKILL.md` (invokable as the
`generating-datahub-changelog` skill / `/generating-datahub-changelog` command). It is
vendored into this repo and version-controlled — **prefer it over** the plugin's
`connectors-accelerator:generating-datahub-changelog`, which is only a fallback for repos
that don't have the in-repo copy.

If available, invoke it via your native skill tool (e.g. Claude Code's `Skill` tool). **Your
first action inside the invoked skill MUST be to `Read` the template file that matches the
audience mode** (`changelog-external.md` for public release notes, `changelog-internal.md`
otherwise) — do not produce output from the surrounding prose alone. Template fidelity is
non-negotiable: mismatches between the promised format and the delivered notes create work
downstream (release page edits, customer comms regeneration).

**Scope the changelog to what actually ships.** When invoking the changelog skill, pass a
filter that matches what the OSS CLI actually ships — typically
`filter:custom:metadata-ingestion/,metadata-ingestion-modules/,docs/`. Frontend, backend,
and web-react changes do not appear in the `acryl-datahub` Python wheel and should not
appear in the notes.

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

Once version and release notes are approved. First capture the commit SHA
(single command, auto-allowed):

```bash
git rev-parse HEAD
```

Then invoke `cut-release.sh` as a single command, substituting the values you
already have in hand: `$NEXT_VERSION` (strip the leading `v`), the SHA just
captured, and `$NOTES_FILE` (from Step 3.5):

```bash
.agent-skills/oss-release/scripts/cut-release.sh <VERSION_NO_V> <SHA> <NOTES_FILE> --prerelease [--dry-run]
```

**Include `--dry-run`** if Step 0 exported `OSS_RELEASE_DRY_RUN=true`. Omit it
for a real release. Intentionally NOT in `allowed-tools` — every `cut-release.sh`
invocation prompts as a defense-in-depth confirmation gate.

`cut-release.sh` in dry-run prints the commit range (`PRIOR_TAG..SHA`) before the
notes preview — if the range is zero, it shows a loud warning. Use that as the last
chance to abort before any real tag is created.

**After the script returns**, re-render its output as a readable Markdown block per
`.agent-skills/oss-release/references/cut-release-render.md`. Always render —
both real and dry-run. Use the dry-run column of the placeholder table for dry-run
output, the real-run column for live output.

## Step 5 — Wait for the pypi wheel

The connector-tests workflow runs `pip install acryl-datahub==<version>` as
its first step. Dispatching it before the pypi-release workflow has finished
publishing the wheel races the publish and produces false-negative test runs.
Gate the dispatch on `pypi-release metadata-ingestion` success:

```bash
.agent-skills/oss-release/scripts/wait-for-pypi-release.sh <NEXT_VERSION>
```

**In dry-run mode, skip this step entirely** — no real release was created,
so there's no pypi-release run to wait on. Announce "dry-run: skipping pypi
wait" and proceed to Step 6.

**Interpret the exit code:**

| Exit | Meaning                                                   | Action                                                                                                                                                |
| ---- | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `0`  | `pypi-release metadata-ingestion` succeeded.              | Proceed to Step 6 (dispatch connector-tests).                                                                                                         |
| `2`  | No run found yet — release event hasn't scheduled it.     | Wait ~30s and re-invoke. Or set up `/loop 2m wait-for-pypi-release.sh <NEXT_VERSION> && dispatch-connector-tests.sh <NEXT_VERSION>` to auto-progress. |
| `3`  | Run is queued or in progress. Typical end-to-end 5-10min. | Same — retry, or use `/loop` to auto-poll. Do NOT dispatch connector-tests yet.                                                                       |
| `4`  | Run completed but not successfully (failure, cancelled…). | Do NOT dispatch connector-tests — the wheel isn't on pypi. Surface the failing run URL to the user and stop. Likely needs a new RC with a fix.        |

## Step 6 — Dispatch connector tests

Once `wait-for-pypi-release.sh` exits 0, kick off the `Nightly Connector Tests`
workflow in `acryldata/connector-tests` so a signal is available by the time
`finish` runs. The dispatch is stateless — the resulting run is queryable from
GitHub via `check-connector-tests.sh` as soon as its `set-version` job starts.

Invoke as a single command — append `--dry-run` if Step 0 set
`OSS_RELEASE_DRY_RUN=true`:

```bash
.agent-skills/oss-release/scripts/dispatch-connector-tests.sh <NEXT_VERSION> [--dry-run]
```

**Dry-run:** the script prints the `gh workflow run` command it would execute
and exits.

**Real run:** the script fires the dispatch and prints the resulting run URL
(parsed from `gh workflow run`'s stdout). If cut-release in Step 4 failed or
was skipped, **do not** run this step — there's nothing to test.

Surface the run URL to the user as the final artifact of `prep`.
