# `finish` — Promote RC to Stable

End-to-end workflow for promoting a release candidate to a stable release. Read SKILL.md
first for prerequisites and dry-run gate behavior. The `status` subcommand is documented
at the end of this file (it's a strict subset of `finish`'s CI check).

## Step 0 — Preflight + detect latest RC

```bash
# If the user's invocation included --dry-run, export it now so Step 6 picks it up.
# export OSS_RELEASE_DRY_RUN=true

.agent-skills/oss-release/scripts/finish-preflight.sh
```

`finish-preflight.sh` freshens local state (clean-tree check, fetch `origin/master` + tags),
finds the latest `v*rc*` tag, resolves it to a SHA, and reports how far `origin/master` has
advanced past the RC. Capture `LATEST_RC` and `RC_SHA` from the summary — they feed every
subsequent step.

**Interpret the exit code:**

| Exit | Meaning                                                                                                                              |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `0`  | Preflight clean. Summary block on stdout. If master has advanced, a soft warning prints — agent decides whether to stop or continue. |
| `1`  | Hard blocker: dirty tree, fetch failure, or no RC tags found. Stop.                                                                  |

If the master-ahead warning fires, ask the user whether to proceed with the RC's SHA anyway
or go cut a new RC first. Honor dry-run vs real-run gate behavior per SKILL.md.

## Step 1 — Check CI

```bash
.agent-skills/oss-release/scripts/check-ci.sh <RC_SHA>
```

Substitute `<RC_SHA>` from the preflight summary. Review the output against
`.agent-skills/oss-release/known-flaky-tests.md` — only failures listed there may be
ignored. Any unexpected failure blocks the release.

**If CI is still running**, use `/loop` to poll:

```
/loop check CI every 5 minutes: .agent-skills/oss-release/scripts/check-ci.sh <sha>
```

## Step 2 — Validate RC identity + connector tests

```bash
.agent-skills/oss-release/scripts/validate-rc-for-promotion.sh <RC_TAG> <RC_SHA>
```

Substitute both values from the preflight summary. The script runs two gates back-to-back:

- **Identity guard** — confirms `<RC_TAG>` still resolves to the same SHA we captured at
  preflight. Detects the narrow race where the tag was force-moved between Step 0 and
  Step 2. Exits `1` with an explanation if it diverged.

- **Connector-tests gate** — prints the full RC chain (so the operator sees which sibling
  RCs exist), then invokes `check-connector-tests.sh` for `<RC_TAG>`. Its exit code
  becomes `validate-rc-for-promotion.sh`'s exit code.

**Handling each connector-tests exit code** (`validate-rc-for-promotion.sh` returns these
verbatim from `check-connector-tests.sh`). Dry-run: announce and proceed without blocking,
per SKILL.md. Real run:

| Exit | Meaning                | Action                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ---- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `0`  | Pass — proceed.        | Continue to Step 3.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `2`  | No matching run found. | Offer user: (1) dispatch one now via `dispatch-connector-tests.sh <RC_TAG>`, then **stop finish** and ask them to re-run once CI completes (~15-18 min). Offer `/loop` auto-poll. (2) Widen the scan — re-run with `CONNECTOR_TESTS_SCAN_LIMIT=60 check-connector-tests.sh <RC_TAG>` if they believe an older dispatch exists. (3) Explicit override — user types something unambiguous like "override connector tests" (not just "yes"); continue to Step 3 but print a prominent warning in the Step 5 release-notes preview. |
| `3`  | Still running.         | Do NOT prompt to dispatch (one is already in flight). Stop finish; ask user to re-run when CI completes. Offer `/loop` auto-poll.                                                                                                                                                                                                                                                                                                                                                                                               |
| `4`  | Failed.                | Do NOT offer to re-dispatch — same SHA will fail the same way. Show the run URL, suggest cutting a new RC with a fix, and stop.                                                                                                                                                                                                                                                                                                                                                                                                 |

## Step 3 — Determine stable version

Strip the `rcN` suffix from `<RC_TAG>`:

- `v1.5.0.8rc3` → `v1.5.0.8`

Or run:

```bash
.agent-skills/oss-release/scripts/next-version.sh stable
```

## Step 4 — Mandatory confirmation gate

Present to user:

- RC being promoted: `<RC_TAG>`
- Stable version to create: `<STABLE_VERSION>`
- Commit SHA: `<RC_SHA>`

**Real run: do not proceed until the user explicitly types "yes" or "confirm".**
Dry-run: announce the gate ("In a real run I would block here for explicit
confirmation"), then proceed without blocking.

## Step 5 — Fetch RC notes + cut stable

Fetch the RC's release body from GitHub into the stable notes file — stable tags the same
commit as the RC, so reusing the RC's notes verbatim is the default path:

```bash
.agent-skills/oss-release/scripts/fetch-rc-notes.sh <RC_TAG> <STABLE_VERSION>
```

The script prints the saved notes file path on stdout (e.g.
`.agent-skills/oss-release/notes/release-notes-v1.5.0.13.md`). Capture it.

Show the notes contents to the user and ask whether to reuse as-is or regenerate. If
regenerate, follow the same flow as `prep` Step 3.5: invoke
`connectors-accelerator:generating-datahub-changelog` via your native skill tool if
available (same custom path filter as `prep`), or fall back to skip/provide-file if it
isn't. Apply the standard Release Info header per
`.agent-skills/oss-release/templates/release-info-header.md`.

Then invoke `cut-release.sh` as a single command, substituting values inline:

```bash
.agent-skills/oss-release/scripts/cut-release.sh <STABLE_VERSION_NO_V> <RC_SHA> <NOTES_FILE> [--dry-run]
```

**Include `--dry-run`** if Step 0 exported `OSS_RELEASE_DRY_RUN=true`. Omit it for a real
release. Note: **no `--prerelease`** for stable releases. Intentionally NOT in
`allowed-tools` — every `cut-release.sh` invocation prompts as a defense-in-depth gate.

**After the script returns**, re-render its output per
`.agent-skills/oss-release/references/cut-release-render.md`. For stable releases: omit
`--prerelease` in the rendered commands code block; set `Pre-release` row in the
parameters table to `false`. Everything else identical to the prep render.

---

# `status` — Check CI for Latest RC

Read-only snapshot of CI status for the most recent RC. No tags created, nothing published,
no confirmation gates. Use this as a quick "is the RC ready to promote yet?" check between
`prep` and `finish`.

```bash
.agent-skills/oss-release/scripts/status.sh
```

The script fetches tags, finds the latest `v*rc*`, and runs `check-ci.sh` against its SHA.
Report the output to the user. No further action.
