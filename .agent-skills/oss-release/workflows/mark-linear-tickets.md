# `mark-linear` — Optional manual AUDIT of Linear release tracking

> **This is NOT part of the release flow and NOT the primary mechanism.** Linear release
> tracking is automated by the `.github/workflows/linear-release-tracking.yml` GitHub Action
> (Linear's native "Ingestion CLI/PyPI Releases" pipeline), which fires on the
> `release: published` event that `cut-release.sh` emits and stamps the `CLI Release` version
> label automatically. **Do not run this as a routine step** — only as an on-demand audit.

**What this audit is for.** The automation discovers issues primarily by scanning commit text
for `ING-XXXX` references — but OSS commits carry `#PR` numbers, not ticket IDs, so tickets
linked only via the **PR attachment / `[PR Review]` integration** can be missed. This audit
does a more thorough PR↔ticket discovery (PR-number search **plus** attachment/comment scan)
and reports any tickets the automation may have skipped — optionally filling the gap by
applying the version label to confirmed-missed tickets.

It is **safe to skip**, **never required**, and Claude Code-specific (needs the **Linear MCP**,
`mcp__claude_ai_Linear__*`). Agents without Linear access should skip it and say so.

> **Why it's its own file:** opt-in, touches an external system, and duplicates an automated
> pipeline — so it stays out of `prep.md`/`finish.md`. Read this file only when the operator
> explicitly asks to audit Linear release tracking.

---

## When to run

- **Only on demand**, when you want to verify the automated `linear-release-tracking.yml`
  pipeline didn't miss any tickets for a published release — e.g. a spot-check after a release,
  or when Support reports a fix-version that looks unset.
- Not on every release. The GitHub Action already handles the routine case.

## Prerequisites

- Linear MCP connected (`mcp__claude_ai_Linear__list_issues` available).
- The release tag exists and you know its version and comparison range (same range the
  changelog used — see `scripts/release-range-diff.sh`).

---

## Step 1 — Resolve the version label

The label is the **CLI Release** child matching the shipped version, e.g. `v1.5.0.20`
(4-segment patch). Confirm it exists:

```
mcp__claude_ai_Linear__list_issue_labels  name="<VERSION>"   # e.g. v1.5.0.20
```

Expect one label whose `parent` is `CLI Release` (description `Released in <VERSION>`).

> ⚠️ **`.0` minor releases have no CLI Release label.** The CLI Release family is 4-segment
> patches only (`v1.6.0.1`, `v1.5.0.20`). A bare minor like `v1.6.0` exists only as an
> _ungrouped_ label (and minors live under **OSS Release**, not CLI Release). If you are
> marking a `.0` minor, **stop and ask the operator** which label to apply: the ungrouped
> `vX.Y.0`, a new CLI Release child, or skip. Do not guess.

## Step 2 — Collect the PRs in the release range

**Do not hand-roll the range.** Use the _same_ comparison base the changelog/release used —
the previous **stable** tag (`get_comparison_version` logic), not an arbitrary `git log`
base. The release skill already computes this:

```bash
.agent-skills/oss-release/scripts/release-range-diff.sh   # commits/files since latest stable
```

- If you are running this right after `prep`/`stable`, **reuse the range that workflow already
  resolved** (`LAST_STABLE..RC_SHA`) — it is the same set the changelog was generated from, so
  the tickets you stamp line up exactly with the notes you shipped.
- The changelog skill is the source of truth for "what PRs are in this release." If a
  categorized PR list was already produced for the notes, take the PR numbers from there.

Then extract the PR numbers (add the ingestion path filter only if you want ingestion tickets):

```bash
git log <BASE>..<TAG> --pretty=format:'%s' \
  [-- metadata-ingestion/ metadata-ingestion-modules/] \
  | grep -oE '#[0-9]+' | tr -d '#' | sort -u
```

where `<BASE>` is the previous-stable tag from the step above — never a guessed base. This
keeps mark-linear's ticket set consistent with the changelog (see the comparison-base rule:
minor `.0` releases compare against the previous _stable_ patch, e.g. `v1.6.0` ← `v1.5.0.19`).

## Step 3 — Find the linked Linear tickets (THREE directions — all needed)

Linear's `list_issues` `query` searches **title + description only**. That alone misses PRs
linked through a ticket's **attachment** (the GitHub integration's primary link) or a **comment**.
Run all three directions and union the results. **You must check attachments and comments, not
just title/description.**

**A) Linear text-search (per PR)** — finds the auto-created `[PR Review]` tickets:

```
mcp__claude_ai_Linear__list_issues  query="<PR_NUMBER>"  limit=5
```

Keep a result **only if** its title or description literally contains `#<PR_NUMBER>` or
`/pull/<PR_NUMBER>`. Discard topical-but-unreferenced matches.

**B) PR-side scan (per PR)** — catches tickets the PR itself names (branch `feature/ing-1234`,
title/body `(ING-1234)`, "Refs: CUS-…"):

```bash
gh pr view <PR_NUMBER> --repo datahub-project/datahub --json title,body,headRefName,comments
# extract refs matching (case-insensitive): (ING|OSS|CUS|PLAT|PFP|CAT|OBS|ACR|SE)-[0-9]+
```

**C) Linear attachment + comment scan (per candidate ticket)** — the part title/description
search cannot do. For every ticket surfaced by A or B, read its attachments and comments and
confirm/expand the PR link:

```
mcp__claude_ai_Linear__get_issue       id="<TICKET>"      # returns attachments[] (each has .url)
mcp__claude_ai_Linear__list_comments   issueId="<TICKET>" # discussion + inline comments
```

- In `attachments[].url` and each comment `body`, look for `github.com/.../pull/<N>` and
  `(ING|OSS|CUS|…)-\d+` refs. The PR **attachment** is the authoritative GitHub-integration
  signal — a ticket can carry the PR as an attachment with **no** mention in its description.
- This both **confirms** an A/B match (attachment url == the PR) and **finds sibling PRs**
  attached to the same ticket.
- Keep a ticket as a match for `<PR>` if its attachment url or a comment references that PR.

> **Discovery limit (be honest about it):** there is **no global Linear search by attachment
> url or comment text**. Direction C can only run over tickets you already have a handle on
> (from A/B). A ticket whose _only_ link to a release PR is an attachment — with no text match
> and no PR-side reference — is not discoverable with the current MCP tools. If thorough
> coverage matters, widen the candidate set before running C: e.g. `list_issues team="Ingestion Pod"
label="oss-pr-review"` and recently-updated customer tickets, then scan each one's attachments
> for the release's PR numbers.

For large ranges, dispatch a sub-agent to run A+B+C across all PRs/candidates and return a
compact `PR | ticket | status | customer | link source (A/B/C)` table — keeps the main context clean.

## Step 4 — Decide scope (which tickets get the label)

| Ticket kind                                                  | Label it?                                 |
| ------------------------------------------------------------ | ----------------------------------------- |
| `[PR Review]` ticket (`oss-pr-review`) for a shipped PR      | **Yes**                                   |
| **Non-closed** customer/feature ticket fixed by this release | **Yes** (so Support sees the fix version) |
| **Closed** customer ticket                                   | No, unless the operator asks              |
| `PFP-*` / `OBS-*` / `CAT-*` merge-conflict sign-off tickets  | No, unless the operator asks              |

Default selection rule: **non-closed tickets where the CLI Release label is not already set**,
plus the per-PR `[PR Review]` tickets.

## Step 5 — Build the update plan (read-only) and SHOW it

> ⚠️ **Never overwrite an existing CLI Release version.** If a ticket already carries **any**
> concrete CLI Release version label (a `vX.Y.Z.W` under the `CLI Release` parent — not the
> pending `CLI Next`), **leave it untouched and skip it**, even if that version differs from
> the one you're stamping. A fix can first ship in an earlier CLI build; the _first_ shipped
> version is the one worth recording, so do not clobber or stack a second version label.

First, **read only** — make NO writes yet. For each selected ticket, `get_issue` to read its
current `labels`, then classify it:

- **will-update**: has `CLI Next` or no CLI Release label → would be stamped `<VERSION>`.
- **skip (already versioned)**: already has a concrete `CLI Release` version → leave untouched.

Then present the plan as a table and **stop** — this is the mandatory gate before any mass
update. Required columns:

| Ticket   | Title          | PR(s)  | Current CLI label | → New version | Action                   |
| -------- | -------------- | ------ | ----------------- | ------------- | ------------------------ |
| ING-1234 | <ticket title> | #17527 | `CLI Next`        | `<VERSION>`   | will update              |
| ING-5678 | <ticket title> | #17440 | _none_            | `<VERSION>`   | will update              |
| ING-9012 | <ticket title> | #16842 | `v1.5.0.18`       | —             | skip (already versioned) |

Below the table, state the counts: **N tickets will be updated, M skipped**. Separately list
the skipped-already-versioned tickets (with their existing version) so the operator can spot
fixes that shipped in an earlier build.

## Step 6 — Confirmation gate, then apply

> ⚠️ **`save_issue` `labels` REPLACES the entire label set** (it is NOT append-only, unlike
> `links`). You MUST resubmit the full intended set from the ticket's current labels.

**This is a bulk write to an external system — always gate it:**

| Mode         | Behavior                                                                                                                                                                                                                           |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Real run** | After showing the Step 5 plan table, **wait for the operator to explicitly type "yes" / "confirm"** before a single `save_issue`. Silence is not consent. If they want only a subset, take their edited list and re-show the plan. |
| **Dry-run**  | Show the plan table, announce "In a real run I would pause here for confirmation and then apply these," and make **no** `save_issue` calls.                                                                                        |

Once confirmed, apply to each **will-update** ticket:

1. New label set = current labels **minus** `CLI Next` **plus** `<VERSION>` (dedup; never drop
   any other label — preserve `oss-pr-review`, `Source:*`, `Company:*`, `OSS-Next`, `SaaS-Next`, etc.).
2. `mcp__claude_ai_Linear__save_issue id="<TICKET>" labels=[<full new set, by name>]`.

Pass version labels by exact name; 4-segment version names are unique in the workspace. Re-running
the whole step is idempotent — already-versioned tickets are classified "skip" in Step 5.

## Step 7 — Report

Output a compact results table: `Ticket | Action (set <VERSION> [removed CLI Next] / already set: <existing version> / skipped) | Final CLI label | link source (A/B/C)`, then a short list of any
customer tickets touched. Note any ticket the operator excluded from the plan.

---

## Notes & gotchas

- **No CLI Release label for `.0` minors** — see Step 1 warning.
- **A ticket can reference a PR that never merged** (attachment to a closed/open PR). The
  changelog only contains _merged_ commits, so a PR in a ticket's attachment may not be in any
  release range — don't label off attachments alone; label off the in-range PR set from Step 2.
- **Three release label families** (each with a `…-Next` pending label): `CLI Release`
  (4-segment CLI patches), `OSS Release` (core/GMS minors like `v1.5.0`), `SaaS Release`
  (`-acryl` / `-cloud`). This step only touches **CLI Release**; leave `OSS-Next` / `SaaS-Next`
  for their respective owners unless asked.
