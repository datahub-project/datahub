---
name: generating-datahub-changelog
description: Generate changelogs for DataHub repository merges. Use when creating release notes, summarizing weekly/daily changes, preparing internal team updates, or drafting external announcements for customers.
---

> **Vendored, repo-canonical copy.** Moved into the repo from the `connectors-accelerator`
> plugin so it is version-controlled and independent of the plugin cache. Script/template/
> reference paths below are repo-relative (`.agent-skills/generating-datahub-changelog/…`).
> Entry point: the `/generating-datahub-changelog` slash command
> (`.claude/commands/generating-datahub-changelog.md`).

You are a product marketer creating changelogs for the DataHub team. Your goal is to summarize the latest merges to the main branch, highlighting new features, bug fixes, and giving credit to the hard-working developers.

**Tone by mode:**

- **Internal**: Witty, enthusiastic, fun - developer in-jokes welcome
- **External**: Professional, friendly, informative - no internal references

> ⏱️ **Typical execution**: 1-3 minutes for weekly changelog, 5-10 minutes for large releases (50+ PRs)

---

## 🚀 Efficiency Guidelines

**Follow these rules to minimize API calls and token usage:**

| Task                | ✅ DO                                            | ❌ DON'T                        |
| ------------------- | ------------------------------------------------ | ------------------------------- |
| Fetch PRs           | Single `gh pr list` to file, then `jq` filter    | Read full JSON into context     |
| Filter PRs          | Use `jq` to extract only needed fields           | Parse large JSON in LLM context |
| Linear refs         | Use `extract_linear_refs.py` script              | Manually parse PR bodies        |
| Customer extraction | Use `extract_customers.py` script                | Manually check each ticket      |
| Breaking changes    | `grep` for PR numbers in file                    | Read entire file into context   |
| Linear status       | Batch with `list_issues` OR parallel `get_issue` | Sequential individual calls     |
| PR statistics       | Use `calculate_merge_stats.py` script            | Calculate manually in context   |

**Token-saving techniques:**

- **Save to /tmp files** - Don't read large JSON into context
- **Use jq** - Filter and transform JSON outside LLM context
- **Use scripts** - `extract_linear_refs.py`, `extract_customers.py`, `calculate_merge_stats.py`
- **Parallel tool calls** - Multiple Linear searches in one message

**Execution order for maximum efficiency:**

1. **One `gh pr list` call** → save to `/tmp/prs_raw.json`
2. **`jq` filter** → extract only needed fields to `/tmp/prs_filtered.json`
3. **`extract_linear_refs.py`** → get Linear refs without API calls
4. **One `grep` call** → check breaking changes
5. **Parallel Linear searches** → get ticket details
6. **`extract_customers.py`** → extract customer info from tickets
7. **`calculate_merge_stats.py`** → compute stats from PR JSON

---

## Quick Start

Most common invocations:

```
/generating-datahub-changelog weekly                    # Last 7 days, all components, internal
/generating-datahub-changelog weekly ingestion          # Last 7 days, ingestion only
/generating-datahub-changelog weekly ingestion external # Last 7 days, ingestion, public-facing
/generating-datahub-changelog from:v0.15.0              # All changes since tag
```

---

## Argument Parsing

Parse `$ARGUMENTS` for THREE components:

1. **Selection mode** - Which PRs to include (time-based, PR list, tag range, or date range)
2. **Filter** - Which folders/components to focus on (optional, defaults to `all`)
3. **Audience mode** - Internal or external changelog (optional, defaults to `internal`)

**Filter keywords** (recognized anywhere in arguments):

- `ingestion` → filter:ingestion
- `backend` → filter:backend
- `frontend` → filter:frontend
- `graphql` → filter:graphql
- `docs` → filter:docs
- `all` → no filtering (explicit)

**Audience keywords** (recognized anywhere in arguments):

- `internal` (default) or `external` - See [Audience Mode](#audience-mode-internal-vs-external) for details

**Examples - all of these work:**

```
/changelog weekly ingestion                # internal by default
/changelog weekly ingestion internal       # explicit internal
/changelog weekly ingestion external       # public-facing changelog
/changelog last 2 weeks backend external
/changelog prs:123,456 frontend internal
/changelog from:v0.15.0 ingestion external
/changelog last month                      # defaults to all, internal
/changelog weekly filter:ingestion         # explicit syntax still works
/changelog 14 filter:custom:smoke-test/    # custom filter requires prefix
```

---

## Selection Modes

### Mode 1: Time-based (default)

**Keywords:**

- `daily` or no args: PRs merged in the last 24 hours
- `weekly`: PRs merged in the last 7 days
- `monthly`: PRs merged in the last 30 days

**Numeric:**

- `<number>`: PRs merged in the last N days (e.g., `14` for 14 days)

**Natural language** (parse these to days):

- `"last 2 weeks"` or `"2 weeks"` → 14 days
- `"last two weeks"` → 14 days
- `"last 3 days"` or `"3 days"` → 3 days
- `"last month"` or `"1 month"` → 30 days
- `"last 2 months"` → 60 days

**Parsing rules for natural language:**

1. Extract the number (digit or word: one=1, two=2, three=3, etc.)
2. Extract the unit (day/days, week/weeks, month/months)
3. Convert to days: weeks×7, months×30

Use `gh pr list` with `--search` to find merged PRs. **Fetch ALL needed fields in a single call** to avoid multiple API requests:

```bash
gh pr list --repo datahub-project/datahub --state merged --search "merged:>=$(date -v-<N>d +%Y-%m-%d)" --limit 100 --json number,title,author,labels,mergedAt,createdAt,body,reviews
```

**Why all fields at once**: This single call provides everything needed for:

- PR categorization (title, labels)
- Linear ticket extraction (body contains ING-XXXX references)
- PR statistics (createdAt, mergedAt, reviews)

**Do NOT** make separate calls for reviews or merge times - it's all in this response.

### Mode 2: PR List

- `prs:123,456,789`: Specific PR numbers to include
- Fetch each PR using: `gh pr view <pr> --repo datahub-project/datahub --json number,title,author,labels,body,files`

### Mode 3: Tag Range (for releases)

- `from:<tag>`: All PRs between tag and HEAD (e.g., `from:v0.15.0.1rc13`)
- `from:<tag1>,to:<tag2>`: PRs between two tags (e.g., `from:v0.15.0.1,to:v0.15.0.2`)

Get PR numbers from git log:

```bash
# All PRs between tags (no filter)
git log <from_tag>..<to_ref> --pretty=format:'%s' | grep -oE '#[0-9]+' | grep -oE '[0-9]+' | sort -u

# With folder filter (e.g., ingestion)
git log <from_tag>..<to_ref> --pretty=format:'%s' -- metadata-ingestion/ metadata-ingestion-modules/ | grep -oE '#[0-9]+' | grep -oE '[0-9]+' | sort -u
```

### Mode 4: Date Range

- `date:2024-01-01,2024-01-15`: PRs merged between two dates (inclusive)
- `date:2024-01-01`: PRs merged from date to now

Use `gh pr list` with date search:

```bash
gh pr list --repo datahub-project/datahub --state merged --search "merged:2024-01-01..2024-01-15" --limit 100 --json number,title,author,labels,mergedAt
```

---

## Filter Profiles

Filters determine which folders/components to include. **Default is `all` (no filtering).**

### `filter:all` (default)

Include ALL PRs regardless of which files they touch. No filtering applied.

### `filter:ingestion`

Only include PRs with changes in:

- `metadata-ingestion/`
- `metadata-ingestion-modules/`

### `filter:frontend`

Only include PRs with changes in:

- `datahub-web-react/`
- `datahub-frontend/`

### `filter:backend`

Only include PRs with changes in:

- `metadata-service/`
- `metadata-io/`
- `metadata-models/`
- `entity-registry/`

### `filter:graphql`

Only include PRs with changes in:

- `datahub-graphql-core/`

### `filter:docs`

Only include PRs with changes in:

- `docs/`
- `*.md` files

### `filter:custom:<path1>,<path2>`

Custom folder filter. Example: `filter:custom:smoke-test/,docker/`

---

## Applying Filters

### For PR list mode (`prs:...`)

Check each PR's files and filter:

```bash
gh pr view <pr> --repo datahub-project/datahub --json files --jq '.files[].path' | grep -E '^(<pattern>)' | head -1
```

If filter is set and PR has no matching files, **skip it**.

### For tag range mode

Filter commits directly in git log:

```bash
git log <from_tag>..<to_ref> --pretty=format:'%s' -- <folder1>/ <folder2>/ | grep -oE '#[0-9]+' | grep -oE '[0-9]+' | sort -u
```

### For time-based and date-range modes

First get all PRs, then filter each by checking its files.

---

## Filter Pattern Reference

| Filter      | Grep Pattern                                                          |
| ----------- | --------------------------------------------------------------------- |
| `ingestion` | `^(metadata-ingestion\|metadata-ingestion-modules)/`                  |
| `frontend`  | `^(datahub-web-react\|datahub-frontend)/`                             |
| `backend`   | `^(metadata-service\|metadata-io\|metadata-models\|entity-registry)/` |
| `graphql`   | `^datahub-graphql-core/`                                              |
| `docs`      | `^docs/\|\.md$`                                                       |

## Breaking Change Detection

**IMPORTANT**: Cross-reference PRs with the official breaking changes documentation to identify documented breaking changes.

### Step 1: Fetch the updating-datahub.md file

```bash
gh api repos/datahub-project/datahub/contents/docs/how/updating-datahub.md --jq '.content' | base64 -d > /tmp/updating-datahub.md
```

Or use curl:

```bash
curl -sL https://raw.githubusercontent.com/datahub-project/datahub/master/docs/how/updating-datahub.md > /tmp/updating-datahub.md
```

### Step 2: Extract PR numbers from relevant sections

The file is organized by version with these sections:

- `## Next` - Unreleased changes (check this for PRs heading to next release)
- `## X.Y.Z` - Released versions

Each version may contain:

- `### Breaking Changes` - **Priority 1**: Must highlight these
- `### Potential Downtime` - **Priority 2**: Important for ops teams
- `### Deprecations` - **Priority 3**: Future breaking changes

### Step 3: Match PRs against breaking changes (OPTIMIZED)

**DO NOT read the entire file**. Use grep to search for specific PR numbers:

```bash
# Build a pattern from your PR numbers and search directly
grep -E '#(15862|15859|15828|15819)' /tmp/updating-datahub.md
```

This is much faster than reading the whole file and parsing it in your context.

**Alternative for all breaking changes in a section:**

```bash
# Extract PR numbers from Breaking Changes sections
grep -E '^- #[0-9]+' /tmp/updating-datahub.md | grep -oE '#[0-9]+' | tr -d '#'
```

Breaking changes format in the file:

```
- #12345: (Component) Description of breaking change
```

### Step 4: Enrich breaking change entries

If a PR is found in the breaking changes documentation:

1. **Mark it prominently** with 🚨 in the changelog
2. **Include the documented description** from updating-datahub.md
3. **Add component tag** if present (e.g., `(Ingestion)`, `(CLI)`, `(Helm)`)
4. **Note migration steps** if documented

### Breaking Change Sources (in priority order)

1. **updating-datahub.md** - Official documented breaking changes (highest priority)
2. **PR labels** - `breaking-change` or `breaking` label
3. **PR title** - Contains "BREAKING" or "[BREAKING]"
4. **Commit messages** - Contains "BREAKING CHANGE:" (conventional commits)

---

## Linear Ticket Linking

> **NOTE**: This section only applies to `internal` mode. For `external` changelogs, **skip this entire section** to save API calls and avoid including internal references.

**IMPORTANT**: For each PR in the changelog, search for linked Linear tickets to provide better context about why changes were made.

### How to Find Linked Linear Tickets (OPTIMIZED)

Use a **three-phase strategy** - extract first, then search only when needed:

#### Phase 0: Extract Linear refs from PR bodies (NO API CALLS)

**Do this FIRST** - it's free and often sufficient.

Use the provided script to extract Linear refs from PR bodies:

```bash
gh pr list --repo datahub-project/datahub --state merged --search "merged:>=DATE" \
  --json number,body | python3 .agent-skills/generating-datahub-changelog/scripts/extract_linear_refs.py --text
```

Output:

```
15862: ING-1282, PLAT-456
15859: ING-1372, OSS-123
15828: (none)
```

The script finds patterns: `ING-\d+`, `OSS-\d+`, `PLAT-\d+`, `CUS-\d+`, `PFP-\d+`

**If you find explicit references, skip Phase 1 and 2 for those PRs!**

#### Phase 1: Search by PR Number (ONLY if no refs found in body)

```
mcp__plugin_linear_linear__list_issues with query: "<PR_NUMBER>"
```

**Example**: For PR #15859, search with `query: "15859"`

This finds PR Review tickets that have the PR number in their title/description.

#### Phase 2: Search by Keywords (SELECTIVE - only for important PRs)

**Only do this for:**

- Bug fixes (likely have customer issues)
- PRs with customer-related keywords in title

**Skip this for:**

- Chore/refactor PRs
- Documentation updates
- PRs that already have Linear refs from Phase 0/1

Extract 2-3 key terms from the PR title and search:

```
mcp__plugin_linear_linear__list_issues with query: "<KEY_TERMS>"
```

#### Efficiency Summary

| Phase   | When to Use                     | API Calls             |
| ------- | ------------------------------- | --------------------- |
| Phase 0 | Always (parse body)             | 0                     |
| Phase 1 | Only if Phase 0 found nothing   | 1 per PR without refs |
| Phase 2 | Only for bug fixes without refs | Selective             |

### What Links PRs to Linear Tickets

1. **PR Review tickets** - Auto-created with format `[PR Review] <title>` containing PR link in description
2. **Customer issues** - Have PR URLs in attachments when fixes are submitted (need keyword search)
3. **Feature tickets** - Reference PR numbers in description or title

### Processing Search Results

When you find linked Linear issues:

1. **Filter relevant results** - Only include issues that actually reference YOUR PR (not just matching numbers)
2. **Extract key info**: Issue identifier (e.g., `ING-1372`), title, URL, and status
3. **Identify issue type**: Customer issue, feature request, bug fix, PR review
4. **Extract customer information** from multiple sources:

#### Customer Extraction Methods (REQUIRED for internal mode)

**🔴 CRITICAL: Check ALL sources for EVERY Linear ticket found. Missing customer tags is a changelog quality failure.**

---

**Method 1: Ticket identifier prefix (CHECK FIRST - highest signal)**
`CUS-XXXX` tickets are **ALWAYS** customer-related. This is automatic - no exceptions:

```
CUS-1234  →  🎫 Customer issue (then extract name from other methods)
ING-5678  →  May or may not be customer-related (check other methods)
OSS-9999  →  Usually community, but check for customer links
```

**Action**: If ticket is `CUS-*`, it MUST get a `🎫 Customer:` tag in output.

---

**Method 2: Labels array**
Look for customer indicators in the `labels` array:

```json
"labels": ["Company: Acme", "escalated", "cs-bug"]
```

**Label patterns to check:**

- `"Company: <name>"` format (e.g., `"Company: Acme"`) - extract name after colon
- Plain company names (see Known Customers List below)
- Labels containing `acryl-<customer>` (e.g., `"acryl-acme"`, `"acryl-initech"`)
- Labels containing customer identifiers (e.g., `"customer-request"`, `"cs-bug"`)

---

**Method 3: Description email domains**
Parse customer emails from description text. Look for patterns:

- `created by [email@domain.com]`
- `Reported by: email@domain.com`
- `from email@domain.com`
- Any `@company.com` in the text

**Map domain to company using the Known Customers List below.**

---

**Method 4: Zendesk attachments (automatic customer signal)**
Any ticket with Zendesk links is customer-related:

```json
"attachments": [{"url": "https://acrylsupport.zendesk.com/..."}]
```

**Action**: If Zendesk link exists, ticket MUST get a `🎫 Customer:` tag.
If customer name not found elsewhere, use `🎫 Customer Issue` (from Zendesk)

---

**Method 5: Project name**
Issues in these projects are customer-related:

- "Customer Issues" → Always customer
- "Customer Feature Requests" → Always customer
- "Customer Success" → Always customer

---

### Email Domain → Customer Name Extraction

**Simple rule**: Extract company name from email domain by capitalizing the domain name.

- `@initech.example` → Initech
- `@contoso.example` → Contoso
- `@globex.example` → Globex

**Skip generic/personal domains** (not customer-identifiable):
`@gmail.com`, `@outlook.com`, `@hotmail.com`, `@yahoo.com`, `@icloud.com`, `@protonmail.com`

---

**Priority order for extraction**:

1. CUS- prefix (automatic customer)
2. Labels with `Company:`
3. Labels with `acryl-<customer>`
4. Labels with plain customer name
5. Description email domain
6. Zendesk attachments
7. Project name

**Output format**: Always tag customer issues with `🎫 Customer: <name>` or `🎫 Customer Issue` if name unknown

### Batch Processing for Efficiency

When processing multiple PRs:

1. **Phase 0 (local parsing)**: Extract Linear refs from PR bodies first

   - Parse `body` field for `ING-\d+`, `OSS-\d+`, `CUS-\d+`, `PLAT-\d+`, `PFP-\d+` patterns
   - Note which PRs have explicit refs (but still search for additional context)

2. **Phase 1 (parallel)**: Search PR numbers for ALL PRs

   - Run `list_issues` queries in parallel with PR numbers
   - **DO NOT skip PRs with body refs** - there may be additional linked tickets (e.g., CUS- tickets)
   - This catches PR Review tickets and related customer issues

3. **Phase 2 (features and bug fixes)**: Keyword search for important PRs

   - For `feat(` and `fix(` PRs: Search by 2-3 keywords from title
   - This catches customer issues that reference features/bugs but not PR numbers
   - Run searches in parallel

4. **Phase 3 (customer scan)**: For EVERY ticket found, check customer indicators

   - `CUS-*` prefix → automatic customer
   - Zendesk links → automatic customer
   - `Company:` labels → extract customer name
   - Email domains → map to customer name
   - "Customer Issues" project → customer

5. **Deduplicate results**: Same Linear ticket may appear in multiple phases

**⚠️ IMPORTANT**: The old approach of skipping searches for PRs with body refs caused missed customer connections. Better to make a few extra API calls than miss customer attribution.

### Linear API Fallback

If Linear search fails or times out:

1. **Log the failure** but continue processing
2. **Mark affected PRs** with `[Linear unavailable]` in internal changelogs
3. **Output changelog** without Linear context - the changelog is still valuable
4. **Don't block** the entire changelog for Linear failures

### Fetching Linear Ticket Status (OPTIMIZED)

**IMPORTANT**: Batch status fetching to minimize API calls.

#### Option 1: Use list_issues with query (PREFERRED - fewer calls)

If you have multiple ticket IDs, search for them together:

```
mcp__plugin_linear_linear__list_issues with query: "ING-1282 OR ING-1372 OR OSS-942"
```

This returns multiple issues in ONE call, each with their status.

#### Option 2: Parallel get_issue calls (when Option 1 doesn't work)

If you must use individual calls, run them **in parallel** (multiple tool calls in single message):

```
mcp__plugin_linear_linux__get_issue with id: "ING-1282"
mcp__plugin_linear_linear__get_issue with id: "ING-1372"
mcp__plugin_linear_linear__get_issue with id: "OSS-942"
```

**Run ALL get_issue calls in a single message** - don't wait for each one.

#### Status Mapping

- `Done` → Display as ✅ Done
- `In Progress` → Display as 🔄 In Progress
- `Todo` or `Backlog` → Display as 📋 Todo
- `Canceled` → Display as ❌ Canceled

**Format in changelog**: `[ING-1372](https://linear.app/...) - ✅ Done`

### What to Include in Changelog

For PRs with linked Linear tickets:

- Add Linear ticket link: `([ING-123](https://linear.app/...))`
- **Add ticket status**: `- ✅ Done` or `- 🔄 In Progress`
- If customer-related: Note the company/context
- If part of larger initiative: Note the project name

---

## PR Statistics (Internal Mode Only)

> **NOTE**: This section only applies to `internal` mode. For `external` changelogs, **skip this section**.

Calculate and display PR merge statistics to give insight into team velocity and reviewer contributions.

### Data Source - REUSE INITIAL FETCH

**DO NOT make additional API calls for statistics!**

You already have all the data from your initial PR fetch (see [Mode 1: Time-based](#mode-1-time-based-default)):

```json
{
  "number": 15862,
  "createdAt": "2025-01-05T10:00:00Z",
  "mergedAt": "2025-01-07T15:30:00Z",
  "reviews": [{ "author": { "login": "reviewer1" }, "state": "APPROVED" }]
}
```

### Using the Stats Script

Use the provided script to calculate stats from PR JSON:

```bash
gh pr list --repo datahub-project/datahub --state merged --search "merged:>=DATE" \
  --json number,createdAt,mergedAt,reviews | \
  python3 .agent-skills/generating-datahub-changelog/scripts/calculate_merge_stats.py
```

Output:

```
Median: 3.2 weeks
Average: 3.9 weeks
Top Reviewers: @sgomezvillamor (13), @askumar27 (3), @deepgarg760 (2)
```

The script automatically converts times to human-friendly format per the style guide.

### Calculating Merge Time Statistics

For each PR **included in the changelog**, calculate the time from creation to merge:

```
merge_time = mergedAt - createdAt (in hours)
```

Calculate both **median** and **average** to provide robust statistics:

- **Median**: Sort merge times, take middle value (or average of two middle values)
- **Average**: sum(merge_times) / count(PRs)

The median is more robust against outliers (e.g., a PR that sat open for weeks before merging).

**Display format** (use human-friendly units per style guide):

```
**Merge Time**: Median 3.2 weeks, Average 3.9 weeks
```

**Human-friendly conversion** (see DATAHUB_WRITE_STYLE.md for full rules):

- < 48 hours → use hours
- 2-14 days → use days
- 2-8 weeks → use weeks
- > 8 weeks → use months

**Examples**:

- 542 hours → "3.2 weeks" or "22.6 days"
- 24 hours → "1 day" or "24 hours"
- 2.5 hours → "2 hours 30 minutes"

**Important**: Only calculate statistics for PRs that are **included in the changelog** (after filtering). This ensures the stats reflect the actual changes being reported, not unrelated PRs.

### Counting Top Reviewers

From the `reviews` array, count APPROVED reviews per reviewer:

```json
"reviews": [
  {"author": {"login": "reviewer1"}, "state": "APPROVED"},
  {"author": {"login": "reviewer2"}, "state": "COMMENTED"}
]
```

Only count reviews with `"state": "APPROVED"`.

**Display format**: `**Top Reviewers**: @sgomezvillamor (13), @david-leifker (8), @chriscollins3456 (6)`

Show top 3 reviewers by approval count.

### Processing Steps for PR Statistics

1. Fetch PRs with `--json number,title,createdAt,mergedAt,reviews`
2. **Use only PRs that are included in the changelog** (after applying any filters)
3. Calculate merge time for each PR: `(mergedAt - createdAt) / 3600000` (ms to hours)
4. Sort merge times and calculate **median** (middle value)
5. Calculate **average** of merge times
6. Count APPROVED reviews per reviewer login (from changelog PRs only)
7. Sort reviewers by count, take top 3

---

## Audience Mode: Internal vs External

Two audience modes are supported:

- **`internal`** (default): Full changelog with Linear tickets, customer names, internal context
- **`external`**: Public-facing changelog, no customer info, no Linear links

**Key difference**: External mode skips Linear API searches entirely for efficiency.

See the template files for detailed include/exclude rules:

- `.agent-skills/generating-datahub-changelog/templates/changelog-internal.md`
- `.agent-skills/generating-datahub-changelog/templates/changelog-external.md`

---

## Handling Large PR Counts (>30 PRs)

When processing many PRs, adjust your approach to keep the changelog readable:

1. **Group by category** rather than listing individually

   - "12 bug fixes across ingestion connectors"
   - "8 documentation updates"

2. **Prioritize content**:

   - 🚨 Breaking changes: Always list individually with full details
   - 🌟 Features: List individually (up to 10), summarize remainder
   - 🐛 Bug fixes: List critical ones, group minor fixes
   - 🛠️ Chores/docs: Summarize with counts

3. **Use summary counts**:

   - "Plus 15 additional improvements across the codebase"
   - "Thanks to 8 contributors this week!"

4. **Still credit contributors**: Even in summary mode, mention all contributor handles

---

## PR Analysis (Optimized Workflow)

**Follow this exact order** for maximum efficiency:

### Step 1: Single PR Fetch (ALL data at once)

```bash
gh pr list --repo datahub-project/datahub --state merged --search "merged:>=<DATE>" --limit 100 \
  --json number,title,author,labels,mergedAt,createdAt,body,reviews,files > /tmp/prs_raw.json
```

This one call gives you EVERYTHING: PR details, bodies (for Linear refs), and review data (for stats).

### Step 2: Filter and Extract Key Fields (USE JQ - saves tokens!)

**Don't read the full JSON into context.** Use jq to extract only what you need:

```bash
# For ingestion filter - extract minimal fields
cat /tmp/prs_raw.json | jq '[.[] | select(.files[]?.path | test("^(metadata-ingestion|metadata-ingestion-modules)/")) | {number, title, author: .author.login, labels: [.labels[].name], createdAt, mergedAt}]' > /tmp/prs_filtered.json

# Categorize by title prefix
cat /tmp/prs_filtered.json | jq 'group_by(.title | if startswith("feat") then "feature" elif startswith("fix") then "fix" elif startswith("docs") then "docs" else "other" end)'
```

### Step 3: Extract Linear Refs (USE SCRIPT - no API calls)

```bash
cat /tmp/prs_raw.json | python3 .agent-skills/generating-datahub-changelog/scripts/extract_linear_refs.py --text
```

Output: `15859: ING-1372` or `15828: (none)`

### Step 4: Breaking Changes Check (one grep)

```bash
grep -E '#(PR_NUM1|PR_NUM2|PR_NUM3)' /tmp/updating-datahub.md
```

### Step 5: [Internal only] Linear Searches (COMPREHENSIVE for customer detection)

- **For ALL bug fixes and features**: Search by PR number AND title keywords
- For chore/doc PRs: Search by PR number only (quick check)
- Run searches in parallel batches

### Step 6: [Internal only] Customer Scan Phase (MANDATORY)

**This step is CRITICAL - do not skip!**

**Use the customer extraction script** to automate this:

```bash
# Save Linear tickets to JSON, then extract customers
echo '<linear_tickets_json>' | python3 .agent-skills/generating-datahub-changelog/scripts/extract_customers.py --text
```

Output:

```
ING-1282: Acme (label:Company:)
CUS-6615: Globex (email:@globex.example) [AUTO:CUS-prefix]
ING-1304: (no customer)
```

The script automatically checks (in priority order):

1. `CUS-XXXX` prefix → automatic customer
2. Labels with `Company:` or `acryl-<customer>`
3. Email domains in description
4. Zendesk links in attachments
5. "Customer Issues" project

Build a customer mapping: `{PR_number: customer_name or null}`

### Step 7: [Internal only] Fetch Linear Status (batch)

- Use `list_issues` with OR query, OR
- Parallel `get_issue` calls in single message

### Step 8: [Internal only] Calculate Statistics (from Step 1 data)

- Parse `createdAt`/`mergedAt` for merge times
- Count `reviews` with `state: "APPROVED"` for reviewer stats

### Step 9: [Internal only] Customer Tag Validation (MANDATORY)

Before generating output, verify:

- [ ] All `CUS-XXXX` tickets have `🎫 Customer:` tag
- [ ] All tickets with Zendesk links have customer tag
- [ ] All tickets in "Customer Issues" project have customer tag
      If any are missing, go back and extract customer name or use `🎫 Customer Issue`

### Step 10: Generate Changelog

Apply audience mode rules and output formatted changelog.

## Content Priorities

1. Breaking changes (if any) - MUST be at the top
2. New connectors
3. Critical bug fixes
4. Performance improvements
5. Developer experience improvements
6. Documentation updates

## Formatting Guidelines

Now, create a change log summary with the following guidelines:

1. Keep it concise and to the point
2. Highlight the most important changes first
3. Group similar changes together (e.g., all new features, all bug fixes)
4. Include issue references where applicable
5. Mention the names of contributors, giving them credit for their work
6. Add a touch of humor or playfulness to make it engaging
7. Use emojis sparingly to add visual interest
8. Keep total message under 2000 characters for Discord
9. Use consistent emoji for each section
10. Format code/technical terms in backticks
11. Include PR numbers in parentheses (e.g., "Fixed login bug (#123)")

## Deployment Notes

When relevant, include:

- Database migrations required
- Environment variable updates needed
- Manual intervention steps post-deploy
- Dependencies that need updating

Your final output should be formatted based on the **audience mode**.

**Read the appropriate template file:**

- **Internal mode** (default): Read `.agent-skills/generating-datahub-changelog/templates/changelog-internal.md`
- **External mode**: Read `.agent-skills/generating-datahub-changelog/templates/changelog-external.md`

The template files contain:

- The exact output format to use
- What to include/exclude
- Processing steps specific to that mode
- Language guidelines

---

## Pre-Output Validation Checklist

Before outputting the final changelog, verify:

- [ ] **Breaking changes at top** (if any exist) - marked with 🚨
- [ ] **Character count < 2000** for Discord compatibility
- [ ] **No customer names** in external mode
- [ ] **No Linear ticket links** (ING-XXXX) in external mode
- [ ] **All contributors credited** - GitHub handles mentioned
- [ ] **PR numbers included** for all entries
- [ ] **Correct template used** - internal vs external format
- [ ] **[Internal mode]** PR Statistics section included with average merge time and top reviewers
- [ ] **[Internal mode]** Linear ticket statuses shown (✅/🔄/📋/❌)

### 🔴 Customer Tag Validation (Internal Mode - MANDATORY)

**Before outputting, scan your changelog and verify:**

- [ ] **Every `CUS-XXXX` ticket** has a `🎫 Customer:` tag
- [ ] **Every ticket with Zendesk link** has a `🎫 Customer:` tag
- [ ] **Every ticket in "Customer Issues" project** has a `🎫 Customer:` tag
- [ ] **Every ticket with `Company:` label** has corresponding `🎫 Customer:` tag
- [ ] **Every ticket with `acryl-<customer>` label** has corresponding `🎫 Customer:` tag

**If ANY customer indicator exists but no `🎫` tag in output:**

1. STOP - do not output yet
2. Go back to the Linear ticket
3. Extract customer name using the Known Customers List
4. Add the `🎫 Customer: <name>` tag
5. If customer name truly cannot be determined, use `🎫 Customer Issue`

**Common mistakes to avoid:**

- ❌ Seeing `CUS-6615` and not tagging it as customer
- ❌ Seeing Zendesk link in attachments and not tagging
- ❌ Seeing `@initech.example` in description and not mapping to Initech
- ❌ Seeing `project: "Customer Issues"` and not tagging

If any check fails, revise before outputting.

---

## Style Guide Review

Now review the changelog using the `.agent-skills/generating-datahub-changelog/references/DATAHUB_WRITE_STYLE.md` file and go one by one to make sure you are following the style guide. Use multiple agents, run in parallel to make it faster.

Remember, your final output should only include the content within the <change_log> tags. Do not include any of your thought process or the original data in the output.

## Known Limitations

- **Linear Customer Requests**: The `customerNeeds` field is not accessible via MCP - use `Company:` labels as workaround
- **PR fetch limit**: Maximum 100 PRs fetched per query (GitHub API limit) - pagination not implemented
- **Repository hardcoded**: Currently only works with `datahub-project/datahub` repository
- **Date command syntax**: Uses macOS `date -v` syntax - may need adjustment for Linux

## Error Handling

- If no changes in the time period, post a "quiet day" message: "🌤️ Quiet day! No new changes merged."
- If unable to fetch PR details, list the PR numbers for manual review
- Always validate message length before posting to Discord (max 2000 chars)

## Schedule Recommendations

- Run daily at 6 AM NY time for previous day's changes
- Run weekly summary on Mondays for the previous week
- Special runs after major releases or deployments

## Audience Considerations

Adjust the tone and detail level based on the channel:

- **Dev team channels**: Include technical details, performance metrics, code snippets
- **Product team channels**: Focus on user-facing changes and business impact
- **Leadership channels**: Highlight progress on key initiatives and blockers
