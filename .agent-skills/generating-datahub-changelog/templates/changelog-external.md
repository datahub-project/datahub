# External Changelog Template

Use this template when `external` mode is specified. This is for public-facing changelogs safe to share with customers.

## Output Format

<change_log>

# 🚀 [Daily/Weekly] Change Log: [Current Date]

## 🚨 Breaking Changes (if any)

### Documented Breaking Changes

- 🚨 **PR #XXXX** (Component): Description from updating-datahub.md
  - Migration: [any documented migration steps]

### Other Breaking Changes

- ⚠️ **PR #XXXX**: Description from PR

## ⏰ Potential Downtime (if any)

[PRs documented in "Potential Downtime" section]

## 🌟 New Features

[List new features with PR numbers - NO Linear links, NO customer references]

- Example: Added exponential backoff for Tableau (#15828)
- Example: New Snowflake lineage extraction (#15830)

## 🐛 Bug Fixes

[List bug fixes with PR numbers - NO Linear links, NO customer references]

- Example: Fixed DB2 ARM compatibility (#15859)
- Example: Resolved authentication edge case in BigQuery (#15861)

## 🛠️ Other Improvements

[List other significant changes]

## 🙌 Contributors

[Thank contributors - GitHub handles are public and OK to share]

- @contributor1, @contributor2

</change_log>

## What to Include/Exclude

| Content Type                   | Include?      | Notes                            |
| ------------------------------ | ------------- | -------------------------------- |
| PR numbers and GitHub links    | ✅ Yes        | Public repo, OK to share         |
| Linear ticket links            | ❌ No         | Internal tool                    |
| Customer names                 | ❌ No         | Never expose who reported issues |
| Zendesk ticket references      | ❌ No         | Internal support system          |
| Internal project names         | ❌ No         | Internal planning details        |
| Breaking change details        | ✅ Yes        | Important for users              |
| Contributor names              | ✅ Yes        | GitHub handles are public        |
| Fun facts/humor                | ⚠️ Toned down | Keep professional                |
| Linear Tickets Summary section | ❌ No         | Skip entire section              |

## Processing Steps

1. **Skip Linear ticket searches entirely** (saves API calls)
2. Don't look up customer information
3. Focus only on what the change does, not who requested it
4. Use simplified output template without Linear section
5. Remove any customer-identifying information from PR descriptions before including

## Language Adjustments

When writing external changelogs, rephrase internal language:

| Internal                       | External                         |
| ------------------------------ | -------------------------------- |
| "Fixed issue reported by Acme" | "Fixed authentication edge case" |
| "Resolves customer escalation" | "Fixed critical bug"             |
| "Part of Q1 initiative"        | (omit entirely)                  |
| "See ING-1234 for details"     | (omit entirely)                  |

## Key Rules

- NO "🎫 Linked Linear Tickets" section
- NO "🎉 Fun Fact" section (keep it professional)
- NO customer names or references anywhere
- NO Linear ticket links (ING-XXXX)
- NO Zendesk references
- Keep tone professional but still friendly
