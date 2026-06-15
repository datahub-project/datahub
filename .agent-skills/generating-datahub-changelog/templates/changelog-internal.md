# Internal Changelog Template

Use this template when `internal` mode is specified or no mode is given.

## Output Format

<change_log>

# 🚀 [Daily/Weekly] Change Log: [Current Date]

## 🚨 Breaking Changes (if any)

### Documented Breaking Changes

[PRs found in updating-datahub.md - include the official description and any migration notes]

- 🚨 **PR #XXXX** (Component): Official description from updating-datahub.md
  - Migration: [any documented migration steps]

### Other Breaking Changes

[PRs with breaking change labels/titles but not yet documented]

- ⚠️ **PR #XXXX**: Description from PR

## ⏰ Potential Downtime (if any)

[PRs documented in "Potential Downtime" section of updating-datahub.md]

## 🌟 New Features

[List new features here with PR numbers and Linear links if available]
[For customer-driven features, add a witty comment celebrating the customer win!]

- Example: Added exponential backoff for Tableau ([#15828](https://github.com/datahub-project/datahub/pull/15828)) - [ING-1282](https://linear.app/acryl-data/issue/ING-1282) - 🎫 Acme asked, we delivered!
- Example: New DB2 connector with full lineage support ([#14968](https://github.com/datahub-project/datahub/pull/14968)) - [CUS-6615](https://linear.app/acryl-data/issue/CUS-6615) - 🎫 Globex's wish is our command!

**Witty customer comment examples** (vary these, be creative!):

- "🎫 [Customer] asked, we delivered!"
- "🎫 [Customer]'s wish is our command!"
- "🎫 Another win for [Customer]!"
- "🎫 [Customer] can finally sleep well!"
- "🎫 Making [Customer] happy, one PR at a time!"
- "🎫 [Customer] reported it, we squashed it!"

## 🐛 Bug Fixes

[List bug fixes here with PR numbers and Linear links if available]
[For customer-reported bugs, add a witty comment acknowledging the fix!]

- Example: Fixed DB2 ARM compatibility ([#15859](https://github.com/datahub-project/datahub/pull/15859)) - [ING-1372](https://linear.app/acryl-data/issue/ING-1372) - 🎫 No more ARM wrestling for Acme!
- Example: Fixed Snowflake connection timeout ([#15860](https://github.com/datahub-project/datahub/pull/15860)) - [CUS-456](https://linear.app/acryl-data/issue/CUS-456) - 🎫 Hooli's timeouts are history!
- Example: Fixed query parsing edge case ([#15861](https://github.com/datahub-project/datahub/pull/15861)) - 🎫 Customer issue squashed!

## 🛠️ Other Improvements

[List other significant changes or improvements]

## 📊 PR Statistics

- **Merge Time**: Median X.X days/weeks, Average Y.Y days/weeks _(use human-friendly units per style guide)_
- **Top Reviewers**: @reviewer1 (N), @reviewer2 (N), @reviewer3 (N)

## 🎫 Linked Linear Tickets

[Summary of Linear tickets addressed by these PRs - separated by type]

**Status icons:**

- ✅ Done/Completed
- 🔄 In Progress
- 📋 Todo/Backlog
- ❌ Canceled

### Customer Issues

[List customer-related tickets with prominent customer attribution]
Format: `[TICKET-ID](url) - STATUS - 🎫 Customer: NAME - Description → Addressed by #PR`

- Example: [ING-1282](https://linear.app/acryl-data/issue/ING-1282) - 📋 Todo - 🎫 Customer: Acme - Tableau 503 errors → Addressed by #15828
- Example: [CUS-6615](https://linear.app/acryl-data/issue/CUS-6615) - ✅ Done - 🎫 Customer: Globex - DB2 z/OS guidance → Enabled by #14968

### Internal Issues

[List internal/engineering tickets without customer context]
Format: `[TICKET-ID](url) - STATUS - Description → Addressed by #PR`

- Example: [ING-1372](https://linear.app/acryl-data/issue/ING-1372) - ✅ Done - DB2 ARM CI build issues → Fixed by #15859

### Feature Requests & Projects

- **Feature Requests Completed**: List feature tickets closed by PRs
- **Projects Advanced**: Note any project milestones hit

## 🙌 Shoutouts

[Mention contributors and their contributions]

## 🎉 Fun Fact of the Day

[Include a brief, work-related fun fact or joke]

</change_log>

## What to Include

| Content Type                    | Include? | Example                                                  |
| ------------------------------- | -------- | -------------------------------------------------------- |
| PR numbers and GitHub links     | ✅ Yes   | ([#15859](https://github.com/.../pull/15859))            |
| Linear ticket links             | ✅ Yes   | [ING-1372](https://linear.app/acryl-data/issue/ING-1372) |
| Linear ticket status            | ✅ Yes   | [ING-1372](url) - ✅ Done                                |
| Customer names (Company: label) | ✅ Yes   | 🎫 Customer: Acme                                        |
| Customer names (plain label)    | ✅ Yes   | 🎫 Customer: Hooli                                       |
| **Witty customer comments**     | ✅ Yes   | 🎫 Acme asked, we delivered!                             |
| CUS-XXXX tickets                | ✅ Yes   | [CUS-456](url) - always customer-related                 |
| Zendesk ticket references       | ✅ Yes   | 🎫 Customer Issue (from Zendesk)                         |
| Internal project names          | ✅ Yes   | Part of Q1 Ingestion Initiative                          |
| Breaking change details         | ✅ Yes   | Full migration steps                                     |
| Contributor names               | ✅ Yes   | Thanks @john-doe!                                        |
| Fun facts/humor                 | ✅ Yes   | Developer in-jokes welcome                               |
| Linear Tickets Summary section  | ✅ Yes   | Full section with customer details                       |
| PR Statistics section           | ✅ Yes   | Average merge time, top reviewers                        |

## Processing Steps

1. Run Linear ticket searches (both phases)
2. Extract customer information from Linear tickets
3. **Fetch Linear ticket status** using `get_issue` for each linked ticket
4. **Calculate PR statistics**:
   - Fetch PR data with `createdAt`, `mergedAt`, and `reviews` fields
   - Calculate average merge time (mergedAt - createdAt) for all PRs
   - Count APPROVED reviews per reviewer to find top reviewers
5. Include all internal context
6. **Add witty customer comments** for customer-driven features and bug fixes in the main sections
7. **Separate Linear tickets** into Customer Issues vs Internal Issues subsections
8. Use full output template with Linear section and PR Statistics
