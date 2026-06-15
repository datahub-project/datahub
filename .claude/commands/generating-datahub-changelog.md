---
description: Generate a changelog / release notes for a DataHub release range (repo-canonical, vendored from the connectors-accelerator plugin)
argument-hint: '[daily|weekly|"last 2 weeks"|prs:...|from:tag] [ingestion|backend|frontend|all] [internal|external]'
allowed-tools:
  # GitHub CLI - read-only repo queries
  - Bash(gh pr view:*)
  - Bash(gh pr list:*)
  - Bash(gh api:*)
  # Git - read-only log/tag queries
  - Bash(git log:*)
  - Bash(git tag:*)
  # Utilities - safe read-only operations
  - Bash(date:*)
  - Bash(curl:*)
  - Bash(jq:*)
  # Vendored python scripts
  - Bash(python3 .agent-skills/generating-datahub-changelog/scripts/extract_linear_refs.py:*)
  - Bash(python3 .agent-skills/generating-datahub-changelog/scripts/extract_customers.py:*)
  - Bash(python3 .agent-skills/generating-datahub-changelog/scripts/calculate_merge_stats.py:*)
  # File tools
  - Read
  - Grep
  - Glob
  - Write
  # Linear API - read-only (changelog enrichment, internal mode)
  - mcp__claude_ai_Linear__list_issues
  - mcp__claude_ai_Linear__get_issue
---

# Generate DataHub Changelog

**User's request:** $ARGUMENTS

**Read `.agent-skills/generating-datahub-changelog/SKILL.md` and follow it exactly.** That file
is the repo-canonical, version-controlled copy of the changelog workflow (vendored from the
`connectors-accelerator` plugin). Its script, template, and reference paths are repo-relative
under `.agent-skills/generating-datahub-changelog/`.

Prefer this in-repo skill over the plugin's `connectors-accelerator:generating-datahub-changelog`
when working inside this repository.
