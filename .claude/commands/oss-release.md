---
description: Cut an RC or stable release of the acryl-datahub OSS CLI, or check its status
argument-hint: [rc|stable|status] [patch|minor|major|fourth] [--dry-run]
---

# OSS CLI Release

Run the OSS CLI release workflow for acryldata/datahub.

**User's request:** $ARGUMENTS

**Read `.agent-skills/oss-release/SKILL.md` and follow its workflow exactly.**

## Subcommands

- **`rc`** (alias: `prep`) — Cut a new release candidate. Diff analysis, safety check, version
  bump, changelog, RC pre-release creation, connector-tests dispatch. Done in minutes. The RC
  is a complete deliverable on its own — ship it to customers for testing without promoting.
  Append `patch`, `minor`, `major`, or `fourth` to override the default bump (e.g.
  `/oss-release rc minor` → `v1.6.0.0rc1`).

- **`stable`** (alias: `finish`) — Promote the latest RC to a stable release. Auto-detects the
  most recent RC tag. Checks release CI, verifies connector tests passed, requires explicit
  confirmation, then cuts the final tag.

- **`status`** — Check CI status (release + connector tests) for the latest RC without taking action.

If no subcommand is given, ask the user which they want.
