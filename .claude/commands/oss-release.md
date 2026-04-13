# OSS CLI Release

Run the OSS CLI release workflow for acryldata/datahub.

**User's request:** $ARGUMENTS

**Read `.agent-skills/oss-release/SKILL.md` and follow its workflow exactly.**

## Subcommands

- **`prep`** — Diff analysis, safety check, and RC pre-release creation. Determines the next
  version automatically. Done in minutes. The RC is a complete deliverable on its own.

- **`finish`** — Promote the latest RC to an official release. Auto-detects the most recent RC
  tag. Checks for new commits, checks CI, waits for explicit confirmation, then cuts the final tag.

- **`status`** — Check CI status for the latest RC without taking action.

If no subcommand is given, ask the user which they want.
