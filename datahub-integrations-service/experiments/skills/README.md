# Claude Code Skills

This folder contains Claude Code skills for common DataHub operations. Skills teach Claude how to perform specific tasks using scripts and tools in this repository.

## What are Skills?

Skills are markdown files that describe how to accomplish specific tasks. When you ask Claude to do something that matches a skill's description, Claude will follow the instructions in the skill to complete the task.

## Available Skills

| Skill                                                         | Description                                                          |
| ------------------------------------------------------------- | -------------------------------------------------------------------- |
| [chatbot-events-fetcher](./chatbot-events-fetcher/skill.md)   | Fetch and analyze ChatbotInteraction events from DataHub deployments |
| [datahub-token-generator](./datahub-token-generator/skill.md) | Generate authentication tokens for DataHub instances                 |

## Installation

To install these skills for Claude Code, create symlinks in your user skills directory:

```bash
# Create the skills directory if it doesn't exist
mkdir -p ~/.claude/skills

# Symlink each skill
ln -s /Users/alex/work/datahub-fork/datahub-integrations-service/experiments/skills/chatbot-events-fetcher ~/.claude/skills/chatbot-events-fetcher
ln -s /Users/alex/work/datahub-fork/datahub-integrations-service/experiments/skills/datahub-token-generator ~/.claude/skills/datahub-token-generator
```

Or copy the skill folders directly:

```bash
cp -r /Users/alex/work/datahub-fork/datahub-integrations-service/experiments/skills/* ~/.claude/skills/
```

## Verifying Installation

After installation, you can verify skills are available:

```bash
ls -la ~/.claude/skills/
```

You should see both skill directories listed.

## Usage

Once installed, simply ask Claude to perform tasks related to the skills:

- "Fetch chatbot events from the last 7 days"
- "Find chatbot conversations with errors"
- "Generate a token for the Xero DataHub instance"
- "Search chatbot history for complaints about lineage"

Claude will automatically use the appropriate skill based on your request.

## Skill Structure

Each skill is a folder containing a `skill.md` file with:

```yaml
---
name: Skill Name
description: Brief description shown in skill listings
---
# Skill Name

Detailed instructions for Claude on how to perform the task...
```

## Creating New Skills

1. Create a new folder under `~/.claude/skills/` (or here and symlink)
2. Add a `skill.md` file with frontmatter and instructions
3. Include examples, prerequisites, and troubleshooting tips
4. Claude will automatically discover and use the skill
