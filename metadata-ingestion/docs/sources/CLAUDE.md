# Connector Documentation — Structure & Authoring Guide

This directory contains hand-authored documentation for DataHub source connectors.
The public docs site assembles each connector's final page by combining these files in order:

```
{connector}_pre.md          → Prerequisites, setup instructions
[Auto-generated]            → Capabilities table, install command
{connector}_recipe.yml      → Example configuration
[Auto-generated]            → Config options table + JSON schema
{connector}_post.md         → Additional notes, troubleshooting
```

## What to author

Only edit `_pre.md`, `_post.md`, and `_recipe.yml`. Everything else is generated at build time and will be overwritten.

| File                     | Purpose                                                   | Required? |
| ------------------------ | --------------------------------------------------------- | --------- |
| `{connector}_pre.md`     | Prerequisites, credentials, permissions, setup steps      | Yes       |
| `{connector}_recipe.yml` | Minimal working example config                            | Yes       |
| `{connector}_post.md`    | Troubleshooting, known limitations, advanced config notes | Optional  |

## Style conventions

- **Tone**: second-person ("you"), present tense, direct
- **Terminology**: use shared vocabulary consistently

### Heading structure (CRITICAL)

**All connector docs must use consistent heading hierarchy:**

- **Baseline: H3 (`###`)** for main sections (Prerequisites, Authentication, Setup, etc.)
- **Maximum depth: H5 (`#####`)** for subsections
- **Never use H1 or H2** in `_pre.md`, `_post.md`, or `README.md` files

**Rationale:** The docgen script generates this structure:

```
# Platform Name (H1) ← auto-generated
[README.md content with H3 sections]
## Module `plugin-name` (H2) ← auto-generated (always shown)
### Important Capabilities (H3) ← auto-generated
[_pre.md content with H3 sections]
### Install the Plugin (H3) ← auto-generated (if extra_deps exist)
### Starter Recipe (H3) ← auto-generated (if recipe exists)
### Config Details (H3) ← auto-generated
[_post.md content with H3 sections]
### Code Coordinates (H3) ← auto-generated
```

Using H3 as baseline ensures proper hierarchy whether there's one plugin or multiple plugins per platform.

**Example structure for \_pre.md:**

```markdown
### Prerequisites

Your connector requires specific privileges...

#### Required Permissions

Grant the following permissions:

##### Option 1: Using Service Account

Follow these steps...
```

| Preferred         | Avoid                                  |
| ----------------- | -------------------------------------- |
| ingestion recipe  | config file, pipeline config           |
| source plugin     | connector plugin, integration          |
| platform instance | source instance, deployment            |
| credential        | secret, token (unless vendor-specific) |

## Snowflake as the canonical reference

When in doubt, use `snowflake_pre.md` and `snowflake_recipe.yml` as the reference for structure, tone, and level of detail.

## Formatting gate

All `.md` files must pass Prettier before merging. Run:

```bash
# Check
./gradlew :datahub-web-react:mdPrettierCheck

# Auto-fix
./gradlew :datahub-web-react:mdPrettierWrite
```

Always run `mdPrettierWrite` before committing hand-authored files. The check runs in CI and will fail the build if formatting is off. Do not manually fix whitespace or list indentation — let Prettier handle it.
