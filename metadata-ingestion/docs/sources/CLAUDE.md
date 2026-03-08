# Connector Documentation — Structure & Authoring Guide

This directory contains hand-authored documentation for DataHub source connectors.
The public docs site assembles each connector's final page by combining these files in order:

```
README.md                    → Platform-level Overview + Concept Mapping
[Auto-generated]             → Module heading + support badge + Important Capabilities
{connector}_pre.md           → Module narrative before injected sections
[Auto-generated]            → Capabilities table, install command
{connector}_recipe.yml      → Example configuration
[Auto-generated]            → Config options table + JSON schema
{connector}_post.md           → Module narrative after injected sections
[Auto-generated]            → Code Coordinates
```

## What to author

Only edit `README.md`, `_pre.md`, `_post.md`, and `_recipe.yml`.
Everything in `docs/generated/...` is generated at build time and will be overwritten.

| File                     | Purpose                                                         | Required?       |
| ------------------------ | --------------------------------------------------------------- | --------------- |
| `README.md`              | Platform-level context, common overview, concept mapping        | Yes             |
| `{connector}_pre.md`     | Module-level pre-injection sections                             | Yes             |
| `{connector}_recipe.yml` | Minimal working example config                                  | Yes             |
| `{connector}_post.md`    | Module-level post-injection sections                            | Yes             |

## Style conventions

- **Tone**: second-person ("you"), present tense, direct
- **Terminology**: use shared vocabulary consistently

### Required generated structure (CRITICAL)

Every generated platform page must follow this high-level order:

1. `# Platform Name` (auto-generated)
2. Platform `README.md` content with:
   - `## Overview`
   - `## Concept Mapping`
3. For each module:
   - `## Module \`module-name\`` + badge (auto-generated)
   - `### Important Capabilities` (auto-generated)
   - PRE sections (authored in `_pre.md`)
   - injected sections (`Install the Plugin`, `Starter Recipe`, `Config Details`)
   - POST sections (authored in `_post.md`)
   - `### Code Coordinates` (auto-generated)

### Heading-level rules by file type

- `README.md`: **H2 baseline** (`##`) for platform sections.
- `_pre.md`, `_post.md`: **H3 baseline** (`###`) for module sections.
- Maximum depth: H5 (`#####`).
- Never use H1 (`#`) in authored source docs under `docs/sources/**`; H1 is reserved for generated platform page titles.

### Platform `## Overview` authoring rules

`README.md` overview should be two short paragraphs:

1. **Source description paragraph**  
   Briefly explain what the source platform is and include a link to the official public site/docs when possible.
2. **Integration scope paragraph**  
   Summarize what DataHub covers (core entities) and mention important optional features (for example lineage, usage, profiling, ownership, tags, stateful deletion).

Avoid generic placeholders like "X can be integrated with DataHub..." without source context.

### Required module section contract

For each module, ensure the generated page effectively has:

```markdown
### Overview

### Important Capabilities (auto-generated)

### Prerequisites

### Install the Plugin (auto-generated)

### Starter Recipe (auto-generated from \_recipe.yml)

### Config Details (auto-generated)

### Capabilities

### Limitations

### Troubleshooting

### Code Coordinates (auto-generated)
```

In practice:

- Put `### Overview` and `### Prerequisites` in `{connector}_pre.md`.
- Put `### Capabilities`, `### Limitations`, and `### Troubleshooting` in `{connector}_post.md` so they render after injected `Config Details`.
- For **single-module platforms**, module `### Overview` should usually mirror platform `## Overview`.
- For **multi-module platforms**, module `### Overview` should emphasize what is specific about that module compared to sibling modules.

### Nesting rules for non-canonical sections

Any connector-specific deep-dive sections should be nested as `####` under one of:

- `### Prerequisites`
- `### Capabilities`
- `### Limitations`
- `### Troubleshooting`

Examples:

- `#### Lineage and Usage Computation Details` under `### Capabilities`
- `#### Profiling Details` under `### Capabilities`
- `#### Caveats` under `### Limitations`

Do not introduce extra module-level `###` sections outside the canonical contract.

| Preferred         | Avoid                                  |
| ----------------- | -------------------------------------- |
| ingestion recipe  | config file, pipeline config           |
| source plugin     | connector plugin, integration          |
| platform instance | source instance, deployment            |
| credential        | secret, token (unless vendor-specific) |

## Snowflake as the canonical reference

When in doubt, use `snowflake/README.md`, `snowflake_pre.md`, and `snowflake_recipe.yml` as references for structure, tone, and level of detail.

## Formatting gate

All `.md` files must pass Prettier before merging. Run:

```bash
# Check
./gradlew :datahub-web-react:mdPrettierCheck

# Auto-fix
./gradlew :datahub-web-react:mdPrettierWrite
```

Always run `mdPrettierWrite` before committing hand-authored files. The check runs in CI and will fail the build if formatting is off. Do not manually fix whitespace or list indentation — let Prettier handle it.
