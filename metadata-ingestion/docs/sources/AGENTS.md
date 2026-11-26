# Connector Documentation — Structure & Authoring Guide

This directory contains hand-authored documentation for DataHub source connectors.
The public docs site assembles each connector's final page by combining these files in order:

```
README.md                    → Platform-level Overview + Concept Mapping
[Auto-generated]             → Module heading + support badge + Important Capabilities table
{connector}_pre.md           → Module narrative before injected sections
[Auto-generated]            → Install command
{connector}_recipe.yml      → Starter recipe
[Auto-generated]            → Config options table + JSON schema
{connector}_post.md           → Module narrative after injected sections
[Auto-generated]            → Code Coordinates
```

## What to author

Only edit `README.md`, `_pre.md`, `_post.md`, and `_recipe.yml`.
Everything under the generated docs tree is produced at build time and will be overwritten.

| File                     | Purpose                                                  | Required? |
| ------------------------ | -------------------------------------------------------- | --------- |
| `README.md`              | Platform-level context, common overview, concept mapping | Yes       |
| `{connector}_pre.md`     | Module-level pre-injection sections                      | Yes       |
| `{connector}_recipe.yml` | Minimal working example config                           | Yes       |
| `{connector}_post.md`    | Module-level post-injection sections                     | Yes       |

## Style conventions

- **Tone**: second-person ("you"), present tense, direct
- **Terminology**: use shared vocabulary consistently

### Terminology

| Preferred         | Avoid                                  |
| ----------------- | -------------------------------------- |
| ingestion recipe  | config file, pipeline config           |
| source plugin     | connector plugin, integration          |
| platform instance | source instance, deployment            |
| credential        | secret, token (unless vendor-specific) |

### Generated page structure

The heading contract below is enforced by `validate_source_doc_headings` in
`scripts/docgen.py`, which `tests/unit/test_source_doc_headings.py` runs over every
authored file. The generated platform page reads: `# Platform Name`, the platform
`README.md` (`## Overview`, `## Concept Mapping`), then per module the auto-generated
`## Module` heading and `### Important Capabilities`, the `_pre.md` sections, the injected
`Install the Plugin` / `Starter Recipe` / `Config Details`, the `_post.md` sections, and
`### Code Coordinates`.

### Heading-level rules by file type

- `README.md`: **H2 baseline** (`##`) for platform sections.
- `_pre.md`, `_post.md`: **H3 baseline** (`###`) for module sections.
- Maximum depth: H5 (`#####`) by convention; the check only enforces H2/H3 placement.
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

## Snowflake as the canonical reference

When in doubt, use `snowflake/README.md`, `snowflake_pre.md`, and `snowflake_recipe.yml` as references for structure, tone, and level of detail.

## Formatting

Run `./gradlew :datahub-web-react:mdPrettierWrite` before committing; CI runs the matching
check (root `AGENTS.md`, "Code Formatting and Linting"). Regenerate the site docs with
`./gradlew :metadata-ingestion:docGen`.
