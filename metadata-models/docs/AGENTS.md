# Entity Documentation — Authoring Guide

This directory (`metadata-models/docs/entities/`) contains hand-authored descriptions of DataHub
metadata entities. These files are **inputs** to the `modelDocGen` build step — they are not served
directly.

## How it works

The Gradle task `:metadata-ingestion:modelDocGen` (runs `metadata-ingestion/scripts/modeldocgen.py`)
combines three sources to produce auto-generated entity reference pages:

1. **Entity registry** — `metadata-models/src/main/resources/entity-registry.yml`
2. **Avro schemas** — generated from PDL files during the build
3. **Entity docs** — the markdown files in this directory

Output lands in `docs/generated/metamodel/entities/` (gitignored).

## Authoring an entity doc

Each file is named `{entityName}.md` (matching the entity name in the registry). The content is
free-form markdown that describes what the entity represents, its key aspects, and how it relates to
other entities. This prose is spliced into the auto-generated reference page.

Existing files to use as reference: `dataset.md`, `dashboard.md`, `chart.md`.

## After editing

Regenerate and preview:

```bash
./gradlew :metadata-ingestion:modelDocGen    # regenerate entity reference pages
scripts/dev/datahub-dev.sh docs              # preview the docs site
```
