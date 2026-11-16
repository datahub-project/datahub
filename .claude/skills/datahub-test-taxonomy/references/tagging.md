# Tagging a test file

Instructions for turning one test file into a `test_tags.yaml` entry. Used by modes 1
(`tag`) and 2 (`retag`) of `/datahub-test-taxonomy`.

## Inputs

Read these before tagging:

1. `.github/smart-testing/taxonomy.yaml`, plus `taxonomy.overlay.yaml` when it exists.
   Every tag you use must appear in one of them, under the dimension you put it in. Read each tag's `definition` and `covers`, and the
   `annotator-guidance` section at the end.
2. The "Dimensions" section of `.github/smart-testing/taxonomy-rubric.md`.
3. The test file, in full. Also skim its local helpers or fixtures only when the test body
   is a thin wrapper around them.

## What a tag means

A test gets a tag when **a PR that breaks that concept would plausibly make this test
fail**. Selection runs on this, so over-tagging costs CI time and under-tagging risks a
missed failure. When in doubt between adding and leaving out a `feature-*` tag, add it.

## Per dimension

| Dimension         | Tag…                                                                                                                                                                                                                                          |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `feature-user`    | The product features the test asserts on, meaning what a DataHub user would call the thing under test. Every real test has at least one `feature-*` tag across `feature-user`, `feature-dev` and `compound`; without one it's never selected. |
| `feature-dev`     | The internal mechanisms the test depends on to pass, e.g. `search-indexing` when it waits on the index, or `patch-semantics` when it sends patch MCPs.                                                                                        |
| `compound`        | Typed relationships. Use `propagation:<payload>` plus a `propagation:<path>` tag for propagation tests, and `ingestion:connector` for connector-specific tests. Prefer these over the flat pair they refine.                                  |
| `technical-layer` | The API surface the test drives (`graphql`, `openapi`, `restli`, `cli`, `kafka-events`, …). This is how the test calls the system, not what the server uses internally.                                                                       |
| `entity-scope`    | Only when the test is specific to an entity type. Skip it for entity-agnostic tests and for entities already covered by a `feature-user` tag.                                                                                                 |
| `change-type`     | Never. It describes PRs (CI, build, test infrastructure, docs, tooling), not tests, and `test_smart_testing.py` fails if a test carries it.                                                                                                   |

Rules:

- Tag what the test exercises through the running system, not what it merely imports.
- Apply every `annotator-guidance` rule in the taxonomy files, for example `search` vs
  `search-indexing` and `documentation` vs `propagation:docs`.
- Never invent a tag. If nothing fits, leave the dimension out and report the gap, since it
  may call for a taxonomy change (mode 3).
- Helper modules and fixtures that aren't tests get no entry.

## Entry format

Paths follow the tag file's convention: relative to `smoke-test/` for pytest, and
repo-relative for Playwright and ingestion. Leave out empty dimensions. Ingestion entries
for a single connector also carry `connector: <source dir name>`.

Use an exact path per file. Collapse to a directory glob only when every file in the
directory would get identical tags. An exact entry overrides a glob; among globs, the
longest pattern wins. So when one file in a globbed directory differs, add an exact entry
for it rather than editing the glob.

```yaml
- path: "tests/e2e/actions/doc_propagation/test_propagation.py"
  tags:
    feature-user: [documentation]
    feature-dev: [executor]
    compound: [propagation:docs, propagation:lineage-path]
    technical-layer: [kafka-events]
    entity-scope: [dataset]

- path: "tests/e2e/authorization/*.py"
  tags:
    feature-user: [authorization]
    technical-layer: [graphql]

- path: "metadata-ingestion/tests/unit/snowflake/*.py"
  connector: snowflake
  tags:
    feature-user: [ingestion]
    feature-dev: [sql-lineage, profiling, classification]
    compound: [ingestion:connector]
    technical-layer: [cli]
    entity-scope: [dataset]
```

## Where the entry goes

Put it under the matching `# ── <dir>/ ──` section comment, keeping the file's alphabetical
section order. Create the section when it's new. Don't touch `always_run`.
