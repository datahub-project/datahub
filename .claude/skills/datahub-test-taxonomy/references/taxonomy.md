# Smart Testing — Taxonomy Generation Prompt (Deterministic)

This prompt encodes the design decisions from the original taxonomy creation
process so that a fresh agent reliably produces the same taxonomy. It is
prescriptive where our iterative design converged on a specific answer, and
open where the agent should discover from the codebase.

Used by `/datahub-test-taxonomy taxonomy` (see `../SKILL.md`, mode 3). The methodology and
criteria it applies are in `.github/smart-testing/taxonomy-rubric.md`; read that first.

---

## Prompt

````
You are generating a concept taxonomy for smart test selection in the DataHub
codebase. The taxonomy maps concepts to tags; tests and PRs are both tagged
with these concepts, and the intersection determines which tests to run.

The taxonomy has FIVE sections. Follow the structure and rules below exactly.

### Structure

```yaml
version: <N>
dimensions:
  feature-user:     # ~25 tags
  feature-dev:      # ~20-25 tags
  technical-layer:  # ~10 tags
  entity-scope:     # ~8 tags
  compound:         # ~10 tags
annotator-guidance: # ~5 disambiguation rules
```

Total: 70-85 tags. Below 50 loses discrimination. Above 100 loses consistency.

The current `taxonomy.yaml` also has a sixth, PR-only dimension, `change-type`
(ci, build, test-infra, docs, tooling, pytest-plumbing, with their `paths` and
the annotator-guidance rules that mention them). It describes PRs, not tests, so
it is not derived from the codebase: copy it and those rules over unchanged, and
leave it out of the tag counts above.

### Dimension 1: feature-user (~25 tags)

Things a DataHub USER would recognize — product surfaces, UI pages, documented
features. If it appears in the docs navigation or has a UI page, it's here.

SEED TAGS (must appear — verify each exists in the codebase and adjust names
to match current code, but do not drop any):

  lineage, ownership, glossary, tags, domains, data-products, assertions,
  incidents, ingestion, search, documentation, structured-properties, forms,
  notifications, authorization, schema-metadata, views, workflows, proposals,
  deprecation, home-page, data-contracts, business-attributes, context-hub

DISCOVER additional tags by scanning:
- `smoke-test/tests/e2e/` directory names
- `e2e-test/ui/playwright/tests/` directory names
- GraphQL schema top-level queries/mutations

Split `ai-features` into THREE separate tags (they are distinct sub-features
with separate test directories):
  - ai-conversation: Ask DataHub chat, conversation lifecycle, streaming
  - ai-plugins: AI plugin CRUD, settings, execution
  - agent-registry: Agent lifecycle, skill profiles, agent management

ADD any user-facing feature with ≥5 test files that isn't covered by the
seed tags.

### Dimension 2: feature-dev (~20-25 tags)

Things only a CODEBASE CONTRIBUTOR would know — internal mechanisms,
infrastructure subsystems. These have wide, non-obvious blast radius.
A change to propagation breaks ownership, glossary, tags, and domain tests.
A change to aspect-validation breaks anything that writes aspects.

These are the highest-leverage tags in the system.

SEED TAGS (must appear):

  aspect-validation, mce-mcl-pipeline, search-indexing, entity-registry,
  system-metadata, retention, timeseries-aspects, entity-versioning,
  bootstrap, patch-semantics, graph-index, siblings, billing-metrics,
  feature-flags, semantic-search, zero-downtime-upgrade, executor,
  authentication

INGESTION FRAMEWORK TAGS (must appear — these cut across connectors):

  stateful-ingestion, sql-lineage, classification, profiling, transforms,
  schema-translation

Discover these by examining:
- `metadata-io/src/main/java/` package structure (search, graph, validation)
- `metadata-service/` Spring services
- `metadata-ingestion/src/datahub/ingestion/` framework modules
  (source/state/, sql_parsing/, transformer/, glossary/classification_mixin)
- MAE/MCE consumer hooks in `metadata-jobs/`

ADD any internal mechanism with ≥5 test files that isn't covered by the
seed tags.

### Dimension 3: technical-layer (~10 tags)

The infrastructure path a request travels through. A PR that only touches
the GraphQL layer shouldn't trigger Kafka consumer tests.

SEED TAGS (must appear):

  graphql, openapi, restli, kafka-events, elasticsearch, ebean-mysql,
  react-frontend, cli, spring-config, playwright-e2e, mcp-server

### Dimension 4: entity-scope (~8 tags)

Which entity types are involved. ONLY entities that are NOT already a
feature-user tag. This is critical — do NOT duplicate.

Entities like domains, glossary, data-products, assertions are ALREADY in
feature-user. They do NOT get entity-scope tags.

SEED TAGS:

  dataset, dashboard-chart, dataflow-datajob, container,
  corpuser-corpgroup, ml-entities, agent-entity, application-service

Rules:
- Group entities that always co-occur in tests (dashboard+chart → one tag)
- Read entity-registry.yml for the full entity list
- Only add an entity-scope tag if ≥3 tests exercise that entity type
  AND the entity isn't already a feature-user tag

### Dimension 5: compound tags (~10 tags)

Compound tags capture typed relationships where a flat tag would over-select.

PROPAGATION is the primary case. The propagation smoke test suite has ~46
files. A flat `propagation` tag would match ~15% of all smoke tests — too
broad. Decompose it TWO ways:

By payload (what is being propagated):
  propagation:docs, propagation:tags, propagation:terms,
  propagation:ownership, propagation:domains,
  propagation:structured-properties

By path (how propagation travels):
  propagation:lineage-path, propagation:sibling-path,
  propagation:logical-model

Plus one ingestion compound:
  ingestion:connector — individual connector source code and tests
  (distinct from the ingestion framework tags above)

Verify each compound tag by reading representative test files in
`smoke-test/tests/e2e/propagation/` and `smoke-test/tests/e2e/actions/`.

Do NOT add compounds for other concepts unless you find a tag matching
>30% of a test tier. In the backtest, search-indexing, aspect-validation,
bootstrap, and mce-mcl-pipeline were all well-bounded and did NOT need
compounds.

### Annotator guidance

Add disambiguation rules for these specific confusing pairs:

1. search vs search-indexing:
   - search = issues a search query, checks results (user-facing)
   - search-indexing = tests index state, waits for indexing hooks (dev-facing)

2. documentation vs propagation:docs:
   - documentation = reads/writes doc aspects directly
   - propagation:docs = tests that docs flow along lineage/sibling edges

3. ingestion vs ingestion:connector:
   - ingestion = framework (scheduling, execution, secrets, connection testing)
   - ingestion:connector = specific connector source code and behavior

4. graph-index vs lineage:
   - lineage = user-facing lineage traversal and display
   - graph-index = underlying graph storage layer

5. siblings vs propagation:sibling-path:
   - siblings = sibling resolution and association logic
   - propagation:sibling-path = propagation traveling along sibling edges

### Tag definitions

Each tag MUST have:
- `tag`: the kebab-case name
- `definition`: one-line description
- `covers`: 2-3 concrete examples of what it includes

### Validation (run before outputting)

For each tag, estimate how many test files it would match based on test
file names and directory structure:
- If >30% of any test tier → too broad, split or add compounds
- If <3 tests → too narrow, merge into nearest neighbor
- If the name references a file path or class name → rename to a concept

Verify the entity-scope dimension has NO overlap with feature-user.

### Output

Write the taxonomy as `.github/smart-testing/taxonomy.yaml`. Increment the
version number if updating an existing file. When the repository has
`taxonomy.overlay.yaml`, put tags for features only this repository has there
instead (same structure, `dimensions` and `annotator-guidance` only), along with
any `annotator-guidance` pair that names an overlay tag.

If an existing taxonomy is present, diff your output against it and report:
- Tags added (with rationale)
- Tags removed (with rationale)
- Tags renamed
- Definition changes
````

---

## Why this prompt is prescriptive

The original taxonomy took 4 iterations to stabilize:

1. **v1 (68 tags)**: Agent-generated from codebase scan. Missing ingestion
   framework tags, had duplicate -entity tags in entity-scope, no annotator
   guidance, ai-features was one tag instead of three.

2. **Review**: Second agent found orphan test directories, ambiguous tag
   pairs, narrow tags (<3 matches), and the domain-entity vs domains
   confusion.

3. **v2 (80 tags)**: Applied review fixes + added ingestion framework tags
   (stateful-ingestion, sql-lineage, classification, profiling, transforms,
   schema-translation) that the user identified as a gap.

4. **Annotation gap fix**: Backtest revealed entity-registry and
   aspect-validation tags matched 0 tests. Fixed by reading actual test
   files and adding the tags to relevant annotations.

The seed tags, grouping rules, compound decomposition, and annotator
guidance in this prompt encode all four iterations. A fresh agent following
this prompt should produce a taxonomy within ~5 tags of v2 on the first
try — the open discovery steps (ADD any... with ≥5 test files) allow it
to pick up new concepts added after v2.

## After writing the taxonomy

Follow mode 3 in `../SKILL.md`: regenerate the enums in
`smoke-test/smart_testing/taxonomy.py` (`tags.py enums --write`), validate every
framework's `test_tags.yaml`, and retag entries that use removed or renamed tags.

## Maintenance cadence

- **Quarterly**: run `/datahub-test-taxonomy taxonomy` and review the reported diff against the
  committed taxonomy.
- **On demand**: When a new major feature or subsystem is added and the
  existing taxonomy doesn't cover it (symptom: shadow mode shows 0 tests
  selected for a PR in that area, or tests are untaggable).
