# Smart Testing — Taxonomy Rubric

This rubric defines how to generate and evaluate a concept taxonomy for smart test
selection. The taxonomy is the foundation: PR diffs are annotated with concepts they
affect, tests are annotated with concepts they exercise, and the intersection determines
which tests to run.

## Where things live

| File                                               | Role                                                                                                                                          |
| -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `.github/smart-testing/taxonomy.yaml`              | The common vocabulary for every test framework, and the PR classifier's prompt. A framework need not use every tag.                           |
| `.github/smart-testing/classify_pr.py`             | Framework-agnostic PR classifier (CI setup job). Validates the LLM answer against `taxonomy.yaml`. Deps in `requirements.txt`.                |
| `smoke-test/smart_testing/taxonomy.py`             | Typed mirror of `taxonomy.yaml` (one enum per dimension); the runtime source of known tags for pytest.                                        |
| `smoke-test/smart_testing/test_tags.yaml`          | Smoke-test pytest module → tags. Applied as markers during collection. The only framework wired up today.                                     |
| `.github/smart-testing/<framework>/test_tags.yaml` | Playwright and metadata-ingestion tags. Not consumed yet.                                                                                     |
| `*.overlay.yaml`, `taxonomy_overlay.py`            | Optional overlay next to each base file, for tags and tests only one repository has. Merged with the base by every consumer.                  |
| `.github/smart-testing/selection.py`               | Names the pytest selection a classification plans (smart, plumbing, canary or p0). Run by `plan_pytest_batches.sh` in CI.                     |
| `.github/smart-testing/replay_prs.py`              | Replays the PRs in `golden_prs.yaml` through the current classifier and reports what each would run. Use it before merging a taxonomy change. |
| `.claude/skills/datahub-test-taxonomy/`            | `/datahub-test-taxonomy` skill: tags new, changed or untagged tests, regenerates all tags, and regenerates this taxonomy (with its prompts).  |

When a tag is added, renamed, or removed in `taxonomy.yaml`, regenerate the enum block in
`taxonomy.py` (`/datahub-test-taxonomy taxonomy` does this, via `tags.py enums --write`).
`smoke-test/tests/unit/test_smart_testing.py` fails until both match, and also fails if
`test_tags.yaml` uses a tag the taxonomy does not define, or the base `test_tags.yaml` uses an
overlay tag. At collection
time, conftest logs an error for any unknown tag on a test marker.

## What the taxonomy is

A bounded set of **concept tags** organized across orthogonal dimensions. Each tag names
a concept at the right level of abstraction — specific enough to discriminate between
tests, stable enough to survive refactors, and recognizable enough for an LLM to apply
consistently.

## Dimensions

The taxonomy has four orthogonal dimensions. A test or PR is annotated with tags from
each applicable dimension. Test selection uses the **intersection across dimensions** —
a PR tagged `{lineage, graphql, dataset}` runs tests that match on those tags, not all
lineage tests or all GraphQL tests.

### 1. Feature area — user-facing concepts

Things a DataHub **user** would recognize and talk about. These map to product surfaces,
UI pages, and documented features.

**How to identify:** If it appears in the DataHub docs navigation, has a UI page, or is
something a user would mention in a support ticket, it belongs here.

**Target count:** ~25 tags.

**Examples:** lineage, ownership, glossary, tags, domains, data-products, assertions,
incidents, ingestion, search, documentation, structured-properties, forms, notifications,
deprecation, schema-metadata, data-contracts, business-attributes.

**What to avoid:**

- Tags so broad they match half the suite ("metadata", "entities")
- Tags that are just entity names repackaged — entity scope is a separate dimension

### 2. Feature area — developer-facing concepts

Things only someone **working on the codebase** would know about. These are internal
mechanisms, infrastructure systems, and architectural patterns.

**How to identify:** If it requires reading source code to understand, doesn't appear in
user docs, and tends to affect tests across multiple user-facing features, it belongs
here.

**Target count:** ~20-25 tags.

**Examples:** propagation, aspect-validation, mce-mcp-pipeline, search-indexing,
entity-registry, system-metadata, retention, timeseries-aspects, bootstrap,
change-log, auth-token-resolution, feature-flags, entity-versioning, patch-semantics,
graph-index, stateful-ingestion, sql-lineage, classification, profiling, transforms,
schema-translation.

**Why this tier matters most:** Developer-facing concepts have wide, non-obvious blast
radii. A change to propagation breaks ownership tests, glossary tests, and domain tests.
A change to aspect validation breaks anything that writes aspects. These are the tags
that prevent the "PR changed something deep, no tests were run" failure mode. They are
also the hardest for an LLM to apply — annotating them correctly requires understanding
the architecture, not just the feature surface.

### 3. Technical layer

The infrastructure path a request or operation travels through. A PR that only touches
the GraphQL layer shouldn't trigger Kafka consumer tests for the same feature.

**How to identify:** This is the "how" — which protocol, storage system, or runtime
boundary is involved.

**Target count:** ~10 tags.

**Examples:** graphql, openapi, restli, kafka-events, elasticsearch, ebean-mysql,
react-frontend, cli, spring-config, playwright-e2e, mcp-server.

### 4. Entity scope

Which entity types are involved. A PR changing dataset schema handling shouldn't trigger
dashboard ownership tests even if both involve "ownership."

**How to identify:** This is the "on what" — which entity types are exercised.

**Target count:** ~8 tags (group related entities).

**Examples:** dataset, dashboard-chart, dataflow-datajob, container, corpuser-corpgroup,
ml-entities, agent-entity, application-service.

**Grouping:** Entities that are always tested together should share a tag. Dashboard and
Chart rarely appear independently in tests — `dashboard-chart` is one tag, not two.

**Deduplication rule:** Entities that are also user-facing features (domains, glossary,
data-products, assertions) live ONLY in feature-user, not duplicated here. Entity-scope
should contain only entities that aren't also a feature.

### Change type — PR-only

What kind of non-product change a PR makes: `ci`, `build`, `test-infra`, `docs`, `tooling`,
and `pytest-plumbing`. It exists so a PR that changes no product behavior has a tag that
fits, instead of borrowing unrelated feature tags or falling back to p0 on LOW confidence.

- **Tests never carry it.** It describes PRs, so it selects no tests by itself. A PR with
  only change-type tags runs the canary, like any PR without feature tags.
- **Paths assign it too.** A tag's `paths` globs in `taxonomy.yaml` tag every matching
  file, and a PR whose files all match is classified without the model. Keep the globs to
  locations that cannot change what the product or the smoke tests run.
- **`pytest-plumbing` comes only from paths** (`path-assigned: true`, left out of the
  classifier's schema). It marks a change to the shared pytest harness and adds
  `plumbing_sample` from `smoke-test/smart_testing/test_tags.yaml` to the selection.
- **It never replaces feature tags.** Product dependency bumps and product image
  Dockerfiles are not `build` alone; the annotator guidance says to tag the features they
  affect, or answer LOW confidence.

## Properties of a good taxonomy

Use these criteria to evaluate a draft taxonomy. A taxonomy that fails multiple criteria
should be revised before annotating tests against it.

### Discriminating

Every tag must split the test suite into a meaningfully smaller subset. Measure: if a tag
matches more than 30% of all tests, it is too broad and should be split or removed. If a
tag matches fewer than 3 tests, it may be too narrow and should be merged into a neighbor.

### Predictive

The relationship "PR affects concept X → tests tagged X are the ones that might break"
must actually hold. A tag that is conceptually correct but does not predict test failures
is noise. Validate by sampling: pick 5–10 recent PRs that caused test failures, annotate
them with the draft taxonomy, and check whether the failing tests would have been selected.

### Stable

Tags should survive refactors, renames, and file moves. Test: if a class is renamed or
a module is split, does the tag still apply without changes? "authorization" survives;
"AuthorizationUtils" does not.

### Orthogonal across dimensions

The four dimensions should be independently combinable. If knowing the feature area
always determines the technical layer (e.g., "ingestion" always means "cli"), the
dimensions are not orthogonal and one should be collapsed. Each dimension should add
independent discriminating power.

### Consistent

An LLM must be able to apply tags the same way across different tests and PRs. Test:
give Claude 10 tests without context and ask it to tag them. Then give Claude the same 10
tests a second time. If the tags differ significantly between runs, the taxonomy has
ambiguous boundaries.

### Complete

Every test must be taggable with at least one tag from each applicable dimension. Orphan
tests — those with no matching tags — either always run (defeating the purpose) or never
run (dangerous). If more than 5% of tests are orphaned, the taxonomy has gaps.

### Bounded

Small enough for an LLM to hold in context and apply consistently. The total tag count
across all dimensions should be 50–80. Below 30, discrimination is poor. Above 100,
annotation consistency degrades.

## How to generate the taxonomy

### Step 1: Derive candidate tags from the codebase

Analyze the codebase to extract candidate concepts. Sources of signal, in priority order:

1. **Test file names and paths** — these are the most direct signal for what concepts
   tests exercise (`test_lineage.py`, `OwnershipTest.java`, `SearchTest`).
2. **Module and package structure** — `metadata-ingestion/`, `datahub-graphql-core/`,
   `metadata-io/` reveal technical layers.
3. **Entity registry** (`entity-registry.yml`) — the authoritative list of entity types.
4. **GraphQL schema** — user-facing API surface, maps directly to user-facing features.
5. **Spring configuration and service classes** — reveal developer-facing subsystems
   (propagation, retention, validation).
6. **CI workflow definitions** — existing test groupings reveal implicit concepts.

### Step 2: Cluster and deduplicate

Group candidate tags that always co-occur. If "tag" and "label" always appear together in
tests, they are one concept, not two. Merge aggressively — err on the side of fewer, more
discriminating tags.

### Step 3: Validate against the criteria

For each candidate tag, check:

- [ ] Does it match a meaningful (5–30%) slice of the test suite?
- [ ] Would a PR affecting this concept predict failures in these tests?
- [ ] Is it stable across refactors?
- [ ] Can an LLM apply it consistently?
- [ ] Is it orthogonal to tags in other dimensions?

Remove or merge tags that fail two or more checks.

### Step 4: Validate against historical PRs

Take 10–20 recent PRs (mix of clean merges and ones that broke tests). Annotate each
with the draft taxonomy. Check:

- Would the selected tests have caught the failures? (safety)
- How many unnecessary tests would have run? (efficiency)
- Were any PRs untaggable? (completeness)

Target: ≥95% of historical failures would be caught, ≤50% of the full suite selected
per PR on average.

`replay_prs.py` automates the selection half of this check: it replays the PRs in
`golden_prs.yaml` through the current classifier and flags any whose selection got
narrower than expected (`coverage-risk`) or broader (`costlier`). PRs that paths alone
classify replay offline as unit tests. The rest need classifier calls: the Smart Testing
Replay workflow (`.github/workflows/smart-testing-replay.yml`) replays them three times on
every PR that changes `.github/smart-testing/`, with the same Workload Identity Federation
credentials as the classifier, and fails on any `coverage-risk`. Add PRs with
`replay_prs.py fetch <number>`.

### Step 5: Write tag definitions

Each tag gets a one-line definition and 2–3 examples of what it covers. This is the
reference that Claude uses during annotation — ambiguous definitions produce inconsistent
annotations.

Format:

```yaml
- tag: ownership
  dimension: feature-user
  definition: "Ownership assignment, transfer, and display for any entity type."
  covers:
    - "Ownership aspect reads and writes"
    - "Owner search and filtering"
    - "Ownership propagation (also tag: propagation)"
```

## Anti-patterns

| Anti-pattern                                         | Why it's bad                                                         | Fix                                              |
| ---------------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------ |
| Tags mirror file paths                               | Breaks on refactor; no abstraction benefit over path-based selection | Name concepts, not files                         |
| "Umbrella" tags matching >30% of tests               | No discrimination; equivalent to "run all"                           | Split into sub-concepts                          |
| Tags that are just entity names                      | Redundant with the entity scope dimension                            | Remove from feature area; keep in entity scope   |
| Singleton tags matching 1–2 tests                    | Maintenance burden for no discrimination value                       | Merge into nearest neighbor                      |
| Tags requiring deep architectural knowledge to apply | LLM will annotate inconsistently                                     | Add concrete examples to definition, or simplify |
| Overlapping tags with unclear boundaries             | "auth" vs "authorization" vs "access-control" — which one?           | Pick one name, alias the rest                    |

## When to escalate from flat tags to structured relationships

Flat tags use implicit AND across dimensions: a test tagged `{propagation, ownership,
graphql}` matches a PR tagged with those same concepts. This works well when concepts
combine independently. But flat tags cannot express **directionality** or **typed
relationships** — they can't distinguish "propagation _of_ ownership" from "propagation
_of_ tags" if both share the `propagation` tag.

### The over-selection problem

A flat tag like `propagation` may match far too many tests because it co-occurs with
every feature that uses propagation — ownership, tags, glossary terms, domains. If a PR
only changes how **ownership** is propagated, flat tags still select tag-propagation
tests, glossary-propagation tests, etc. When a single tag matches more than 30% of the
test suite, it loses its discriminating power and the system degenerates toward "run all."

### Compound tags as a middle ground

Before building a full ontology, introduce **compound tags** — structured pairs that
capture typed relationships without requiring a graph model:

```yaml
# Instead of separate flat tags:
- propagation
- ownership

# Use a compound tag when the relationship matters:
- propagation:ownership # propagation OF ownership
- propagation:tags # propagation OF tags
- propagation:domains # propagation OF domains
```

A test for "ownership propagation along lineage" gets tagged
`{propagation:ownership, lineage, kafka-events, dataset}`. A PR that changes ownership
propagation matches `propagation:ownership` — it does not falsely match
`propagation:tags` or `propagation:domains`.

### When to introduce a compound tag

Escalate from a flat tag to compound tags when **any** of these conditions hold:

1. **Over-selection:** A flat tag matches >30% of tests, and splitting it by relationship
   target would bring each compound tag under 15%.
2. **False-positive signal:** Historical analysis shows the flat tag would have selected
   tests that had zero probability of failing for the PR in question — the tag matched
   because of the mechanism, but the payload/target was completely unrelated.
3. **Natural grouping:** The codebase already separates the concept by target (e.g.,
   `OwnershipPropagation.java`, `TagPropagation.java`) — the compound tag mirrors
   existing code structure.

### When NOT to escalate

- The flat tag matches a reasonable slice of tests (<30%) and false positives are
  tolerable — running a few extra tests is cheaper than maintaining compound tags.
- The concept is not naturally decomposable — "search-indexing" affects many entity types
  but the indexing code is shared; splitting by entity would create artificial boundaries.
- Fewer than 3 PRs per quarter would benefit from the finer distinction — the maintenance
  cost exceeds the savings.

### Full ontology: when compound tags aren't enough

If the system matures and compound tags still produce unacceptable over-selection, the
next step is a lightweight ontology — a graph of typed relationships between concepts:

```yaml
relationships:
  - subject: propagation
    predicate: mechanism-for
    object: ownership
  - subject: propagation
    predicate: travels-along
    object: lineage
  - subject: ownership
    predicate: displayed-via
    object: graphql
```

This enables inference: a PR that "changes how propagation travels along lineage" can
match tests that exercise that specific path without pulling in all propagation tests
or all lineage tests.

**Do not start here.** A full ontology is harder to generate, harder to maintain, and
harder for an LLM to apply consistently. The escalation path is:

1. **Flat tags** — start here. Simple, consistent, good enough for most concepts.
2. **Compound tags** — introduce per-concept when flat tags over-select.
3. **Ontology** — only if compound tags still produce >50% suite selection on typical PRs
   after 2+ quarters of tuning.

At each level, measure the false-positive rate (tests selected that couldn't have failed)
and only escalate when that rate makes the system materially less useful than running the
full suite with a P0 list.

## Output format

The taxonomy is stored as a YAML file committed to the repo. Structure:

```yaml
version: 1
dimensions:
  feature-user:
    description: "User-facing product concepts"
    tags:
      - tag: lineage
        definition: "..."
        covers: [...]
      - tag: ownership
        definition: "..."
        covers: [...]
  feature-dev:
    description: "Developer-facing internal mechanisms"
    tags: [...]
  technical-layer:
    description: "Infrastructure and protocol boundaries"
    tags: [...]
  entity-scope:
    description: "Entity types involved"
    tags: [...]
```

## Maintenance

- **Adding a tag:** When a new feature or subsystem is introduced and existing tags
  don't cover it. Must meet the criteria above. Add the tag and re-annotate affected
  tests in the same PR.
- **Merging tags:** When two tags always co-occur across all tests, merge them. Update
  all test annotations.
- **Retiring a tag:** When a feature is removed. Remove from taxonomy, remove from test
  annotations.
- **Periodic refresh:** Quarterly full re-derivation. Diff against the committed taxonomy
  to surface drift — new concepts that emerged, tags that no longer discriminate, tests
  that became orphaned.
