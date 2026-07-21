# Snowflake Semantic View → semanticModel/metric migration

Opt-in Snowflake flag `semantic_views.emit_semantic_model_entities` (default `false`) changes the entity type for semantic views. This doc covers URN mapping, what breaks, what the migration CLI does, and the operator sequence.

Related code:

- Flag / emission: `metadata-ingestion/src/datahub/ingestion/source/snowflake/snowflake_config.py`, `snowflake_schema_gen.py`, `snowflake_semantic_model.py`, `snowflake_utils.py`
- CLI: `datahub migrate snowflake-semantic-views` → `metadata-ingestion/src/datahub/cli/snowflake_semantic_view_migration.py`
- Why a migrate CLI (alternatives): [snowflake-semantic-view-migration-strategy.md](./snowflake-semantic-view-migration-strategy.md)
- Deployment / how to run: [snowflake-semantic-view-migration-deployment.md](./snowflake-semantic-view-migration-deployment.md)
- Release note: `docs/how/updating-datahub.md` (Snowflake semantic views bullet)
- Connector docs: `metadata-ingestion/docs/sources/snowflake/snowflake_post.md` (“What changes with emit_semantic_model_entities”)

## Problem

| Flag | Entity | URN shape |
| --- | --- | --- |
| OFF (default) | `dataset`, subtype `Semantic View` | `urn:li:dataset:(urn:li:dataPlatform:snowflake,[instance.]db.schema.view,ENV)` |
| ON | `semanticModel` + one `metric` per METRIC column | `urn:li:semanticModel:(urn:li:dataPlatform:snowflake,[instance.]db.schema,view)` / `urn:li:metric:(...,[instance.]db.schema.view,metric)` |

Enabling the flag stops emitting the old dataset URNs. New URNs appear. Human governance on the old datasets does not move. With stateful ingestion, old datasets are soft-deleted on the next successful run.

## URN mapping

Built by `SnowflakeIdentifierBuilder` (`snowflake_utils.py`). `snowflake_identifier` lowercases when `convert_urns_to_lowercase` is true (connector default).

```
path_sm     = [platform_instance.] + id(db.schema)
dataset     = urn:li:dataset:(…snowflake, [instance.]id(db.schema.view), ENV)
semanticModel = urn:li:semanticModel:(…snowflake, path_sm, id(view))
metric      = urn:li:metric:(…snowflake, path_sm.id(view), id(metric))
```

`DatasetKey` has `origin` (env). `SemanticModelKey` / `MetricKey` do not — env is dropped. Reverse mapping needs `--env` (and the same `--platform-instance` / casing flags as the recipe).

Examples (from unit tests):

```
# lowercase, no instance
dataset:  …snowflake,test_db.public.sales_analytics,PROD)
SM:       …snowflake,test_db.public,sales_analytics)
metric:   …snowflake,test_db.public.sales_analytics,total_revenue)

# instance + no lowercase
SM:       …snowflake,my_instance.TEST_DB.PUBLIC,Sales_Analytics)
```

Only METRIC columns become metric entities. Dimensions/facts stay as fields on `semanticModelInfo`.

## Soft-delete (not the CLI’s job)

`AutoStaleEntityRemovalProcessor` / `StaleEntityRemovalHandler`: any URN in the previous checkpoint but missing from the current run gets `status.removed=true`. Aspects stay until hard-delete / GC.

- Flag ON + stateful ingestion + `remove_stale_metadata` (default): old SV datasets soft-deleted. No Snowflake special-case.
- Flag OFF again: semanticModel/metric soft-deleted; datasets recreated.
- Stateful ingestion off: old entities remain; clean up manually.
- Fail-safes (source failure, zero events, delta > ~75%) skip soft-delete for that run.
- Default `datahub-gc` hard-deletes soft-deleted **datasets** after ~10 days. `semanticModel` / `metric` are not in the default GC entity-type list.

## What moves vs what doesn’t

### Entity-level (CLI copies)

`ownership`, `domains`, `globalTags`, `glossaryTerms`, `institutionalMemory`, `structuredProperties`, `documentation`, `deprecation`, `applications`.

Also best-effort: `editableDatasetProperties.description` → `documentation` when source has no `documentation` and dest has none.

### Column tags / terms (CLI fans out)

| Source (dataset mode) | Destination (flag ON) |
| --- | --- |
| DataHub UI / `editableSchemaMetadata` column tags & glossary | METRIC → metric entity `globalTags` / `glossaryTerms`; DIMENSION/FACT → `schemaField` URN under the semanticModel |
| Same on `schemaMetadata` fields (minus synthetic tags) | Same fan-out |

Synthetic connector tags `DIMENSION` / `FACT` / `METRIC` on legacy `schemaMetadata` are **stripped** — not customer tags. With the flag ON they are remodeled as `SemanticField.type` / metric entities by the connector.

Snowflake-sourced view/column tags do not need the CLI; the connector re-extracts them on ingest.

### Connector regenerates (do not copy)

`browsePathsV2`, `subTypes`, `dataPlatformInstance`, `status`, `upstreamLineage`, `datasetUsageStatistics`, `semanticModelInfo` / `metricInfo`, schema / dataset / view properties.

### Not on semanticModel/metric as entity aspects

`container`, `siblings`, `incidentsSummary`, `testResults`, `forms`, `editableDatasetProperties` (no editable\* shield on SM/metric).

### Inbound refs that stay dataset-typed (cannot repoint without model changes)

| Reference | Why |
| --- | --- |
| Downstream `upstreamLineage.upstreams[].dataset` | `DatasetUrn` only (`Upstream.pdl`) |
| Chart / dashboard inputs | `entityTypes: ["dataset"]` |
| Data product `assets` | no semanticModel/metric in entityTypes |
| Incidents / assertions / contracts | dataset-typed |
| Policies UI privileges | no `semanticModel`/`metric` in `PoliciesConfig.ENTITY_RESOURCE_PRIVILEGES` |

Known connector limitation: SQL `FROM SEMANTIC_VIEW(...)` lineage cannot target a semanticModel; query parsing may keep resolving to the legacy (soft-deleted) dataset URN.

## Migration CLI

```bash
datahub migrate snowflake-semantic-views --direction dataset-to-sm --dry-run
datahub migrate snowflake-semantic-views --direction sm-to-dataset --env PROD --dry-run
```

| Direction | Use when |
| --- | --- |
| `dataset-to-sm` | Turning the flag ON |
| `sm-to-dataset` | Turning the flag OFF (writes column tags back to `editableSchemaMetadata`) |

Does not create thin semanticModel/metric shells: **source and destination must already exist**. Skips (with an error/note) when the mapped destination is missing. Entity-level aspects are last-write-wins; `editableSchemaMetadata` is merged field-by-field (descriptions preserved). Does not soft-delete, rewrite lineage, or touch policies/data products.

Also:

- Discovery: Snowflake datasets with subtype `Semantic View`, or Snowflake `semanticModel`s; or `--urn` / `--urn-file`. **Includes soft-deleted sources by default** (`--include-soft-deleted`, on) so post-ingest cutover still finds the legacy side
- `--platform-instance` / `--convert-urns-to-lowercase` must match the recipe (instance also filters discovery)
- `--report-inbound-refs`: lists DownstreamOf/Consumes/… still pointing at the source
- Field fan-out: metrics must exist; dim/fact columns must appear on `semanticModelInfo`
- URN mapping must match the Snowflake connector’s `SnowflakeIdentifierBuilder` / `SnowflakeSemanticModelMapper` (ship CLI with or after that connector change)

**Depends on:** connector PR that adds `emit_semantic_model_entities` and the identifier builders this CLI mirrors.

**Caveat:** if the next Snowflake ingest emits `globalTags` on a metric/semanticModel from Snowflake tags, it replaces the aspect (connector does not merge). Migrated DataHub-only tags survive when the connector emits nothing for that entity (empty tag list → early return).

## Operator sequence (flag ON)

1. GMS/CLI on a release that registers `semanticModel`/`metric` (and usage-on-semanticModel).
2. Set `semantic_views.emit_semantic_model_entities: true`; keep stateful ingestion on.
3. Ingest so new semanticModel/metric entities exist (and old SV datasets are soft-deleted if stateful). Confirm destinations are active **before** migrate.
4. Dry-run migrate; fix `--platform-instance` / casing / `--env` until the mapped URNs look right. Missing destinations are reported — do not expect the CLI to create them.
5. Run migrate for real **before** GC hard-deletes soft-deleted datasets (source governance must still be readable).
6. Manually handle data products, policies, bookmarks, unrepointable lineage.
7. Hold hard-delete of soft-deleted SV datasets until governance sign-off.

Flag OFF: re-ingest with the flag off so datasets exist again, then `--direction sm-to-dataset --env …`. Tag-derived structured-property definitions may drop `semanticModel`/`metric` from `entityTypes` on rollback (benign; values can remain).

## Design choices

| Choice | Reason |
| --- | --- |
| Separate CLI, not connector `ctx.graph` | Keeps ingest pure; migration is intentional and reviewable; avoids half-migrated state on failed runs |
| No soft-delete in CLI | Stateful ingestion already does this generically |
| No lineage rewrite | Blocked by `Upstream.dataset: DatasetUrn` |
| URN helpers reimplemented in CLI module | Avoid pulling `snowflake-connector-python` into every `datahub migrate` invocation |
| Bidirectional | Rollback path is real; reverse needs `--env` because SM URNs omit it |
| Column tags → metric / schemaField | Matches how the connector places Snowflake column tags under the flag; strips synthetic subtype tags |
| Require src + dst exist | Avoids thin semanticModel/metric shells without connector `semanticModelInfo` / metric entities; ingest ON first, then migrate |

## Out of scope / follow-ups

- Expanding `Upstream` / data-product / incident / policy typing to semanticModel
- Auto-repointing charts, dashboards, bookmarks
- Connector merge of Snowflake + DataHub-only tags on re-ingest (would make migrated column tags fully durable when Snowflake also has tags on that column)

## Tests

`metadata-ingestion/tests/unit/cli/test_snowflake_semantic_view_migration.py` — URN golden cases, round-trip, allowlist, dry-run, both directions, discovery/subtype filters, column tag fan-out (strip synthetic tags, metric vs schemaField targets).

Manual check after flag flip (also in `updating-datahub.md`): ingest OFF then ON with stateful ingestion; assert old dataset soft-deleted and new semanticModel active.
