- Start Date: 2026-04-14
- Updated: 2026-10-05
- RFC PR: [datahub-project/datahub#17034](https://github.com/datahub-project/datahub/pull/17034)
- RFC branch: [manuschillerdev/datahub:docs/rfc-openlineage-spec-compliance](https://github.com/manuschillerdev/datahub/tree/docs/rfc-openlineage-spec-compliance)
- Implementation PR: [datahub-project/datahub#19257](https://github.com/datahub-project/datahub/pull/19257)
- Implementation branch: [manuschillerdev/datahub:feat/openlineage-conformance-combined](https://github.com/manuschillerdev/datahub/tree/feat/openlineage-conformance-combined)
- Specification: OpenLineage 1.53.0, event schema 2-0-2, [pinned commit 8ad5c14](https://github.com/OpenLineage/OpenLineage/tree/8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe)

# OpenLineage REST endpoint specification compliance

## Summary

`POST /openapi/openlineage/api/v1/lineage` accepts OpenLineage `RunEvent`, `JobEvent`, and `DatasetEvent` objects, validates their typed envelopes and recognized official facets, maps supported metadata to native DataHub entities and aspects, and applies authorized metadata through the standard `EntityService` APIs with synchronous aspect writes.

The endpoint targets the OpenLineage 2-0-2 event model with facets from release 1.53.0. Historical Airflow, Spark, and Marquez mappings remain supported. Request-provided schema URLs are metadata and are never fetched.

This RFC proposes the endpoint contract and records how the separate implementation approaches it, including remaining limitations. Design review and implementation review have separate branches and PRs. This document does not establish merge readiness or complete specification conformance. The comparison evidence was captured September 30–October 1; the October 5 verification checkpoint below covers the rebased implementation and does not refresh upstream comparison verdicts.

The [known-gap evidence appendix](17034-openlineage-spec-compliance-known-gap-appendix.md) compares 34 concrete cases across pinned upstream master, the six cumulative PR heads, and the captured implementation source. It links each case to an ordered payload and reproduction check. The [HTML report](17034-openlineage-evidence/openlineage-known-bug-head-matrix-2026-10-01.html) is a searchable view of that appendix.

## Motivation

OpenLineage producers describe jobs, runs, datasets, and their relationships through one shared event contract. Catalog users need the resulting DataHub metadata to keep the same identity and meaning across partial events, repeated facets, deletion, and design-time versus runtime events. Accepting a request while losing its declared lineage, keeping obsolete owners, or changing a job identity makes impact analysis and operational history unreliable.

The pinned comparison shows both inherited receiver defects and incomplete new PR mappings. It provides specific feedback to those PR authors while keeping this implementation's additional capabilities and remaining defects visible.

## Scope and acceptance criteria

The work covers the receiver, shared conversion, supported facet projections, and their persisted/read-API behavior. Existing Spark and Airflow integrations supply compatibility requirements; this RFC does not propose independent feature work for those producers.

Completion requires:

1. Every pinned upstream specification and compatibility fixture passes through the receiver, with explicit provenance for any corrected schema declarations and compatibility exceptions.
2. All existing DataHub OpenLineage tests pass, including the existing Spark converter tests and the live receiver smoke suite; failures are resolved before a clean-suite claim.
3. Each event/facet/attachment has a documented native mapping, retained-only representation, intentional unsupported boundary, or identified remaining verification. Schema, profile, tag, and lineage field references resolve consistently.
4. Named-facet replacement, omission, typed Job/Dataset tombstones, partial RunEvent accumulation, and COMPLETE_SNAPSHOT semantics follow the pinned specification first, then DataHub contributor and aspect practices.
5. Stateful examples are checked after storage and through the relevant read API. Evidence identifies exact revisions, payloads, expected and observed/predicted results, and end-user consequences separately for master, each PR head, and ours.
6. Migration, authorization, partial failure/retry, and remaining product limits are documented. A passing converter or mocked HTTP corpus alone does not meet these criteria.

The [comparison and specification appendix](./17034-openlineage-spec-compliance-appendix.md) indexes the broader 72-contract inventory, facet/attachment matrices, and PR feedback. The [known-gap appendix](./17034-openlineage-spec-compliance-known-gap-appendix.md) covers the 34 known cases. Neither appendix claims a completed runtime audit of every target.

### Extensibility

Schema and compatibility catalogs are pinned, versioned, and updated with provenance. New official facets need declared attachment points, native or retained-only targets, update/delete behavior, and end-to-end evidence. A producer-specific adapter is selected only by its explicit compatibility contract; unknown custom facets remain opaque.

### Non-requirements

This proposal does not require arbitrary remote schema retrieval, scheduling job dependencies, exact raw-event replay, a historical graph per emission window, automatic entity/history migration on RENAME, or receiver redaction. Independent producer feature work and unrelated performance changes are outside this RFC. Migration guidance for identity corrections and honest failure/retry guarantees remain required.

## Changes from the original proposal

The [original April proposal](https://github.com/manuschillerdev/datahub/blob/169ae6dc7c83db5c2afa12b9ac1eb92892e418f0/docs/rfcs/active/17034-openlineage-spec-compliance.md) used baseline `7ed8710c65`. The following decisions supersede its conflicting sections; its earlier Marquez/source crosswalk remains historical reference.

| Earlier proposal                                                         | Current proposed contract and implementation boundary                                                                                                                                               |
| ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Direct Kafka publishing with 202, streaming enabled by default           | Standard EntityService ingest with synchronous aspect writes and 200; indexing/projections remain asynchronous. Other PRs may choose async transport and must define their acknowledgment contract. |
| Event-level all-or-none persistence                                      | Authorize the full batch before writes. Storage does not provide an event-level transaction; failure can leave earlier writes committed. Failure/retry verification remains open.                   |
| Derive job orchestrator from optional engine/integration/producer facets | Use explicit configured orchestrator, default `unknown`; optional facets remain attributes so a stable namespace/name does not split identity.                                                      |
| Split dotted job name into flow prefix and task suffix                   | Use the prefix for flow grouping and preserve the complete job name as the DataJob ID.                                                                                                              |
| Symlinks rewrite REST dataset identity                                   | Stable REST namespace/name identity; symlinks contribute siblings. Existing SDK alias-resolution compatibility is assessed separately.                                                              |
| Request schema validation and generic facet retention deferred           | Bundled recognized schemas validate local contracts; opaque named-facet retention is supported. No remote schema fetch or exact raw-event archive is promised.                                      |
| Implicit facet targets and optional receiver redaction                   | Canonical aspect targets and contributor-scoped updates are documented below; environment values are preserved. Redaction is an opt-in follow-up.                                                   |

## Contract

The request must use `Content-Type: application/json`. A valid request is a JSON event object, not a JSON-encoded string.

```bash
curl -X POST http://localhost:8080/openapi/openlineage/api/v1/lineage \
  -H 'Content-Type: application/json' \
  -d '{
    "eventTime": "2026-04-14T10:00:00Z",
    "producer": "https://example.com/my-pipeline-tool",
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent",
    "job": { "namespace": "crm", "name": "load.customer" },
    "inputs": [{ "namespace": "postgres://warehouse", "name": "crm.customer" }],
    "outputs": [{ "namespace": "snowflake://analytics", "name": "crm.customer" }]
  }'
```

The endpoint:

1. parses the raw body with duplicate-key and trailing-content detection;
2. validates the event envelope and recognized standard facets against bundled OpenLineage 1.53.0 schemas;
3. dispatches structurally to `RunEvent`, `JobEvent`, or `DatasetEvent`;
4. maps the typed event to MCPs and validates the resulting aspects through normal batch construction;
5. authorizes the complete batch with standard REST ingest authorization; and
6. waits for aspect writes and returns `200 OK`; search and graph indexing remain asynchronous.

A root `schemaURL` or facet `_schemaURL` must be a valid absolute URI when present. Schema URL equality is not used for event dispatch, and no request-provided URL is resolved.

Unknown custom facet objects remain opaque. A recognized standard facet is validated by key and attachment point even when DataHub does not map it. A standard key at the wrong attachment point is rejected.

### Validation policy

The endpoint uses one validation policy. It preserves historical acceptance of missing root/facet schema metadata, legacy run identifiers, offsetless timestamps, and supported payload-free tombstones. It checks the event envelope and recognized standard facet payloads against bundled schemas, with JSON Schema `format` annotations disabled; supplied schema URLs and facet producer URLs must be absolute URIs. Offsetless timestamps use the GMS host's time zone, and historical non-UUID root run identifiers are normalized deterministically from the Job namespace, Job name, and reported identifier. Upstream compatibility scenarios remain ingestion tests, not rejection tests.

The 1.53.0 spec examples and historical compatibility scenarios serve different purposes. A separate audit with format assertions enabled found 14 historical scenarios whose placeholders or datasource URI strings fail those assertions. This observation does not change production request acceptance and does not prove a conversion error.

## Gap closure status

This table records local implementation follow-ups and their acceptance checks. The separate [known-gap appendix](17034-openlineage-spec-compliance-known-gap-appendix.md) gives the master/PR/ours comparison; a support gap on master or the PR stack is not automatically the same bug as a local mapping defect. “Investigating” means the requirement has not yet been established or discharged by evidence; it is not a waiver. The pinned 1.53.0 schema and prose under `openlineage/schemas/1.53.0/` are the semantic reference. The code and tests named here are implementation evidence at the stated checkpoints, not proof of merge readiness.

| Item                               | Verified requirement/problem and origin                                                                                                                                                                                                                                                                                                                                                        | Chosen solution                                                                                                                                                                                                                                                                                                           | Status                                                                                                | Verification evidence required                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Native column transformations   | The column-lineage facet defines per-input type, subtype, description, and masking; the previous mapper folded inputs into one edge and kept only type/subtype text. Origin: existing converter.                                                                                                                                                                                               | One native fine-grained relationship per source field with typed transformations; retain legacy output-field type/description.                                                                                                                                                                                            | Focused converter, projection, GraphQL, and graph tests pass; full integration pending                | The focused regression failed with one edge instead of three, then passed with distinct typed transformations. Storage-side projection readback/deletion, GraphQL mapper, and graph extraction tests passed. Verify broader suites and goldens.                                                                                                                                                                                                                                                                                                                      |
| 2. Dataset-wide column lineage     | The facet's `dataset[]` carries input fields and transformations; the previous mapper emitted only coarse dataset upstreams. Origin: existing converter.                                                                                                                                                                                                                                       | Native field-to-dataset relationship with the same typed transformation record.                                                                                                                                                                                                                                           | Focused converter, projection, GraphQL, and graph tests pass; full integration pending                | The regression sees a dataset downstream from the source field. Projection/readback and GraphQL tests pass; graph extraction creates the expected field-to-dataset edge. Verify broader suites and goldens.                                                                                                                                                                                                                                                                                                                                                          |
| 3. Late schema arrival             | Field paths can be emitted before a schema; current request-scoped `OpenLineageSchemaState` resolves only schemas already stored or earlier in the batch. Origin: current reconciliation boundary.                                                                                                                                                                                             | Resolve canonical field identity on later schema arrival at an existing storage boundary, including old references.                                                                                                                                                                                                       | Confirmed, open                                                                                       | Observation before schema, schema later, then readback with one field identity and no orphaned reference.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 4. Ordering and concurrency        | The 1.53 facets guide says a same-name facet emission replaces its previous instance, but gives no `eventTime` arbitration rule. DataHub applies same-aspect patches under row locking or optimistic retry; OpenLineage currently uses arrival order for the same named facet and committed source version for contributions. Origin: proposed stale-event rule and current storage semantics. | Verify named-facet and independent-contributor concurrency at the real patch boundary; retain a documented arrival-order rule unless a spec or producer contract demands event-time arbitration.                                                                                                                          | Investigating; no spec basis found for a universal event-time winner                                  | Replay older event, concurrent distinct facets, concurrent same facet, and contributor readback; distinguish DataHub ordering from a producer contract.                                                                                                                                                                                                                                                                                                                                                                                                              |
| 5. Storage failure                 | `LineageApiImpl.ingestMcps` splits writes around aspect deletes. Earlier writes may commit before a later failure. Origin: current endpoint.                                                                                                                                                                                                                                                   | Define a retry-safe persisted outcome using existing batch/storage guarantees where possible.                                                                                                                                                                                                                             | Confirmed, open                                                                                       | Inject a failure after an earlier write, retry, and verify no silent success or duplicate/lost owned state.                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 6. Time and lifecycle              | The 1.53 event schema allows fractional `eventTime`; native run events use milliseconds and map both START and RUNNING to STARTED. DataHub's time-series index keys primarily by millisecond and entity, so same-millisecond transitions also collide without a message identifier. Origin: native representation/indexing.                                                                    | Retain source timestamp on OpenLineage-derived time-series aspects and original event type on run events, alongside native millisecond/status fields. Set deterministic time-series message IDs from source time and run event type so retries overwrite the same observation while distinct transitions remain separate. | Converter and GraphQL mapper tests pass; persisted readback pending                                   | Nanosecond START/RUNNING regression and its same-millisecond message-ID extension failed before fixes and pass now; all 97 converter tests and the GraphQL readback mapper test pass. GraphQL compilation and Rest.li model gate pass. DataHub's time-series transformer includes `messageId` in its document key. Persisted readback remains to verify.                                                                                                                                                                                                             |
| 7. Custom-schema validation        | The pinned 1.53 `BaseFacet` explicitly permits additional properties, and facet maps accept base facet objects. `_schemaURL` identifies a schema but the spec does not require consumers to fetch and validate arbitrary producer schemas. Origin: proposed conformance claim, not a demonstrated bug.                                                                                         | Require custom facets to be objects, check supplied URI syntax, validate recognized standard facets locally, and project producer-specific compatibility fields only when provenance and consumed field shape match a declared contract. Retain other custom payloads opaquely.                                           | Closed: arbitrary custom-schema resolution is not required by 1.53; bounded compatibility checks pass | Pinned `OpenLineage.json` `BaseFacet.additionalProperties: true`; validator tests cover custom retention, invalid URI, and no remote resolution. `CustomRunFacetProcessorTest` covers compatible and incompatible payload shapes; the 97-test converter suite passes.                                                                                                                                                                                                                                                                                                |
| 8. Rename and emission windows     | The pinned lifecycle facet defines `previousIdentifier` as the dataset's former namespace/name; it does not instruct consumers to migrate entities. The pinned `JobTypeJobFacet` defines `COMPLETE_SNAPSHOT` as a self-sufficient event for its window, not a requirement for a historical lineage graph. Origin: proposed capabilities, not demonstrated conformance bugs.                    | Retain RENAME and its `previousIdentifier` in the native operation's retained facet, and replace run I/O for each complete snapshot; preserve the emission pattern on the job.                                                                                                                                            | Single-run `COMPLETE_SNAPSHOT` live readback and focused tests pass; RENAME readback pending          | Pinned `LifecycleStateChangeDatasetFacet.json` and `JobTypeJobFacet.json`; converter lifecycle test retains `previousIdentifier` for DatasetEvent and RunEvent RENAME operations; `OpenLineageOwnLineageTest` removes prior run I/O in stored complete-snapshot mode after the declaration is omitted. The [October 1 two-window live probe](17034-openlineage-evidence/openlineage-goal-2026-10-01-live-complete-snapshot.json) showed input A clearing and output B appearing on the same run; rename persistence and cross-run window behavior remain unverified. |
| 9. Contributor ownership           | The publisher read `change.getSystemMetadata().getVersion()` for every source change. A hard-delete MCL has no new system metadata, so deletion crashed before it could publish tombstones. Recreation restarts source versions and may still leave old target contributions. Origin: ownership publisher and DataHub rollback MCL.                                                            | Use the previous committed version plus one for delete tombstones, then reconcile source generations and target cleanup without touching independent writers.                                                                                                                                                             | Hard-delete tombstone regression passes; recreation open                                              | Focused publisher test failed with null new system metadata before the fix and now passes, verifying a version-six null tombstone after source version five. Hard delete/recreate, external edge, replacement, and replay against real patch/storage behavior remain.                                                                                                                                                                                                                                                                                                |
| 10. Identity migration             | Stable job identity, corrected legacy run IDs, and encoded field paths can change URNs/field references on upgrade. Origin: recent converter fixes.                                                                                                                                                                                                                                            | Provide one tested identity-preserving upgrade path or migration for each changed identity.                                                                                                                                                                                                                               | Confirmed, open                                                                                       | Pre-upgrade identity fixtures upgraded through the new mapper and persisted references remain connected.                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 11. Logging and environment values | Exception and converter logging exposed request-derived values. Environment variables may also contain sensitive values, but OpenLineage specifies their collection and does not require receiver-side redaction. Origin: logging and an unrequested mapping policy.                                                                                                                           | Preserve supplied environment values; log only failure class, counts, and fixed labels. Defer receiver-side redaction to a follow-up with opt-in configuration.                                                                                                                                                           | Default value preservation verified; opt-in redaction deferred                                        | [Reference source audit](./17034-openlineage-evidence/openlineage-environment-values-reference-implementations-2026-09-28.md) confirms Marquez preserves values. The latest selected runs pass 97 converter and 243 servlet tests, including original native/retained values and forwarded copies. The unchanged persisted-sequence live smoke test passes replacement and omission checks. Request-value logging regression checks remain.                                                                                                                          |
| 12. Historical fixtures            | An earlier strict default rejected 14 of 74 upstream compatibility events intended for ingestion. The production policy now preserves their historical acceptance. Origin: unrequested validator modes.                                                                                                                                                                                        | One production validation path with recognized facet checks and preserved upstream acceptance.                                                                                                                                                                                                                            | HTTP and 122-case corpus pass; semantic golden review pending                                         | The latest ordinary servlet run passed all 122 corpus test cases with stored proposal-golden equality enabled, plus the remaining OpenLineage servlet tests. Earlier 102 mismatches were with pre-update goldens; fixture acceptance still does not establish persisted semantics. Review proposal changes before closing.                                                                                                                                                                                                                                           |
| 13. Completion                     | Recent fixes changed proposals and behavior; proposal review, complete documentation reconciliation, and full checks remain incomplete. Origin: current worktree.                                                                                                                                                                                                                              | Review golden changes and full diff, run required checks, and reconcile public docs.                                                                                                                                                                                                                                      | Open                                                                                                  | Inspected golden diff, relevant integration gates, formatting, and requirement-by-requirement review.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 14. GraphQL source-code readback   | A valid `SourceCodeJobFacet` writes native `QueryLanguage.UNKNOWN`, while the GraphQL `QueryLanguage` enum exposes only `SQL`; `DataTransformLogicMapper` throws when reading the DataJob. Origin: current source-code projection crossing an existing GraphQL enum boundary.                                                                                                                  | Align the native source-code representation with the GraphQL API so the DataJob and source text remain readable for non-SQL languages.                                                                                                                                                                                    | Confirmed on the current local branch; open                                                           | The [saved live RunEvent](17034-openlineage-evidence/openlineage-goal-2026-09-30-live-graphql.json) returned HTTP 200 and native transformation readback succeeded, but GraphQL returned `dataJob=null` with `QueryLanguage.UNKNOWN` enum error. Add a non-SQL GraphQL readback check after the fix; the pinned upstream stack lacks this source-code projection, so its support gap is separate.                                                                                                                                                                    |

## Event mapping

| Event          | DataHub entities                                            | Behavior                                                                                                              |
| -------------- | ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `RunEvent`     | DataFlow, DataJob, DataProcessInstance, referenced Datasets | `eventType` may emit a `dataProcessInstanceRunEvent`; inputs and outputs are recorded on the job and process instance |
| `JobEvent`     | DataFlow, DataJob, referenced Datasets                      | Emits `dataJobInputOutput`; does not emit DataProcessInstance aspects                                                 |
| `DatasetEvent` | Dataset                                                     | Always emits a Dataset key and status anchor                                                                          |

### Entity identity

For namespace `prod` and Job name `orders_etl.count_orders`:

```text
DataFlow URN: urn:li:dataFlow:(<orchestrator>,orders_etl,prod)
DataJob URN:  urn:li:dataJob:(urn:li:dataFlow:(<orchestrator>,orders_etl,prod),orders_etl.count_orders)
Display name: count_orders
```

The prefix before the first dot groups the DataJob into a DataFlow. The complete Job name remains the DataJob ID for current, parent, and dependency jobs. A name without a dot is used as both DataFlow name and DataJob ID. The Spark integration retains its existing opt-in enhanced `MERGE INTO` identity behavior.

The REST endpoint uses configured `DATAHUB_OPENLINEAGE_ORCHESTRATOR`, defaulting to `unknown`. Optional integration, engine and producer metadata do not change job identity. Custom orchestrator platforms must be registered separately.

Dataset identity follows namespace/name, environment and explicit instance configuration. Connection-qualified non-filesystem namespaces supply a component-safe default instance; filesystem resolution preserves object paths. Symlinks create native siblings without changing REST dataset identity. The event producer is not part of identity. See the [identity and migration limitations](https://github.com/manuschillerdev/datahub/blob/feat/openlineage-conformance-combined/docs/lineage/openlineage.md#known-limitations).

`run.runId` identifies the DataProcessInstance. `JobEvent` and `DatasetEvent` do not create DataProcessInstances.

### Updates and relationship ownership

Omitted run facets survive lifecycle changes, including `START` to `COMPLETE`. A supplied facet replaces its previous instance, clearing optional fields omitted from the replacement. Supported job/dataset facet deletions retract their mapped contribution, including for schema-valid populated `_deleted: true` payloads.

`JobEvent` preserves omitted I/O directions. A supplied direction replaces its own previous contribution; an explicit empty array clears that direction. Run I/O accumulates unless a complete snapshot is declared. Explicit lineage preserves own job/run I/O and independent parent/dependency relationships while avoiding inferred dataset combinations.

Explicit references retain the reporting entity as their contributor. Replacement or deletion retracts that contribution while preserving other recorded contributors and the target's own relationships. See the ownership limitations below for historical edges and competing writers.

## Standard facet mappings

### Run facets

| Facet                          | DataHub mapping                                                                                                              |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| `NominalTimeRunFacet`          | Process-instance creation time and nominal start/end custom properties                                                       |
| `ParentRunFacet`               | Parent process-instance relationship and retained parent/root identities; forwarded facets do not update referenced entities |
| `ErrorMessageRunFacet`         | Process-instance diagnostic properties; status remains determined by eventType                                               |
| `ProcessingEngineRunFacet`     | Complete facet in run properties; no job identity or pipeline version changes                                                |
| `ExternalQueryRunFacet`        | Run properties and output Dataset operation properties                                                                       |
| `EnvironmentVariablesRunFacet` | Original values in process-instance custom properties under `env.*` and the complete retained facet                          |
| `TagsRunFacet`                 | Run properties; no native job tags                                                                                           |
| `JobDependenciesRunFacet`      | Complete run property plus run-owned native job dependency edges and, when run IDs are supplied, upstream-run relationships  |
| `ExtractionErrorRunFacet`      | Run diagnostic properties; no status override                                                                                |
| `ExecutionParametersRunFacet`  | Run properties                                                                                                               |
| `GcpComposerRunFacet`          | Retained run facet properties                                                                                                |
| `GcpDataprocRunFacet`          | Retained run facet properties                                                                                                |

Environment-variable names and values are preserved. Receiver-side redaction is a follow-up with opt-in configuration, rather than part of specification compliance.

Job dependency omission preserves the reporting run's previous contribution. A supplied facet replaces it, and an explicit empty replacement retracts it while preserving other contributors. Dependency type and sequence/status trigger rules are retained on job edges; the complete facet retains all remaining metadata. These relationships do not schedule or enforce execution. Forwarded parent/root facets are retained on the reporting run with their supplied values and do not overwrite the referenced entities.

### Job facets

| Facet                        | DataHub mapping                                                                                                                                 |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `DocumentationJobFacet`      | `dataJobInfo.description`                                                                                                                       |
| `SourceCodeLocationJobFacet` | `dataJobInfo.externalUrl`                                                                                                                       |
| `SourceCodeJobFacet`         | DataJob transformation metadata and source-language custom property; current `UNKNOWN` query language breaks GraphQL DataJob readback (item 14) |
| `SQLJobFacet`                | DataJob transformation query and output Dataset operation query                                                                                 |
| `OwnershipJobFacet`          | DataJob `ownership`                                                                                                                             |
| `TagsJobFacet`               | DataJob `globalTags`                                                                                                                            |
| `JobTypeJobFacet`            | DataJob subtype/type, integration and emission-pattern properties; controls snapshot mode                                                       |
| `GcpComposerJobFacet`        | Retained job facet properties                                                                                                                   |
| `GcpLineageJobFacet`         | Retained job facet properties                                                                                                                   |

Job documentation, ownership, and tags have one canonical target: DataJob. Existing DataFlow aspects written by older behavior are not deleted.

### Dataset facets

| Facet                              | DataHub mapping                                                                           |
| ---------------------------------- | ----------------------------------------------------------------------------------------- |
| `SchemaDatasetFacet`               | `schemaMetadata`, including recursively flattened nested fields                           |
| `DatasourceDatasetFacet`           | Dataset external URL and retained source properties; no facet-derived platform instance   |
| `ColumnLineageDatasetFacet`        | Owning Dataset upstreamLineage, with replacement, omission and deletion semantics         |
| `OwnershipDatasetFacet`            | Dataset `ownership`                                                                       |
| `LifecycleStateChangeDatasetFacet` | Dataset status and native operation history retaining the original action                 |
| `SymlinksDatasetFacet`             | Dataset siblings; REST namespace/name identity stays stable                               |
| `StorageDatasetFacet`              | Storage layer and file-format custom properties                                           |
| `DatasetVersionDatasetFacet`       | `datasetProperties.customProperties["openlineage.datasetVersion"]`                        |
| `DocumentationDatasetFacet`        | `datasetProperties.description`                                                           |
| `DatasetTypeDatasetFacet`          | Dataset `subTypes`                                                                        |
| `CatalogDatasetFacet`              | Retained catalog properties; no facet-derived platform instance                           |
| `HierarchyDatasetFacet`            | Container hierarchy and Dataset-to-nearest-Container relationship                         |
| `TagsDatasetFacet`                 | Dataset globalTags and matching schema-field tags                                         |
| `DataQualityMetricsDatasetFacet`   | Dataset profile with retained numeric precision; measured fields do not imply columnCount |

Dataset version metadata does not create DataHub entity-version history. Column lineage is Dataset-owned regardless of the enclosing event. Explicit dataset lineage takes precedence; deleting it reveals the retained column facet. Independent explicit relationship contributors retain their ownership.

### Input Dataset facets

| Facet                                 | DataHub mapping                                                     |
| ------------------------------------- | ------------------------------------------------------------------- |
| `DataQualityMetricsInputDatasetFacet` | Qualified input/run properties; not a whole-dataset profile         |
| `InputStatisticsInputDatasetFacet`    | Dataset read `operation` with available row, byte, and file metrics |
| `DataQualityAssertionsDatasetFacet`   | Assertion entities and, for RunEvent, assertion run events          |
| `BaseSubsetDatasetFacet`              | Qualified dataset summary and run input/output report properties    |
| `IcebergScanReportInputDatasetFacet`  | Qualified dataset summary and run input report properties           |

A `JobEvent` emits assertion definitions but not assertion run events because it has no current run.

### Output Dataset facets

| Facet                                   | DataHub mapping                                                              |
| --------------------------------------- | ---------------------------------------------------------------------------- |
| `OutputStatisticsOutputDatasetFacet`    | Dataset write `operation` with affected rows and byte/file custom properties |
| `BaseSubsetDatasetFacet`                | Qualified dataset summary and run input/output report properties             |
| `IcebergCommitReportOutputDatasetFacet` | Qualified dataset summary and run output report properties                   |

## Custom facet compatibility

OpenLineage permits platform-defined facets through facet-map additional properties. Unknown custom facets are retained as opaque JSON with named-facet replacement, omission, and supported job/dataset deletion semantics. Job, run, and dataset facets use `openlineage.facet.<name>` properties. Input/output observations use qualified dataset/direction keys on the reporting run, or on the job for a `JobEvent`. Retention does not validate the custom schema or provide a native mapping for its contents. Specialized mapping is selected only when attachment point, key, producer family, schema identity, and consumed field shape match a declared compatibility contract.

| Key                      | Attachment | Producer family and accepted URI shape                                            | Accepted schema identity          | Lifecycle  |
| ------------------------ | ---------- | --------------------------------------------------------------------------------- | --------------------------------- | ---------- |
| `airflow`                | Run        | OpenLineage Airflow integration or Apache Airflow OpenLineage provider            | Generic `RunFacet` or `BaseFacet` | Active     |
| `spark_jobDetails`       | Run        | OpenLineage Spark integration                                                     | Generic `RunFacet`                | Active     |
| `spark_properties`       | Run        | OpenLineage Spark integration                                                     | Generic `RunFacet`                | Active     |
| `spark.logicalPlan`      | Run        | OpenLineage Spark integration                                                     | Generic `RunFacet`                | Active     |
| `spark_version`          | Run        | Historical OpenLineage Spark integration                                          | Generic `RunFacet`                | Deprecated |
| `unknownSourceAttribute` | Run        | Historical OpenLineage Airflow integration or Apache Airflow OpenLineage provider | Generic `RunFacet` or `BaseFacet` | Deprecated |

The OpenLineage integration producer URI families are HTTPS GitHub URIs under `OpenLineage/OpenLineage`, with versioned `tree` or `blob` paths to the relevant integration. Historical Airflow also accepts the unversioned integration path. Apache Airflow provider URIs are HTTPS GitHub paths under `apache/airflow/tree/providers-openlineage/<version>`.

The generic identities are:

```text
https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet
https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet
```

A familiar key with a nonmatching producer, schema, or shape remains opaque and does not select producer-specific behavior. Compatibility contributions are merged in catalog order and retain the first value on collision. The typed `processing_engine` facet is applied afterward and takes precedence. Submitted values are not included in collision or deprecation logs.

## Responses and ingestion guarantees

| Status | Meaning                                                                                      |
| ------ | -------------------------------------------------------------------------------------------- |
| `200`  | Synchronous aspect writes completed; indexing remains asynchronous                           |
| `400`  | Malformed JSON, invalid structure, schema violation, or deserialization failure              |
| `401`  | Authentication is missing or invalid                                                         |
| `403`  | The caller lacks required create/edit privileges or delete privileges for an aspect deletion |
| `415`  | The request is not JSON                                                                      |
| `500`  | Unexpected mapping, validation, or ingestion failure                                         |

Errors use the structured `{code, message, details}` response shape. Validation errors include deterministic paths and rules without echoing submitted values.

Requests without aspect deletion submit one `AspectsBatch`. Requests containing deletion preserve proposal order by applying write batches between synchronous native aspect deletes. This is not an event-level transaction or exactly-once guarantee. Aspect writes complete before success, but indexing and downstream projections remain asynchronous. A failure can leave earlier writes applied, and retries can create additional time-series writes.

The endpoint does not provide a direct Kafka mode, streaming toggle, raw-event retention, remote schema resolution, platform-registration checks, producer-scoped Dataset identities, configurable facet targets, automatic stale-aspect cleanup, or UI behavior.

## Compatibility and limitations

- Independent producers that resolve the same Dataset namespace/name and DataHub mapping update the same Dataset.
- Custom orchestrators may create dangling DataFlow platform references unless the corresponding DataPlatform is registered.
- **Column transformations:** the implementation mapping emits one native fine-grained relationship per source field with typed transformation details. Focused storage, graph, and GraphQL tests pass; broader integration checks remain pending. The complete facet is also retained as JSON.
- **Dataset-wide column lineage:** the implementation mapping emits field-to-dataset fine-grained relationships for `columnLineage.dataset[]`; focused graph and API-mapping checks pass, while broader integration checks remain pending.
- **Schema availability:** REST mapping resolves fields using stored schemas, current-event schemas, and accepted schema changes earlier in the same batch. When no matching schema is available, field references use fallback paths. Later schema arrival does not automatically repair earlier references; schema reads and subsequent writes are not a cross-request transaction.
- **Ordering:** snapshot updates are not arbitrated by producer `eventTime`; replaying an older event can replace newer metadata. Concurrent requests and asynchronous projections do not guarantee global HTTP-arrival order. This remains an existing operational limitation.
- **Ownership boundaries:** contributor tracking does not automatically reconcile historical unowned edges, hard deletion/recreation of reporting entities, or arbitrary competing non-OpenLineage writers. These are boundaries of the new ownership mechanism.
- **Identity migration:** stable job identities, corrected legacy run identifiers, and encoded schema-field components can change existing URNs or field references. No automatic migration or history backfill is provided. These corrections introduce migration obligations; literal dotted field names, for example, are encoded separately from nested paths.
- **Representation:** native timestamps use milliseconds and `START`/`RUNNING` share the native `STARTED` status, as in the previous converter. Retained facets do not constitute a complete raw-event archive or guarantee exact event replay.
- **Lifecycle and windows:** dataset rename records the previous identifier without migrating entity identity. Emission windows do not create separate historical graphs or automatically expire lineage. These capabilities remain unsupported by the added lifecycle/window handling.
- **Environment-value policy:** supplied values are preserved, including forwarded parent/root copies. Configurable receiver-side redaction is an opt-in follow-up. Mapping and request-failure logs avoid request-derived values; metadata access controls govern retained content.
- **Validation and schema versions:** remote custom schemas are not fetched or validated. Standard facets use the bundled key/attachment contracts, not arbitrary schemas selected by the payload URL. Accepted offsetless timestamps depend on the GMS host time zone.
- Existing DataFlow documentation, ownership, and tags are not removed when future events write canonical DataJob aspects.
- Hierarchy facet levels are interpreted highest-to-lowest; nonterminal levels become Containers and the terminal level remains the Dataset.

## Conformance verification

The conformance corpus contains 47 fixtures from OpenLineage 1.53.0 `spec/tests` and 74 OpenLineage compatibility-test events. Standalone events pass through HTTP validation, deserialization, conversion, authorization, and ingestion submission. Standard facet fragments are attached to minimal events at their official attachment points and pass through the same path. Focused converter tests assert identity, aspect routing, lifecycle, lineage, compatibility, and mapped values.

At the [September 30–October 1 evidence checkpoint](17034-openlineage-evidence/openlineage-goal-progress-2026-09-30.md), the selected runs passed 97 converter tests, 243 OpenLineage servlet tests (including 122 fixture-corpus cases with ordinary stored-golden comparison), 14 metadata-io tests, one GraphQL run-event mapper test, and three WAR authentication tests. The earlier 102 proposal-golden mismatches described a pre-update state and are no longer current test failures. The proposal changes still need semantic review. An earlier optional format-assertion audit rejected 14 historical compatibility events; the restored production validation policy accepts them. Fixture success does not prove stateful replacement, persisted aspects, read APIs, or full 1.53 conformance.

The 42 selected local smoke cases yielded 40 passes and two failures in existing B13/B14 assertions. Those assertions expect the previous policies for native job-dependency edges and forwarded-facet retention; the [B13 live sequence](17034-openlineage-evidence/openlineage-goal-2026-10-01-live-dependency-ownership.json) and [B14 live sequence](17034-openlineage-evidence/openlineage-goal-2026-10-01-live-forwarded-authority.json) verify the current intended behavior through stored aspects. The smoke suite is **not green**. Separate current-branch probes also read persisted lineage, identity, facet replacement/deletion, hierarchy, lifecycle, and profile values; their exact scope is listed in the checkpoint. A [source-code RunEvent probe](17034-openlineage-evidence/openlineage-goal-2026-09-30-live-graphql.json) exposed the open GraphQL `QueryLanguage.UNKNOWN` readback failure in item 14.

No master or PR-head receiver was deployed for the [known-gap appendix](17034-openlineage-spec-compliance-known-gap-appendix.md). Its upstream verdicts are pinned source assessments, while local outcomes are limited to the cited tests and live readbacks. Full integration gates, semantic proposal review, the two stale smoke assertions, unresolved local defects, and target-matched upstream persistence/read-API checks remain. This RFC does not claim merge readiness or complete specification conformance.

### October 5 rebase verification checkpoint

The implementation was rebased onto fork `origin/master` at `0a556e75ca3440995a26e001e603c89a9fc20298`. With the local implementation tree based on commit `20940d38ed06af55bca70015a17da9db7526ddf5`, the selected checks passed: 97 converter tests, 244 servlet tests (including the 122 fixture-corpus cases), 14 metadata-io tests, two GraphQL mapper tests, 45 existing Spark converter tests, 61 development-tool tests, and 12 graph-index tests: **475 passing tests**. The Rest.li model compatibility check also passed. This is a selected verification run, not the entire DataHub test suite.

The rebase moved smoke tests to `smoke-test/tests/e2e/openapi/test_openapi.py`. The live smoke suite was not rerun on October 5; its latest recorded outcome remains 40 passes and two stale-assertion failures from the October 1 checkpoint. The non-SQL GraphQL `QueryLanguage.UNKNOWN` readback bug, late-schema reconciliation, failure/retry, recreation, and other pending readbacks remain open. The local rebased commit and formatter-only import fix are not yet the published PR head; use the implementation PR/branch above for the currently published revision.

## 1.53.0 implementation follow-up

The fixture corpus contains 121 upstream-derived JSON files, including ten official fixtures with locally corrected schema declarations. Payload values are unchanged; provenance is recorded in the fixture README. Both `LineageFacet` attachment
points are validated, and the complete HTTP specification and model prose are pinned alongside the
JSON schemas. The Java converter dependency is 1.53.0; the Spark integration artifact remains
1.50.0. Compatibility across those integration paths requires separate verification.

The [batch and incremental-lineage contract](https://github.com/manuschillerdev/datahub/blob/feat/openlineage-conformance-combined/docs/lineage/openlineage.md#batches-and-incremental-lineage)
describes the new endpoint, accumulation and replacement rules, richer assertions, explicit lineage,
and representation limits. Focused regression tests apply run patches to DataHub aspect models,
exercise replay and snapshot behavior, and validate request-to-aspect mapping. Persistence remains
mocked in the HTTP corpus; the separately linked local live probes establish only their specific
stored and read-API outcomes.

## Alternatives and trade-offs

| Choice                                        | Reason and cost                                                                                                                                                                                                 |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Synchronous EntityService writes              | Reuses native validation, authorization, patches, and stored-write error reporting. Request latency includes storage; indexing is still asynchronous and a request is not one transaction.                      |
| Direct asynchronous Kafka transport           | Can reduce acknowledgment latency, as proposed by other PRs. It needs a separate durable-acceptance, validation/authorization, and partial-failure contract; acceptance alone cannot certify persisted aspects. |
| Native projections plus named-facet retention | Enables DataHub lineage/catalog features and inspection of unsupported detail. SDK serialization and native type/time limits still prevent a claim of exact original-payload fidelity.                          |
| Configured stable orchestrator                | Keeps identity independent of optional facets. Deployments must configure their desired orchestrator and register any custom platform; existing differently resolved identities need migration planning.        |

## Rollout and how we teach this

Publish the receiver contract, support/retention table, and compatibility exceptions in the [implementation's OpenLineage guide](https://github.com/manuschillerdev/datahub/blob/feat/openlineage-conformance-combined/docs/lineage/openlineage.md). Explain design-time events, run observations, and named-facet update semantics with the linked payload sequences and native readbacks.

Clients generated for a JSON-encoded string body must move to a JSON event object. Operators must review ingest/delete permissions and configured orchestrator/instance settings. URN and field-path corrections require explicit upgrade guidance and reference checks; no automatic migration or history backfill is implemented. The implementation's [updating guide](https://github.com/manuschillerdev/datahub/blob/feat/openlineage-conformance-combined/docs/how/updating-datahub.md) owns release migration notes.

Implementation merge readiness still requires semantic golden review, a green live smoke suite, fixes or agreed handling for open defects, and the remaining persisted/read-API and failure/retry checks. The RFC can be reviewed independently while those gates remain open.

## Evidence appendix

The [Markdown known-gap appendix](17034-openlineage-spec-compliance-known-gap-appendix.md)
contains the pinned 34-case master/PR/ours matrix, end-user effects, reproduction index, and direct
payload links. The [HTML view](17034-openlineage-evidence/openlineage-known-bug-head-matrix-2026-10-01.html)
adds search and filters. The broader [core semantic audit](17034-openlineage-evidence/openlineage-153-core-semantic-target-audit-2026-10-01.md)
and [facet audit](17034-openlineage-evidence/openlineage-153-facet-target-audit-2026-10-01.md) cover the
OpenLineage 1.53 contracts beyond those 34 known cases.
