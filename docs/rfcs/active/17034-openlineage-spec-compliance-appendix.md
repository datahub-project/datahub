# Appendix — OpenLineage endpoint: methodology, test suite, mapping, gaps

Companion to [`17034-openlineage-spec-compliance.md`](./17034-openlineage-spec-compliance.md).

File and line references are against `master` at commit `7ed8710c65`.

## Sections

- [A.1 Methodology](#a1-methodology)
- [A.2 OpenLineage ↔ DataHub ↔ Marquez mapping](#a2-openlineage--datahub--marquez-mapping)
- [A.3 Status quo and gaps](#a3-status-quo-and-gaps)
- [A.4 OpenAPI contract drift](#a4-openapi-contract-drift)
- [A.5 Milestone roll-up](#a5-milestone-roll-up)
- [A.6 Linked issues and PRs](#a6-linked-issues-and-prs)

---

## A.1 Methodology

Findings are obtained through two complementary techniques. Every row in
the status tables in §A.3 is backed by at least one of them.

### A.1.1 Source-level audit

Full read of the four files that implement the endpoint:

- `metadata-service/openapi-servlet/src/main/java/io/datahubproject/openapi/openlineage/controller/LineageApiImpl.java`
- `metadata-service/openapi-servlet/src/main/java/io/datahubproject/openapi/openlineage/mapping/RunEventMapper.java`
- `metadata-integration/java/openlineage-converter/src/main/java/io/datahubproject/openlineage/converter/OpenLineageToDataHub.java` (1373 lines)
- `metadata-service/openapi-servlet/src/main/resources/openlineage/openlineage.json` (412 lines)

Each of the 32 standard OpenLineage facets is located in the converter by
name-based search and its call site is traced. Anything reachable only via
untyped JSON (`RunFacet.getAdditionalProperties().get(...)`) is noted as
custom-facet handling and listed separately in §A.3.7.

### A.1.2 Aspect-store verification

For every claim about "what was actually persisted", the DataHub OpenAPI v3
entity endpoint is queried directly:

```
GET /openapi/v3/entity/<entityType>/<encoded urn>?systemMetadata=false
```

This returns the authoritative aspect set for each URN. Responses confirm
which aspects are present, the shape of each written aspect including typed
field values, and whether referenced `dataPlatform` URNs actually exist as
entities.

A concrete example that backs the "ghost platform" finding in the main RFC
Motivation:

```
$ curl -s -w '\n%{http_code}\n' \
    http://localhost:8080/openapi/v3/entity/dataPlatform/urn%3Ali%3AdataPlatform%3Aclient
{"statusCode":404,"message":"No aspects could be found"}
404
```

The `urn:li:dataPlatform:client` referenced by `DataFlow` aspects emitted
by the current converter does not resolve to a backing `dataPlatform`
entity — the ghost-platform fragmentation the Motivation section calls
out as one of the audit findings.

---

## A.2 OpenLineage ↔ DataHub ↔ Marquez mapping

A cross-walk for each spec element. The **Marquez** column records what
Marquez persists (derived from `marquez.db.OpenLineageDao` and the row mappers
under `api/src/main/java/marquez/db/mappers/`). The **DataHub target** column
records what the converter should produce. Row-level implementation status is
in §A.3.

**Upstream home.** The OpenLineage project maintains a cross-consumer
compatibility repo at `github.com/OpenLineage/compatibility-tests` where
each consumer ships its own mapping as
`consumer/consumers/<name>/mapping.json` in a standardized JSON format
(`mapped.core`, `mapped.<FacetName>`, `knownUnmapped` sections).
Currently the repo has one consumer entry (`dataplex`). Re-expressing
the cross-walk below as `consumer/consumers/datahub/mapping.json` and
contributing it upstream is tracked in the RFC's Future Work.

### A.2.1 Envelope

| OpenLineage element | Marquez | DataHub target |
|---|---|---|
| `RunEvent` | `runs` + `run_states` rows | `DataProcessInstance` MCPs + referenced `DataFlow`/`DataJob`/`Dataset` |
| `JobEvent` | `jobs` + `job_versions` rows, no run linkage | `DataFlow` + `DataJob` + `dataJobInputOutput` + referenced `Dataset` (no DPI) |
| `DatasetEvent` | `datasets` + `dataset_versions` rows, no job/run linkage | `Dataset` aspects only |
| `eventTime` | `run_states.transitioned_at` | `dataProcessInstanceRunEvent.timestampMillis` + `dataProcessInstanceProperties.created` |
| `producer` | `lineage_events.producer` (verbatim) | signal for orchestrator derivation |
| `schemaURL` | `lineage_events.schema_url` (verbatim) | not load-bearing; accepted as-is |
| `run.runId` | `runs.uuid` | `dataProcessInstance` URN |
| `job.{namespace,name}` | `jobs.namespace_name` + `jobs.name` | `dataFlowKey` + `dataJobKey` (via split-on-`.`) |
| `inputs[]` / `outputs[]` | `dataset_versions_io_mapping` rows | `dataJobInputOutput.inputDatasetEdges` / `outputDatasetEdges` |
| `eventType` enum | `run_states.state` | `dataProcessInstanceRunEvent` aspect: `status` field + `result.type` field (sibling fields on the same MCP) |

### A.2.2 Run facets

| OpenLineage facet | Marquez persistence | DataHub target aspect |
|---|---|---|
| `NominalTimeRunFacet` | `run_args` promoted columns (`nominal_start_time`, `nominal_end_time`) | `dataProcessInstanceProperties.created` (nominal start) + custom property `nominalEndTime`. The end time lands in `customProperties` because `dataProcessInstanceProperties` has no typed field for a nominal end; adding one is a schema change outside the scope of this RFC. |
| `ParentRunFacet` | `runs.parent_run_uuid` | `dataProcessInstanceRelationships.parentInstance` |
| `ErrorMessageRunFacet` | JSONB only (`run_facets`) | `dataProcessInstanceRunEvent` FAILURE + custom properties `errorMessage`, `programmingLanguage`, `stackTrace` |
| `ProcessingEngineRunFacet` | JSONB only | orchestrator-name input + `versionInfo.version` on DataFlow |
| `ExternalQueryRunFacet` | JSONB only | `operation.customProperties.externalQueryId` on outputs |
| `EnvironmentVariablesRunFacet` | JSONB only | `dataProcessInstanceProperties.customProperties` (prefixed `env.`) |
| `TagsRunFacet` | JSONB only | `globalTags` on parent DataJob |
| `JobDependenciesRunFacet` | JSONB only | `dataJobInputOutput.inputDatajobs` entries |
| `ExtractionErrorRunFacet` | JSONB only | `dataProcessInstanceRunEvent` FAILURE + custom property `extractionErrors` |

### A.2.3 Job facets

| OpenLineage facet | Marquez persistence | DataHub target aspect |
|---|---|---|
| `DocumentationJobFacet` | `jobs.description` promoted column | `dataJobInfo.description` |
| `SourceCodeLocationJobFacet` | `jobs.location` promoted column | `dataJobInfo.externalUrl` |
| `SourceCodeJobFacet` | JSONB only (`job_facets`) | `dataTransformLogic.transformations[].rawTransformation` |
| `SQLJobFacet` | JSONB only | `dataTransformLogic.transformations[].queryStatement` on DataJob + mirror to `operation.customProperties.queryStatement` on each output. The mirror is so the output dataset carries the exact query that produced it, matching `operation` semantics; consumers of `operation` aspects on a dataset can read the producing SQL without joining through `dataJob`. |
| `OwnershipJobFacet` | JSONB only | `ownership` on DataJob (OL `type` → DataHub `OwnershipType`) |
| `TagsJobFacet` | JSONB only | `globalTags` on DataJob |
| `JobTypeJobFacet` | JSONB only | `subTypes.typeNames[0] = jobType`; `processingType` → `dataJobInfo.type`; `integration` → DataPlatform override |

### A.2.4 Dataset facets

| OpenLineage facet | Marquez persistence | DataHub target aspect |
|---|---|---|
| `SchemaDatasetFacet` | `dataset_fields` rows | `schemaMetadata.fields` (recursive for nested structs) |
| `DatasourceDatasetFacet` | `sources.name` + `sources.uri` | `dataPlatformInstance.instance` + `datasetProperties.externalUrl` |
| `ColumnLineageDatasetFacet` | `column_lineage` table (one row per upstream field pair) | `upstreamLineage.fineGrainedLineages[]` on the output Dataset |
| `OwnershipDatasetFacet` | JSONB only (`dataset_facets`) | `ownership` on Dataset |
| `LifecycleStateChangeDatasetFacet` | `datasets.is_deleted` (derived) | `status.removed = true` for DROP/TRUNCATE |
| `SymlinksDatasetFacet` | `dataset_symlinks` rows | `siblings.siblings[]` |
| `StorageDatasetFacet` | JSONB only | `datasetProperties.customProperties` (`storageLayer`, `fileFormat`) |
| `DatasetVersionDatasetFacet` | JSONB only | `versionProperties.version.versionTag` |
| `DocumentationDatasetFacet` | `datasets.description` promoted column | `datasetProperties.description` |
| `DatasetTypeDatasetFacet` | JSONB only | `subTypes.typeNames` |
| `CatalogDatasetFacet` | JSONB only | `dataPlatformInstance.instance` |
| `HierarchyDatasetFacet` | JSONB only | `container` aspect |
| `TagsDatasetFacet` | JSONB only | `globalTags` on Dataset |

### A.2.5 Input / output dataset facets

| OpenLineage facet | Marquez persistence | DataHub target aspect |
|---|---|---|
| `DataQualityMetricsInputDatasetFacet` | JSONB only (`input_dataset_facets`) | `datasetProfile` time-series MCP on the input |
| `InputStatisticsInputDatasetFacet` | JSONB only | `operation` on input, `customOperationType = READ` |
| `OutputStatisticsOutputDatasetFacet` | JSONB only (`output_dataset_facets`) | `operation` on output, `numAffectedRows = rowCount`, `numAffectedBytes = size` |
| `DataQualityAssertionsDatasetFacet` | JSONB only | `assertion` entity + `assertionRunEvent` per assertion. Unique among the mapped facets: creates a DataHub entity rather than writing an aspect to an existing one. |

### A.2.6 Storage-model note

Marquez retains the full raw payload of every facet as JSONB alongside any
promoted typed columns. Facets Marquez does not specifically understand are
still persisted and remain queryable. DataHub has no equivalent raw-event
store: facets the converter does not map are dropped. The unmapped-facet
debug log proposed in the RFC makes the drop observable.

---

## A.3 Status quo and gaps

Status legend:
- **✅** — implemented and written to the target aspect.
- **🟡** — read and written to a divergent target (wrong entity, wrong
  aspect, or a wrong-but-adjacent field). Closer to ✅ than to ❌ for
  effort estimation; the rerouting work is mechanical.
- **🟠** — read by the converter but discarded before persistence, or
  consumed for a side effect (URN rewriting, regex extraction) without
  the facet itself being emitted as an aspect. Further from ✅ than 🟡:
  the field extraction code exists but the output wiring has to be
  written from scratch.
- **❌** — not read by the converter at all. Full implementation required.

Priority legend: **P0** baseline spec compliance. **P1** standard producer
coverage. **P2** quality/edge cases.

### A.3.1 Endpoint dispatch and envelope

| Element | Target | Prio | Status | Notes |
|---|---|---|---|---|
| `RunEvent` deserialization | `OpenLineageClientUtils.runEventFromJson` | P0 | ✅ | `LineageApiImpl.java:60` |
| `JobEvent` dispatch | Polymorphic deserializer | P0 | ❌ | Controller calls `runEventFromJson` unconditionally. Downstream NPE at `RunEvent.getRun().getFacets()`. Surfaces as #15196. |
| `DatasetEvent` dispatch | Polymorphic deserializer | P0 | ❌ | No code branch exists. |
| Free-form `producer` URI | Total orchestrator derivation | P0 | ❌ | `OpenLineageToDataHub.java:1275-1301`. Hard-coded regex + prefix allow-list throws `RuntimeException("Unable to determine orchestrator")` on unknown producers. Surfaces as #16961, #13011. |
| Orchestrator name for canonical OpenLineage producer URLs | Registered `dataPlatform` entity | P0 | ❌ | The regex `https://github.com/OpenLineage/OpenLineage/.*/(.*)$` captures the last path segment of the producer URL. `.../blob/v1-0-0/client` yields orchestrator `client`; `.../evaluation/openlineage-examples` yields `openlineage-examples`. Both resulting `urn:li:dataPlatform:*` URNs return `404` from the entity endpoint — ghost references written into `dataFlowKey.orchestrator` and `dataPlatformInstance.platform`. |
| `dataJobInfo.type` | Derived from `JobTypeJobFacet` | P0 | 🟡 | Currently set from the orchestrator name (e.g. `{"string":"client"}` for canonical-OL jobs). `JobTypeJobFacet.processingType`/`.jobType` is not consulted. |
| `RunEvent.eventType` enum mapping | `dataProcessInstanceRunEvent.status` + `result.type` | P0 | 🟡 | `:1193-1237`. `START`/`RUNNING`/`COMPLETE`/`FAIL`/`ABORT` map correctly. **`OTHER` sets `result.type = RunResultType.$UNKNOWN`, a Pegasus sentinel that fails aspect validation** (`/result/type :: "$UNKNOWN" is not an enum symbol`). The endpoint returns 200, the MCP is published, and the aspect is dropped by the downstream validator — a silent semantic failure rather than a crash. Missing `eventType` (optional per spec) NPEs on `.ordinal()` before reaching the switch. |
| `RunEvent.eventTime` | DPI `created` + run-event `timestampMillis` | P0 | ✅ | `:1077-1081`, `:1203` |
| `Run.runId` | `dataProcessInstanceKey` + properties | P0 | ✅ | `:1075` |
| `Job.{namespace,name}` | `dataJobKey` + `dataFlowKey` via split-on-`.` | P0 | 🟡 | `getFlowUrn` at `:1248`. No-dot job names produce `flowId == taskId` (degenerate URN; functional but inconsistent). |
| `RunEvent.inputs[]` | `dataJobInputOutput.inputDatasetEdges` | P0 | ✅ | `:1133-1161`. Gated by `materializeDataset` / `captureColumnLevelLineage` config flags. |
| `RunEvent.outputs[]` | `dataJobInputOutput.outputDatasetEdges` | P0 | ✅ | `:1163-1191` |
| Required-field validation | 400 on missing `schemaURL` / `producer` / `eventTime` | P0 | ❌ | No pre-dispatch validation. Missing fields cause downstream NPEs that surface as 500. Missing `schemaURL` on a request with all other fields is silently accepted and the event is ingested. |
| Error response shape | Structured JSON `{code, message, details}` | P0 | ❌ | `LineageApiImpl.java:63-66` returns empty 500. `:99` re-throws as `RuntimeException`. |
| Request atomicity | All MCPs from one event commit together | P0 | ❌ | The controller loops over MCPs and calls `_entityService.ingestProposal` per MCP. **A mid-stream validation failure (e.g. the `OTHER` eventType case above) leaves earlier MCPs (`dataFlowInfo`, `status`, `dataJobInfo`, `dataJobInputOutput`) committed in the aspect store before the run-event MCP fails**, with no client-visible indication of which subset landed. A single event can leave the aspect store in an inconsistent partial-write state. Replaced by the Kafka MCP publish path described in the main RFC §"Streaming ingest". |
| Auth null-safety | 401 on missing actor | P0 | ❌ | `:70` calls `AuthenticationContext.getAuthentication()` without null check. |

### A.3.2 Run facets

| Facet | Target aspect | Prio | Status | Notes |
|---|---|---|---|---|
| `NominalTimeRunFacet` | `dataProcessInstanceProperties.created` + `nominalEndTime` custom property | P0 | ❌ | Facet never read. |
| `ParentRunFacet` | `dataProcessInstanceRelationships.parentInstance` | P0 | 🟡 | Read at `:1107-1131`. Resolved to a parent `DataJob` URN and written to `dataJobInputOutput.inputDatajobEdges`, not to the DPI relationship aspect. Run-to-run link is lost. |
| `ErrorMessageRunFacet` | FAILURE run event + custom properties | P0 | ❌ | Facet never read. FAIL/ABORT events lose the message, language, and stack trace. |
| `ProcessingEngineRunFacet` | Orchestrator input + `versionInfo.version` on DataFlow | P0 | 🟡 | `name` is used for orchestrator derivation at `:317`. `name`/`version`/`openlineageAdapterVersion` copied into DPI/Flow `customProperties` at `:580-601`. Not written to `versionInfo`. |
| `ExternalQueryRunFacet` | `operation.customProperties.externalQueryId` on outputs | P1 | ❌ | Not read. |
| `EnvironmentVariablesRunFacet` | `dataProcessInstanceProperties.customProperties` | P2 | ❌ | Not read. |
| `TagsRunFacet` | `globalTags` on parent DataJob | P2 | ❌ | The only tag path (`generateTags`, `:540-578`) reads from the Airflow custom run facet `run.facets.airflow.dag.tags`. Standard `TagsRunFacet` is never consulted. |
| `JobDependenciesRunFacet` | `dataJobInputOutput.inputDatajobs` | P2 | ❌ | Not read. |
| `ExtractionErrorRunFacet` | FAILURE run event + custom property | P2 | ❌ | Not read. |

### A.3.3 Job facets

| Facet | Target aspect | Prio | Status | Notes |
|---|---|---|---|---|
| `DocumentationJobFacet` | `dataJobInfo.description` | P0 | 🟡 | Read at `:405-411`. Written to `dataJobInfo.description` (`:817`, correct target) **and** to `dataFlowInfo.description` (`:346`, surplus). The DataJob write is correct; the DataFlow write is spurious and will move behind the `documentationTarget` config flag. |
| `SourceCodeLocationJobFacet` | `dataJobInfo.externalUrl` | P0 | ❌ | Not read. No `getSourceCodeLocation()` call exists in the converter. |
| `SourceCodeJobFacet` | `dataTransformLogic.transformations[].rawTransformation` | P1 | ❌ | Not read. |
| `SQLJobFacet` | `dataTransformLogic.transformations[].queryStatement` + mirror to outputs | P0 | 🟠 | Read at `:506-525` and `:935-958`. The query text is parsed only to extract a target table name from `MERGE INTO` statements (regex-based extraction). The SQL itself is never persisted as an aspect — read for a side effect, then discarded. |
| `OwnershipJobFacet` | `ownership` on DataJob | P0 | 🟠 | Read at `:373-403`. An `Ownership` object is built (owner type hardcoded to `DEVELOPER`, source hardcoded to `SERVICE`) and assigned to `jobBuilder.flowOwnership(...)`. The downstream `DatahubJob.toMcps()` serializer does not emit an `ownership` MCP from this field. Direct aspect-store query confirms neither the DataJob nor the DataFlow receives an `ownership` aspect. Read but dropped. Surfaces as #14458. |
| `TagsJobFacet` | `globalTags` on DataJob | P0 | ❌ | Not read. The only tag path reads the Airflow custom run facet. Surfaces as #14458. |
| `JobTypeJobFacet` | `subTypes` + `dataJobInfo.type` + DataPlatform override | P0 | 🟡 | Read at `:1038-1041` only to special-case `RDD_JOB` (skip RDD-stage events). `processingType`, `integration`, and `jobType` are not persisted. |

### A.3.4 Dataset facets

| Facet | Target aspect | Prio | Status | Notes |
|---|---|---|---|---|
| `SchemaDatasetFacet` | `schemaMetadata.fields` (recursive nested) | P0 | 🟡 | `:1323-1356`. Field lists and recursive nested structs are written. Two bugs folded into the Milestone A rework: (1) the type-name mapping at `:1303` is case-sensitive, so lowercase OL types (`"string"`, `"int"`) map to `StringType`/`NumberType` but uppercase (`"STRING"`, `"INT"`, common from Trino/JDBC producers) map to `NullType` — a Trino producer sending uppercase types currently loses every column type; (2) `platformSchema` is hardcoded to `MySqlDDL` with the raw OL field list encoded into `tableSchema`, regardless of the actual dataset platform. Both are fixed as part of the P0 `SchemaDatasetFacet` work. |
| `DatasourceDatasetFacet` | `dataPlatformInstance.instance` + `datasetProperties.externalUrl` | P0 | 🟡 | Used for dataset-URN platform/instance derivation in `convertOpenlineageDatasetToDatasetUrn`. The `uri` is not written to `externalUrl`. No dataset-level `dataPlatformInstance` aspect is emitted; the aspect present on each dataset URN is auto-derived from the URN itself. |
| `ColumnLineageDatasetFacet` | `upstreamLineage.fineGrainedLineages[]` on the output Dataset | P0 | 🟡 | Read at `:413-538`. Emitted as a JSON-patch MCP on `dataJob.dataJobInputOutput.fineGrainedLineages[]` — i.e. on the DataJob, not on the output Dataset. Aspect-store query for an output Dataset on the column-lineage test confirms no `upstreamLineage` aspect; query for the corresponding DataJob confirms the `fineGrainedLineages` patch content. Secondary divergences: `transformations[].description` and the `masking` flag are discarded; `confidenceScore` is hardcoded to `0.5`; the 1.0 form of the facet (without `transformations[]`) degrades to a literal `transformOperation = "TRANSFORM"`. |
| `OwnershipDatasetFacet` | `ownership` on Dataset | P1 | ❌ | Not read. |
| `LifecycleStateChangeDatasetFacet` | `status.removed` for DROP/TRUNCATE | P1 | ❌ | Not read. |
| `SymlinksDatasetFacet` | `siblings.siblings[]` | P1 | 🟠 | Read at `:132-138` and `:980-982`. Used to rewrite the dataset URN (replacing the OL identifier with the symlinked one) and to find merge-target tables. No `siblings` aspect is ever emitted. The current URN-rewriting use is a legitimate side channel for cross-platform aliases but is not the spec-intended semantics of the facet; the proposal emits a `siblings` aspect in addition to preserving the URN rewrite. |
| `StorageDatasetFacet` | `datasetProperties.customProperties` | P2 | ❌ | Not read. |
| `DatasetVersionDatasetFacet` | `versionProperties.version.versionTag` | P1 | ❌ | Not read. |
| `DocumentationDatasetFacet` | `datasetProperties.description` | P0 | ❌ | Not read. |
| `DatasetTypeDatasetFacet` | `subTypes.typeNames` | P1 | ❌ | Not read. |
| `CatalogDatasetFacet` | `dataPlatformInstance.instance` | P1 | ❌ | Not read. |
| `HierarchyDatasetFacet` | `container` aspect | P2 | ❌ | Not read. |
| `TagsDatasetFacet` | `globalTags` on Dataset | P1 | ❌ | Not read. |
| `DataQualityAssertionsDatasetFacet` | native `assertion` entity + `assertionRunEvent` per assertion | P0 | ❌ | Not read. Unique among the P0 facets: creates a DataHub entity rather than writing an aspect to an existing one. |

### A.3.5 Input dataset facets

| Facet | Target aspect | Prio | Status | Notes |
|---|---|---|---|---|
| `DataQualityMetricsInputDatasetFacet` | `datasetProfile` time-series MCP | P1 | ❌ | Not read. |
| `InputStatisticsInputDatasetFacet` | `operation` on input, `READ` | P1 | ❌ | Not read. |

### A.3.6 Output dataset facets

| Facet | Target aspect | Prio | Status | Notes |
|---|---|---|---|---|
| `OutputStatisticsOutputDatasetFacet` | `operation` on output (`numAffectedRows`, `numAffectedBytes`) | P0 | ❌ | Not read. |

### A.3.7 Custom (non-standard) facet handlers

The converter handles a set of producer-specific custom facets via
`RunFacet.getAdditionalProperties()`. These are not standard OpenLineage
facets and are listed for completeness; the RFC does not modify them.

| Custom facet key | Handler | Source location |
|---|---|---|
| `spark_jobDetails` | `processSparkJobDetails` | `:646-660` |
| `processing_engine` (additionalProperties path) | `processProcessingEngine` | `:647` area |
| `spark_version` | `processSparkVersion` | `:678` |
| `spark_properties` | `processSparkProperties` | `:690` |
| `airflow` | `processAirflowProperties` | `:703` |
| `spark.logicalPlan` | `processSparkLogicalPlan` | `:715` |
| `unknownSourceAttribute` | `processUnknownSourceAttributes` | `:723` |

`processAirflowProperties` reads `run.facets.airflow.dag.tags` and emits
`GlobalTags` — functionally overlapping with the standard `TagsJobFacet` /
`TagsRunFacet`. Resolving the overlap is an unresolved question in the main
RFC.

### A.3.8 Baseline aspect set per RunEvent

Against the current implementation, every successful `RunEvent` request
produces the same 10-aspect baseline regardless of which facets it carries:

```
 dataFlow:            dataFlowInfo, status
 dataJob:             dataJobInfo, dataJobInputOutput, status
 dataProcessInstance: dataProcessInstanceInput, dataProcessInstanceOutput,
                      dataProcessInstanceProperties, dataProcessInstanceRunEvent,
                      dataProcessInstanceRelationships
```

Datasets referenced via `inputs[]`/`outputs[]` receive only `datasetKey` +
`status`. A `schemaMetadata` aspect is written for each dataset that carries
a `SchemaDatasetFacet`. A `dataJobInputOutput` PATCH MCP carries
`fineGrainedLineages` entries for each dataset that carries a
`ColumnLineageDatasetFacet`. No `upstreamLineage` aspect is ever written on
the dataset side, regardless of whether column lineage is present.

---

## A.4 OpenAPI contract drift

`metadata-service/openapi-servlet/src/main/resources/openlineage/openlineage.json`:

| Element | Spec expectation | Current state | Status |
|---|---|---|---|
| `requestBody.content.application/json.schema` | `oneOf [RunEvent, JobEvent, DatasetEvent]` | `{ "type": "string" }` (lines 22-28) | ❌ |
| Declared spec version | matches embedded component schemas | `info.version: "2-0-2"` (line 5) but `components.schemas` still use the older `BaseFacet` shape (`_producer`/`_schemaURL`, no `JobEvent`/`DatasetEvent` definitions) | 🟡 |
| Documented responses | At least `200`, `400`, `500` | Only `200` documented (lines 30-34) | ❌ |
| Operation set | `POST /lineage` | `POST /lineage` only | ✅ |

The `requestBody.schema` row is the single most damaging item in the
contract: every generated client (swagger-codegen, openapi-generator, or
third-party Stainless-style generators) reads a `string` request body and
emits code that JSON-encodes the OpenLineage event into a string literal
before sending. A client built from the shipped contract will never
successfully send a real OpenLineage event without the developer manually
patching the request method, which accounts for a substantial share of
the integration confusion users hit on their first attempt.

---

## A.5 Milestone roll-up

A third of the P0 items already have partial converter code that writes
to an adjacent but incorrect target; those reduce to rerouting work
rather than full implementation. The remainder is new code (event-type
dispatch, request atomicity via the MCP Kafka topic, structured error
model, registered-platform validation, `NominalTimeRunFacet`,
`ErrorMessageRunFacet`, `SourceCodeLocationJobFacet`, `TagsJobFacet` /
`OwnershipJobFacet` MCP persistence, `DocumentationDatasetFacet`,
`OutputStatisticsOutputDatasetFacet`, `DataQualityAssertionsDatasetFacet`).

### Milestone A — P0 baseline

| Section | Total P0 | ✅ (no change) | 🟡 (reroute) | 🟠 (read but discarded) | ❌ (new) |
|---|---|---|---|---|---|
| Envelope / dispatch / contract / errors / atomicity | 16 | 5 | 3 | 0 | 8 |
| Run facets | 4 | 0 | 2 | 0 | 2 |
| Job facets | 6 | 0 | 1 | 3 | 2 |
| Dataset facets | 5 | 0 | 3 | 0 | 2 |
| OutputDataset facets | 1 | 0 | 0 | 0 | 1 |
| **Total** | **32** | **5** | **9** | **3** | **15** |

The Dataset facets row includes `DataQualityAssertionsDatasetFacet`, which
lands on the DataHub native `assertion` entity rather than on an aspect of
an existing entity — the only P0 item that requires creating a new entity
type rather than writing an aspect to an existing one.

### Milestone B — P1

14 items. None of the P1 facets are currently read by the converter.

### Milestone C — P2

Environment variables, hierarchy, run-level tags, extraction-error
reporting.

---

## A.6 Linked issues and PRs

All open issues on `datahub-project/datahub` matching
`state:open openlineage in:title,body` at the time of writing, filtered for
relevance to the REST endpoint.

### A.6.1 In scope — closed or substantially addressed by this RFC

| # | Title | Relevant appendix section |
|---|---|---|
| [#16961](https://github.com/datahub-project/datahub/issues/16961) | OpenLineage with custom producer not working | A.3.1 (free-form producer) |
| [#15196](https://github.com/datahub-project/datahub/issues/15196) | OpenLineage JobEvents are not processed | A.3.1 (JobEvent dispatch) |
| [#14458](https://github.com/datahub-project/datahub/issues/14458) | TagsJobFacet and OwnershipJobFacet are ignored on OpenLineage REST-Calls | A.3.3 (`OwnershipJobFacet`, `TagsJobFacet`) |
| [#13011](https://github.com/datahub-project/datahub/issues/13011) | Error when Integrating openlineage api with Trino's OpenLineage event listener | A.3.1 (free-form producer) — same root cause as #16961 |

### A.6.2 Adjacent — shares code paths with the REST endpoint

| # | Title | Relationship |
|---|---|---|
| [#13929](https://github.com/datahub-project/datahub/issues/13929) | Data lineage is not sent to datahub gms | Spark agent path (`acryl-spark-lineage`) calling into `OpenLineageToDataHub`. The orchestrator-resolution and facet-rerouting work in Milestone A runs through the same converter, so the reported `Query execution is null` symptom may improve as a side effect; not a gating criterion for this RFC. |
| [#16018](https://github.com/datahub-project/datahub/issues/16018) | Feature request: disable lineage ingestion per source (Tableau) to avoid overwriting external lineage | The same cross-producer overwrite concern that the RFC addresses for `DatasetEvent` via `datasetEventNamespaceByProducer`. Directly out of scope for this RFC; the recommendation informs the design. |

### A.6.3 Open PRs touching the OpenLineage area

| # | Title | Relationship |
|---|---|---|
| [#16890](https://github.com/datahub-project/datahub/pull/16890) | feat(ingest/spark): upgrade OpenLineage from 1.38.0 to 1.45.0 with full shadow JAR shading | Client library version bump for the Spark agent. Does not regenerate `openlineage.json` or change the converter. |
| [#14911](https://github.com/datahub-project/datahub/pull/14911) | feat(ingest/spark): Initial commit for openlineage 1_38 upgrade | Earlier client version bump. Stale. |
| [#13066](https://github.com/datahub-project/datahub/pull/13066) | feat: accept trino as orchestrator | Adds Trino to the hard-coded orchestrator allow-list. Partial fix for #13011. Expected to become redundant once the allow-list is removed in favor of the free-form producer resolution described in §A.3.1. |
| [#14573](https://github.com/datahub-project/datahub/pull/14573) | fix(ingest/spark): Map sqlserver to mssql in dialect detection | Spark agent only. |

None of the open PRs address the dispatch, contract, atomicity, or
facet-coverage gaps cataloged in this appendix.
