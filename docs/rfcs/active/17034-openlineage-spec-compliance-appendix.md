# Appendix — OpenLineage endpoint mapping and compatibility

Companion to [`17034-openlineage-spec-compliance.md`](./17034-openlineage-spec-compliance.md).

## Sections

- [A.1 Event mapping](#a1-event-mapping)
- [A.2 Entity identity](#a2-entity-identity)
- [A.3 Standard facet mapping](#a3-standard-facet-mapping)
- [A.4 Custom-facet compatibility](#a4-custom-facet-compatibility)
- [A.5 Validation and HTTP behavior](#a5-validation-and-http-behavior)
- [A.6 Ingestion guarantees](#a6-ingestion-guarantees)
- [A.7 Conformance corpus](#a7-conformance-corpus)
- [A.8 Original proposal disposition](#a8-original-proposal-disposition)
- [A.9 Linked issues](#a9-linked-issues)

---

## A.1 Event mapping

| OpenLineage event | DataHub entities                                            | Notes                                                                                                             |
| ----------------- | ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `RunEvent`        | DataFlow, DataJob, DataProcessInstance, referenced Datasets | `eventType` may add a `dataProcessInstanceRunEvent`; input/output URNs are also stored on the DataProcessInstance |
| `JobEvent`        | DataFlow, DataJob, referenced Datasets                      | Emits `dataJobInputOutput`; never emits DataProcessInstance aspects                                               |
| `DatasetEvent`    | Dataset                                                     | Always emits a Dataset key and status anchor even when ordinary dataset materialization is disabled               |

The envelope contributes:

| OpenLineage field        | DataHub mapping                                                                                                        |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| `eventTime`              | DataJob audit/custom timing, DataProcessInstance creation fallback, operation timestamps, and run-event timestamp      |
| `producer`               | Known producer families may contribute an orchestrator candidate; otherwise it does not participate in entity identity |
| `schemaURL`              | Locally validated URI metadata for strict events; never fetched and never used as the sole event discriminator         |
| `run.runId`              | DataProcessInstance URN and run identifier                                                                             |
| `job.namespace`          | DataFlow cluster unless overridden by configured platform instance                                                     |
| `job.name`               | DataFlow grouping prefix plus complete DataJob ID                                                                      |
| `inputs[]` / `outputs[]` | DataJob input/output edges, DataProcessInstance input/output URNs for RunEvent, and Dataset metadata                   |
| `eventType`              | DataProcessInstance run status/result; absent or `OTHER` does not emit the invalid Pegasus `$UNKNOWN` sentinel         |

## A.2 Entity identity

### A.2.1 DataFlow and DataJob

For Job name `orders_etl.count_orders` in namespace `prod`:

```text
DataFlow URN: urn:li:dataFlow:(<orchestrator>,orders_etl,prod)
DataJob URN:  urn:li:dataJob:(urn:li:dataFlow:(<orchestrator>,orders_etl,prod),orders_etl.count_orders)
Display name: count_orders
```

The prefix before the first dot groups the job into a DataFlow. The complete name remains the DataJob ID. A name without a dot is used for both the DataFlow name and DataJob ID. Parent-run and dependency jobs use the same rules. The HTTP endpoint does not enable the Spark integration's existing opt-in enhanced `MERGE INTO` extraction, which may append a target-table suffix to Spark DataJob identity.

OpenLineage has no DataFlow object. The DataFlow is a DataHub grouping inferred from the OpenLineage Job.

### A.2.2 Orchestrator

Candidates are normalized and evaluated in this order:

1. configured `orchestrator`;
2. `JobTypeJobFacet.integration`;
3. `ProcessingEngineRunFacet.name`;
4. a recognized Airflow, Flink, Spark, or Trino producer URI; and
5. `unknown`.

Unknown producer URIs are accepted but are not converted into arbitrary platform names. The selected name is not checked against the DataHub entity registry. A custom name can therefore produce a dangling DataFlow platform reference unless the corresponding `dataPlatform` entity is registered.

### A.2.3 Dataset

Dataset URNs reuse the converter's established namespace, platform, PathSpec, symlink, environment, connection-instance, and platform-instance rules. Filesystem URI schemes map to DataHub platforms where supported.

The event producer is not part of Dataset identity. Two producers that resolve the same OpenLineage namespace/name to the same DataHub platform, environment, and platform instance update the same Dataset. Genuinely different datasets must use distinct namespaces or platform-instance mapping.

When those rules cannot resolve a Dataset identity, DatasetEvent materializes an `unknown`-platform anchor because its contract requires an emitted Dataset key and status. RunEvent and JobEvent omit unresolvable Dataset references rather than adding unknown lineage edges.

### A.2.4 DataProcessInstance

`Run.runId` identifies the DataProcessInstance. A historical non-UUID root identifier is deterministically converted from the Job namespace, Job name, and reported run identifier for compatibility. JobEvent and DatasetEvent do not create DataProcessInstances.

## A.3 Standard facet mapping

### A.3.1 Run facets

| OpenLineage facet              | DataHub mapping                                                                                                                 |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `NominalTimeRunFacet`          | Nominal start determines `dataProcessInstanceProperties.created`; start/end values are retained as custom properties            |
| `ParentRunFacet`               | `dataProcessInstanceRelationships.parentInstance` and a parent DataJob input edge using full Job identity                       |
| `ErrorMessageRunFacet`         | DataProcessInstance custom properties and a failure run event when error content is present                                     |
| `ProcessingEngineRunFacet`     | Orchestrator candidate, DataFlow `versionInfo`, and custom properties                                                           |
| `ExternalQueryRunFacet`        | Output Dataset `operation.customProperties`                                                                                     |
| `EnvironmentVariablesRunFacet` | DataProcessInstance custom properties under `env.*`; common secret-bearing names are redacted                                   |
| `TagsRunFacet`                 | DataJob `globalTags`                                                                                                            |
| `JobDependenciesRunFacet`      | Upstream DataJob edges plus edge/custom properties; downstream dependency metadata is retained without creating reverse lineage |
| `ExtractionErrorRunFacet`      | DataProcessInstance custom properties and failure run-event semantics when failures are reported                                |

### A.3.2 Job facets

| OpenLineage facet            | DataHub mapping                                                                                |
| ---------------------------- | ---------------------------------------------------------------------------------------------- |
| `DocumentationJobFacet`      | `dataJobInfo.description`                                                                      |
| `SourceCodeLocationJobFacet` | `dataJobInfo.externalUrl`                                                                      |
| `SourceCodeJobFacet`         | DataJob transformation metadata; source language is also retained in DataJob custom properties |
| `SQLJobFacet`                | DataJob transformation query plus output Dataset operation query metadata                      |
| `OwnershipJobFacet`          | DataJob `ownership`                                                                            |
| `TagsJobFacet`               | DataJob `globalTags`                                                                           |
| `JobTypeJobFacet`            | DataJob `subTypes`, `dataJobInfo.type`, and orchestrator candidate                             |

Job documentation, ownership, and standard Job/Run tags have one canonical target: DataJob. Standard Job facets do not write those aspects to the inferred DataFlow. Existing DataFlow aspects from older ingestion are not deleted.

### A.3.3 Dataset facets

| OpenLineage facet                  | DataHub mapping                                                                         |
| ---------------------------------- | --------------------------------------------------------------------------------------- |
| `SchemaDatasetFacet`               | `schemaMetadata`, including recursively flattened nested fields                         |
| `DatasourceDatasetFacet`           | Dataset external URL and `dataPlatformInstance` contribution                            |
| `ColumnLineageDatasetFacet`        | Fine-grained lineage contribution merged into the owning DataJob's `dataJobInputOutput` |
| `OwnershipDatasetFacet`            | Dataset `ownership`                                                                     |
| `LifecycleStateChangeDatasetFacet` | Dataset `status`; `DROP` marks the Dataset removed, while `TRUNCATE` does not           |
| `SymlinksDatasetFacet`             | Dataset `siblings` plus established TABLE-symlink identity resolution                   |
| `StorageDatasetFacet`              | Dataset custom properties for storage layer and file format                             |
| `DatasetVersionDatasetFacet`       | `datasetProperties.customProperties["openlineage.datasetVersion"]`                      |
| `DocumentationDatasetFacet`        | `datasetProperties.description`                                                         |
| `DatasetTypeDatasetFacet`          | Dataset `subTypes`                                                                      |
| `CatalogDatasetFacet`              | Dataset catalog custom properties and platform-instance contribution                    |
| `HierarchyDatasetFacet`            | Container keys, properties, status, parent links, and Dataset-to-nearest-Container link |
| `TagsDatasetFacet`                 | Dataset `globalTags`                                                                    |

OpenLineage Dataset version metadata does not create DataHub version history.

Column lineage remains DataJob-owned. The converter may temporarily carry lineage alongside a mapped Dataset contribution, but `DatahubJob.toMcps` aggregates and emits it through `DataJobInputOutput`; no Dataset `upstreamLineage` aspect is emitted for this mapping.

### A.3.4 Input Dataset facets

| OpenLineage facet                     | DataHub mapping                                                                                      |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `DataQualityMetricsInputDatasetFacet` | Input Dataset `datasetProfile`, including available field profiles                                   |
| `InputStatisticsInputDatasetFacet`    | Input Dataset `operation` with read semantics and available row/byte/file metrics                    |
| `DataQualityAssertionsDatasetFacet`   | Native Assertion entities and assertion run events associated with the input Dataset and current run |

A JobEvent emits Assertion definitions but not assertion run events because it has no current run.

### A.3.5 Output Dataset facets

| OpenLineage facet                    | DataHub mapping                                                                                 |
| ------------------------------------ | ----------------------------------------------------------------------------------------------- |
| `OutputStatisticsOutputDatasetFacet` | Output Dataset `operation` with write semantics, affected rows, and byte/file custom properties |

## A.4 Custom-facet compatibility

Historical producer facets are not treated as new standard facets. Specialized conversion is limited to the compatibility catalog:

| Key                      | Attachment | Producer family              |
| ------------------------ | ---------- | ---------------------------- |
| `airflow`                | Run        | Apache Airflow               |
| `spark_jobDetails`       | Run        | OpenLineage Spark            |
| `spark_version`          | Run        | Historical OpenLineage Spark |
| `spark_properties`       | Run        | OpenLineage Spark            |
| `spark.logicalPlan`      | Run        | OpenLineage Spark            |
| `unknownSourceAttribute` | Run        | Historical Airflow           |

A specialized mapping is selected only when attachment point, key, producer family, schema identity, and consumed field shapes match the catalog contract. A known key with a different producer or schema remains an opaque custom facet.

Compatibility contributions are merged in fixed order. The official typed `processing_engine` facet is applied after historical compatibility properties and takes precedence for overlapping fields. Unmapped custom facets are accepted but are not retained as raw event data.

Detailed producer/schema evidence is maintained in [`../../lineage/openlineage-custom-facet-compatibility.md`](../../lineage/openlineage-custom-facet-compatibility.md).

## A.5 Validation and HTTP behavior

### A.5.1 Structural selection

The root OpenLineage `oneOf` determines whether a payload is a RunEvent, JobEvent, or DatasetEvent. Exact schema URL equality is not required. An object with valid `run` and `job` fields remains a RunEvent even if it contains an unknown root property. A `job` plus `dataset` payload without `run` is invalid because it ambiguously matches official variants.

### A.5.2 Compatibility boundary

Strict OpenLineage 2-0-2 events include typed `eventTime`, `producer`, and `schemaURL` fields. Pinned historical compatibility events may omit older metadata such as root `schemaURL` when the event can still be selected and safely deserialized.

Every recognized official facet is validated locally against its bundled schema, including facets the converter does not map. Unknown custom facet payloads remain opaque. No request-provided schema URL is resolved over the network.

Timezone-less timestamps receive `ZoneId.systemDefault()`. Producers requiring a stable instant must include an explicit offset.

### A.5.3 Responses

| Status | Meaning                                                                         |
| ------ | ------------------------------------------------------------------------------- |
| `202`  | Event converted and proposals submitted through standard asynchronous ingestion |
| `400`  | Invalid JSON, event structure, schema-constrained field, or typed value         |
| `401`  | Authentication missing or invalid                                               |
| `403`  | Caller lacks create or edit permission for one or more mapped entities          |
| `415`  | Content type is not JSON                                                        |
| `500`  | Unexpected mapping or ingestion failure                                         |

The OpenAPI contract declares an object request body over the three event variants and uses the controller's `/openapi/openlineage/api/v1` base route.

## A.6 Ingestion guarantees

One request follows this path:

```text
validated event
  → typed OpenLineage event
  → converter MCP list
  → AspectsBatchImpl
  → standard REST ingest authorization
  → EntityService.ingestProposal(operationContext, batch, true)
  → HTTP 202
```

The endpoint does not publish directly through `EventProducer`, does not add a Kafka transaction, and does not expose synchronous, streaming, or transactional modes.

One `AspectsBatch` submission is not an event-level atomicity guarantee. Aspects are independent DataHub write units and downstream application/indexing is eventually consistent. Clients can poll for metadata they need to observe. Event retries are not generally guaranteed to be idempotent because time-series aspects and compatibility defaults can produce additional writes.

## A.7 Conformance corpus

The corpus vendors 126 unchanged upstream JSON fixtures:

- 37 fixtures from OpenLineage 1.45.0 `spec/tests`;
- 74 OpenLineage compatibility-test event payloads; and
- 15 Marquez event payloads.

The 90 standalone event payloads must return `202 Accepted`, submit a nonempty ingestion batch, and emit the entity families implied by their event type. The 36 OpenLineage facet fragments are attached to minimal DataHub-authored event envelopes at their official attachment points and pass through the same HTTP, validation, deserialization, conversion, and ingestion-submission path.

The fixture README lists the three upstream repositories, pinned commits, source directories, file counts, and licenses. Focused converter tests protect detailed aspect values and identity rules, while Spark lineage tests protect behavior shared through the converter.

## A.8 Original proposal disposition

The original RFC PR proposed several mechanisms that are not part of the implementation:

| Proposal                                                   | Disposition                                                             |
| ---------------------------------------------------------- | ----------------------------------------------------------------------- |
| Direct MCP Kafka publication                               | Rejected in favor of standard asynchronous `EntityService` ingestion    |
| Kafka transactional or request-boundary atomic ingestion   | Descoped; no event-level atomicity guarantee                            |
| `useStreamingIngest`                                       | Removed; no ingestion-mode switch                                       |
| `documentationTarget`                                      | Removed; Job documentation always maps to DataJob                       |
| `ownershipTarget`                                          | Removed; Job ownership always maps to DataJob                           |
| Parallel DataFlow/DataJob writes with a later default flip | Removed; no transitional routing states                                 |
| `requireRegisteredPlatform` and entity-registry lookups    | Removed; dangling platform references are documented                    |
| `datasetEventNamespaceByProducer` and producer hashing     | Removed; producer is not Dataset identity and collisions are documented |
| Configurable `orchestratorDefault`                         | Removed; fallback is fixed to `unknown`                                 |
| Output Dataset `upstreamLineage` for column lineage        | Rejected; canonical lineage remains on DataJob `DataJobInputOutput`     |
| DataJob ID derived from only the dotted suffix             | Rejected; complete OpenLineage Job name is preserved                    |
| UTC coercion for timezone-less timestamps                  | Rejected in favor of established host-default compatibility             |
| Deferring request schema validation                        | Reversed; local validation is included                                  |
| Class-per-facet registration framework                     | Rejected in favor of dataset/job/run/platform mapper boundaries         |

These names are not retained as deprecated aliases because they never became part of a released endpoint contract.

## A.9 Linked issues

| Issue                                                             | Implemented behavior                                                                         |
| ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| [#16961](https://github.com/datahub-project/datahub/issues/16961) | Unknown producers no longer cause orchestrator-derivation failure                            |
| [#15196](https://github.com/datahub-project/datahub/issues/15196) | JobEvent is dispatched and mapped without Run-only aspects                                   |
| [#14458](https://github.com/datahub-project/datahub/issues/14458) | Job ownership and tags are persisted on DataJob                                              |
| [#13011](https://github.com/datahub-project/datahub/issues/13011) | Trino and other supported orchestrator signals resolve without a producer allow-list failure |

Adjacent source-specific lineage issues remain outside this endpoint RFC unless they reproduce through the shared converter behavior documented here.
