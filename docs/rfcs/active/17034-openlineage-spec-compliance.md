# OpenLineage REST endpoint specification compliance

## Summary

`POST /openapi/openlineage/api/v1/lineage` accepts OpenLineage `RunEvent`, `JobEvent`, and `DatasetEvent` objects, validates their typed envelopes and recognized official facets, maps supported metadata to native DataHub entities and aspects, and submits one `AspectsBatch` through the standard asynchronous `EntityService` ingestion path.

The endpoint targets the OpenLineage 2-0-2 event model and retains compatibility with documented historical Airflow, Spark, and Marquez payloads. Request-provided schema URLs are metadata and are never fetched.

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
2. validates the event envelope and recognized standard facets against bundled OpenLineage 1.45.0 schemas;
3. dispatches structurally to `RunEvent`, `JobEvent`, or `DatasetEvent`;
4. maps the typed event to MCPs and validates the resulting aspects through normal batch construction;
5. authorizes the complete batch with standard REST ingest authorization; and
6. submits the batch asynchronously and returns `202 Accepted`.

A root `schemaURL` or facet `_schemaURL` must be a valid absolute URI when present. Schema URL equality is not used for event dispatch, and no request-provided URL is resolved. Historical events may omit root `schemaURL` and facet schema metadata where they can still be selected and safely deserialized.

Unknown custom facet objects remain opaque. A recognized standard facet is validated by key and attachment point even when DataHub does not map it. A standard key at the wrong attachment point is rejected.

Timezone-less timestamps receive the GMS host's default time zone. Producers that require an unambiguous instant must include an explicit offset. Historical non-UUID root run identifiers are converted deterministically from the Job namespace, Job name, and reported identifier.

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

DataFlow orchestrator candidates are evaluated in this order:

1. configured `DATAHUB_OPENLINEAGE_ORCHESTRATOR`;
2. `JobTypeJobFacet.integration`;
3. `ProcessingEngineRunFacet.name`;
4. a recognized Airflow, Flink, Spark, or Trino producer URI; and
5. `unknown`.

Unknown producer URIs are accepted and do not become arbitrary platform names. DataHub does not verify that the selected orchestrator has a registered `dataPlatform` entity.

Dataset URNs use the converter's namespace, platform, PathSpec, symlink, environment, connection-instance, and platform-instance rules. The producer is not part of Dataset identity. An unresolvable Dataset in a `DatasetEvent` receives an `unknown`-platform anchor; unresolvable references in `RunEvent` and `JobEvent` are omitted.

`run.runId` identifies the DataProcessInstance. `JobEvent` and `DatasetEvent` do not create DataProcessInstances.

## Standard facet mappings

### Run facets

| Facet                          | DataHub mapping                                                                                                 |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `NominalTimeRunFacet`          | Process-instance creation time and nominal start/end custom properties                                          |
| `ParentRunFacet`               | Parent process-instance relationship and parent DataJob input edge                                              |
| `ErrorMessageRunFacet`         | Process-instance custom properties and failure run-event semantics                                              |
| `ProcessingEngineRunFacet`     | Orchestrator candidate, DataFlow version, and custom properties                                                 |
| `ExternalQueryRunFacet`        | Output Dataset operation custom properties                                                                      |
| `EnvironmentVariablesRunFacet` | Redacted process-instance custom properties under `env.*`                                                       |
| `TagsRunFacet`                 | DataJob `globalTags`                                                                                            |
| `JobDependenciesRunFacet`      | Upstream DataJob edges and dependency properties; downstream metadata is retained without reverse lineage edges |
| `ExtractionErrorRunFacet`      | Process-instance custom properties and failure run-event semantics                                              |
| `ExecutionParametersRunFacet`  | Accepted and validated; not mapped                                                                              |
| `GcpComposerRunFacet`          | Accepted and validated; not mapped                                                                              |
| `GcpDataprocRunFacet`          | Accepted and validated; not mapped                                                                              |

Common secret-bearing environment-variable names, including tokens, passwords, keys, credentials, and authorization values, are redacted.

### Job facets

| Facet                        | DataHub mapping                                                     |
| ---------------------------- | ------------------------------------------------------------------- |
| `DocumentationJobFacet`      | `dataJobInfo.description`                                           |
| `SourceCodeLocationJobFacet` | `dataJobInfo.externalUrl`                                           |
| `SourceCodeJobFacet`         | DataJob transformation metadata and source-language custom property |
| `SQLJobFacet`                | DataJob transformation query and output Dataset operation query     |
| `OwnershipJobFacet`          | DataJob `ownership`                                                 |
| `TagsJobFacet`               | DataJob `globalTags`                                                |
| `JobTypeJobFacet`            | DataJob subtype/type and orchestrator candidate                     |
| `GcpComposerJobFacet`        | Accepted and validated; not mapped                                  |
| `GcpLineageJobFacet`         | Accepted and validated; not mapped                                  |

Job documentation, ownership, and tags have one canonical target: DataJob. Existing DataFlow aspects written by older behavior are not deleted.

### Dataset facets

| Facet                              | DataHub mapping                                                    |
| ---------------------------------- | ------------------------------------------------------------------ |
| `SchemaDatasetFacet`               | `schemaMetadata`, including recursively flattened nested fields    |
| `DatasourceDatasetFacet`           | Dataset external URL and `dataPlatformInstance` contribution       |
| `ColumnLineageDatasetFacet`        | Fine-grained lineage on the owning DataJob's `DataJobInputOutput`  |
| `OwnershipDatasetFacet`            | Dataset `ownership`                                                |
| `LifecycleStateChangeDatasetFacet` | Dataset `status`; `DROP` marks removed and `TRUNCATE` does not     |
| `SymlinksDatasetFacet`             | Dataset `siblings` and TABLE-symlink identity resolution           |
| `StorageDatasetFacet`              | Storage layer and file-format custom properties                    |
| `DatasetVersionDatasetFacet`       | `datasetProperties.customProperties["openlineage.datasetVersion"]` |
| `DocumentationDatasetFacet`        | `datasetProperties.description`                                    |
| `DatasetTypeDatasetFacet`          | Dataset `subTypes`                                                 |
| `CatalogDatasetFacet`              | Catalog custom properties and platform-instance contribution       |
| `HierarchyDatasetFacet`            | Container hierarchy and Dataset-to-nearest-Container relationship  |
| `TagsDatasetFacet`                 | Dataset `globalTags`                                               |
| `DataQualityMetricsDatasetFacet`   | Accepted and validated; not mapped                                 |

Dataset version metadata does not create DataHub entity-version history. Fine-grained lineage remains DataJob-owned and is not emitted as Dataset `upstreamLineage`.

### Input Dataset facets

| Facet                                 | DataHub mapping                                                     |
| ------------------------------------- | ------------------------------------------------------------------- |
| `DataQualityMetricsInputDatasetFacet` | Dataset `datasetProfile`, including available field profiles        |
| `InputStatisticsInputDatasetFacet`    | Dataset read `operation` with available row, byte, and file metrics |
| `DataQualityAssertionsDatasetFacet`   | Assertion entities and, for RunEvent, assertion run events          |
| `BaseSubsetDatasetFacet`              | Accepted and validated; not mapped                                  |
| `IcebergScanReportInputDatasetFacet`  | Accepted and validated; not mapped                                  |

A `JobEvent` emits assertion definitions but not assertion run events because it has no current run.

### Output Dataset facets

| Facet                                   | DataHub mapping                                                              |
| --------------------------------------- | ---------------------------------------------------------------------------- |
| `OutputStatisticsOutputDatasetFacet`    | Dataset write `operation` with affected rows and byte/file custom properties |
| `BaseSubsetDatasetFacet`                | Accepted and validated; not mapped                                           |
| `IcebergCommitReportOutputDatasetFacet` | Accepted and validated; not mapped                                           |

## Custom facet compatibility

OpenLineage permits platform-defined facets through facet-map additional properties. Unknown custom facets are accepted as opaque objects. Specialized mapping is selected only when attachment point, key, producer family, schema identity, and consumed field shape match a declared compatibility contract.

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

| Status | Meaning                                                                         |
| ------ | ------------------------------------------------------------------------------- |
| `202`  | The mapped batch was submitted through standard asynchronous ingestion          |
| `400`  | Malformed JSON, invalid structure, schema violation, or deserialization failure |
| `401`  | Authentication is missing or invalid                                            |
| `403`  | The caller cannot create or edit one or more mapped entities                    |
| `415`  | The request is not JSON                                                         |
| `500`  | Unexpected mapping, validation, or ingestion failure                            |

Errors use the structured `{code, message, details}` response shape. Validation errors include deterministic paths and rules without echoing submitted values.

One request produces one `AspectsBatch` submission. This is not an event-level transaction or exactly-once guarantee. Aspect application and indexing are eventually consistent, and retries can create additional time-series writes. Clients requiring confirmation must poll for the resulting metadata.

The endpoint does not provide a synchronous mode, direct Kafka mode, streaming toggle, raw-event retention, remote schema resolution, platform-registration checks, producer-scoped Dataset identities, configurable facet targets, automatic stale-aspect cleanup, or UI behavior.

## Compatibility and limitations

- Independent producers that resolve the same Dataset namespace/name and DataHub mapping update the same Dataset.
- Custom orchestrators may create dangling DataFlow platform references unless the corresponding DataPlatform is registered.
- Timezone-less timestamps depend on the GMS host time zone.
- Unsupported and unknown custom facets are not retained for replay.
- Existing DataFlow documentation, ownership, and tags are not removed when future events write canonical DataJob aspects.
- Downstream job-dependency entries are retained as custom properties and do not create reverse lineage edges.
- Hierarchy facet levels are interpreted highest-to-lowest; nonterminal levels become Containers and the terminal level remains the Dataset.

## Conformance verification

The conformance corpus contains 37 fixtures from OpenLineage 1.45.0 `spec/tests`, 74 OpenLineage compatibility-test events, and 15 Marquez OpenLineage events. Standalone events pass through HTTP validation, deserialization, conversion, authorization, and ingestion submission. Standard facet fragments are attached to minimal events at their official attachment points and pass through the same path. Focused converter tests assert identity, aspect routing, lifecycle, lineage, compatibility, and mapped values.
