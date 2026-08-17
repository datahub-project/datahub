- Start Date: 2026-04-14
- RFC PR: [datahub-project/datahub#17034](https://github.com/datahub-project/datahub/pull/17034)
- Discussion Issue:
- Implementation PR(s):

# OpenLineage REST endpoint — spec compliance

## Summary

Bring `POST /openapi/openlineage/api/v1/lineage` into alignment with the OpenLineage 2-0-2 event model while preserving DataHub's established entity identities and ingestion infrastructure.

The endpoint accepts `RunEvent`, `JobEvent`, and `DatasetEvent`, validates their typed envelopes and recognized official facets, maps supported facets to native DataHub entities and aspects, submits one `AspectsBatch` through the standard asynchronous `EntityService` path, and returns `202 Accepted`. Unknown and producer-specific facets remain compatible without resolving request-provided schema URLs.

The implementation deliberately does not add OpenLineage-specific ingestion modes, event-level atomicity, producer-scoped dataset identities, platform-registration policy, or facet-target configuration. Those ideas appeared in earlier drafts and are recorded as rejected or deferred alternatives below.

## Basic example

A `JobEvent` has no `run` block or `eventType` and is still a complete OpenLineage event:

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
# HTTP 202 Accepted
```

The event produces an inferred DataFlow, a DataJob whose ID is the complete `load.customer` name, `dataJobInputOutput`, and the declared Datasets. It does not produce a DataProcessInstance because a JobEvent is not a run-state transition.

## Motivation

The previous endpoint treated every request as a `RunEvent`, shipped an OpenAPI request body typed as a JSON string, and depended on producer-specific parsing. Valid `JobEvent` and `DatasetEvent` payloads could not follow their own semantics, unknown producers could cause conversion failures, and several standard facets were ignored or routed inconsistently.

The endpoint needs a producer-neutral contract:

- Accept all three official event variants.
- Dispatch from the event's typed structure rather than an exact schema URL.
- Preserve compatibility with historical OpenLineage, Airflow, Spark, and Marquez events.
- Route standard facets to native DataHub aspects.
- Keep canonical lineage and job identity behavior shared with the Spark lineage integration.
- Use standard DataHub ingestion instead of adding endpoint-specific transport infrastructure.
- Make accepted asynchronous behavior explicit through HTTP `202`.

Known user-visible gaps include:

- Unknown producer URIs could fail orchestrator derivation.
- `JobEvent` payloads could reach RunEvent-only code and fail.
- Job ownership and tags could be read without being persisted correctly.
- Generated clients encoded an event object as a string because of the shipped OpenAPI contract.
- Missing or malformed typed fields produced inconsistent failures.

## Requirements

1. Accept valid OpenLineage 2-0-2 `RunEvent`, `JobEvent`, and `DatasetEvent` objects.
2. Retain documented compatibility for pinned historical producer payloads where doing so does not weaken structural event dispatch.
3. Select the event variant from its required root fields; do not require one exact `schemaURL` value and never fetch request-provided URLs.
4. Validate required typed envelope fields and every recognized official facet against bundled schemas.
5. Treat unknown custom facet objects as opaque input.
6. Map standard facets to native DataHub entities and aspects as listed in the [appendix](./17034-openlineage-spec-compliance-appendix.md).
7. Preserve the complete dotted OpenLineage Job name as the DataJob ID at the HTTP endpoint. Retain the Spark integration's existing opt-in enhanced `MERGE INTO` identity behavior.
8. Keep fine-grained column lineage on `DataJobInputOutput`, matching established DataHub Spark lineage behavior.
9. Authorize mapped proposals through standard REST ingest authorization, submit authorized proposals through the standard asynchronous `EntityService` ingestion path, and return `202 Accepted` after submission.
10. Return structured client errors for malformed JSON, invalid event structure, authentication or authorization failure, unsupported media types, and typed deserialization failures.
11. Exercise the complete HTTP-to-MCP contract against pinned OpenLineage and Marquez fixture corpora.
12. Retain the public static `OpenLineageToDataHub` compatibility facade while separating mapper responsibilities internally.

### Extensibility

- Additive OpenLineage fields and facets can be mapped inside the dataset, job, run, or platform mapper without changing the controller contract.
- Producer-specific compatibility remains isolated in a fixed custom-facet catalog rather than one class or endpoint branch per producer.
- Unknown facets remain accepted so additive producer extensions do not require a DataHub release merely to pass request validation.
- Shared converter callers, including Spark lineage, continue to use `DatahubOpenlineageConfig` and `OpenLineageToDataHub` without depending on servlet infrastructure.

## Non-Requirements

- Event-level transactional or exactly-once ingestion.
- A Kafka transactional producer or OpenLineage-specific Kafka publication path.
- Synchronous-versus-asynchronous ingestion controls.
- Configurable routing of job documentation, ownership, or tags between DataJob and DataFlow.
- Producer-derived Dataset identity.
- Validation that a selected DataFlow orchestrator has a registered `dataPlatform` entity.
- Persistence of raw OpenLineage event JSON for replay.
- Remote resolution of `schemaURL` or facet `_schemaURL` values.
- Changes to the OpenLineage event producers in the Spark or Airflow integrations.
- Automatic deletion of stale DataFlow aspects written by older converter behavior.
- UI changes.

## Detailed design

### Request validation and dispatch

The controller receives the raw JSON body so validation can detect duplicate keys and trailing content before typed deserialization.

Validation and authorization proceed in five stages:

1. The controller requires `application/json`.
2. `JsonSchemaOpenLineageRequestValidator` parses the body, validates required typed envelope fields, applies the OpenLineage root `oneOf`, and validates recognized official facets by key and attachment point against bundled schemas, including facets the converter does not map.
3. The validated tree is deserialized into `OpenLineage.RunEvent`, `OpenLineage.JobEvent`, or `OpenLineage.DatasetEvent`.
4. Generated DataHub aspect payloads pass through normal ingestion batch construction and validation.
5. The completed batch passes standard REST ingest authorization before submission.

`schemaURL` is metadata, not the event discriminator. Strict OpenLineage 2-0-2 events include a URI-valued root `schemaURL`; documented historical compatibility events may use an older URL or omit it. Request-provided schema URLs are never fetched.

Unknown root properties follow the official `oneOf` behavior. Unknown custom facets remain opaque. A recognized official facet at the wrong attachment point or with malformed fields is rejected even when the converter does not map it.

### Event mapping

- `RunEvent` maps DataFlow, DataJob, DataProcessInstance, and referenced Datasets. Its `eventType` may emit a `dataProcessInstanceRunEvent`.
- `JobEvent` maps DataFlow, DataJob, `dataJobInputOutput`, and declared Datasets. It does not emit DataProcessInstance aspects.
- `DatasetEvent` maps Dataset metadata only and always emits a Dataset key and status anchor.

Resolvable jobs, datasets, and platforms use the same mapping functions across event variants. A DatasetEvent uses an `unknown`-platform fallback when Dataset identity cannot be resolved so it can still emit its required anchor; unresolvable Dataset references in RunEvent and JobEvent are omitted.

### Entity identity

OpenLineage does not define a DataFlow entity. DataHub infers one to group DataJobs.

For OpenLineage namespace `prod` and Job name `orders_etl.count_orders`:

```text
DataFlow: DataFlow(<orchestrator>, orders_etl, prod)
DataJob ID: orders_etl.count_orders
DataJob display name: count_orders
```

The complete dotted Job name remains the DataJob ID for current jobs, parent jobs, and dependency jobs. This prevents collisions between jobs that share the same final segment. The HTTP endpoint does not enable the Spark integration's existing opt-in enhanced `MERGE INTO` extraction, which may append a target-table suffix to Spark DataJob identity.

Dataset identity is derived from OpenLineage namespace/name through the existing DataHub platform, environment, platform-instance, PathSpec, symlink, and connection-instance rules. The event producer is not part of Dataset identity. Independent producers that map to the same DataHub identity therefore update the same Dataset. When those rules cannot resolve an identity, DatasetEvent materializes an `unknown`-platform anchor while RunEvent and JobEvent omit the unresolvable reference.

### DataFlow platform resolution

The first nonblank normalized candidate wins:

1. `DATAHUB_OPENLINEAGE_ORCHESTRATOR`
2. `JobTypeJobFacet.integration`
3. `ProcessingEngineRunFacet.name`
4. An explicit mapping for a known producer URI
5. `unknown`

Arbitrary producer URIs are accepted; an unknown URI does not itself become a platform name. DataHub does not check whether the selected orchestrator has a backing `dataPlatform` entity. Operators must register custom platform entities or configure a known orchestrator to avoid dangling DataFlow references.

### Facet routing

Job-scoped semantics belong on DataJob:

- `DocumentationJobFacet` → `dataJobInfo.description`
- `OwnershipJobFacet` → DataJob `ownership`
- `TagsJobFacet` and `TagsRunFacet` → DataJob `globalTags`
- `SourceCodeJobFacet` and `SQLJobFacet` → DataJob transformation metadata
- `JobTypeJobFacet` → DataJob subtype/type and a platform-resolution candidate

No target-routing controls are exposed. Older DataFlow descriptions, ownership, or tags are not automatically deleted.

Dataset facets map to Dataset-native aspects where available. Dataset version remains a custom property because it does not represent DataHub entity-version history. Hierarchy levels materialize Containers. Input/output statistics map to Dataset `operation` or `datasetProfile` aspects.

Fine-grained lineage is accumulated from input and output Dataset facets but emitted canonically on the owning DataJob's `DataJobInputOutput` aspect. It is not emitted as Dataset `upstreamLineage`.

Run facets map to DataProcessInstance properties, relationships, and run events. Parent and dependency Job identities use the current job's resolved orchestrator and the same full-name identity rules.

The complete mapping is in the appendix.

### Custom facets

Historical Airflow and Spark facets are accepted through a fixed compatibility catalog. Specialized mapping requires the expected attachment point, key, producer family, schema identity, and consumed field shape. A familiar key with a different producer or schema remains an opaque custom facet and does not select producer-specific behavior.

The standard typed `processing_engine` facet takes precedence over overlapping compatibility properties. Sensitive environment-variable values are redacted before they become DataProcessInstance custom properties.

### Ingestion and response semantics

The controller converts one event to MCPs, builds one `AspectsBatch`, and calls:

```java
entityService.ingestProposal(operationContext, aspectsBatch, true);
```

The endpoint returns `202 Accepted` after that asynchronous submission. Downstream aspect application and indexing are eventually consistent; clients requiring confirmation must poll for resulting metadata.

The batch is a submission unit, not an event-level transaction. This RFC does not promise that every aspect derived from one event is committed atomically, and it does not add Kafka transaction machinery to provide such a guarantee.

There is no synchronous mode, Kafka mode, or ingestion-mode toggle specific to OpenLineage.

### Error responses

- `202 Accepted` — proposals submitted through asynchronous ingestion.
- `400 Bad Request` — malformed JSON, invalid event structure, schema violations, or typed deserialization failure.
- `401 Unauthorized` — missing or invalid authentication.
- `403 Forbidden` — the caller lacks permission to create or edit one or more mapped entities.
- `415 Unsupported Media Type` — request is not JSON.
- `500 Internal Server Error` — unexpected mapping or ingestion failure.

Controller-owned errors use the servlet's structured `{code, message, details}` response shape. Ingestion failures currently include the cause's simple class name in `details.exception`; stack traces and cause messages remain in server logs.

### OpenAPI contract

The OpenLineage OpenAPI document is pinned locally and declares a JSON request body over `RunEvent`, `JobEvent`, and `DatasetEvent`. Its server URL matches the controller base route. Generated clients therefore send an event object rather than a JSON-encoded string.

### Test strategy

The test suite has three layers:

1. Focused converter tests assert entity identity, aspect routing, facet values, lifecycle behavior, and compatibility behavior.
2. Full-Spring HTTP tests cover allowed and denied ingest authorization. The corpus submits pinned upstream payloads through HTTP: standalone events assert acceptance and the entity families implied by their event type, while upstream facet fragments assert `202 Accepted` and a nonempty batch after attachment to minimal event envelopes.
3. Spark lineage tests protect shared converter behavior, including DataJob-owned fine-grained lineage and coalescing.

The fixture corpus contains all JSON fixtures from the pinned OpenLineage 1.45.0 `spec/tests`, the pinned OpenLineage compatibility-test scenarios, and the pinned Marquez OpenLineage event resources. The fixture README lists each source, pinned commit, source directory, file count, and license.

## Decisions changed from the original RFC draft

Earlier drafts explored a broader policy and transport layer. The implementation is intentionally narrower.

| Earlier proposal                                                                       | Final decision                                                                                                        | Rationale                                                                                                                   |
| -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Publish each event's MCPs directly to the MCP Kafka topic                              | Submit one `AspectsBatch` through standard asynchronous `EntityService` ingestion                                     | Avoid OpenLineage-specific transport and shared Kafka infrastructure changes                                                |
| Add `useStreamingIngest` with synchronous and Kafka-backed modes                       | No ingestion-mode control; always use the standard async path                                                         | One endpoint contract is easier to operate and test                                                                         |
| Guarantee request-boundary atomicity, with Kafka transactions as follow-up             | No event-level atomicity guarantee                                                                                    | An `AspectsBatch` is not a cross-aspect transaction; exactly-once support belongs in shared ingestion infrastructure        |
| Add `documentationTarget` and `ownershipTarget` with a parallel-write migration window | Always write job documentation and ownership to DataJob                                                               | OpenLineage Job facets describe the Job; DataFlow is inferred grouping metadata                                             |
| Add `requireRegisteredPlatform`                                                        | Do not query the entity registry during conversion                                                                    | Preserve permissive converter semantics and avoid event-time registry policy at the mapping boundary                        |
| Add `datasetEventNamespaceByProducer`                                                  | Producer never participates in Dataset identity                                                                       | OpenLineage identifies Datasets by namespace/name; producer scoping would split DatasetEvent metadata from RunEvent lineage |
| Add configurable `orchestratorDefault`                                                 | Use fixed `unknown` fallback                                                                                          | Avoid another policy knob while guaranteeing total resolution                                                               |
| Put column lineage on output Dataset `upstreamLineage`                                 | Keep fine-grained lineage on DataJob `DataJobInputOutput`                                                             | Preserve established DataHub Spark lineage semantics                                                                        |
| Use the suffix of a dotted Job name as the DataJob ID                                  | Preserve the complete dotted name at the HTTP endpoint; retain Spark's opt-in enhanced `MERGE INTO` identity behavior | Prevent collisions without breaking the existing Spark compatibility option                                                 |
| Interpret timezone-less timestamps as UTC                                              | Use the GMS host default time zone for historical compatibility                                                       | Match the established compatibility behavior; explicit offsets remain recommended                                           |
| Defer request-layer JSON Schema validation                                             | Bundle and apply local envelope and standard-facet validation                                                         | Conformance requires deterministic request behavior and structured errors                                                   |
| Add one registration point per facet or producer                                       | Split mapping by dataset, job, run, and platform responsibilities                                                     | Keep module boundaries coarse and avoid class-per-facet infrastructure                                                      |

These rejected controls are not reserved configuration names and carry no compatibility lifecycle.

## How we teach this

The endpoint is presented as an OpenLineage receiver that maps into DataHub's native model, not as a second ingestion subsystem.

Documentation should emphasize:

- the three supported event types;
- DataFlow as inferred DataHub grouping rather than an OpenLineage entity;
- full OpenLineage Job names as HTTP endpoint DataJob identity, with the existing opt-in Spark enhanced `MERGE INTO` exception;
- DataJob-owned job facets and fine-grained lineage;
- asynchronous `202` behavior without an atomicity promise;
- opaque compatibility for unknown custom facets; and
- the Dataset-collision and dangling-platform limitations.

The OpenAPI contract remains the source of truth for generated clients. Upgrade documentation covers request-body, response-status, and facet-target changes.

## Drawbacks

- `202 Accepted` removes read-after-write expectations. Clients may need polling.
- One OpenLineage event can still be partially applied because standard asynchronous ingestion is not an event-level transaction.
- A custom orchestrator can create a DataFlow reference to an unregistered DataPlatform.
- Independent producers with the same mapped Dataset identity update the same Dataset.
- Existing DataFlow documentation, ownership, and tags are not cleaned up when future events write only to DataJob.
- DataHub does not retain the raw event, so unsupported facets cannot be replayed later.
- Timezone-less timestamps depend on the GMS host's time zone.

These tradeoffs are documented as limitations instead of being hidden behind endpoint-specific configuration.

## Alternatives

### Direct or transactional Kafka publication

Publishing directly through `EventProducer` was rejected. Per-message publication would still allow partial publication, while true all-or-nothing behavior would require Kafka transactions and coordinated shared-consumer semantics. That architecture is broader than OpenLineage and should not be introduced from one endpoint.

### Configurable facet targets

Writing Job facets to DataFlow, DataJob, or both was rejected. It expands configuration and migration states around a semantic question with one canonical answer: Job facets map to DataJob.

### Producer-scoped Dataset URNs

Hashing the producer URI into standalone DatasetEvent URNs was rejected because the same Dataset carried in a RunEvent would then resolve to a different entity. Operators should distinguish genuinely different Datasets through namespace or platform-instance configuration.

### Runtime platform registration checks

Checking `EntityService.exists` for every platform candidate was rejected. It adds service coupling and policy to conversion, can turn custom integrations into `unknown`, and does not belong in the shared converter. Dangling references remain a documented operational limitation.

### Raw OpenLineage event storage

A pass-through event entity would preserve every facet but requires a new storage, retention, indexing, and replay model. Native aspect mapping remains the scope of this RFC.

### Producer-specific receiver implementations

One handler or class per producer conflicts with OpenLineage's producer-neutral model. Compatibility handling remains narrowly cataloged for historical custom facets while standard facets use typed mappings.

## Rollout / Adoption Strategy

The conformance behavior ships as one contract without transitional feature flags.

Clients must:

- send a JSON event object rather than a JSON-encoded string;
- accept `202 Accepted` and poll when they require confirmation;
- include explicit timestamp offsets when host-dependent interpretation is unacceptable; and
- register custom orchestrator DataPlatforms or configure a known orchestrator when dangling references are undesirable.

Job documentation, ownership, and tags move to DataJob immediately. Existing DataFlow aspects remain stored but receive no new writes from standard Job/Run facets. No automatic backfill or deletion is performed.

Dataset identities do not change merely because the producer differs. Operators with genuinely distinct Datasets that currently share a mapped identity must distinguish them through OpenLineage namespace or DataHub platform-instance mapping.

## Future Work

- Evaluate event-level atomicity only as a shared DataHub ingestion capability with explicit cross-API semantics.
- Consider raw OpenLineage event retention if replay or unsupported-facet inspection becomes a product requirement.
- Contribute DataHub mappings to the OpenLineage compatibility-test consumer matrix.
- Add mappings for future additive OpenLineage facets and versions without changing existing identities.

## Unresolved questions

None for this implementation. Future transport, retention, or identity policy changes require separate proposals rather than dormant configuration switches.

## Appendix

The implemented event, facet, identity, compatibility, and test mappings are documented in [`17034-openlineage-spec-compliance-appendix.md`](./17034-openlineage-spec-compliance-appendix.md).
