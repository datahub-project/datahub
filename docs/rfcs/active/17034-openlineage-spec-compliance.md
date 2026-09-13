- Start Date: 2026-04-14
- Base Commit: `7ed8710c65` (source-level audit references in appendix §A.3 are tied to this commit)
- RFC PR: [datahub-project/datahub#17034](https://github.com/datahub-project/datahub/pull/17034)
- Discussion Issue:
- Implementation PR(s):

# OpenLineage REST endpoint — spec compliance

## Summary

Bring the `POST /openapi/openlineage/api/v1/lineage` endpoint into alignment with
the OpenLineage 2-0-2 specification. Accept all three root event types
(`RunEvent`, `JobEvent`, `DatasetEvent`), accept any spec-conforming `producer`
URI, route the standard facets to their target DataHub aspects on the entities
the spec attaches them to, route ingestion through the existing Kafka MCP
topic to decouple REST latency from aspect-store writes, and ship an OpenAPI
contract that matches the events the endpoint accepts. The orchestrator /
platform name is derived by a total function — one that always returns a
value and never throws — from typed facets, with the producer URI as a
fallback.

## Basic example

A `JobEvent` (no `run` block, no `eventType`) is a valid OpenLineage event. The
target behavior:

```bash
curl -X POST http://localhost:8080/openapi/openlineage/api/v1/lineage \
  -H 'Content-Type: application/json' \
  -d '{
    "eventTime": "2026-04-14T10:00:00Z",
    "producer":  "https://example.com/my-pipeline-tool",
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent",
    "job":     { "namespace": "crm", "name": "load_dp_customer" },
    "inputs":  [ { "namespace": "crm_unload",   "name": "customer" } ],
    "outputs": [ { "namespace": "dataproducts", "name": "dp_customer" } ]
  }'
# HTTP 202
# Publishes to the MCP Kafka topic: DataFlow + DataJob + dataJobInputOutput
# + Dataset(key,status) ×2. No DataProcessInstance MCPs (JobEvent is not a
# run state transition). Today this returns HTTP 500 with a NullPointerException
# (#15196).
```

A spec-invalid event returns a structured error response from the servlet's
global exception handler:

```json
{ "code": "INVALID_EVENT",
  "message": "Missing required field: schemaURL",
  "details": { "field": "schemaURL" } }
```

## Motivation

The DataHub OpenLineage endpoint is advertised as spec-compatible but
implements a producer allow-list: the converter recognizes a fixed set of
`producer` URIs (canonical OL, Airflow, Trino, a handful of others) and
crashes or silently drops payloads from anything outside that set. The
architectural goal of this RFC is to make **"spec-compliant OpenLineage
producer" a sufficient condition** for interoperating with DataHub: any
event that conforms to OpenLineage 2-0-2 must be accepted, dispatched on
its actual type, and routed to the DataHub entities and aspects the spec
attaches each facet to, without per-producer special-casing.

The known user-visible symptoms (each traceable to the same root cause —
producer-specific code paths instead of spec-driven routing):

- HTTP 500 (`Unable to determine orchestrator`) when the producer URI is not on
  a hard-coded allow-list
  ([#16961](https://github.com/datahub-project/datahub/issues/16961),
  [#13011](https://github.com/datahub-project/datahub/issues/13011)).
- HTTP 500 (`NullPointerException`) when the event is a `JobEvent`
  ([#15196](https://github.com/datahub-project/datahub/issues/15196)).
- `TagsJobFacet` and `OwnershipJobFacet` are read from the payload but never
  persisted on the REST path
  ([#14458](https://github.com/datahub-project/datahub/issues/14458)).
- The shipped OpenAPI contract declares the request body as `{"type":
  "string"}`, so every generated client sends JSON-encoded strings instead of
  OpenLineage events — this single contract bug accounts for a substantial
  share of the integration confusion users hit on their first attempt.

Three findings from the source-level audit (appendix §A.3) that are not
captured in the open tickets:

- **`DataFlow` URNs point at ghost platforms.** The current converter's
  orchestrator-derivation regex captures the last path segment of the
  producer URL, so the canonical OpenLineage Python/Java reference client
  producer URI turns into `urn:li:dataPlatform:client` after the DataHub
  converter processes it — a platform entity that does not exist in the
  DataHub model. A `GET /openapi/v3/entity/dataPlatform/urn:li:dataPlatform:client`
  returns 404. Different canonical-OL producer URLs yield different ghost
  platforms, fragmenting related lineage across non-entities.
- **The endpoint produces aspects its own backend rejects.** The
  `RunEvent.eventType = OTHER` case maps to `RunResultType.$UNKNOWN` in the
  current converter — a Pegasus sentinel value that fails DataHub's own
  aspect validation. The endpoint succeeds at the HTTP layer, publishes the
  MCP, and the aspect is silently dropped downstream. This is a semantic
  failure, not a crash: producers see a 200 and lineage disappears.
- **No request atomicity.** The current `LineageApiImpl` loops over MCPs and
  calls `EntityServiceImpl.ingestProposal` per item synchronously. A failure
  partway through leaves earlier MCPs committed and later ones lost, with no
  client-visible indication of which. A single event can leave the aspect
  store in an inconsistent partial-write state.

Making the endpoint spec-compliant therefore requires three aligned changes:
(a) remove the per-producer special-casing in the converter and route every
standard facet to a documented target aspect, (b) dispatch on the event's
actual type rather than hardcoding `RunEvent`, and (c) publish the entire MCP
batch for a single event through the existing MCP Kafka topic so the request
succeeds or fails as a unit without blocking on synchronous aspect-store
writes.

## Requirements

1. Every spec-valid OpenLineage 2-0-2 event is accepted with HTTP 2xx and
   produces at least one MCP. The endpoint never returns an empty 500 — all
   error paths produce a structured error body via the servlet's global
   exception handler.
2. All three root event types — `RunEvent`, `JobEvent`, `DatasetEvent` — are
   dispatched and ingested on paths appropriate to the event's semantics.
3. The `producer` field is treated as a free-form URI per spec. The
   orchestrator/platform name is derived from typed facets first; the
   derivation function is total (always returns a value, never throws).
4. Standard facets enumerated in the appendix are routed to their target
   DataHub aspects on the entity the spec attaches them to (Job facets →
   `DataJob`, Run facets → `DataProcessInstance`, Dataset facets → `Dataset`).
5. The `openlineage.json` OpenAPI contract that the servlet ships matches the
   events the server accepts.
6. The implementation is testable against the OpenLineage spec without
   DataHub-specific assumptions. A parameterized test harness loads JSON
   fixtures from the OpenLineage / Marquez ecosystem and asserts both the HTTP
   response and the resulting MCP set.
7. Behavior changes are observable. Unmapped facets log at debug level so new
   producer additions are visible without producing 500s.
8. Ingestion of a single event is atomic at the request boundary. Either every
   MCP produced from the event is published to the downstream MCP channel or
   none is — no partial writes.

### Extensibility

- Adding a facet mapping is a matter of writing a method in the converter and
  registering it in the per-event-type dispatcher; the controller does not
  need to change.
- Per-producer customization (orchestrator override, platform-instance
  override, dataset-URN namespace prefix) is supported through
  `DatahubOpenlineageConfig` so site operators can adapt the endpoint without
  code changes.
- New OpenLineage spec versions are absorbed by regenerating `openlineage.json`
  and bumping the embedded `io.openlineage:openlineage-java` client. The
  dispatcher and facet mappings are version-stable as long as the spec stays
  additive, which matches OpenLineage's declared versioning policy
  (`spec/Versioning.md`: "schema changes should only add optional fields").

## Non-Requirements

- Changes to the Spark or Airflow agents (`acryl-spark-lineage`,
  `metadata-ingestion-modules/airflow-plugin`) are out of scope; their event
  shapes already match what the REST endpoint will accept.
- Persisting raw OpenLineage event payloads is out of scope. Marquez stores
  the full JSON in `lineage_events` for later replay; DataHub has no analogous
  store and adding one is outside this RFC.
- Request-layer validation of payloads is out of scope. Typed Jackson
  deserialization against the `io.openlineage:openlineage-java` beans
  catches whatever the beans' own typed fields catch (wrong JSON shapes,
  unknown enum values, malformed timestamps) and nothing beyond that.
- Custom (non-standard) facets sent via `additionalProperties` keep their
  producer-specific handlers (Spark, Airflow). Unifying them with the standard
  facet table is a follow-up.
- This RFC does not propose UI changes.

## Detailed design

The design is organized around three concerns: dispatch at the controller,
entity-and-URN shape in the converter, and facet-to-aspect routing. The
companion [appendix](./17034-openlineage-spec-compliance-appendix.md) carries
the detailed reference material:

- **Appendix §A.2 — OpenLineage ↔ DataHub ↔ Marquez mapping.** Per-element
  cross-walk covering the envelope, every standard facet, and how each
  receiver stores the same OL concept.
- **Appendix §A.3 — Status quo and gaps.** Per-facet implementation status
  against the converter source, backed by aspect-store verification.
- **Appendix §A.5 — Milestone roll-up.** Per-section / per-status counts
  driving the implementation-effort estimate.

### Endpoint dispatch

`LineageApiImpl.postRunEventRaw(String body)` currently calls
`OpenLineageClientUtils.runEventFromJson(body)` unconditionally — hardcoded
to `RunEvent`, which is the root cause of the `JobEvent` / `DatasetEvent`
dispatch gap. The new shape:

1. **Deserialize** the body into `LineageBody` — the existing Jackson
   discriminated-union interface at
   `metadata-service/openapi-servlet/src/main/java/io/datahubproject/openapi/openlineage/model/LineageBody.java`.
   `LineageBody` selects one of `OpenLineage.RunEvent`,
   `OpenLineage.JobEvent`, `OpenLineage.DatasetEvent`. Today the interface
   discriminates on exact-string match against three hard-coded `schemaURL`
   values pinned to the pre-`$defs` `2-0-0/OpenLineage.json#/definitions/`
   path — this breaks on any real producer, because producers emit
   arbitrary `schemaURL` values (different OL versions, different cached
   schema references, vendor extensions). This RFC replaces the strategy
   with a shape-based custom `TypeIdResolver`: `run` present → `RunEvent`;
   `dataset` present → `DatasetEvent`; else → `JobEvent`. `schemaURL` is
   still logged as a hint but does not gate dispatch.
2. **Dispatch** to one of three mapper entry points on `RunEventMapper`:
   - `mapRunEvent(RunEvent, MappingConfig)` — emits `DataFlow`, `DataJob`,
     `DataProcessInstance`, and referenced `Dataset` aspects.
   - `mapJobEvent(JobEvent, MappingConfig)` — emits `DataFlow`, `DataJob`,
     `dataJobInputOutput`, and referenced `Dataset` aspects. No DPI aspects.
   - `mapDatasetEvent(DatasetEvent, MappingConfig)` — emits `Dataset` aspects
     only.
3. **Publish** every MCP produced for a single event to the MCP Kafka topic
   via `EventProducer.produceMetadataChangeProposal(urn, mcp)`, which is the
   same path the MCE consumer already reads from
   (`metadata-dao-impl/kafka-producer/src/main/java/com/linkedin/metadata/dao/producer/KafkaEventProducer.java`).
   See §"Streaming ingest" below for atomicity and response semantics.
4. `AuthenticationContext.getAuthentication()` is null-checked and returns
   401 when the actor is missing. The controller's current
   `catch (Exception e) → 500` block is removed; Jackson's typed exceptions
   (`MismatchedInputException` on wrong JSON shapes, unknown enum values,
   malformed `ZonedDateTime`, unparseable JSON) and mapper-layer runtime
   failures propagate to the servlet's global exception handler
   (`GlobalControllerExceptionHandler`), which produces a structured
   error body.

### Timestamp coercion

The openapi-servlet `ObjectMapper` used for OpenLineage deserialization
installs a permissive `ZonedDateTime` deserializer that falls back to
`LocalDateTime.parse(s).atZone(UTC)` on `DateTimeParseException`, so naive
timestamps are treated as UTC and accepted. This matches Marquez's behavior
and accommodates producers such as the OpenLineage reference Python client,
which emits `eventTime` without a timezone suffix (e.g.
`"2021-11-03T10:53:52.427343"`) as the default serialization shape.
Unconditional — no config knob.

### Streaming ingest

Every event produces a batch of MCPs. This RFC publishes the batch to the
MCP Kafka topic rather than calling `EntityServiceImpl.ingestProposal` in a
loop synchronously, matching the path the MCE consumer uses and decoupling
the REST endpoint from GMS write latency.

**Rationale.** Producers emit OpenLineage events at their own cadence —
Spark, Airflow, Trino, dbt — and expect fire-and-forget semantics. Under
the current synchronous path, a bursty Spark job blocks on per-MCP aspect-
store writes, compounding latency proportional to the number of input /
output datasets. Routing through the existing MCP topic uses the same
async ingestion path the rest of DataHub uses for high-throughput metadata
emission and removes aspect-store latency from the REST response time.

**Atomicity.** The controller collects the full MCP list for the event,
then publishes each via
`EventProducer.produceMetadataChangeProposal(urn, mcp)` and awaits the
returned `Future<?>`. If any publish fails, the request returns 5xx.
MCPs already published before the failure are not rolled back and may
still be consumed downstream — this is a known gap relative to true
transactional semantics, which would require Kafka's transactional
producer (exactly-once batch commit) and is tracked as a follow-up. It
is nonetheless strictly stronger than the current behavior, where
partial writes succeed under HTTP 200 with no client-visible indication
of which MCPs landed and which did not (appendix §A.3.1 "Request
atomicity"). The contract for clients is "5xx on any publish failure;
retry the event" — matching producer-side retry expectations (OL
producers are retry-capable at the event level).

**Response semantics.** HTTP 2xx means "accepted and published to the MCP
channel". Downstream consumer ingestion is eventually consistent; clients
that need confirm-on-ingest can poll `GET /openapi/v3/entity/<type>/<urn>`
for the aspect they expect. This is a semantic change from the current
endpoint, which returns 2xx only after synchronous aspect-store writes
have landed. The status code changes from `200` to `202 Accepted` to
reflect the async semantics.

**Configuration.** `DatahubOpenlineageConfig.useStreamingIngest` (boolean,
default `true`) toggles between the Kafka path and the legacy synchronous
`EntityServiceImpl.ingestProposal` path. Operators who depend on
confirm-on-ingest semantics during the transition set this to `false` and
accept the per-request latency cost. See §"Rollout / Adoption Strategy"
for the default flip and removal timeline.

### Conceptual entity mapping

| OpenLineage concept | DataHub entity | URN shape |
|---|---|---|
| `Job` (`namespace`, `name`) | `DataJob` | `urn:li:dataJob:(<DataFlow URN>, <task id>)` |
| Containing flow (orchestrator + namespace) | `DataFlow` | `urn:li:dataFlow:(<orchestrator>, <flow id>, <namespace/instance>)` |
| `Run` (`runId`) | `DataProcessInstance` | `urn:li:dataProcessInstance:<runId>` |
| `Dataset` (`namespace`, `name`) | `Dataset` | `urn:li:dataset:(urn:li:dataPlatform:<derived>, <dataset name>, PROD)` |
| Run state transition (`eventType`) | `dataProcessInstanceRunEvent` time-series MCP | — |
| `inputs[]` / `outputs[]` | `dataJobInputOutput` on DataJob + `upstreamLineage` on each output Dataset | — |

Shape decisions:

- **Job → DataJob / DataFlow split.** The existing convention is preserved:
  split `Job.name` on the first `.` (prefix = flow id, suffix = task id). If
  no `.` is present, `flowId == jobName` and the flow contains a single task
  that shares the flow's id. Per-producer override is configurable.
- **Orchestrator name.** Resolution order:
  `ProcessingEngineRunFacet.name` (lower-cased) →
  `JobTypeJobFacet.integration` (lower-cased) →
  `DatahubOpenlineageConfig.orchestrator` →
  a best-effort parse of the producer URI →
  the configured default (`openlineage`). Total: always returns a value,
  never throws.
- **DataPlatform URN for the flow.** Derived from the orchestrator name above.
  A validation step verifies the resulting `urn:li:dataPlatform:<name>`
  resolves to a registered platform entity; if it does not, the configured
  default is used instead. This prevents ghost platform references of the
  kind surfaced by the current endpoint.
- **DataProcessInstance URN.** Uses `Run.runId` directly.
- **Dataset URN.** Reuses `convertOpenlineageDatasetToDatasetUrn`. The
  `DatasourceDatasetFacet.uri`, when present, is written to
  `datasetProperties.externalUrl`.
- **`eventType` handling.** Per spec, `eventType` is optional on `RunEvent`.
  A missing `eventType` is treated as `OTHER` (supplementary-metadata event)
  and produces no `dataProcessInstanceRunEvent` MCP. Each enum value maps to
  exactly one status / result-type pair; the `OTHER` branch emits no run-event
  MCP at all rather than writing the Pegasus sentinel value that currently
  fails aspect validation.

### Facet routing

The full facet-to-aspect table is in
[appendix §A.3](./17034-openlineage-spec-compliance-appendix.md#a3-status-quo-and-gaps),
the OpenLineage ↔ DataHub ↔ Marquez cross-walk is in
[appendix §A.2](./17034-openlineage-spec-compliance-appendix.md#a2-openlineage--datahub--marquez-mapping),
and the per-section / per-status milestone roll-up is in
[appendix §A.5](./17034-openlineage-spec-compliance-appendix.md#a5-milestone-roll-up).

**Milestone A — P0 baseline.** Every item from appendix §A.5's Milestone A
roll-up: 32 P0 items (envelope concerns plus 16 P0 standard facets).
Envelope: event-type dispatch, free-form producer, structured error
body, request atomicity via the MCP Kafka topic, registered-platform
validation, URN split, `eventType` enum handling, authentication
null-safety. P0 facets: `NominalTimeRunFacet`, `ParentRunFacet`,
`ErrorMessageRunFacet`, `ProcessingEngineRunFacet`,
`DocumentationJobFacet`, `SourceCodeLocationJobFacet`, `SQLJobFacet`,
`OwnershipJobFacet`, `TagsJobFacet`, `JobTypeJobFacet`,
`SchemaDatasetFacet`, `DatasourceDatasetFacet`,
`ColumnLineageDatasetFacet`, `DocumentationDatasetFacet`,
`OutputStatisticsOutputDatasetFacet`, and
**`DataQualityAssertionsDatasetFacet`** — the only P0 item that
materializes a new DataHub entity (the native `assertion` entity, paired
with `assertionRunEvent` per assertion) rather than writing an aspect to
an existing entity, so OL-emitted quality checks land alongside Great
Expectations and dbt tests in the same UI surface. Closes #16961,
#15196, #13011, #14458.

**Milestone B — P1 standard producer coverage.** `ExternalQueryRunFacet`,
`SourceCodeJobFacet`, `OwnershipDatasetFacet`,
`LifecycleStateChangeDatasetFacet`, `SymlinksDatasetFacet`,
`DatasetVersionDatasetFacet`, `DatasetTypeDatasetFacet`,
`CatalogDatasetFacet`, `TagsDatasetFacet`,
`DataQualityMetricsInputDatasetFacet`, `InputStatisticsInputDatasetFacet`,
plus `JobEvent` / `DatasetEvent` polish (multi-event idempotency,
cross-producer dataset scoping).

**Milestone C — P2 quality and edge cases.** Environment variables,
hierarchy, run-level tags, extraction-error reporting.

### Configuration surface

`DatahubOpenlineageConfig` gains the following options. Each default preserves
current behavior where feasible so operators see no change without an opt-in.

- `orchestratorDefault` (string, default `openlineage`) — value used when no
  facet- or producer-derived orchestrator is found.
- `documentationTarget` (enum: `dataJob`, `dataFlow`, `both`, default `both`
  during the parallel-write window (see §"Rollout / Adoption Strategy"),
  then `dataJob`) — controls where `DocumentationJobFacet` is written.
- `ownershipTarget` (enum: same shape as above, same default trajectory) —
  controls where `OwnershipJobFacet` is written.
- `datasetEventNamespaceByProducer` (boolean, default `false`) — when true,
  dataset URNs ingested via `DatasetEvent` carry a producer-scoped platform
  instance, preventing cross-producer overwrite.
- `requireRegisteredPlatform` (boolean, default `true`) — when true, an
  orchestrator name that does not resolve to a registered `dataPlatform` entity
  is rejected in favor of the configured default, preventing ghost-platform
  URNs.
- `useStreamingIngest` (boolean, default `true`) — when true, MCPs produced
  from an OpenLineage event are published to the MCP Kafka topic via
  `EventProducer.produceMetadataChangeProposal`; when false, the endpoint
  falls back to per-MCP synchronous `EntityServiceImpl.ingestProposal` calls.
  Lifecycle in §"Rollout / Adoption Strategy".

`documentationTarget`, `ownershipTarget`, and `useStreamingIngest` all
ship with a transitional default that's removed in a later minor release.
Their consolidated lifecycle is in §"Rollout / Adoption Strategy".

### Error responses

The endpoint returns a structured JSON body matching the shape used elsewhere
in the openapi-servlet:

```json
{ "code": "INVALID_EVENT",
  "message": "Unknown eventType value: DONE",
  "details": { "field": "eventType" } }
```

HTTP status mapping:

- `202` — event accepted and MCPs published to the MCP Kafka topic.
  Downstream aspect-store ingestion is eventually consistent (see
  §"Streaming ingest").
- `400` — Jackson-native deserialization failure. The OL Java client beans
  are typed (`ZonedDateTime`, `URI`, typed Java enums for `eventType`),
  so Jackson catches type mismatches (`run` as a string instead of an
  object), unknown `eventType` values (the typed `OpenLineage.RunEvent.EventType`
  Java enum rejects strings outside its set), malformed `ZonedDateTime`,
  unparseable JSON, and `Content-Type` mismatches. JSON Schema validation
  of the full spec (missing required envelope fields, `format: uri`,
  facet-internal constraints) is out of scope for this RFC and tracked in
  Future Work.
- `401` — missing or invalid authentication.
- `500` — unexpected runtime failure. The `details` object contains the
  exception class and message.

All MCPs produced from a single event are published as a single batch
through `EventProducer.produceMetadataChangeProposal`. If any publish fails
mid-batch, the request returns 5xx and the client is expected to retry —
see §"Streaming ingest" for the atomicity story.

### OpenAPI contract

`metadata-service/openapi-servlet/src/main/resources/openlineage/openlineage.json`
is regenerated from upstream OpenLineage 2-0-2. The request body schema
references a `oneOf` over `RunEvent`, `JobEvent`, and `DatasetEvent`. Error
response schemas are documented for `400`, `401`, `500`. Generated clients
stop sending JSON-encoded strings.

### Test strategy

Two layers of tests, following the openapi-servlet house style.

1. **Controller + MCP mapping — Spring `MockMvc` test** at
   `metadata-service/openapi-servlet/src/test/java/io/datahubproject/openapi/openlineage/LineageApiImplTest.java`.
   Pattern matches `EntityControllerTest` and the other `*ControllerTest`
   classes in the module:
   `@SpringBootTest(classes = SpringWebConfig.class)` +
   `@Import({LineageApiImpl.class, TestConfig.class, GlobalControllerExceptionHandler.class})` +
   `@AutoConfigureMockMvc` + `AbstractTestNGSpringContextTests`. Drives
   `POST /openapi/openlineage/api/v1/lineage` with fixture bodies
   vendored from two upstream Apache-2.0 corpora (attribution in
   `NOTICE`): Marquez's `api/src/test/resources/open_lineage/` for
   envelope and facet shapes, and
   `github.com/OpenLineage/compatibility-tests`
   `consumer/scenarios/*/events/*.json` for cross-consumer canonical
   scenarios (`simple_run_event`, `CLL`, `airflow`, `spark_dataproc_*`).
   Fixtures live in `src/test/resources/openlineage/fixtures/` grouped
   by concern (envelope, event types, run facets, job facets, dataset
   facets, input/output facets, edge cases), with per-fixture metadata
   (expected HTTP status, expected MCP tuples) encoded either inline or
   in a sibling YAML. A TestNG `@DataProvider` walks the directory and
   parameterizes the test method. A mocked `EventProducer` captures every
   `produceMetadataChangeProposal(urn, mcp)` call via
   `ArgumentCaptor<MetadataChangeProposal>`; assertions run against the
   captured MCP set per fixture. Runs in
   `./gradlew :metadata-service:openapi-servlet:test`.
2. **Aspect-store verification** as a pre-merge integration step, querying
   `GET /openapi/v3/entity/<type>/<urn>` against a running instance to
   confirm that per-fixture expected aspects are persisted end-to-end
   after the MCP Kafka path round-trips through the consumer.

## How we teach this

The DataHub OpenLineage endpoint is an existing feature; this RFC sharpens its
contract rather than introducing new terminology. Documentation changes:

- `docs/lineage/openlineage.md` gains a "Spec compliance" section listing
  supported event types, the orchestrator-resolution order, and the
  configuration knobs above.
- The producer-compatibility subsection links to OpenLineage's producer
  registry rather than maintaining a DataHub-side allow-list.
- `docs/how/updating-datahub.md` documents the documentation-target and
  ownership-target config changes, the parallel-write window, and the
  migration procedure for sites that depend on the current DataFlow-side
  writes.
- The regenerated OpenAPI contract is the source of truth for clients;
  consumers generating from the spec observe the new request-body shape
  automatically.

The audience is primarily backend developers and operators integrating
third-party OpenLineage producers. Frontend changes are not required.

## Drawbacks

- **Read-after-write semantics change (200 → 202).** The endpoint
  currently returns `200 OK` only after every MCP has been written
  synchronously to the aspect store, which means a client that posts an
  event and immediately queries `GET /openapi/v3/entity/<type>/<urn>`
  for the resulting aspect can rely on the aspect being present.
  Post-RFC the endpoint returns `202 Accepted` after publishing the MCP
  batch to the Kafka topic; downstream consumer ingestion is eventually
  consistent and the same client may observe a delay before the aspect
  is queryable. Pipelines that depend on read-after-write consistency
  (e.g. post-an-event-then-verify integration tests) need to either
  poll or set `useStreamingIngest = false` to keep the synchronous
  path during the transition.
- **Behavioral change for existing users.** Sites that depend on
  `DocumentationJobFacet` or `OwnershipJobFacet` landing on `DataFlow` see the
  aspects move to `DataJob` after the parallel-write window closes. The
  `documentationTarget` and `ownershipTarget` config knobs allow an extended
  transition.
- **Larger facet surface to maintain.** Three event types plus the P0 facet
  set is more surface than today's converter. Every facet has a fixture in
  the `LineageApiImplTest` corpus and every fixture asserts a specific MCP
  shape, replacing the implicit safety net the removed allow-list
  previously provided.
- **Loss of an implicit safety net.** The current allow-list rejects events
  from unknown producers, which accidentally prevents some misconfigured
  pipelines from writing to DataHub. Post-RFC any producer is accepted.
  Operators relying on the rejection as a safety net enforce it at the
  auth/network layer instead.
- **Divergence from Marquez's storage model.** Marquez persists raw event JSON
  for later replay; DataHub does not. Facets that DataHub does not map are
  dropped. The unmapped-facet debug log makes the gap observable.
- **Atomicity is best-effort, not transactional.** The MCP Kafka publish
  path returns 5xx on any per-message failure but does not roll back
  MCPs already published earlier in the batch. Strict
  exactly-once-batch semantics would require Kafka transactional
  producers and are tracked as a follow-up in §"Streaming ingest".

## Alternatives

- **Request-layer JSON Schema validation.** Deferred. The scope of this
  RFC is dispatch, routing, and mapping. A validation layer is a natural
  follow-up once the mapping surface is stable and is tracked under
  "JSON Schema validation for request payloads" in Future Work.
- **Replace the converter with a pass-through to a new `rawLineageEvent`
  entity.** Storing the raw JSON event the way Marquez does gives
  forward-compatibility with any facet shape but requires a new entity type,
  time-series indexing, and a UI to browse it. Rejected: out of scope.
- **DataHub-side producer registry.** One Java class per supported producer
  (Airflow, Trino, dbt, Spark, …) with its own field-extraction logic is
  the direction the converter currently implements. It does not scale and
  conflicts with the spec's producer-agnostic intent. Rejected.
- **Vendor-fork the OpenLineage Java client.** Tempting for fields like
  `nominalEndTime` that need typed access. Reading through
  `additionalProperties` is the lower-maintenance path. Rejected.

**Prior art.** Marquez is the de-facto receiver implementation for OpenLineage
in the wider ecosystem. It ingests events by persisting raw JSONB facet blobs
in `run_facets` / `job_facets` / `dataset_facets` and selectively promotes a
few well-known fields into typed columns (`jobs.description`, `jobs.location`,
`runs.parent_run_uuid`, `dataset_fields`, `column_lineage`). This RFC follows
the same shape on the DataHub side: typed mapping for standard facets,
graceful tolerance for everything else, observable debug logging for the
unmapped ones. The Marquez JSON test fixtures are reused as DataHub's spec
corpus (appendix §A.2 cross-walks each fixture to the DataHub aspect it
exercises).

## Rollout / Adoption Strategy

- **Default on.** The behavior changes (event-type dispatch, free-form
  producer, facet rerouting, structured error responses, MCP Kafka
  publish path) ship enabled by default in the release that lands this
  RFC.
- **Transitional config knobs and removal timeline.** Three knobs ship
  with a transitional default and are removed in the release after they
  flip:

  | Knob | First-release default | Second-release default | Removal release |
  |---|---|---|---|
  | `documentationTarget` | `both` (DataJob + DataFlow) | `dataJob` | one release after the flip |
  | `ownershipTarget` | `both` (DataJob + DataFlow) | `dataJob` | one release after the flip |
  | `useStreamingIngest` | `true` (Kafka publish path), with the legacy synchronous `EntityServiceImpl.ingestProposal` path retained behind `false` for sites that depend on read-after-write semantics | `true`, no change | the release that removes the synchronous fallback (one release after the first) |

  The `documentationTarget` / `ownershipTarget` parallel-write window
  exists so operators on `DataFlow`-side reads have one minor release
  to migrate. The `useStreamingIngest` legacy path exists so
  read-after-write integrations have one minor release to switch to
  polling or to the streaming-aware contract. All three knobs are gone
  one minor release after the flip.
- **Migration documentation.** A section in `docs/how/updating-datahub.md`
  lists affected aspects, the config knobs, the response-status change
  (200 → 202), the read-after-write semantic change, and the rollback
  procedure. The note cross-links the tracked issues so operators
  searching for a known symptom land on the migration page.
- **Backfill.** Not required. Existing `DataFlow.dataFlowInfo.description`
  and `DataFlow.ownership` records remain valid. New events populate the
  DataJob location; the DataFlow location continues to receive writes
  during the parallel-write window.

## Future Work

- **JSON Schema validation for request payloads.** Enforce the full
  OpenLineage 2-0-2 envelope schema at the request layer via an OpenAPI
  request validator (`com.atlassian.oai:swagger-request-validator-spring-webmvc`
  against the regenerated `openlineage.json` contract) or a raw JSON
  Schema validator (`networknt/json-schema-validator` against
  `spec/OpenLineage.json`), wired as a Spring `RequestBodyAdvice` or
  `HandlerInterceptor` with `additionalProperties: true` preserved for
  forward-compat. Closes the gaps typed Jackson leaves open: missing
  required envelope fields, `format: uri` on `producer` / `schemaURL`,
  facet-internal `required` fields, `_producer` / `_schemaURL` presence
  on facets, and `format: date-time` edge cases beyond what
  `ZonedDateTime` catches.
- **OpenLineage-driven DataProduct entity.** `JobEvent` ingestion provides
  enough information to materialize the OL "data product" concept once that
  part of the spec stabilizes.
- **Marquez-as-oracle integration test.** Run each Marquez fixture through
  both Marquez and DataHub and compare the semantic output (entity count,
  lineage edges, schema fields). Gives a black-box conformance gate anchored
  to the reference implementation.
- **Contribute a `datahub` entry to `OpenLineage/compatibility-tests`.** The
  upstream cross-consumer compatibility repo at
  `github.com/OpenLineage/compatibility-tests` hosts canonical scenarios
  under `consumer/scenarios/` and per-consumer mappings under
  `consumer/consumers/<name>/`. Only `dataplex` is currently listed. The
  conceptual facet-to-entity mapping in appendix §A.2 is re-expressed as
  `consumer/consumers/datahub/mapping.json` in the repo's standardized
  format (`mapped.core`, `mapped.<FacetName>`, `knownUnmapped`) and
  paired with a `validator/` that calls
  `GET /openapi/v3/entity/<type>/<urn>` to verify each scenario's expected
  DataHub aspects. Gives DataHub an official seat in the OL conformance
  matrix and a vendor-neutral regression target.
- **Upstream receiver-side conformance proposal.** The OpenLineage project
  versioning document (`spec/Versioning.md`) is silent on receiver
  expectations. A short receiver-side guideline codifying the behavior
  shipped here (`additionalProperties: true`, naive-tz coercion,
  shape-based dispatch) gives downstream implementations a neutral
  target. Natural companion to the `compatibility-tests` contribution.
- **OpenLineage 2-0-3 / future versions.** Additive spec changes are absorbed
  by regenerating `openlineage.json` and adding mapping methods; no
  architectural change required.

## Unresolved questions

1. **Default DataPlatform when the orchestrator cannot be derived.** The
   proposed default is the literal `openlineage`. An alternative derives it
   from `Job.namespace` by parsing a URI scheme. Decision pending observation
   of real producer populations.
2. **DataFlow scope when `Job.name` contains no `.`.** The proposal is a
   single-task flow named after the job. An alternative uses `Job.namespace`
   as the flow id and `Job.name` as the task id. Choice affects URN stability
   for producers that do not follow the `flow.task` convention.
3. **Length of the parallel-write window for `documentationTarget` /
   `ownershipTarget`.** One minor release may be too short for sites on
   quarterly upgrade cadences. Two minor releases delays cleanup.
4. **Overlap between the existing Airflow-specific and Spark-specific
   custom-facet handlers and the standard facets.** `processAirflowProperties`
   reads `run.facets.airflow.dag.tags` for DataHub-side tag ingestion, which
   overlaps functionally with standard `TagsJobFacet` / `TagsRunFacet`. The
   proposal leaves the custom handlers in place; a follow-up RFC can audit
   them for duplication.
5. **Cross-producer dataset overwrite behavior for `DatasetEvent`.** The
   `datasetEventNamespaceByProducer` knob ships off by default. If real-world
   cross-producer collisions are observed, a later RFC flips the default.

## Appendix

Per-facet mapping tables, test suite overview, methodology, the
OpenLineage / DataHub / Marquez cross-walk, and the linked issues/PRs are in
[`17034-openlineage-spec-compliance-appendix.md`](./17034-openlineage-spec-compliance-appendix.md).
