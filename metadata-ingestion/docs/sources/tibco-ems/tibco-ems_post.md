### Capabilities

#### Containers

One EMS REST Proxy can administer several fault-tolerant server groups at once, and each group is an independent destination namespace. Every server group is emitted as a container holding its own destinations, which is also what gives destinations their browse path.

#### Naming

Two things share a namespace in EMS and so both lead the dataset name, giving `<server group>.<queue|topic>.<name>`:

- **Server groups** are independent of one another, so `orders.new` in `group1` and `orders.new` in `group2` are two unrelated queues on two different servers.
- **Queue and topic namespaces** are independent within a server, so a queue and a topic can share the same name.

The display name remains the bare destination name. On a proxy that predates server groups, the single implicit group is named `default`.

#### Lineage

Lineage edges are derived from EMS bridges: each bridge target's upstream is the bridge source. Because a bridge endpoint lives on the same EMS server, its dataset URN is deterministic from the source's `platform_instance` and `env`, so an edge is produced even when the endpoint was excluded from dataset ingestion by an allow/deny pattern (the referenced dataset may have been ingested by an earlier, unfiltered run). Only wildcard subscription endpoints (`*` / `>`), which do not correspond to a single destination, cannot be mapped to a concrete dataset and are reported as unresolved rather than guessed.

##### Column-level lineage (opt-in)

EMS itself stores no message schema, but a bridge copies whole messages unchanged, so any field common to both endpoints is the same field. When `emit_column_lineage` is enabled, the connector reads the source and target destination schemas from DataHub (populated for those datasets by a schema-registry or other connector) and emits field-level lineage for the fields they share. Matching is **case-insensitive** — the same field is frequently cased differently across platforms (e.g. `OrderId` vs `orderid`) — while the emitted `schemaField` URNs preserve each side's real field path. This is best-effort: destinations without a schema in DataHub simply produce no column lineage, and the coarse table-level edge still stands.

#### Derived message schemas (opt-in)

EMS has no message schema registry (see Limitations), and unlike a Kafka log a JMS destination cannot be sampled without consuming the customer's production messages. Where the publisher's declared schema is not available, the only surviving evidence of a message's shape is what came out of it — so with `derive_schemas_from_lineage` enabled, the connector estimates each destination's fields from the schemas of its downstream consumers, read from DataHub.

The rule is the **union** of consumer fields, not the intersection: consumers keep different subsets of a message, and any field one of them landed must have been on the wire. Columns the consuming pipeline writes *after* the message lands (`ingested_at`, `_source_topic`, `etl_*` and similar) are excluded via `generated_field_pattern` — keeping one would assert that the bus published a value the pipeline invented. Field names are matched case-insensitively, types are taken from the consuming schema, and a field two consumers landed with different types keeps the first and is reported.

Two properties of the result are worth being explicit about:

- **It is downstream-shaped.** It reflects what consumers kept, not what was published. Every derived schema is marked `schema_source: derived-from-lineage` in the dataset's custom properties so it is visible as an estimate rather than read as a contract.
- **It never replaces a declared schema.** A destination already carrying a schema is left untouched unless that schema was itself derived, in which case it is refreshed. A schema with no recorded provenance is treated as declared — overwriting someone else's work silently is the worse failure.

A destination with nothing downstream of it in DataHub yet produces no schema and is listed in the report; ingest the consuming platform first and re-run. This needs a DataHub graph (a `datahub-rest` sink or a `datahub_api` block).

### Limitations

- **No destination schemas of its own.** EMS is a Jakarta Messaging (JMS) provider and has no message schema registry: the `JMSType` header exists to reference a provider repository but EMS does not implement one, and the only schema-aware path in the product is the `schema_repository` setting on a Kafka *transport*, which is used to deserialize imported Kafka payloads at runtime rather than to describe a destination. Sampling is not an alternative — reading a JMS queue destroys the message, a topic only yields messages published while subscribed, and the `QueueBrowser` that would browse non-destructively is not exposed in the Python binding. Destinations are therefore emitted without field-level schemas unless `derive_schemas_from_lineage` is enabled or another connector contributes one.
- Point-to-point routing that is not modelled as an EMS bridge is not captured.

### Troubleshooting

#### No datasets are produced

Confirm the configured user can reach the EMS admin/monitoring API and that the queue/topic allow/deny patterns are not filtering everything out. Wildcard subscription endpoints (`*` / `>`) are intentionally skipped.

#### `derive_schemas_from_lineage` produces no schemas

Derivation reads a destination's consumers out of DataHub, so it needs both a graph connection and the consuming platform already ingested with its lineage back to the destination. Check the report for destinations listed under `destinations_without_consumers`, and confirm the consuming datasets really do carry the destination as an upstream — a pipeline whose lineage bypasses the bus leaves nothing to derive from.

#### The run fails with "EMS REST Proxy returned a partial result"

A list call that reaches some but not all server groups still returns HTTP 200 and names the unreachable ones in the response's `errors` array. The connector treats this as a run failure so that stateful ingestion does not commit its checkpoint — otherwise the missing group's destinations would look deleted and would be soft-deleted from DataHub. Restore connectivity to the reported server group, or scope the recipe to the groups you can reach, and re-run.
