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

### Limitations

- **No destination schemas.** EMS is a Jakarta Messaging (JMS) provider and has no message schema registry: the `JMSType` header exists to reference a provider repository but EMS does not implement one, and the only schema-aware path in the product is the `schema_repository` setting on a Kafka *transport*, which is used to deserialize imported Kafka payloads at runtime rather than to describe a destination. Destinations are therefore emitted without field-level schemas, and column-level lineage relies on schemas contributed by other connectors, matched by name.
- Point-to-point routing that is not modelled as an EMS bridge is not captured.

### Troubleshooting

#### No datasets are produced

Confirm the configured user can reach the EMS admin/monitoring API and that the queue/topic allow/deny patterns are not filtering everything out. Wildcard subscription endpoints (`*` / `>`) are intentionally skipped.

#### The run fails with "EMS REST Proxy returned a partial result"

A list call that reaches some but not all server groups still returns HTTP 200 and names the unreachable ones in the response's `errors` array. The connector treats this as a run failure so that stateful ingestion does not commit its checkpoint — otherwise the missing group's destinations would look deleted and would be soft-deleted from DataHub. Restore connectivity to the reported server group, or scope the recipe to the groups you can reach, and re-run.
