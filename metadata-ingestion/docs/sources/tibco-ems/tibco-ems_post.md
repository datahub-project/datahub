### Capabilities

#### Naming

EMS queue and topic namespaces are independent — a queue and a topic can share the same name. To keep their dataset URNs distinct, the dataset name is prefixed with the destination type (`queue.<name>` or `topic.<name>`), while the display name remains the bare destination name.

#### Lineage

Lineage edges are derived from EMS bridges: each bridge target's upstream is the bridge source. Because a bridge endpoint lives on the same EMS server, its dataset URN is deterministic from the source's `platform_instance` and `env`, so an edge is produced even when the endpoint was excluded from dataset ingestion by an allow/deny pattern (the referenced dataset may have been ingested by an earlier, unfiltered run). Only wildcard subscription endpoints (`*` / `>`), which do not correspond to a single destination, cannot be mapped to a concrete dataset and are reported as unresolved rather than guessed.

##### Column-level lineage (opt-in)

EMS itself stores no message schema, but a bridge copies whole messages unchanged, so any field common to both endpoints is the same field. When `emit_column_lineage` is enabled, the connector reads the source and target destination schemas from DataHub (populated for those datasets by a schema-registry or other connector) and emits field-level lineage for the fields they share. Matching is **case-insensitive** — the same field is frequently cased differently across platforms (e.g. `OrderId` vs `orderid`) — while the emitted `schemaField` URNs preserve each side's real field path. This is best-effort: destinations without a schema in DataHub simply produce no column lineage, and the coarse table-level edge still stands.

### Limitations

- Message payloads carry no registered schema in EMS, so datasets are emitted without field-level schemas; column-level lineage relies on schemas contributed by other connectors and matches fields by name only.
- Point-to-point routing that is not modelled as an EMS bridge is not captured.

### Troubleshooting

#### No datasets are produced

Confirm the configured user can reach the EMS admin/monitoring API and that the queue/topic allow/deny patterns are not filtering everything out. Wildcard subscription endpoints (`*` / `>`) are intentionally skipped.
