### Naming

EMS queue and topic namespaces are independent — a queue and a topic can share the same name. To keep their dataset URNs distinct, the dataset name is prefixed with the destination type (`queue.<name>` or `topic.<name>`), while the display name remains the bare destination name.

### Lineage

Lineage edges are derived from EMS bridges: each bridge target's upstream is the bridge source. Because a bridge endpoint lives on the same EMS server, its dataset URN is deterministic from the source's `platform_instance` and `env`, so an edge is produced even when the endpoint was excluded from dataset ingestion by an allow/deny pattern (the referenced dataset may have been ingested by an earlier, unfiltered run). Only wildcard subscription endpoints (`*` / `>`), which do not correspond to a single destination, cannot be mapped to a concrete dataset and are reported as unresolved rather than guessed.

### Limitations

- Message payloads carry no registered schema in EMS, so datasets are emitted without field-level schemas.
- Point-to-point routing that is not modelled as an EMS bridge is not captured.
