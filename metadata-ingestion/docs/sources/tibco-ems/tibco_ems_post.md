### Naming

EMS queue and topic namespaces are independent — a queue and a topic can share the same name. To keep their dataset URNs distinct, the dataset name is prefixed with the destination type (`queue.<name>` or `topic.<name>`), while the display name remains the bare destination name.

### Lineage

Lineage edges are derived from EMS bridges: each bridge target's upstream is the bridge source. Only bridges whose source and target both resolve to ingested destinations produce an edge. Bridge endpoints that are wildcards, or destinations excluded by the allow/deny patterns, cannot be mapped to a concrete dataset and are reported as unresolved rather than guessed.

### Limitations

- Message payloads carry no registered schema in EMS, so datasets are emitted without field-level schemas.
- Point-to-point routing that is not modelled as an EMS bridge is not captured.
