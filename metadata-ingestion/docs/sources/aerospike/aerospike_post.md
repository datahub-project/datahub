### Capabilities

- **Schema inference** — bin names and types are inferred by sampling records. Nested maps and lists are traversed up to the configured depth.
- **Namespace containers** — namespaces are represented as DataHub containers, with sets organized under them.
- **XDR metadata** — when `include_xdr` is enabled, sets are annotated with the data centers they replicate to.
- **Stateful ingestion** — enables automatic removal of datasets from DataHub when the corresponding sets are dropped from Aerospike.

### Limitations

- Schema inference relies on sampling and may not capture all bin names if the data is sparse or heterogeneous.
- Aerospike does not store schema definitions; all type information is derived from record values at ingestion time.
- The primary key (`PK`) field is always reported as a string since Aerospike returns digest-based keys.

### Troubleshooting

#### Connection Refused

Ensure the Aerospike node is reachable on the configured host and port. Verify firewall rules and that the service port is correct.

#### Empty Schema

If sets appear with no schema fields, verify that `infer_schema_depth` is not set to `0` and that `schema_sampling_size` is large enough to capture representative records.
