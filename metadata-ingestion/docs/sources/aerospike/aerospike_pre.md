### Overview

The `aerospike` module ingests metadata from Aerospike clusters into DataHub. It connects to an Aerospike node, discovers namespaces and sets, and infers schema by sampling records.

### Prerequisites

Before running ingestion, ensure you have:

1. **Network connectivity** to at least one Aerospike node on its service port (default 3000).
2. **Authentication credentials** (username/password) if the cluster has security enabled.
3. **Read permissions** on the namespaces and sets you want to ingest.

#### Authentication Modes

Aerospike supports three authentication modes:

- `AUTH_INTERNAL` (default) — standard username/password authentication.
- `AUTH_EXTERNAL` — external authentication (e.g., LDAP) over TLS.
- `AUTH_EXTERNAL_INSECURE` — external authentication without TLS.

#### Schema Inference

Schema is inferred by sampling records from each set. You can control:

- `infer_schema_depth` — how many nesting levels to traverse (default `1`, use `-1` for all levels, `0` to skip).
- `schema_sampling_size` — number of records to sample per set (default `1000`, use `null` for full scan).
- `max_schema_size` — cap on schema fields emitted (default `300`).
