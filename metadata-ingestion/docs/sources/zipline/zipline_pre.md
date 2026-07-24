### Overview

The `zipline` module ingests metadata from a compiled Chronon/Zipline repository into DataHub. It reads the thrift-as-JSON output produced by Chronon's `compile.py` directly from disk, so it does not require a running Zipline service or the `chronon-ai` / `zipline-ai` packages at ingestion time.

### Prerequisites

Before running ingestion, compile your Chronon repository so the `production/` output directory exists. The connector expects a directory containing `group_bys/`, `joins/`, and `staging_queries/` sub-directories (you may point `path` either at that directory or at a repo root that contains a `production/` folder).

#### Resolving backing source platforms

Chronon `Source` structs reference warehouse tables and streaming topics by name only — the DataHub platform cannot be inferred from the compiled config. Map the first segment of each table name (its namespace/database) to a DataHub platform via `source_platform_map`, and set `default_source_platform` for any namespaces you do not list. Streaming topics resolve to `stream_platform` (Kafka by default). Namespaces that fall back to the default are surfaced in the ingestion report so you can extend the map.
