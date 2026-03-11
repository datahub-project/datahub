## Overview

Kafka Connect is a streaming or integration platform. Learn more in the [official Kafka Connect documentation](https://kafka.apache.org/documentation/#connect).

The DataHub integration for Kafka Connect covers streaming/integration entities such as topics, connectors, pipelines, or jobs. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Source Concept                                                                  | DataHub Concept                                                                           | Notes |
| ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ----- |
| `"kafka-connect"`                                                               | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |       |
| [Connector](https://kafka.apache.org/documentation/#connect_connectorsandtasks) | [DataFlow](https://docs.datahub.com/docs/generated/metamodel/entities/dataflow/)          |       |
| Kafka Topic                                                                     | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)            |       |

## Column-Level Lineage

Column-level lineage maps individual fields/columns through the Kafka Connect pipeline. It is supported for both directions:

- **Source connectors** (DB table → Kafka topic): field names are taken from the source table schema in DataHub
- **Sink connectors** (Kafka topic → DB table): field names are taken from the Kafka topic schema in DataHub

**Prerequisites:**

Both the source and target schemas must already be ingested into DataHub before running Kafka Connect ingestion:

- For source connectors: ingest the source database (e.g. run the Postgres source)
- For sink connectors: ingest both the Kafka topics (e.g. run the Kafka source) and the destination database

**Configuration:**

```yaml
source:
  type: kafka-connect
  config:
    use_schema_resolver: true # required — enables DataHub graph queries
    schema_resolver_finegrained_lineage: true # required — enables column-level mapping
```

`use_schema_resolver` is automatically enabled for Confluent Cloud environments. `schema_resolver_finegrained_lineage` defaults to `true` when `use_schema_resolver` is enabled.

Column matching is case-insensitive, so Kafka fields with lowercase names (e.g. `order_id`) will match Snowflake columns stored in uppercase (`ORDER_ID`).

Topic routing transforms (RegexRouter, EventRouter, etc.) work transparently — the complex transform pipeline correctly identifies which topic maps to which dataset, and column-level lineage operates on those resolved pairs.

The following field-level transforms are applied when building the column mapping:

| Transform                             | Effect on column lineage                                                                              |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `ReplaceField$Value`                  | include/exclude filter and field renames are respected                                                |
| `ExtractField$Value`                  | sub-fields of the extracted struct are promoted as top-level (e.g. Debezium `field=after` unwrapping) |
| `HoistField$Value`                    | all fields are nested under the new struct field (e.g. `id` → `data.id`)                              |
| `Flatten$Value`                       | nested struct paths are joined using the configured delimiter (default: `.`)                          |
| `MaskField`, `Cast`, `Filter`, `Drop` | field names unchanged — lineage is unaffected                                                         |
