### Capabilities

#### Lineage Extraction

The connector extracts table-level lineage by analyzing Flink execution plans. It handles two distinct cases:

**DataStream API (Kafka only):** The connector recognizes `KafkaSource-{topic}` and `KafkaSink-{topic}` patterns in operator descriptions. Platform is always `kafka`, and the topic name is extracted directly from the description. No SQL Gateway needed.

**SQL/Table API (all connectors):** The connector parses `TableSourceScan(table=[[catalog, db, table]])` and `Sink(table=[[catalog, db, table]])` patterns. These are generic Flink plan formats — the same for Kafka, JDBC, Iceberg, Paimon, and every other connector. The connector resolves the actual platform via SQL Gateway catalog introspection:

1. **`catalog_platform_map` config** — user-provided overrides; take priority over all auto-detection
2. **DESCRIBE CATALOG** (Flink 1.20+) — determines catalog type (jdbc, iceberg, paimon, hive, etc.)
3. **SHOW CREATE TABLE** — reads the `connector` property from the table DDL (for hive/generic_in_memory catalogs with mixed connector types)

#### Platform Resolution Examples

A Flink job reads from a Postgres JDBC catalog table `pg_catalog.mydb.public.users`:

```
Plan: TableSourceScan(table=[[pg_catalog, mydb, public.users]])
  → SQL Gateway: DESCRIBE CATALOG → type=jdbc, base-url=jdbc:postgresql://  (Flink 1.20+)
  → URN: urn:li:dataset:(urn:li:dataPlatform:postgres, mydb.public.users, PROD)
```

A Flink job reads from an Iceberg catalog table `ice_catalog.lake.events`:

```
Plan: TableSourceScan(table=[[ice_catalog, lake, events]])
  → SQL Gateway: DESCRIBE CATALOG → type=iceberg  (Flink 1.20+)
  → URN: urn:li:dataset:(urn:li:dataPlatform:iceberg, lake.events, PROD)
```

#### Platform Instance Mapping

If your datasets belong to specific platform instances (e.g., a particular Kafka cluster or Postgres deployment), use `catalog_platform_map` for per-catalog mapping or `platform_instance_map` as a platform-wide fallback:

```yml
source:
  type: "flink"
  config:
    connection:
      rest_api_url: "http://localhost:8081"
      sql_gateway_url: "http://localhost:8083"
    # Per-catalog: takes priority
    catalog_platform_map:
      pg_us:
        platform_instance: "us-postgres"
      pg_eu:
        platform_instance: "eu-postgres"
    # Platform-wide fallback
    platform_instance_map:
      kafka: "prod-kafka-cluster"
```

#### Iceberg/Paimon on Flink < 1.20

On Flink versions before 1.20, `DESCRIBE CATALOG` is not available. The connector falls back to `SHOW CREATE TABLE`, but Iceberg and Paimon tables do not have a `connector` property in their DDL. In this case, provide the platform explicitly via `catalog_platform_map`:

```yml
source:
  type: "flink"
  config:
    connection:
      rest_api_url: "http://localhost:8081"
      sql_gateway_url: "http://localhost:8083"
    catalog_platform_map:
      ice_catalog:
        platform: "iceberg"
      paimon_catalog:
        platform: "paimon"
```

On Flink 1.20+, this config is not needed — the platform is auto-detected from the catalog type.

#### Operator Granularity

By default (`operator_granularity: job`), the connector emits **one DataJob per Flink job** with all source and sink lineage coalesced into that single DataJob.

Set `operator_granularity: vertex` to emit **one DataJob per operator/vertex** in the execution plan. This gives finer-grained lineage at the cost of more entities.

#### Run History

When `include_run_history` is enabled (the default), the connector emits `DataProcessInstance` entities that track individual job executions:

- **Start and end timestamps** from the Flink job timeline
- **Run result**: `FINISHED` maps to SUCCESS, `FAILED` maps to FAILURE, `CANCELED` maps to SKIPPED
- **Process type**: STREAMING or BATCH, based on the Flink job type

Jobs in `RUNNING` state emit a start event only. Completed jobs emit both start and end events.

### Limitations

1. **SQL Gateway required for SQL/Table API lineage.** Without a SQL Gateway URL, the connector cannot resolve `TableSourceScan(table=[[catalog, db, table]])` references to their actual platform. DataStream Kafka lineage (`KafkaSource-{topic}`) works without SQL Gateway.

2. **Catalogs must be visible to the SQL Gateway session.** Catalogs registered programmatically in job code, via ephemeral SQL client sessions, or in a separate FileCatalogStore are invisible to the connector. Production deployments should use a persistent catalog (e.g., HiveCatalog backed by Hive Metastore) so that table definitions are visible across sessions.

3. **Iceberg/Paimon on Flink < 1.20 require config.** `DESCRIBE CATALOG` was introduced in Flink 1.20. On earlier versions, Iceberg and Paimon catalogs cannot be auto-detected because their tables don't have a `connector` property in `SHOW CREATE TABLE`. Use `catalog_platform_map` to specify the platform manually.

4. **Operator-chained sinks have no catalog info.** The `tableName[N]: Writer` pattern produced by Flink's operator chaining does not include catalog or database information. Only the bare table name is available. These sinks cannot be resolved to a platform and are reported as unclassified.

5. **DataStream non-Kafka connectors are not supported.** Only `KafkaSource-{topic}` and `KafkaSink-{topic}` DataStream patterns are recognized. Other DataStream connectors (Kinesis, Pulsar, RabbitMQ, custom) produce user-provided names with no platform information.

6. **No column-level lineage.** Only table-level (coarse) lineage is extracted from execution plans.

### Troubleshooting

**"Failed to connect to Flink cluster"**
Verify the `rest_api_url` is correct and reachable. Test manually: `curl http://<host>:8081/v1/config`

**Jobs appear but no lineage is extracted**
Check the ingestion report for "unclassified" nodes. Common causes:

- SQL/Table API jobs without `sql_gateway_url` configured — add the SQL Gateway URL
- Tables in `default_catalog` (GenericInMemoryCatalog) created in ephemeral sessions — use a persistent catalog like HiveCatalog
- DataStream jobs using non-Kafka connectors — not currently supported

**SQL Gateway configured but platform not resolved**
On Flink < 1.20, `DESCRIBE CATALOG` is unavailable. Check if the table's `SHOW CREATE TABLE` output includes a `connector` property. For Iceberg/Paimon catalogs, add `catalog_platform_map` config.

**Lineage URNs don't match other connectors (e.g., Kafka connector)**
Ensure `platform_instance_map` or `catalog_platform_map` produces the same platform instance as your other ingestion sources. For example, if the Kafka connector uses `platform_instance: "prod-cluster"`, configure:

```yml
platform_instance_map:
  kafka: "prod-cluster"
```
