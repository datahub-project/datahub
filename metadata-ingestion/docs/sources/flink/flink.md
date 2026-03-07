### Concept Mapping

| Source Concept | DataHub Concept                                                                 | Notes                                         |
| -------------- | ------------------------------------------------------------------------------- | --------------------------------------------- |
| Flink Job      | [DataFlow](docs/generated/metamodel/entities/dataFlow.md)                       | One DataFlow per Flink job                    |
| Flink Operator | [DataJob](docs/generated/metamodel/entities/dataJob.md)                         | Granularity depends on `operator_granularity` |
| Job Execution  | [DataProcessInstance](docs/generated/metamodel/entities/dataProcessInstance.md) | When `include_run_history` is enabled         |
| Kafka Topic    | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | Via lineage (inlets/outlets)                  |
| Catalog Table  | [Dataset](docs/generated/metamodel/entities/dataset.md)                         | When `include_catalog_metadata` is enabled    |
| Catalog        | [Container](docs/generated/metamodel/entities/container.md)                     | _subType_: `Catalog`                          |
| Database       | [Container](docs/generated/metamodel/entities/container.md)                     | _subType_: `Database`, nested under Catalog   |

### Extracting Lineage from Kafka Connectors

The connector automatically extracts table-level lineage from Kafka sources and sinks by analyzing Flink execution plans. It supports both the **DataStream API** and the **SQL/Table API**.

Supported plan node formats:

| API Style                      | Source Pattern                                      | Sink Pattern                         |
| ------------------------------ | --------------------------------------------------- | ------------------------------------ |
| DataStream API                 | `KafkaSource-{topic}`                               | `KafkaSink-{topic}`                  |
| SQL/Table API                  | `TableSourceScan(table=[[catalog, db, table]])`     | `Sink(table=[[catalog, db, table]])` |
| SQL/Table API (operator chain) | `[N]:TableSourceScan(table=[[catalog, db, table]])` | `tableName[N]: Writer`               |

Lineage is emitted as DataJob inlets (sources) and outlets (sinks), linking Flink jobs to the Kafka topics they read from and write to.

The lineage extractor is designed for Kafka connectors. For DataStream API jobs, non-Kafka connectors (e.g., `JdbcSource`, `ElasticsearchSink`) will be reported as "unclassified" in the ingestion report. For SQL/Table API jobs, the `TableSourceScan` and `Writer` patterns are generic Flink plan formats — any connector using these patterns will be matched and the extracted table name will be attributed to the `kafka` platform. This means non-Kafka SQL connectors (e.g., datagen, blackhole, JDBC via SQL) may produce lineage with incorrect platform attribution.

### Mapping Kafka Topics to Platform Instances

If your Kafka topics belong to a specific platform instance (e.g., a particular Kafka cluster), use `platform_instance_map` to ensure lineage URNs are constructed correctly:

```yml
source:
  type: "flink"
  config:
    connection:
      rest_api_url: "http://localhost:8081"
    platform_instance_map:
      kafka: "my-kafka-cluster"
```

### Operator Granularity

By default (`operator_granularity: job`), the connector emits **one DataJob per Flink job** with all source and sink lineage coalesced into that single DataJob.

Set `operator_granularity: vertex` to emit **one DataJob per operator/vertex** in the execution plan. This gives finer-grained lineage at the cost of more entities:

```yml
source:
  type: "flink"
  config:
    connection:
      rest_api_url: "http://localhost:8081"
    operator_granularity: "vertex"
```

### Catalog Metadata via SQL Gateway

When `include_catalog_metadata` is enabled with a `sql_gateway_url`, the connector queries the Flink SQL Gateway to extract:

- **Catalogs** as top-level containers
- **Databases** as nested containers within catalogs
- **Tables** as Dataset entities with column names and types

```yml
source:
  type: "flink"
  config:
    connection:
      rest_api_url: "http://localhost:8081"
      sql_gateway_url: "http://localhost:8083"
    include_catalog_metadata: true
    catalog_pattern:
      allow:
        - "^my_catalog$"
```

### Run History

When `include_run_history` is enabled (the default), the connector emits `DataProcessInstance` entities that track individual job executions. Each instance captures:

- **Start and end timestamps** from the Flink job timeline
- **Run result**: `FINISHED` maps to SUCCESS, `FAILED` maps to FAILURE, `CANCELED` maps to SKIPPED
- **Process type**: STREAMING or BATCH, based on the Flink job type

Jobs in `RUNNING` state emit a start event only (no end event). Completed jobs emit both start and end events.

### Known Limitations

1. **Kafka-centric lineage**: Lineage extraction is designed for Kafka connectors. DataStream API non-Kafka operators (e.g., JDBC, Elasticsearch) appear as "unclassified." SQL/Table API non-Kafka connectors using `TableSourceScan`/`Writer` patterns will be matched but incorrectly attributed to the `kafka` platform.
2. **No column-level lineage**: Only table-level lineage is extracted from execution plans.
3. **SQL Gateway dependency**: Catalog metadata extraction requires a running Flink SQL Gateway, which is not always deployed alongside the Flink cluster.

### Troubleshooting

**"Failed to connect to Flink cluster"**
Verify the `rest_api_url` is correct and reachable. Test manually: `curl http://<host>:8081/v1/config`

**Jobs appear but no lineage is extracted**
The connector currently only extracts Kafka lineage. Check the ingestion report for "unclassified" operators — these are nodes the connector could not parse. If the job has no Kafka sources or sinks, no lineage will be emitted.

**Catalog metadata not appearing**
Ensure both `include_catalog_metadata: true` and `sql_gateway_url` are configured. Verify the SQL Gateway is running and can open a session: `curl -X POST http://<host>:8083/v1/sessions -H 'Content-Type: application/json' -d '{"properties":{}}'`
