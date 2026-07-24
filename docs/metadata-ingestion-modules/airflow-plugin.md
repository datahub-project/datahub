<!-- PyPI long description. Keep concise, feature-discovery-first. -->

# DataHub Airflow Plugin

**Automatic lineage and run metadata from Apache Airflow into DataHub** — captures DAG structure, task inputs/outputs, and run history with no manual instrumentation.

## What you can do

- **Capture pipeline lineage** — automatically extract dataset-level and column-level lineage from SQL operators
- **Track run history** — record task execution status, duration, and failures in DataHub
- **Enhance OpenLineage** — patches Airflow's OpenLineage extractors with DataHub's advanced SQL parser for richer lineage
- **Support multiple emitters** — send metadata via REST, Kafka, or file

## Version compatibility

| Airflow Version | Support                                        |
| --------------- | ---------------------------------------------- |
| 3.0+            | ✅ Fully supported                             |
| 2.x             | ❌ Use `acryl-datahub-airflow-plugin <= 1.6.0` |

## Installation

```bash
pip install acryl-datahub-airflow-plugin

# With Kafka emitter
pip install 'acryl-datahub-airflow-plugin[datahub-kafka]'
```

## Configuration

Add to `airflow.cfg`:

```ini
[datahub]
enabled = True
conn_id = datahub_rest_default   # Airflow connection pointing to your DataHub GMS
```

Set up the Airflow connection:

```bash
airflow connections add datahub_rest_default \
  --conn-type HTTP \
  --conn-host http://localhost:8080
```

The plugin activates automatically — no changes to your DAG code required.

## Key configuration options

| Option                               | Default | Description                                       |
| ------------------------------------ | ------- | ------------------------------------------------- |
| `enable_extractors`                  | `True`  | Enhance OpenLineage extractors                    |
| `patch_sql_parser`                   | `True`  | Use DataHub's SQL parser for column-level lineage |
| `enable_multi_statement_sql_parsing` | `False` | Resolve temp tables across multi-statement tasks  |

## Links

- [Full documentation](/docs/lineage/airflow)
- [Apache Airflow](https://airflow.apache.org/)
- [GitHub](https://github.com/datahub-project/datahub)
- [Slack community](https://datahub.com/slack)
