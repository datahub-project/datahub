<!-- PyPI long description. Keep concise, feature-discovery-first. acryl-datahub ≤700 words, others ≤400. -->
# acryl-datahub

**DataHub's ingestion framework and CLI** — extract metadata from 50+ data sources and push it into your DataHub catalog.

## What you can do

- **Ingest metadata** from databases, data warehouses, BI tools, orchestrators, and more
- **Emit metadata programmatically** using the Python or Java SDK
- **Transform and filter** metadata in transit using built-in transformers
- **Schedule and manage** ingestion pipelines via CLI or the DataHub UI
- **Automate lineage, ownership, tags, and documentation** across your data assets

## Supported sources

Snowflake · BigQuery · Redshift · dbt · Databricks · Looker · Tableau · Power BI · Airflow · Spark · Kafka · PostgreSQL · MySQL · Hive · Glue · S3 · Iceberg · Unity Catalog · Sigma · Mode · Superset · Metabase · and [many more](https://docs.datahub.com/integrations)

## Installation

```bash
pip install acryl-datahub
datahub version
```

## Quickstart

### Pull metadata from a source (recipe)

```yaml
# snowflake_recipe.yml
source:
  type: snowflake
  config:
    account_id: my_account
    username: my_user
    password: my_password
    role: DATAHUB_ROLE
    warehouse: COMPUTE_WH

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```

```bash
datahub ingest -c snowflake_recipe.yml
```

### Emit metadata from code (SDK)

```python
import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter("http://localhost:8080")

dataset_urn = builder.make_dataset_urn("snowflake", "mydb.schema.table")
ownership = builder.make_ownership_mce(dataset_urn, ["urn:li:corpuser:jane"])
emitter.emit(ownership)
```

### Ingest via the DataHub UI

No CLI required — configure and run ingestion directly from the DataHub UI under **Ingestion → Create new source**.

## Ingestion methods

| Method | Best for |
|---|---|
| **CLI + YAML recipe** | Scheduled batch ingestion, CI/CD pipelines |
| **Python SDK** | Programmatic or event-driven metadata emission |
| **Java SDK** | JVM-based integrations (Spark, Flink, etc.) |
| **UI Ingestion** | One-click setup, no code required |

## Key CLI commands

```bash
datahub ingest -c recipe.yml          # Run an ingestion pipeline
datahub check graph                   # Verify connectivity to DataHub
datahub delete --urn <urn>            # Delete a metadata entity
datahub get --urn <urn>               # Fetch metadata for an entity
datahub timeline --urn <urn>          # View metadata change history
datahub migrate                       # Run platform migrations
```

## Links

- [Documentation](https://docs.datahub.com/)
- [Integrations catalog](https://docs.datahub.com/integrations)
- [Quickstart guide](https://docs.datahub.com/docs/quickstart)
- [Developing a custom source](https://docs.datahub.com/docs/metadata-ingestion/adding-source)
- [GitHub](https://github.com/datahub-project/datahub)
- [Slack community](https://datahub.com/slack)
