<!-- PyPI long description. Keep concise, feature-discovery-first. -->

# acryl-datahub

**DataHub's ingestion framework and CLI** — pull metadata from 50+ data sources into your catalog, or push it programmatically from your own pipelines and applications.

**Pull-based** integrations crawl your data systems on a schedule: Snowflake, BigQuery, dbt, Looker, Airflow, and many more.

**Push-based** integrations let you emit metadata directly from code as it happens: Python SDK, Java SDK, Spark, Great Expectations, and others.

## What you can do

- **Pull metadata** from databases, warehouses, BI tools, and orchestrators using 50+ ready-made connectors
- **Push metadata** programmatically using the Python or Java SDK
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
from datahub.sdk import DataHubClient, Dataset

client = DataHubClient.from_env()

dataset = Dataset(platform="snowflake", name="mydb.schema.table")
dataset.set_description("My table description")
dataset.set_owners(["urn:li:corpuser:jane"])

client.entities.upsert(dataset)
```

### Ingest via the DataHub UI

No CLI required — configure and run ingestion directly from the DataHub UI under **Ingestion → Create new source**.

## Ingestion methods

| Method                | Best for                                       |
| --------------------- | ---------------------------------------------- |
| **CLI + YAML recipe** | Scheduled batch ingestion, CI/CD pipelines     |
| **Python SDK**        | Programmatic or event-driven metadata emission |
| **Java SDK**          | JVM-based integrations (Spark, Flink, etc.)    |
| **UI Ingestion**      | One-click setup, no code required              |

## Key CLI commands

```bash
datahub ingest -c recipe.yml          # Run an ingestion pipeline
datahub check server-config           # Verify connectivity to DataHub
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
