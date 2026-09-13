<!-- PyPI long description. Keep concise, feature-discovery-first. -->

# DataHub SDK and CLI

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

Snowflake · BigQuery · Redshift · dbt · Databricks · Looker · Tableau · Power BI · Airflow · Spark · Kafka · PostgreSQL · MySQL · Hive · Glue · S3 · Iceberg · Unity Catalog · Sigma · Mode · Superset · Metabase · and [many more](/integrations)

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
datahub init                          # Connect to a DataHub instance (interactive)
datahub init --username datahub --password datahub  # Quickstart with local defaults
datahub ingest -c recipe.yml          # Run an ingestion pipeline
datahub search "my table"             # Search for entities by keyword
datahub search "*" --filter platform=snowflake --filter entity_type=dataset
datahub graphql --list-operations     # Explore all available GraphQL operations
datahub graphql --query "{ me { corpUser { urn } } }"  # Run a GraphQL query
datahub get --urn <urn>               # Fetch metadata for an entity
datahub delete --urn <urn>            # Delete a metadata entity
datahub timeline --urn <urn>          # View metadata change history
```

## Links

- [Documentation](/)
- [Integrations catalog](/integrations)
- [Quickstart guide](/docs/quickstart)
- [Developing a custom source](/docs/metadata-ingestion/adding-source)
- [GitHub](https://github.com/datahub-project/datahub)
- [Slack community](https://datahub.com/slack)
