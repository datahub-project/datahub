<!-- PyPI long description. Keep concise, feature-discovery-first. -->

# DataHub Dagster Plugin

**Automatic lineage and run metadata from Dagster into DataHub** — captures asset definitions, job runs, and dataset-level lineage with no manual instrumentation.

## What you can do

- **Capture asset lineage** — automatically extract upstream/downstream relationships between Dagster assets and external datasets
- **Track run history** — record job execution status and task-level outcomes in DataHub
- **Map Dagster assets to DataHub entities** — assets appear as datasets in your DataHub catalog
- **Link assets to warehouse tables** — with `emit_siblings`, merge each Dagster asset with the underlying warehouse table (Snowflake/BigQuery/Databricks/…) into a single entity
- **Works with any DataHub deployment** — self-hosted or DataHub Cloud

## Installation

```bash
pip install acryl-datahub-dagster-plugin
```

## Quickstart

Add the DataHub sensor to your Dagster project:

```python
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub_dagster_plugin.sensors.datahub_sensors import (
    DatahubDagsterSourceConfig,
    make_datahub_sensor,
)

config = DatahubDagsterSourceConfig(
    datahub_client_config=DatahubClientConfig(server="http://localhost:8080"),
    dagster_url="http://localhost:3000",
)
datahub_sensor = make_datahub_sensor(config=config)
```

Register it alongside your jobs and assets in your `Definitions`:

```python
from dagster import Definitions

defs = Definitions(
    assets=[...],
    jobs=[...],
    sensors=[datahub_sensor],
)
```

Once the sensor is running, every job run will emit lineage and status metadata to DataHub automatically.

## Sibling links to warehouse tables

Set `emit_siblings=True` in `DatahubDagsterSourceConfig` to link each Dagster asset to the warehouse table it materializes via a `siblings` relationship. The Dagster asset and the warehouse dataset then render as a single merged entity in DataHub. Use `dagster_is_primary_sibling` to choose which side is primary (defaults to the warehouse table). See `examples/basic_setup.py`.

## Links

- [Full documentation](https://docs.datahub.com/docs/lineage/dagster)
- [Dagster](https://dagster.io/)
- [GitHub](https://github.com/datahub-project/datahub)
- [Slack community](https://datahub.com/slack)
