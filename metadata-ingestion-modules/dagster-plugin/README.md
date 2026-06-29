# acryl-datahub-dagster-plugin

**Automatic lineage and run metadata from Dagster into DataHub** — captures asset definitions, job runs, and dataset-level lineage with no manual instrumentation.

## What you can do

- **Capture asset lineage** — automatically extract upstream/downstream relationships between Dagster assets and external datasets
- **Track run history** — record job execution status and task-level outcomes in DataHub
- **Map Dagster assets to DataHub entities** — assets appear as datasets in your DataHub catalog
- **Works with any DataHub deployment** — self-hosted or DataHub Cloud

## Installation

```bash
pip install acryl-datahub-dagster-plugin
```

## Quickstart

Add the DataHub sensor to your Dagster project:

```python
from datahub_dagster_plugin.sensors.datahub_sensors import DatahubSensors

datahub_sensor = DatahubSensors(
    datahub_rest_url="http://localhost:8080"
).make_datahub_sensor()
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

## Links

- [Full documentation](https://docs.datahub.com/docs/lineage/dagster)
- [GitHub](https://github.com/datahub-project/datahub)
- [Slack community](https://datahub.com/slack)
