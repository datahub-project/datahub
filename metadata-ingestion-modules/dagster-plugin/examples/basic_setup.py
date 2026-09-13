from dagster import Definitions

from datahub.ingestion.graph.client import DatahubClientConfig
from datahub_dagster_plugin.sensors.datahub_sensors import (
    DatahubDagsterSourceConfig,
    make_datahub_sensor,
)

config = DatahubDagsterSourceConfig(
    datahub_client_config=DatahubClientConfig(
        server="https://your_datahub_url/gms", token="your_datahub_token"
    ),
    dagster_url="https://my-dagster-cloud.dagster.cloud",
    # Link each asset to the warehouse table it materializes (Dagster asset and
    # warehouse dataset merge into one entity in DataHub).
    emit_siblings=True,
)

datahub_sensor = make_datahub_sensor(config=config)

defs = Definitions(
    sensors=[datahub_sensor],
)
