# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
)

datahub_sensor = make_datahub_sensor(config=config)

defs = Definitions(
    sensors=[datahub_sensor],
)
