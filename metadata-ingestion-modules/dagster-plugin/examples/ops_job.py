# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from dagster import Definitions, In, Out, PythonObjectDagsterType, job, op

from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub_dagster_plugin.sensors.datahub_sensors import (
    DatahubDagsterSourceConfig,
    make_datahub_sensor,
)


@op
def extract():
    results = [1, 2, 3, 4]
    return results


@op(
    ins={
        "data": In(
            dagster_type=PythonObjectDagsterType(list),
            metadata={"datahub.inputs": [DatasetUrn("snowflake", "tableA").urn()]},
        )
    },
    out={
        "result": Out(
            metadata={"datahub.outputs": [DatasetUrn("snowflake", "tableB").urn()]}
        )
    },
)
def transform(data):
    results = []
    for each in data:
        results.append(str(each))
    return results


@job
def do_stuff():
    transform(extract())


config = DatahubDagsterSourceConfig(
    datahub_client_config=DatahubClientConfig(server="http://localhost:8080"),
    dagster_url="http://localhost:3000",
)

datahub_sensor = make_datahub_sensor(config=config)
defs = Definitions(jobs=[do_stuff], sensors=[datahub_sensor])
