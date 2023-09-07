from dagster import Definitions, In, Out, PythonObjectDagsterType, job, op
from datahub.api.entities.dataset import Dataset

from datahub_dagster_plugin.sensors.datahub_sensors import datahub_sensor


@op
def extract():
    results = [1, 2, 3, 4]
    return results


@op(
    ins={
        "data": In(
            dagster_type=PythonObjectDagsterType(list),
            metadata={"datahub.inputs": [Dataset("snowflake", "tableA").urn]},
        )
    },
    out={
        "result": Out(
            metadata={"datahub.outputs": [Dataset("snowflake", "tableB").urn]}
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


defs = Definitions(jobs=[do_stuff], sensors=[datahub_sensor])
