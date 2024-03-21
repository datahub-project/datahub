from typing import Dict, Set, Tuple

from dagster import (
    Definitions,
    In,
    Out,
    PythonObjectDagsterType,
    RunStatusSensorContext,
    job,
    op,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.urns.dataset_urn import DatasetUrn

from datahub_dagster_plugin.client.dagster_generator import DagsterGenerator
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
            metadata={"datahub.inputs": [DatasetUrn("snowflake", "tableA").urn]},
        )
    },
    out={
        "result": Out(
            metadata={"datahub.outputs": [DatasetUrn("snowflake", "tableB").urn]}
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


config = DatahubDagsterSourceConfig.parse_obj(
    {
        "rest_sink_config": {
            "server": "http://localhost:8080",
        },
        "dagster_url": "http://localhost:3000",
    }
)


def asset_lineage_extractor(
    context: RunStatusSensorContext,
    dagster_generator: DagsterGenerator,
    graph: DataHubGraph,
) -> Tuple[Dict[str, Set], Dict[str, Set]]:
    from dagster._core.events import DagsterEventType

    logs = context.instance.all_logs(
        context.dagster_run.run_id,
        {
            DagsterEventType.ASSET_MATERIALIZATION,
            DagsterEventType.ASSET_OBSERVATION,
            DagsterEventType.HANDLED_OUTPUT,
            DagsterEventType.LOADED_INPUT,
        },
    )
    dataset_inputs: Dict[str, Set] = {}
    dataset_outputs: Dict[str, Set] = {}

    for log in logs:
        if not log.dagster_event or not log.step_key:
            continue

        if log.dagster_event.event_type == DagsterEventType.ASSET_MATERIALIZATION:
            if log.step_key not in dataset_outputs:
                dataset_outputs[log.step_key] = set()

            materialization = log.asset_materialization
            if not materialization:
                continue

            properties = {
                key: str(value) for (key, value) in materialization.metadata.items()
            }
            asset_key = materialization.asset_key.path
            dataset_urn = dagster_generator.emit_asset(
                graph, asset_key, materialization.description, properties
            )
            dataset_outputs[log.step_key].add(dataset_urn)
    return dataset_inputs, dataset_outputs


config.asset_lineage_extractor = asset_lineage_extractor
datahub_sensor = make_datahub_sensor(config=config)
defs = Definitions(jobs=[do_stuff], sensors=[datahub_sensor])
