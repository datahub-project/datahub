from typing import Dict

from dagster import (
    Definitions,
    In,
    Out,
    PythonObjectDagsterType,
    RunStatusSensorContext,
    job,
    op,
)

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub_dagster_plugin.client.dagster_generator import (
    DagsterGenerator,
    DatasetLineage,
)
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


def asset_lineage_extractor(
    context: RunStatusSensorContext,
    dagster_generator: DagsterGenerator,
    graph: DataHubGraph,
) -> Dict[str, DatasetLineage]:
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

    dataset_lineage: Dict[str, DatasetLineage] = {}

    for log in logs:
        if not log.dagster_event or not log.step_key:
            continue

        if log.dagster_event.event_type == DagsterEventType.ASSET_MATERIALIZATION:
            if log.step_key not in dataset_lineage:
                dataset_lineage[log.step_key] = DatasetLineage(set(), set())

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
            dataset_lineage[log.step_key].outputs.add(dataset_urn)

    return dataset_lineage


config = DatahubDagsterSourceConfig(
    datahub_client_config=DatahubClientConfig(server="http://localhost:8080"),
    dagster_url="http://localhost:3000",
    asset_lineage_extractor=asset_lineage_extractor,
)
datahub_sensor = make_datahub_sensor(config=config)
defs = Definitions(jobs=[do_stuff], sensors=[datahub_sensor])
