import json
import pathlib
import tempfile
import uuid
from typing import Dict, List, Mapping, Sequence, Set
from unittest.mock import Mock, patch

import dagster._core.utils
from dagster import (
    DagsterInstance,
    In,
    Out,
    SkipReason,
    build_run_status_sensor_context,
    build_sensor_context,
    job,
    op,
)
from dagster._core.definitions.job_definition import JobDefinition
from dagster._core.definitions.repository_definition import (
    RepositoryData,
    RepositoryDefinition,
)
from dagster._core.definitions.resource_definition import ResourceDefinition
from freezegun import freeze_time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.testing.compare_metadata_json import assert_metadata_files_equal
from datahub_dagster_plugin.client.dagster_generator import DatahubDagsterSourceConfig
from datahub_dagster_plugin.sensors.datahub_sensors import (
    DatahubSensors,
    make_datahub_sensor,
)

FROZEN_TIME = "2024-07-11 07:00:00"

call_num = 0


def make_new_run_id_mock() -> str:
    global call_num
    call_num += 1
    return f"test_run_id_{call_num}"


dagster._core.utils.make_new_run_id = make_new_run_id_mock


@patch("datahub_dagster_plugin.sensors.datahub_sensors.DataHubGraph", autospec=True)
def test_datahub_sensor(mock_emit):
    instance = DagsterInstance.ephemeral()

    class DummyRepositoryData(RepositoryData):
        def __init__(self):
            self.sensors = []

        def get_all_jobs(self) -> Sequence["JobDefinition"]:
            return []

        def get_top_level_resources(self) -> Mapping[str, "ResourceDefinition"]:
            """Return all top-level resources in the repository as a list,
            such as those provided to the Definitions constructor.

            Returns:
                List[ResourceDefinition]: All top-level resources in the repository.
            """
            return {}

        def get_env_vars_by_top_level_resource(self) -> Mapping[str, Set[str]]:
            return {}

    repository_defintion = RepositoryDefinition(
        name="testRepository", repository_data=DummyRepositoryData()
    )
    context = build_sensor_context(
        instance=instance, repository_def=repository_defintion
    )
    mock_emit.return_value = Mock()

    config = DatahubDagsterSourceConfig(
        datahub_client_config=DatahubClientConfig(
            server="http://localhost:8081",
        ),
        dagster_url="http://localhost:3000",
    )

    datahub_sensor = make_datahub_sensor(config)
    skip_reason = datahub_sensor(context)
    assert isinstance(skip_reason, SkipReason)


TEST_UUIDS = ["uuid_{}".format(i) for i in range(10000)]


@patch.object(uuid, "uuid4", side_effect=TEST_UUIDS)
@patch("datahub_dagster_plugin.sensors.datahub_sensors.DataHubGraph", autospec=True)
@freeze_time(FROZEN_TIME)
def test_emit_metadata(mock_emit: Mock, mock_uuid: Mock) -> None:
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    @op(
        out={
            "result": Out(
                metadata={
                    "datahub.outputs": [
                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableB,PROD)"
                    ]
                }
            )
        }
    )
    def extract():
        results = [1, 2, 3, 4]
        return results

    @op(
        ins={
            "data": In(
                metadata={
                    "datahub.inputs": [
                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableA,PROD)"
                    ]
                }
            )
        }
    )
    def transform(data):
        results = []
        for each in data:
            results.append(str(each))
        return results

    @job
    def etl():
        transform(extract())

    instance = DagsterInstance.ephemeral()
    test_run_id = "12345678123456781234567812345678"
    result = etl.execute_in_process(instance=instance, run_id=test_run_id)

    # retrieve the DagsterRun
    dagster_run = result.dagster_run

    # retrieve a success event from the completed execution
    dagster_event = result.get_run_success_event()

    # create the context
    run_status_sensor_context = build_run_status_sensor_context(
        sensor_name="my_email_sensor",
        dagster_instance=instance,
        dagster_run=dagster_run,
        dagster_event=dagster_event,
    )

    with tempfile.TemporaryDirectory() as tmp_path:
        DatahubSensors()._emit_metadata(run_status_sensor_context)
        mcpws: List[Dict] = []
        for mock_call in mock_emitter.method_calls:
            if not mock_call.args:
                continue
            mcpw = mock_call.args[0]
            if isinstance(mcpw, MetadataChangeProposalWrapper):
                mcpws.append(mcpw.to_obj(simplified_structure=True))

        with open(f"{tmp_path}/test_emit_metadata_mcps.json", "w") as f:
            json_object = json.dumps(mcpws, indent=2)
            f.write(json_object)

        assert_metadata_files_equal(
            output_path=pathlib.Path(f"{tmp_path}/test_emit_metadata_mcps.json"),
            golden_path=pathlib.Path(
                "tests/unit/golden/golden_test_emit_metadata_mcps.json"
            ),
            ignore_paths=["root[*]['systemMetadata']['created']"],
        )
