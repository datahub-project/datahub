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
from dagster._core.events import DagsterEventType
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


@patch.object(uuid, "uuid4", side_effect=TEST_UUIDS)
@patch("datahub_dagster_plugin.sensors.datahub_sensors.DataHubGraph", autospec=True)
@freeze_time(FROZEN_TIME)
def test_emit_metadata_on_failure_preserves_lineage(
    mock_emit: Mock, mock_uuid: Mock
) -> None:
    """Test that lineage is not overwritten when a run fails.

    When a Dagster run fails, no ASSET_MATERIALIZATION events are produced,
    so the lineage dicts are empty. Without the fix, this would emit
    DataJobInputOutput with empty lists, overwriting existing lineage.

    See: https://github.com/datahub-project/datahub/issues/16024
    """
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    @op
    def my_failing_op():
        raise ValueError("Simulated failure")

    @job
    def failing_job():
        my_failing_op()

    instance = DagsterInstance.ephemeral()
    test_run_id = "abcdef78123456781234567812345678"
    result = failing_job.execute_in_process(
        instance=instance, run_id=test_run_id, raise_on_error=False
    )

    dagster_run = result.dagster_run

    # Extract the RUN_FAILURE event
    failure_event = None
    for event in result.all_events:
        if event.event_type == DagsterEventType.RUN_FAILURE:
            failure_event = event
            break
    assert failure_event is not None, "Expected a RUN_FAILURE event"

    run_status_sensor_context = build_run_status_sensor_context(
        sensor_name="datahub_failure_sensor",
        dagster_instance=instance,
        dagster_run=dagster_run,
        dagster_event=failure_event,
    )

    DatahubSensors()._emit_metadata(run_status_sensor_context)

    emitted_mcps: List[MetadataChangeProposalWrapper] = []
    for mock_call in mock_emitter.method_calls:
        if not mock_call.args:
            continue
        mcpw = mock_call.args[0]
        if isinstance(mcpw, MetadataChangeProposalWrapper):
            emitted_mcps.append(mcpw)

    # Verify that no DataJobInputOutput aspect was emitted.
    # On failure with no lineage data, this aspect should be skipped
    # to preserve existing lineage from previous successful runs.
    datajob_input_output_mcps = [
        mcp for mcp in emitted_mcps if mcp.aspectName == "dataJobInputOutput"
    ]
    assert len(datajob_input_output_mcps) == 0, (
        f"Expected no DataJobInputOutput MCPs for failed run, "
        f"but found {len(datajob_input_output_mcps)}. "
        f"Lineage would be incorrectly overwritten."
    )

    # Verify the DataJob entity itself IS still emitted (just without lineage)
    datajob_info_mcps = [mcp for mcp in emitted_mcps if mcp.aspectName == "dataJobInfo"]
    assert len(datajob_info_mcps) > 0, (
        "DataJob entity should still be emitted for failed runs"
    )


def _get_failure_event(result):
    """Extract the RUN_FAILURE DagsterEvent from an execute_in_process result."""
    for event in result.all_events:
        if event.event_type == DagsterEventType.RUN_FAILURE:
            return event
    raise AssertionError("Expected a RUN_FAILURE event")


def _collect_emitted_mcps(mock_emitter: Mock) -> List[MetadataChangeProposalWrapper]:
    """Collect all MetadataChangeProposalWrapper instances from mock calls."""
    mcps: List[MetadataChangeProposalWrapper] = []
    for mock_call in mock_emitter.method_calls:
        if not mock_call.args:
            continue
        mcpw = mock_call.args[0]
        if isinstance(mcpw, MetadataChangeProposalWrapper):
            mcps.append(mcpw)
    return mcps


@patch.object(uuid, "uuid4", side_effect=TEST_UUIDS)
@patch("datahub_dagster_plugin.sensors.datahub_sensors.DataHubGraph", autospec=True)
@freeze_time(FROZEN_TIME)
def test_failure_still_emits_run_status_tracking(
    mock_emit: Mock, mock_uuid: Mock
) -> None:
    """Verify that run and op execution tracking still works for failed runs.

    Even when lineage emission is skipped, DataProcessInstance MCPs
    (run events) must still be emitted so that the failure is recorded.
    """
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    @op
    def my_failing_op():
        raise ValueError("Simulated failure")

    @job
    def failing_job():
        my_failing_op()

    instance = DagsterInstance.ephemeral()
    result = failing_job.execute_in_process(
        instance=instance,
        run_id="aabb00112233445566778899aabbccdd",
        raise_on_error=False,
    )

    ctx = build_run_status_sensor_context(
        sensor_name="datahub_failure_sensor",
        dagster_instance=instance,
        dagster_run=result.dagster_run,
        dagster_event=_get_failure_event(result),
    )

    DatahubSensors()._emit_metadata(ctx)

    emitted_mcps = _collect_emitted_mcps(mock_emitter)

    # DataProcessInstance MCPs should still be emitted for run status tracking
    dpi_mcps = [mcp for mcp in emitted_mcps if mcp.entityType == "dataProcessInstance"]
    assert len(dpi_mcps) > 0, (
        "DataProcessInstance MCPs should be emitted for failed runs "
        "to track execution status"
    )

    # Should include run event MCPs (start/end events)
    run_event_mcps = [
        mcp for mcp in dpi_mcps if mcp.aspectName == "dataProcessInstanceRunEvent"
    ]
    assert len(run_event_mcps) > 0, (
        "Run event MCPs should be emitted to record the failure"
    )

    # DataFlow should still be emitted
    dataflow_mcps = [mcp for mcp in emitted_mcps if mcp.entityType == "dataFlow"]
    assert len(dataflow_mcps) > 0, "DataFlow should still be emitted for failed runs"


@patch.object(uuid, "uuid4", side_effect=TEST_UUIDS)
@patch("datahub_dagster_plugin.sensors.datahub_sensors.DataHubGraph", autospec=True)
@freeze_time(FROZEN_TIME)
def test_failure_with_static_input_metadata_still_emits_lineage(
    mock_emit: Mock, mock_uuid: Mock
) -> None:
    """When an op has datahub.inputs metadata on its definition, the lineage
    should still be emitted even on failure because the static metadata provides
    inlets regardless of runtime events.
    """
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

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
    def failing_op_with_metadata(data):
        raise ValueError("Simulated failure")

    @op(out={"result": Out()})
    def source_op():
        return [1, 2, 3]

    @job
    def job_with_metadata():
        failing_op_with_metadata(source_op())

    instance = DagsterInstance.ephemeral()
    result = job_with_metadata.execute_in_process(
        instance=instance,
        run_id="cc112233445566778899aabbccddeeff",
        raise_on_error=False,
    )

    ctx = build_run_status_sensor_context(
        sensor_name="datahub_failure_sensor",
        dagster_instance=instance,
        dagster_run=result.dagster_run,
        dagster_event=_get_failure_event(result),
    )

    DatahubSensors()._emit_metadata(ctx)

    emitted_mcps = _collect_emitted_mcps(mock_emitter)

    # The op with datahub.inputs metadata should have its lineage emitted
    # even though the run failed, because the static metadata provides inlets
    datajob_io_mcps = [
        mcp for mcp in emitted_mcps if mcp.aspectName == "dataJobInputOutput"
    ]

    # Find the MCP for the failing op that has datahub.inputs metadata
    failing_op_io = [
        mcp
        for mcp in datajob_io_mcps
        if "failing_op_with_metadata" in str(mcp.entityUrn)
    ]
    assert len(failing_op_io) > 0, (
        "DataJobInputOutput should be emitted for ops with static metadata "
        "even when the run fails"
    )

    # Verify the static metadata lineage is present
    io_aspect = failing_op_io[0].aspect
    assert io_aspect is not None
    assert hasattr(io_aspect, "inputDatasets")
    assert len(io_aspect.inputDatasets) > 0, (
        "Input datasets from datahub.inputs metadata should be present"
    )
    assert "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableA,PROD)" in [
        str(d) for d in io_aspect.inputDatasets
    ]


@patch.object(uuid, "uuid4", side_effect=TEST_UUIDS)
@patch("datahub_dagster_plugin.sensors.datahub_sensors.DataHubGraph", autospec=True)
@freeze_time(FROZEN_TIME)
def test_partial_failure_preserves_lineage_for_failed_op(
    mock_emit: Mock, mock_uuid: Mock
) -> None:
    """In a multi-op job where the first op succeeds and the second fails,
    the failed op (without metadata) should not have its lineage overwritten.
    """
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    @op(out={"result": Out()})
    def successful_extract():
        return [1, 2, 3]

    @op(ins={"data": In()})
    def failing_transform(data):
        raise ValueError("Transform failed")

    @job
    def partial_fail_job():
        failing_transform(successful_extract())

    instance = DagsterInstance.ephemeral()
    result = partial_fail_job.execute_in_process(
        instance=instance,
        run_id="dd112233445566778899aabbccddeeff",
        raise_on_error=False,
    )

    ctx = build_run_status_sensor_context(
        sensor_name="datahub_failure_sensor",
        dagster_instance=instance,
        dagster_run=result.dagster_run,
        dagster_event=_get_failure_event(result),
    )

    DatahubSensors()._emit_metadata(ctx)

    emitted_mcps = _collect_emitted_mcps(mock_emitter)

    datajob_io_mcps = [
        mcp for mcp in emitted_mcps if mcp.aspectName == "dataJobInputOutput"
    ]

    # Neither op has datahub.inputs/datahub.outputs metadata and the run failed,
    # so neither should emit DataJobInputOutput (which would overwrite lineage).
    # The successful op also has no metadata-based lineage to emit.
    for mcp in datajob_io_mcps:
        io_aspect = mcp.aspect
        assert io_aspect is not None
        assert hasattr(io_aspect, "inputDatasets")
        assert hasattr(io_aspect, "outputDatasets")
        # If a DataJobInputOutput IS emitted, it should have actual lineage data
        # (not be empty), otherwise it would incorrectly clear existing lineage.
        has_lineage = (
            len(io_aspect.inputDatasets) > 0 or len(io_aspect.outputDatasets) > 0
        )
        assert has_lineage, (
            f"DataJobInputOutput for {mcp.entityUrn} was emitted with empty "
            f"lineage during a failed run - this would overwrite existing lineage"
        )


@patch.object(uuid, "uuid4", side_effect=TEST_UUIDS)
@patch("datahub_dagster_plugin.sensors.datahub_sensors.DataHubGraph", autospec=True)
@freeze_time(FROZEN_TIME)
def test_success_run_still_emits_lineage(mock_emit: Mock, mock_uuid: Mock) -> None:
    """Regression guard: successful runs must still emit DataJobInputOutput
    (our fix must not suppress lineage for successful runs).
    """
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

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
    def my_transform(data):
        return str(data)

    @op(out={"result": Out()})
    def my_extract():
        return [1, 2, 3]

    @job
    def success_job():
        my_transform(my_extract())

    instance = DagsterInstance.ephemeral()
    result = success_job.execute_in_process(
        instance=instance,
        run_id="ee112233445566778899aabbccddeeff",
    )

    ctx = build_run_status_sensor_context(
        sensor_name="datahub_success_sensor",
        dagster_instance=instance,
        dagster_run=result.dagster_run,
        dagster_event=result.get_run_success_event(),
    )

    DatahubSensors()._emit_metadata(ctx)

    emitted_mcps = _collect_emitted_mcps(mock_emitter)

    datajob_io_mcps = [
        mcp for mcp in emitted_mcps if mcp.aspectName == "dataJobInputOutput"
    ]
    assert len(datajob_io_mcps) > 0, (
        "DataJobInputOutput must be emitted for successful runs"
    )

    # Verify the transform op has the correct input lineage
    transform_io = [
        mcp for mcp in datajob_io_mcps if "my_transform" in str(mcp.entityUrn)
    ]
    assert len(transform_io) > 0, (
        "Transform op should have DataJobInputOutput emitted on success"
    )
    io_aspect = transform_io[0].aspect
    assert io_aspect is not None
    assert hasattr(io_aspect, "inputDatasets")
    assert "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableA,PROD)" in [
        str(d) for d in io_aspect.inputDatasets
    ]
