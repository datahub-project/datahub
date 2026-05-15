from typing import Any, Dict, List
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
)
from datahub.ingestion.source.matillion_dpc.matillion import MatillionSource
from datahub.ingestion.source.matillion_dpc.matillion_api import MatillionAPIClient
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionPipelineExecutionStepResult,
    MatillionProject,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRunEventClass,
    RunResultTypeClass,
)


def mock_oauth_generation(self: Any) -> None:
    pass


@pytest.fixture
def source() -> MatillionSource:
    config = MatillionSourceConfig(
        api_config=MatillionAPIConfig(
            client_id=SecretStr("test_client_id"),
            client_secret=SecretStr("test_client_secret"),
            custom_base_url="http://test.com",
        ),
    )
    with patch.object(
        MatillionAPIClient, "_generate_oauth_token", mock_oauth_generation
    ):
        return MatillionSource(config, PipelineContext(run_id="test-run"))


@pytest.fixture
def sample_project() -> MatillionProject:
    return MatillionProject(id="proj-1", name="Test Project")


@pytest.fixture
def sample_pipeline() -> MatillionPipeline:
    return MatillionPipeline(name="Test Pipeline")


@pytest.fixture
def sample_execution() -> MatillionPipelineExecution:
    return MatillionPipelineExecution(
        pipeline_execution_id="exec-1",
        pipeline_name="Test Pipeline",
        status="SUCCESS",
        trigger="schedule",
        environment_name="Production",
        project_id="proj-1",
    )


class TestBuildStepDpiProperties:
    def test_normal_case_with_all_timestamps(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={
                "status": "SUCCESS",
                "message": "Completed successfully",
                "startedAt": "2024-01-01T10:00:00Z",
                "finishedAt": "2024-01-01T10:05:00Z",
            },
        )
        execution = MatillionPipelineExecution(
            pipeline_execution_id="exec-1",
            pipeline_name="Test Pipeline",
            status="SUCCESS",
            trigger="schedule",
            environment_name="Production",
            project_id="proj-1",
        )
        pipeline = MatillionPipeline(name="Test Pipeline")

        properties = source._build_step_dpi_properties(step, execution, pipeline)

        assert isinstance(properties, DataProcessInstancePropertiesClass)
        assert properties.name == "Test Pipeline-Extract Data-exec-1"
        assert properties.created.time > 0
        assert properties.customProperties["execution_id"] == "exec-1"
        assert properties.customProperties["step_id"] == "step-1"
        assert properties.customProperties["step_name"] == "Extract Data"
        assert properties.customProperties["status"] == "SUCCESS"
        assert properties.customProperties["message"] == "Completed successfully"
        assert properties.customProperties["started_at"] == "2024-01-01T10:00:00Z"
        assert properties.customProperties["finished_at"] == "2024-01-01T10:05:00Z"
        assert properties.customProperties["environment_name"] == "Production"
        assert properties.customProperties["project_id"] == "proj-1"

    def test_null_started_at_timestamp(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={
                "status": "SUCCESS",
                "finishedAt": "2024-01-01T10:05:00Z",
            },
        )
        execution = MatillionPipelineExecution(
            pipeline_execution_id="exec-1",
            pipeline_name="Test Pipeline",
            status="SUCCESS",
            trigger="manual",
        )
        pipeline = MatillionPipeline(name="Test Pipeline")

        properties = source._build_step_dpi_properties(step, execution, pipeline)

        assert properties.created.time == 0
        assert "started_at" not in properties.customProperties

    def test_null_finished_at_timestamp(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={
                "status": "RUNNING",
                "startedAt": "2024-01-01T10:00:00Z",
            },
        )
        execution = MatillionPipelineExecution(
            pipeline_execution_id="exec-1",
            pipeline_name="Test Pipeline",
            status="RUNNING",
            trigger="manual",
        )
        pipeline = MatillionPipeline(name="Test Pipeline")

        properties = source._build_step_dpi_properties(step, execution, pipeline)

        assert "finished_at" not in properties.customProperties
        assert properties.customProperties["started_at"] == "2024-01-01T10:00:00Z"

    def test_null_result_dict(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result=None,
        )
        execution = MatillionPipelineExecution(
            pipeline_execution_id="exec-1",
            pipeline_name="Test Pipeline",
            status="SUCCESS",
            trigger="schedule",
        )
        pipeline = MatillionPipeline(name="Test Pipeline")

        properties = source._build_step_dpi_properties(step, execution, pipeline)

        assert properties.created.time == 0
        assert properties.customProperties["status"] == "UNKNOWN"
        assert properties.customProperties["message"] == ""

    def test_invalid_timestamp_format(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={
                "status": "SUCCESS",
                "startedAt": "invalid-timestamp",
                "finishedAt": "2024-01-01T10:05:00Z",
            },
        )
        execution = MatillionPipelineExecution(
            pipeline_execution_id="exec-1",
            pipeline_name="Test Pipeline",
            status="SUCCESS",
            trigger="manual",
        )
        pipeline = MatillionPipeline(name="Test Pipeline")

        properties = source._build_step_dpi_properties(step, execution, pipeline)

        assert properties.created.time == 0

    @pytest.mark.parametrize(
        "trigger,expected_type",
        [
            ("MANUAL", "BATCH_AD_HOC"),
            ("SCHEDULE", "BATCH_SCHEDULED"),
            ("API", "BATCH_AD_HOC"),
        ],
    )
    def test_trigger_type_mapping(
        self, source: MatillionSource, trigger: str, expected_type: str
    ) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={"status": "SUCCESS"},
        )
        execution = MatillionPipelineExecution(
            pipeline_execution_id="exec-1",
            pipeline_name="Test Pipeline",
            status="SUCCESS",
            trigger=trigger,
        )
        pipeline = MatillionPipeline(name="Test Pipeline")

        properties = source._build_step_dpi_properties(step, execution, pipeline)

        assert properties.type == expected_type

    def test_missing_optional_execution_fields(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={"status": "SUCCESS"},
        )
        execution = MatillionPipelineExecution(
            pipeline_execution_id="exec-1",
            pipeline_name="Test Pipeline",
            status="SUCCESS",
            environment_name=None,
            project_id=None,
        )
        pipeline = MatillionPipeline(name="Test Pipeline")

        properties = source._build_step_dpi_properties(step, execution, pipeline)

        assert "environment_name" not in properties.customProperties
        assert "project_id" not in properties.customProperties


class TestBuildChildPipelineUpstreamInstances:
    def test_no_child_pipeline(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            child_pipeline=None,
        )
        project = MatillionProject(id="proj-1", name="Test Project")
        properties = DataProcessInstancePropertiesClass(
            name="test",
            created=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            customProperties={},
        )
        steps_cache: Dict[str, List[MatillionPipelineExecutionStepResult]] = {}

        upstream = source._build_child_pipeline_upstream_instances(
            step, project, properties, steps_cache
        )

        assert upstream == []
        assert "child_pipeline_id" not in properties.customProperties

    def test_child_pipeline_missing_id(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            child_pipeline={"name": "Child Pipeline"},
        )
        project = MatillionProject(id="proj-1", name="Test Project")
        properties = DataProcessInstancePropertiesClass(
            name="test",
            created=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            customProperties={},
        )
        steps_cache: Dict[str, List[MatillionPipelineExecutionStepResult]] = {}

        upstream = source._build_child_pipeline_upstream_instances(
            step, project, properties, steps_cache
        )

        assert upstream == []
        assert "child_pipeline_id" not in properties.customProperties

    def test_child_pipeline_not_in_cache(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            child_pipeline={"id": "child-exec-1", "name": "Child Pipeline"},
        )
        project = MatillionProject(id="proj-1", name="Test Project")
        properties = DataProcessInstancePropertiesClass(
            name="test",
            created=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            customProperties={},
        )
        steps_cache: Dict[str, List[MatillionPipelineExecutionStepResult]] = {}

        upstream = source._build_child_pipeline_upstream_instances(
            step, project, properties, steps_cache
        )

        assert upstream == []
        assert properties.customProperties["child_pipeline_id"] == "child-exec-1"
        assert properties.customProperties["child_pipeline_name"] == "Child Pipeline"

    def test_child_pipeline_with_empty_steps_list(
        self, source: MatillionSource
    ) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            child_pipeline={"id": "child-exec-1", "name": "Child Pipeline"},
        )
        project = MatillionProject(id="proj-1", name="Test Project")
        properties = DataProcessInstancePropertiesClass(
            name="test",
            created=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            customProperties={},
        )
        steps_cache: Dict[str, List[MatillionPipelineExecutionStepResult]] = {
            "child-exec-1": []
        }

        upstream = source._build_child_pipeline_upstream_instances(
            step, project, properties, steps_cache
        )

        assert upstream == []
        assert properties.customProperties["child_pipeline_id"] == "child-exec-1"

    def test_child_pipeline_with_single_step(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            child_pipeline={"id": "child-exec-1", "name": "Child Pipeline"},
        )
        project = MatillionProject(id="proj-1", name="Test Project")
        properties = DataProcessInstancePropertiesClass(
            name="test",
            created=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            customProperties={},
        )
        child_step = MatillionPipelineExecutionStepResult(
            id="child-step-1",
            name="Child Step",
            result={"status": "SUCCESS"},
        )
        steps_cache: Dict[str, List[MatillionPipelineExecutionStepResult]] = {
            "child-exec-1": [child_step]
        }

        upstream = source._build_child_pipeline_upstream_instances(
            step, project, properties, steps_cache
        )

        assert len(upstream) == 1
        assert "urn:li:dataProcessInstance:" in upstream[0]
        # URNs use GUID hashing, so we can't assert the step ID is in the URN string
        assert properties.customProperties["child_pipeline_id"] == "child-exec-1"

    def test_child_pipeline_with_multiple_steps(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            child_pipeline={"id": "child-exec-1", "name": "Child Pipeline"},
        )
        project = MatillionProject(id="proj-1", name="Test Project")
        properties = DataProcessInstancePropertiesClass(
            name="test",
            created=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            customProperties={},
        )
        child_steps = [
            MatillionPipelineExecutionStepResult(
                id="child-step-1", name="Child Step 1", result={"status": "SUCCESS"}
            ),
            MatillionPipelineExecutionStepResult(
                id="child-step-2", name="Child Step 2", result={"status": "SUCCESS"}
            ),
            MatillionPipelineExecutionStepResult(
                id="child-step-3", name="Child Step 3", result={"status": "SUCCESS"}
            ),
        ]
        steps_cache: Dict[str, List[MatillionPipelineExecutionStepResult]] = {
            "child-exec-1": child_steps
        }

        upstream = source._build_child_pipeline_upstream_instances(
            step, project, properties, steps_cache
        )

        assert len(upstream) == 3
        for urn in upstream:
            assert "urn:li:dataProcessInstance:" in urn
            # URNs use GUID hashing, so we verify format only

    def test_child_pipeline_without_name(self, source: MatillionSource) -> None:
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            child_pipeline={"id": "child-exec-1"},
        )
        project = MatillionProject(id="proj-1", name="Test Project")
        properties = DataProcessInstancePropertiesClass(
            name="test",
            created=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            customProperties={},
        )
        child_step = MatillionPipelineExecutionStepResult(
            id="child-step-1", name="Child Step", result={"status": "SUCCESS"}
        )
        steps_cache: Dict[str, List[MatillionPipelineExecutionStepResult]] = {
            "child-exec-1": [child_step]
        }

        upstream = source._build_child_pipeline_upstream_instances(
            step, project, properties, steps_cache
        )

        assert upstream == []
        assert properties.customProperties["child_pipeline_id"] == "child-exec-1"
        assert "child_pipeline_name" not in properties.customProperties


class TestBuildAndEmitRunEvent:
    def test_normal_case_with_finished_at(self, source: MatillionSource) -> None:
        dpi_urn = "urn:li:dataProcessInstance:test"
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={
                "status": "SUCCESS",
                "finishedAt": "2024-01-01T10:05:00Z",
            },
        )

        workunits = list(source._build_and_emit_run_event(dpi_urn, step, "exec-1"))

        assert len(workunits) == 1
        workunit = workunits[0]
        mcp_wrapper = workunit.metadata
        assert hasattr(mcp_wrapper, "entityUrn")
        assert mcp_wrapper.entityUrn == dpi_urn  # type: ignore
        run_event = mcp_wrapper.aspect  # type: ignore
        assert isinstance(run_event, DataProcessInstanceRunEventClass)
        assert run_event.timestampMillis > 0
        assert run_event.result
        assert run_event.result.type == RunResultTypeClass.SUCCESS
        assert run_event.result.nativeResultType == "SUCCESS"

    def test_null_finished_at_returns_nothing(self, source: MatillionSource) -> None:
        dpi_urn = "urn:li:dataProcessInstance:test"
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={
                "status": "RUNNING",
                "startedAt": "2024-01-01T10:00:00Z",
            },
        )

        workunits = list(source._build_and_emit_run_event(dpi_urn, step, "exec-1"))

        assert len(workunits) == 0

    def test_missing_result_dict(self, source: MatillionSource) -> None:
        dpi_urn = "urn:li:dataProcessInstance:test"
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result=None,
        )

        workunits = list(source._build_and_emit_run_event(dpi_urn, step, "exec-1"))

        assert len(workunits) == 0

    @pytest.mark.parametrize(
        "status,expected_result_type",
        [
            ("SUCCESS", RunResultTypeClass.SUCCESS),
            ("FAILED", RunResultTypeClass.FAILURE),
            ("CANCELLED", RunResultTypeClass.SKIPPED),
            ("UNKNOWN_STATUS", RunResultTypeClass.SKIPPED),
        ],
    )
    def test_status_to_result_type_mapping(
        self, source: MatillionSource, status: str, expected_result_type: str
    ) -> None:
        dpi_urn = "urn:li:dataProcessInstance:test"
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={
                "status": status,
                "finishedAt": "2024-01-01T10:05:00Z",
            },
        )

        workunits = list(source._build_and_emit_run_event(dpi_urn, step, "exec-1"))

        assert len(workunits) == 1
        mcp_wrapper = workunits[0].metadata
        run_event = mcp_wrapper.aspect  # type: ignore
        assert isinstance(run_event, DataProcessInstanceRunEventClass)
        assert run_event.result
        assert run_event.result.type == expected_result_type
        assert run_event.result.nativeResultType == status

    def test_invalid_timestamp_format(self, source: MatillionSource) -> None:
        dpi_urn = "urn:li:dataProcessInstance:test"
        step = MatillionPipelineExecutionStepResult(
            id="step-1",
            name="Extract Data",
            result={
                "status": "SUCCESS",
                "finishedAt": "invalid-timestamp",
            },
        )

        workunits = list(source._build_and_emit_run_event(dpi_urn, step, "exec-1"))

        assert len(workunits) == 0
