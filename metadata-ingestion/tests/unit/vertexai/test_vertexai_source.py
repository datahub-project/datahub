from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.aiplatform import ExperimentRun, PipelineJob
from google.cloud.aiplatform_v1 import PipelineTaskDetail
from google.cloud.aiplatform_v1.types import PipelineJob as PipelineJobType

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai.vertexai import VertexAIConfig, VertexAISource
from datahub.ingestion.source.vertexai.vertexai_models import (
    ExperimentMetadata,
    VertexAIResourceCategoryKey,
)
from datahub.metadata.schema_classes import (
    DataProcessInstancePropertiesClass,
)
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_experiment,
)

PROJECT_ID = "acryl-poc"
REGION = "us-west2"


def get_resource_category_container_urn(source: VertexAISource, category: str) -> str:
    return VertexAIResourceCategoryKey(
        project_id=source._get_project_id(),
        platform=source.platform,
        instance=source.config.platform_instance,
        env=source.config.env,
        category=category,
    ).as_urn()


@pytest.fixture
def source() -> VertexAISource:
    return VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id=PROJECT_ID, region=REGION),
    )


@pytest.mark.parametrize(
    "start_time,end_time,task_name",
    [
        (
            None,
            datetime.now(timezone.utc) - timedelta(days=3, hours=1),
            "incomplete_task",
        ),
        (datetime.now(timezone.utc) - timedelta(days=3), None, "running_task"),
    ],
)
def test_pipeline_task_with_none_timestamps(
    source: VertexAISource,
    start_time: datetime | None,
    end_time: datetime | None,
    task_name: str,
) -> None:
    """Test that pipeline tasks with None start_time or end_time don't crash the ingestion."""
    mock_pipeline_job = MagicMock(spec=PipelineJob)
    mock_pipeline_job.name = f"test_pipeline_{task_name}"
    mock_pipeline_job.display_name = f"stable_pipeline_{task_name}"
    mock_pipeline_job.resource_name = (
        "projects/123/locations/us-central1/pipelineJobs/789"
    )
    mock_pipeline_job.labels = {}
    mock_pipeline_job.create_time = datetime.now(timezone.utc) - timedelta(days=3)
    mock_pipeline_job.update_time = datetime.now(timezone.utc) - timedelta(days=2)
    mock_pipeline_job.location = "us-west2"

    gca_resource = MagicMock(spec=PipelineJobType)
    mock_pipeline_job.gca_resource = gca_resource

    task_detail = MagicMock(spec=PipelineTaskDetail)
    task_detail.task_name = task_name
    task_detail.task_id = 123
    task_detail.state = MagicMock()
    task_detail.start_time = start_time
    task_detail.create_time = datetime.now(timezone.utc) - timedelta(days=3)
    task_detail.end_time = end_time
    task_detail.inputs = {}
    task_detail.outputs = {}

    mock_pipeline_job.task_details = [task_detail]
    gca_resource.pipeline_spec = {
        "root": {
            "dag": {
                "tasks": {
                    task_name: {
                        "componentRef": {"name": f"comp-{task_name}"},
                        "taskInfo": {"name": task_name},
                    }
                }
            }
        }
    }

    with patch(
        "google.cloud.aiplatform.PipelineJob.list", return_value=[mock_pipeline_job]
    ):
        mcps = list(source.pipeline_extractor.get_workunits())
        assert len(mcps) > 0, "Should generate MCPs for pipeline task"


def test_experiment_run_with_none_timestamps(source: VertexAISource) -> None:
    """Test that experiment runs with None create_time/update_time don't crash."""
    mock_exp = gen_mock_experiment()
    source.experiment_extractor.experiments = [
        ExperimentMetadata(experiment=mock_exp, name=mock_exp.name)
    ]

    mock_exp_run = MagicMock(spec=ExperimentRun)
    mock_exp_run.name = "test_run_none_timestamps"
    mock_exp_run.create_time = datetime(2022, 3, 21, 10, 0, 0, tzinfo=timezone.utc)
    mock_exp_run.update_time = datetime(2022, 3, 21, 10, 0, 0, tzinfo=timezone.utc)
    mock_exp_run.get_state.return_value = "COMPLETE"
    mock_exp_run.get_params.return_value = {}
    mock_exp_run.get_metrics.return_value = {}

    mock_execution = MagicMock()
    mock_execution.name = "test_execution"
    mock_execution.create_time = None
    mock_execution.update_time = None
    mock_execution.state = "COMPLETE"
    mock_execution.get_input_artifacts.return_value = []
    mock_execution.get_output_artifacts.return_value = []

    mock_exp_run.get_executions.return_value = [mock_execution]

    with patch("google.cloud.aiplatform.ExperimentRun.list") as mock_list:
        mock_list.return_value = [mock_exp_run]

        actual_mcps = list(source.experiment_extractor.get_experiment_run_workunits())

        run_mcps = [
            mcp
            for mcp in actual_mcps
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, DataProcessInstancePropertiesClass)
            and "test_run_none_timestamps" in mcp.metadata.aspect.name
        ]

        assert len(run_mcps) > 0


@pytest.mark.parametrize(
    "pipeline_name,expected_stable_name",
    [
        (
            "my-pipeline-20241107083959",
            "my-pipeline",
        ),
        (
            "training-pipeline-20240315120000",
            "training-pipeline",
        ),
        (
            "stable-pipeline-without-timestamp",
            "stable-pipeline-without-timestamp",
        ),
        (
            "pipeline-with-date-20240315-but-no-timestamp",
            "pipeline-with-date-20240315-but-no-timestamp",
        ),
    ],
)
def test_pipeline_stable_name_strips_kubeflow_timestamp(
    source: VertexAISource,
    pipeline_name: str,
    expected_stable_name: str,
) -> None:
    """Test that Kubeflow-appended timestamps are stripped from pipeline names."""
    mock_pipeline_job = MagicMock(spec=PipelineJob)
    mock_pipeline_job.name = "test_pipeline_name"
    mock_pipeline_job.display_name = pipeline_name

    stable_name = source.pipeline_extractor._get_stable_pipeline_id(mock_pipeline_job)
    assert stable_name == expected_stable_name, (
        f"Expected {expected_stable_name}, got {stable_name}"
    )


def test_multi_project_urns_are_project_specific() -> None:
    project_1 = "project-alpha"
    project_2 = "project-beta"

    config = VertexAIConfig(
        project_id="fallback-project",
        project_ids=[project_1, project_2],
        region="us-central1",
    )
    source = VertexAISource(
        ctx=PipelineContext(run_id="multi-project-test"),
        config=config,
    )

    assert source._projects == [project_1, project_2]

    source._current_project_id = project_1
    project1_job_urn = source.urn_builder.make_training_job_urn("test-job")
    project1_formatted_name = source.name_formatter.format_model_name("test-model")
    project1_url = source.url_builder.make_job_url("test-job")

    source._current_project_id = project_2
    project2_job_urn = source.urn_builder.make_training_job_urn("test-job")
    project2_formatted_name = source.name_formatter.format_model_name("test-model")
    project2_url = source.url_builder.make_job_url("test-job")

    assert project1_job_urn != project2_job_urn
    assert project_1 in project1_job_urn
    assert project_2 in project2_job_urn

    assert project1_formatted_name != project2_formatted_name
    assert project1_formatted_name == f"{project_1}.model.test-model"
    assert project2_formatted_name == f"{project_2}.model.test-model"

    assert project1_url != project2_url
    assert project_1 in project1_url
    assert project_2 in project2_url


def test_multi_region_urls() -> None:
    config = VertexAIConfig(
        project_id="test-project",
        region="us-west1",
        regions=["us-west1", "europe-west4"],
    )
    source = VertexAISource(
        ctx=PipelineContext(run_id="multi-region-test"),
        config=config,
    )

    source._current_region = "us-west1"
    url_west = source.url_builder.make_model_url("my-model")

    source._current_region = "europe-west4"
    url_europe = source.url_builder.make_model_url("my-model")

    assert url_west != url_europe
    assert "us-west1" in url_west
    assert "europe-west4" in url_europe
