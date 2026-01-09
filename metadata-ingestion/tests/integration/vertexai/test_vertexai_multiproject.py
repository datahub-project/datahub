from typing import Any, Dict, List, Optional
from unittest.mock import call, patch

import pytest
from google.api_core.exceptions import PermissionDenied

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.gcp_project_utils import GCPProject
from datahub.ingestion.source.vertexai.vertexai import VertexAISource
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.metadata.schema_classes import ContainerPropertiesClass


@pytest.fixture
def pipeline_ctx() -> PipelineContext:
    return PipelineContext(run_id="test")


@pytest.fixture
def mock_aiplatform():
    with (
        patch("google.cloud.aiplatform.init") as mock_init,
        patch("google.cloud.aiplatform.Model.list") as mock_models,
        patch("google.cloud.aiplatform.Experiment.list") as mock_experiments,
        patch("google.cloud.aiplatform.PipelineJob.list") as mock_pipelines,
        patch("google.cloud.aiplatform.CustomJob.list") as mock_custom_jobs,
        patch("google.cloud.aiplatform.CustomTrainingJob.list") as mock_ct_jobs,
        patch(
            "google.cloud.aiplatform.CustomContainerTrainingJob.list"
        ) as mock_cct_jobs,
        patch(
            "google.cloud.aiplatform.CustomPythonPackageTrainingJob.list"
        ) as mock_cpp_jobs,
        patch("google.cloud.aiplatform.AutoMLTabularTrainingJob.list") as mock_tabular,
        patch("google.cloud.aiplatform.AutoMLTextTrainingJob.list") as mock_text,
        patch("google.cloud.aiplatform.AutoMLImageTrainingJob.list") as mock_image,
        patch("google.cloud.aiplatform.AutoMLVideoTrainingJob.list") as mock_video,
        patch(
            "google.cloud.aiplatform.AutoMLForecastingTrainingJob.list"
        ) as mock_forecast,
    ):
        for m in [
            mock_models,
            mock_experiments,
            mock_pipelines,
            mock_custom_jobs,
            mock_ct_jobs,
            mock_cct_jobs,
            mock_cpp_jobs,
            mock_tabular,
            mock_text,
            mock_image,
            mock_video,
            mock_forecast,
        ]:
            m.return_value = []
        yield {"init": mock_init}


@pytest.fixture
def mock_project_discovery():
    with (
        patch("datahub.ingestion.source.vertexai.vertexai.get_projects") as mock_get,
        patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client"),
    ):
        yield mock_get


def make_source(
    pipeline_ctx: PipelineContext,
    project_ids: Optional[List[str]] = None,
    project_labels: Optional[List[str]] = None,
    region: str = "us-central1",
) -> VertexAISource:
    kwargs: Dict[str, Any] = {"region": region}
    if project_ids is not None:
        kwargs["project_ids"] = project_ids
    if project_labels is not None:
        kwargs["project_labels"] = project_labels
    return VertexAISource(ctx=pipeline_ctx, config=VertexAIConfig(**kwargs))


class TestMultiProjectIntegration:
    """Integration tests that verify SDK interaction patterns."""

    def test_init_called_per_project(self, mock_aiplatform, pipeline_ctx):
        """Verifies aiplatform.init() is called once per project with correct args."""
        source = make_source(
            pipeline_ctx, project_ids=["project-a", "project-b", "project-c"]
        )
        list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 3
        calls = mock_aiplatform["init"].call_args_list
        assert calls[0] == call(project="project-a", location="us-central1")
        assert calls[1] == call(project="project-b", location="us-central1")
        assert calls[2] == call(project="project-c", location="us-central1")

    def test_auto_discovery_integration(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        """Verifies auto-discovery triggers get_projects() and processes all discovered."""
        mock_project_discovery.return_value = [
            GCPProject(id="discovered-1", name="Discovered 1"),
            GCPProject(id="discovered-2", name="Discovered 2"),
        ]
        source = make_source(pipeline_ctx)
        list(source.get_workunits())

        mock_project_discovery.assert_called_once()
        assert mock_aiplatform["init"].call_count == 2

    def test_project_container_urn_generation(self, mock_aiplatform, pipeline_ctx):
        """Verifies container URNs include project_id in custom properties."""
        source = make_source(pipeline_ctx, project_ids=["my-test-project"])
        workunits = list(source.get_workunits())

        container_wus = [wu for wu in workunits if "container" in wu.id.lower()]
        assert len(container_wus) > 0

        for wu in container_wus:
            if hasattr(wu.metadata, "aspect") and isinstance(
                wu.metadata.aspect, ContainerPropertiesClass
            ):
                props = wu.metadata.aspect
                if props.name == "my-test-project":
                    assert props.customProperties.get("project_id") == "my-test-project"
                    return

        pytest.fail("Container properties with project ID not found")

    def test_partial_failure_continues_processing(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        """Verifies discovered project failures don't stop processing of other projects."""
        mock_project_discovery.return_value = [
            GCPProject(id="project-a", name="A"),
            GCPProject(id="project-b", name="B"),
        ]

        def init_side_effect(**kwargs):
            if kwargs["project"] == "project-a":
                raise PermissionDenied("No access")

        mock_aiplatform["init"].side_effect = init_side_effect
        source = make_source(pipeline_ctx, project_labels=["env:prod"])
        list(source.get_workunits())

        # Both projects attempted, one failure recorded
        assert mock_aiplatform["init"].call_count == 2
        assert len(source.report.failures) >= 1
