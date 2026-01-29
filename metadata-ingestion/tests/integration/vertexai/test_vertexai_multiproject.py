from typing import Any, Dict, List, Optional
from unittest.mock import call, patch

import pytest
from google.api_core.exceptions import NotFound, PermissionDenied, ResourceExhausted

from datahub.configuration.common import AllowDenyPattern
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
    def test_init_called_per_project(self, mock_aiplatform, pipeline_ctx):
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
        mock_project_discovery.return_value = [
            GCPProject(id="discovered-1", name="Discovered 1"),
            GCPProject(id="discovered-2", name="Discovered 2"),
        ]
        source = make_source(pipeline_ctx)
        list(source.get_workunits())

        mock_project_discovery.assert_called_once()
        assert mock_aiplatform["init"].call_count == 2

    def test_auto_discovery_passes_pattern_to_get_projects(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        """Verify project_id_pattern is passed to get_projects for filtering."""
        mock_project_discovery.return_value = [GCPProject(id="prod-app", name="Prod")]
        config = VertexAIConfig(
            region="us-central1",
            project_id_pattern={"deny": ["dev-.*"]},
        )
        source = VertexAISource(ctx=pipeline_ctx, config=config)
        list(source.get_workunits())

        mock_project_discovery.assert_called_once()
        call_kwargs = mock_project_discovery.call_args.kwargs
        assert "project_id_pattern" in call_kwargs
        pattern = call_kwargs["project_id_pattern"]
        assert isinstance(pattern, AllowDenyPattern)
        assert pattern.deny == ["dev-.*"]

    def test_project_container_urn_generation(self, mock_aiplatform, pipeline_ctx):
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


class TestProjectErrorHandling:
    @pytest.mark.parametrize(
        "exception",
        [NotFound("Project not found"), PermissionDenied("Access denied")],
    )
    def test_project_errors_continue(self, mock_aiplatform, pipeline_ctx, exception):
        def selective_fail(**kwargs):
            if kwargs["project"] == "fail-project":
                raise exception

        mock_aiplatform["init"].side_effect = selective_fail
        source = make_source(pipeline_ctx, project_ids=["fail-project", "ok-project"])
        list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 2
        assert len(source.report.failures) == 1
        assert "fail-project" in str(source.report.failures)

    def test_all_projects_fail_raises(self, mock_aiplatform, pipeline_ctx):
        mock_aiplatform["init"].side_effect = PermissionDenied("No access")
        source = make_source(pipeline_ctx, project_ids=["project-1", "project-2"])

        with pytest.raises(RuntimeError, match="All .* projects failed"):
            list(source.get_workunits())


class TestRateLimitHandling:
    def test_rate_limit_on_single_project_reports_failure(
        self, mock_aiplatform, pipeline_ctx
    ):
        """Rate limit errors on single project should be reported as failures."""
        mock_aiplatform["init"].side_effect = ResourceExhausted("Quota exceeded")
        source = make_source(pipeline_ctx, project_ids=["rate-limited-project"])

        with pytest.raises(RuntimeError, match="All .* projects failed"):
            list(source.get_workunits())

        assert len(source.report.failures) >= 1
        assert "rate-limited-project" in str(source.report.failures)

    def test_rate_limit_on_multi_project_continues(self, mock_aiplatform, pipeline_ctx):
        """Rate limit errors should not stop processing of other projects."""
        call_count = {"project-a": 0, "project-b": 0}

        def rate_limit_first_project(**kwargs):
            project = kwargs["project"]
            call_count[project] = call_count.get(project, 0) + 1
            if project == "project-a":
                raise ResourceExhausted("Quota exceeded")

        mock_aiplatform["init"].side_effect = rate_limit_first_project
        source = make_source(pipeline_ctx, project_ids=["project-a", "project-b"])
        list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 2
        assert len(source.report.failures) >= 1
        assert "project-a" in str(source.report.failures)
