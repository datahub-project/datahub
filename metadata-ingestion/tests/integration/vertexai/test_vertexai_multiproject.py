import logging
from typing import Any, Dict, List, Optional
from unittest.mock import call, patch

import pytest
from google.api_core.exceptions import (
    GoogleAPICallError,
    PermissionDenied,
    ResourceExhausted,
)

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.gcp_project_utils import (
    GCPProject,
    GCPProjectDiscoveryError,
    _is_rate_limit_error,
)
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
    project_id: Optional[str] = None,
    project_labels: Optional[List[str]] = None,
    project_id_pattern: Optional[AllowDenyPattern] = None,
    region: str = "us-central1",
) -> VertexAISource:
    kwargs: Dict[str, Any] = {"region": region}
    if project_ids is not None:
        kwargs["project_ids"] = project_ids
    if project_id is not None:
        kwargs["project_id"] = project_id
    if project_labels is not None:
        kwargs["project_labels"] = project_labels
    if project_id_pattern is not None:
        kwargs["project_id_pattern"] = project_id_pattern
    return VertexAISource(ctx=pipeline_ctx, config=VertexAIConfig(**kwargs))


class TestVertexAIMultiProjectIntegration:
    def test_multi_project_calls_init_per_project(self, mock_aiplatform, pipeline_ctx):
        source = make_source(
            pipeline_ctx, project_ids=["project-a", "project-b", "project-c"]
        )
        list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 3
        calls = mock_aiplatform["init"].call_args_list
        assert calls[0] == call(project="project-a", location="us-central1")
        assert calls[1] == call(project="project-b", location="us-central1")
        assert calls[2] == call(project="project-c", location="us-central1")

    def test_partial_failure_continues_for_discovered_projects(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        mock_project_discovery.return_value = [
            GCPProject(id="project-a", name="Project A"),
            GCPProject(id="project-b", name="Project B"),
            GCPProject(id="project-c", name="Project C"),
        ]

        def init_side_effect(**kwargs):
            if kwargs["project"] == "project-a":
                raise PermissionDenied("No access to project-a")

        mock_aiplatform["init"].side_effect = init_side_effect
        source = make_source(pipeline_ctx, project_labels=["env:prod"])
        list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 3
        assert len(source.report.failures) >= 1
        assert any("project-a" in str(f) for f in source.report.failures)

    def test_permission_denied_fails_fast_for_explicit_ids(
        self, mock_aiplatform, pipeline_ctx
    ):
        mock_aiplatform["init"].side_effect = PermissionDenied("No access to project")
        source = make_source(pipeline_ctx, project_ids=["project-a", "project-b"])

        with pytest.raises(RuntimeError, match="Permission denied for project"):
            list(source.get_workunits())

    def test_all_projects_fail_raises_runtime_error(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        mock_project_discovery.return_value = [
            GCPProject(id="project-a", name="Project A"),
            GCPProject(id="project-b", name="Project B"),
        ]
        mock_aiplatform["init"].side_effect = GoogleAPICallError("API error")
        source = make_source(pipeline_ctx, project_labels=["env:prod"])

        with pytest.raises(RuntimeError, match="All.*projects failed"):
            list(source.get_workunits())

    def test_pattern_filtering_applied(self, mock_aiplatform, pipeline_ctx):
        source = make_source(
            pipeline_ctx,
            project_ids=["prod-project-1", "dev-project", "prod-project-2"],
            project_id_pattern=AllowDenyPattern(allow=["prod-.*"]),
        )
        list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 2
        projects_called = [
            c.kwargs["project"] for c in mock_aiplatform["init"].call_args_list
        ]
        assert "prod-project-1" in projects_called
        assert "prod-project-2" in projects_called
        assert "dev-project" not in projects_called

    def test_deny_pattern_excludes_projects(self, mock_aiplatform, pipeline_ctx):
        source = make_source(
            pipeline_ctx,
            project_ids=["ml-prod", "ml-staging", "ml-test"],
            project_id_pattern=AllowDenyPattern(deny=[".*-test$", ".*-staging$"]),
            region="us-west2",
        )
        list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 1
        assert mock_aiplatform["init"].call_args.kwargs["project"] == "ml-prod"

    def test_auto_discovery_with_mocked_projects(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        mock_project_discovery.return_value = [
            GCPProject(id="discovered-proj-1", name="Discovered 1"),
            GCPProject(id="discovered-proj-2", name="Discovered 2"),
        ]
        source = make_source(pipeline_ctx)
        list(source.get_workunits())

        mock_project_discovery.assert_called_once()
        assert mock_aiplatform["init"].call_count == 2

    def test_config_validation_fails_when_all_filtered(self):
        with pytest.raises(ValueError, match="filtered out"):
            VertexAIConfig(
                project_ids=["project-a", "project-b"],
                project_id_pattern=AllowDenyPattern(allow=["nonexistent-.*"]),
                region="us-central1",
            )

    def test_backward_compatibility_single_project_id(
        self, mock_aiplatform, pipeline_ctx
    ):
        source = make_source(pipeline_ctx, project_id="legacy-project")
        list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 1
        assert mock_aiplatform["init"].call_args.kwargs["project"] == "legacy-project"

    def test_warning_when_both_project_id_and_project_ids_provided(
        self, mock_aiplatform, caplog
    ):
        with caplog.at_level(logging.WARNING):
            VertexAIConfig(
                project_id="old-project",
                project_ids=["new-project-1", "new-project-2"],
                region="us-central1",
            )

        assert "project_id" in caplog.text
        assert "ignoring deprecated" in caplog.text

    def test_deny_pattern_with_auto_discovery(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        mock_project_discovery.return_value = [
            GCPProject(id="prod-ml-east", name="Prod ML East")
        ]
        source = make_source(
            pipeline_ctx, project_id_pattern=AllowDenyPattern(deny=["dev-.*"])
        )
        list(source.get_workunits())

        mock_project_discovery.assert_called_once()
        assert mock_project_discovery.call_args.kwargs["project_id_pattern"] is not None

    def test_restrictive_allow_pattern_with_auto_discovery_rejected(self):
        with pytest.raises(
            ValueError, match="Auto-discovery with restrictive allow patterns"
        ):
            VertexAIConfig(
                project_id_pattern=AllowDenyPattern(allow=["prod-.*"]),
                region="us-central1",
            )

    def test_project_container_urn_includes_project_id(
        self, mock_aiplatform, pipeline_ctx
    ):
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

    def test_empty_project_ids_triggers_auto_discovery(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        mock_project_discovery.return_value = [
            GCPProject(id="auto-discovered-project", name="Auto Discovered")
        ]
        source = make_source(pipeline_ctx, project_ids=[])
        list(source.get_workunits())

        mock_project_discovery.assert_called_once()
        assert mock_aiplatform["init"].call_count == 1
        assert (
            mock_aiplatform["init"].call_args.kwargs["project"]
            == "auto-discovered-project"
        )


class TestAutoDiscoveryIntegration:
    def test_deny_pattern_passed_to_get_projects(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        mock_project_discovery.return_value = [
            GCPProject(id="prod-ml-east", name="Prod ML East")
        ]
        source = make_source(
            pipeline_ctx, project_id_pattern=AllowDenyPattern(deny=["dev-.*"])
        )
        list(source.get_workunits())

        assert "project_id_pattern" in mock_project_discovery.call_args.kwargs

    def test_all_filtered_by_deny_pattern_raises_error(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        mock_project_discovery.side_effect = GCPProjectDiscoveryError(
            "All projects filtered out by project_id_pattern"
        )
        source = make_source(
            pipeline_ctx, project_id_pattern=AllowDenyPattern(deny=[".*"])
        )

        with pytest.raises(GCPProjectDiscoveryError, match="filtered out"):
            list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 0

    def test_discovery_failure_propagates(
        self, mock_aiplatform, mock_project_discovery, pipeline_ctx
    ):
        mock_project_discovery.side_effect = GCPProjectDiscoveryError(
            "Permission denied"
        )
        source = make_source(pipeline_ctx)

        with pytest.raises(GCPProjectDiscoveryError, match="Permission denied"):
            list(source.get_workunits())

    def test_credentials_not_passed_explicitly_to_init(
        self, mock_aiplatform, pipeline_ctx
    ):
        source = make_source(
            pipeline_ctx, project_ids=["project-1", "project-2", "project-3"]
        )
        list(source.get_workunits())

        assert mock_aiplatform["init"].call_count == 3
        for c in mock_aiplatform["init"].call_args_list:
            assert "credentials" not in c.kwargs


class TestRateLimitRetryBehavior:
    @pytest.mark.parametrize(
        "exception,expected",
        [
            (ResourceExhausted("Quota exceeded"), True),
            (GoogleAPICallError("Quota exceeded"), True),
            (GoogleAPICallError("Rate limit reached"), True),
            (GoogleAPICallError("Permission denied"), False),
            (GoogleAPICallError("Not found"), False),
            (Exception("Some other error"), False),
        ],
    )
    def test_rate_limit_error_detection(self, exception, expected):
        assert _is_rate_limit_error(exception) == expected


class TestCredentialFailureScenarios:
    def test_credential_failure_on_explicit_project_fails_fast(
        self, mock_aiplatform, pipeline_ctx
    ):
        def init_side_effect(**kwargs):
            if kwargs["project"] == "restricted-project":
                raise PermissionDenied("Service account lacks access")

        mock_aiplatform["init"].side_effect = init_side_effect
        source = make_source(
            pipeline_ctx, project_ids=["allowed-project", "restricted-project"]
        )

        with pytest.raises(RuntimeError, match="Permission denied"):
            list(source.get_workunits())
