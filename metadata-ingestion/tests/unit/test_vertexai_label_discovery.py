from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import GoogleAPICallError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.gcp_project_utils import GCPProject
from datahub.ingestion.source.vertexai.vertexai import VertexAISource
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig


class TestVertexAIProjectDiscovery:
    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_label_based_discovery(
        self, mock_get_projects, mock_get_projects_client, mock_aiplatform_init
    ):
        mock_get_projects.return_value = [
            GCPProject(id="dev-project", name="Dev"),
            GCPProject(id="qa-project", name="QA"),
        ]
        mock_get_projects_client.return_value = MagicMock()

        config = VertexAIConfig(
            project_labels=["environment:dev", "environment:qa"], region="us-west2"
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)
        projects = source._get_projects_to_process()

        assert len(projects) == 2
        assert {p.id for p in projects} == {"dev-project", "qa-project"}

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_empty_label_results(
        self, mock_get_projects, mock_get_projects_client, mock_aiplatform_init
    ):
        mock_get_projects.return_value = []
        mock_get_projects_client.return_value = MagicMock()

        config = VertexAIConfig(
            project_labels=["environment:nonexistent"], region="us-west2"
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

        with pytest.raises(RuntimeError, match="No projects"):
            list(source.get_workunits_internal())

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_project_ids_override_labels(self, mock_get_projects, mock_aiplatform_init):
        mock_get_projects.return_value = [
            GCPProject(id="specific-1", name="S1"),
            GCPProject(id="specific-2", name="S2"),
        ]

        config = VertexAIConfig(
            project_ids=["specific-1", "specific-2"],
            project_labels=["environment:dev"],
            region="us-west2",
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)
        projects = source._get_projects_to_process()

        assert len(projects) == 2
        assert {p.id for p in projects} == {"specific-1", "specific-2"}


class TestVertexAIPatternFiltering:
    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_allow_pattern(self, mock_get_projects, mock_aiplatform_init):
        mock_get_projects.return_value = [
            GCPProject(id="dev-project-1", name="D1"),
            GCPProject(id="prod-project-1", name="P1"),
        ]

        config = VertexAIConfig(
            project_ids=["dev-project-1", "prod-project-1", "test-project-1"],
            project_id_pattern=AllowDenyPattern(allow=["^dev-.*", "^prod-.*"]),
            region="us-west2",
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)
        projects = source._get_projects_to_process()

        assert len(projects) == 2
        assert "test-project-1" not in {p.id for p in projects}

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_deny_pattern(self, mock_get_projects, mock_aiplatform_init):
        mock_get_projects.return_value = [GCPProject(id="dev-project-1", name="D1")]

        config = VertexAIConfig(
            project_ids=["test-project-1", "temp-project-1", "dev-project-1"],
            project_id_pattern=AllowDenyPattern(deny=["^test-.*", "^temp-.*"]),
            region="us-west2",
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)
        projects = source._get_projects_to_process()

        assert len(projects) == 1
        assert projects[0].id == "dev-project-1"

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_label_with_pattern(
        self, mock_get_projects, mock_get_projects_client, mock_aiplatform_init
    ):
        mock_get_projects.return_value = [
            GCPProject(id="dev-project-1", name="D1"),
            GCPProject(id="dev-project-2", name="D2"),
        ]
        mock_get_projects_client.return_value = MagicMock()

        config = VertexAIConfig(
            project_labels=["environment:dev"],
            project_id_pattern=AllowDenyPattern(deny=["^.*-test$"]),
            region="us-west2",
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)
        projects = source._get_projects_to_process()

        assert len(projects) == 2
        assert {p.id for p in projects} == {"dev-project-1", "dev-project-2"}


class TestVertexAIConfigValidation:
    @patch("google.cloud.aiplatform.init")
    def test_invalid_label_format(self, mock_aiplatform_init):
        with pytest.raises(ValueError, match="Invalid project_labels format"):
            VertexAIConfig(project_labels=["env=prod"], region="us-west2")

        with pytest.raises(ValueError, match="Invalid project_labels format"):
            VertexAIConfig(project_labels=["Env:prod"], region="us-west2")

    @patch("google.cloud.aiplatform.init")
    def test_invalid_regex_pattern(self, mock_aiplatform_init):
        with pytest.raises(ValueError, match="Invalid regex"):
            VertexAIConfig(
                project_ids=["test-project"],
                project_id_pattern=AllowDenyPattern(allow=["[invalid"]),
                region="us-west2",
            )

    @patch("google.cloud.aiplatform.init")
    def test_pattern_filters_all_projects(self, mock_aiplatform_init):
        with pytest.raises(ValueError, match="filtered out"):
            VertexAIConfig(
                project_ids=["dev-project-1", "dev-project-2"],
                project_id_pattern=AllowDenyPattern(allow=["^prod-.*"]),
                region="us-west2",
            )

    @patch("google.cloud.aiplatform.init")
    def test_empty_string_in_labels_rejected(self, mock_aiplatform_init):
        with pytest.raises(ValueError, match="empty"):
            VertexAIConfig(
                project_labels=["env:prod", "", "team:ml"], region="us-west2"
            )


class TestVertexAIMultiProject:
    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_multiple_projects(self, mock_get_projects, mock_aiplatform_init):
        mock_get_projects.return_value = [
            GCPProject(id="project-1", name="P1"),
            GCPProject(id="project-2", name="P2"),
        ]

        config = VertexAIConfig(
            project_ids=["project-1", "project-2"], region="us-west2"
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)
        projects = source._get_projects_to_process()

        assert len(projects) == 2
        assert projects[0].id == "project-1"
        assert projects[1].id == "project-2"

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_api_error_handling(
        self, mock_get_projects, mock_get_projects_client, mock_aiplatform_init
    ):
        mock_get_projects.side_effect = Exception("API Error")
        mock_get_projects_client.return_value = MagicMock()

        config = VertexAIConfig(project_labels=["environment:dev"], region="us-west2")
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

        with pytest.raises(Exception, match="API Error"):
            source._get_projects_to_process()


class TestVertexAIParallelism:
    """Tests for parallel resource fetching within projects."""

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_parallel_resource_fetching_enabled(
        self, mock_get_projects, mock_aiplatform_init
    ):
        """Resources are fetched in parallel when max_threads > 1."""
        mock_get_projects.return_value = [GCPProject(id="test-project", name="Test")]
        config = VertexAIConfig(
            project_ids=["test-project"],
            region="us-west2",
            max_threads_resource_parallelism=3,
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

        with patch.object(source, "_process_project_with_parallelism") as mock_parallel:
            mock_parallel.return_value = [MagicMock(id="wu1")]
            workunits = list(source.get_workunits_internal())

        assert len(workunits) == 1
        mock_parallel.assert_called_once()

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_parallel_resource_fetching_with_real_workers(
        self, mock_get_projects, mock_aiplatform_init
    ):
        """Verify parallel execution with actual resource fetchers."""
        mock_get_projects.return_value = [GCPProject(id="test-project", name="Test")]
        config = VertexAIConfig(
            project_ids=["test-project"],
            region="us-west2",
            max_threads_resource_parallelism=5,
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

        with (
            patch.object(source, "_gen_project_workunits") as mock_proj_wu,
            patch.object(source, "_get_ml_models_mcps") as mock_models,
            patch.object(source, "_get_training_jobs_mcps") as mock_jobs,
            patch.object(source, "_get_experiments_workunits") as mock_exp,
            patch.object(source, "_get_experiment_runs_mcps") as mock_runs,
            patch.object(source, "_get_pipelines_mcps") as mock_pipes,
        ):
            mock_proj_wu.return_value = [MagicMock(id="project-wu")]
            mock_models.return_value = [MagicMock(id="model-mcp")]
            mock_jobs.return_value = [MagicMock(id="job-mcp")]
            mock_exp.return_value = [MagicMock(id="exp-wu")]
            mock_runs.return_value = [MagicMock(id="run-mcp")]
            mock_pipes.return_value = [MagicMock(id="pipe-mcp")]

            workunits = list(source.get_workunits_internal())

        assert len(workunits) == 6
        wu_ids = {wu.id for wu in workunits}
        assert "project-wu" in wu_ids
        assert "model-mcp" in wu_ids
        assert "job-mcp" in wu_ids
        assert "exp-wu" in wu_ids
        assert "run-mcp" in wu_ids
        assert "pipe-mcp" in wu_ids

        mock_models.assert_called_once()
        mock_jobs.assert_called_once()
        mock_exp.assert_called_once()
        mock_runs.assert_called_once()
        mock_pipes.assert_called_once()

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_parallel_error_handling(self, mock_get_projects, mock_aiplatform_init):
        """API errors in one resource type don't stop others from processing."""
        mock_get_projects.return_value = [GCPProject(id="test-project", name="Test")]
        config = VertexAIConfig(
            project_ids=["test-project"],
            region="us-west2",
            max_threads_resource_parallelism=3,
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

        with (
            patch.object(source, "_gen_project_workunits") as mock_proj_wu,
            patch.object(source, "_get_ml_models_mcps") as mock_models,
            patch.object(source, "_get_training_jobs_mcps") as mock_jobs,
            patch.object(source, "_get_experiments_workunits") as mock_exp,
            patch.object(source, "_get_experiment_runs_mcps") as mock_runs,
            patch.object(source, "_get_pipelines_mcps") as mock_pipes,
        ):
            mock_proj_wu.return_value = [MagicMock(id="project-wu")]
            mock_models.side_effect = GoogleAPICallError("Models API failure")
            mock_jobs.return_value = [MagicMock(id="job-mcp")]
            mock_exp.return_value = [MagicMock(id="exp-wu")]
            mock_runs.return_value = [MagicMock(id="run-mcp")]
            mock_pipes.return_value = [MagicMock(id="pipe-mcp")]

            workunits = list(source.get_workunits_internal())

        assert len(workunits) == 5
        wu_ids = {wu.id for wu in workunits}
        assert "project-wu" in wu_ids
        assert "job-mcp" in wu_ids
        assert "exp-wu" in wu_ids
        assert "run-mcp" in wu_ids
        assert "pipe-mcp" in wu_ids
        assert "model-mcp" not in wu_ids

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_sequential_programming_error_fails_fast(
        self, mock_get_projects, mock_aiplatform_init
    ):
        """Programming errors (TypeError, etc.) fail fast in sequential mode."""
        mock_get_projects.return_value = [GCPProject(id="test-project", name="Test")]
        config = VertexAIConfig(
            project_ids=["test-project"],
            region="us-west2",
            max_threads_resource_parallelism=1,
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

        with (
            patch.object(source, "_gen_project_workunits") as mock_proj_wu,
            patch.object(source, "_get_ml_models_mcps") as mock_models,
            patch.object(source, "_get_training_jobs_mcps"),
            patch.object(source, "_get_experiments_workunits"),
            patch.object(source, "_get_experiment_runs_mcps"),
            patch.object(source, "_get_pipelines_mcps"),
        ):
            mock_proj_wu.return_value = [MagicMock(id="project-wu")]
            mock_models.side_effect = TypeError("Programming bug: unexpected type")

            with pytest.raises(TypeError, match="Programming bug"):
                list(source.get_workunits_internal())

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_sequential_processing_when_parallelism_disabled(
        self, mock_get_projects, mock_aiplatform_init
    ):
        """Resources are processed sequentially when max_threads = 1."""
        mock_get_projects.return_value = [GCPProject(id="test-project", name="Test")]
        config = VertexAIConfig(
            project_ids=["test-project"],
            region="us-west2",
            max_threads_resource_parallelism=1,
        )
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

        with patch.object(source, "_process_current_project") as mock_sequential:
            mock_sequential.return_value = [MagicMock(id="wu1")]
            workunits = list(source.get_workunits_internal())

        assert len(workunits) == 1
        mock_sequential.assert_called_once()

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_default_parallelism_is_sequential(
        self, mock_get_projects, mock_aiplatform_init
    ):
        """Default config uses sequential processing for backward compatibility."""
        mock_get_projects.return_value = [GCPProject(id="test-project", name="Test")]
        config = VertexAIConfig(project_ids=["test-project"], region="us-west2")
        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

        assert config.max_threads_resource_parallelism == 1

        with patch.object(source, "_process_current_project") as mock_sequential:
            mock_sequential.return_value = [MagicMock(id="wu1")]
            workunits = list(source.get_workunits_internal())

        assert len(workunits) == 1
        mock_sequential.assert_called_once()
