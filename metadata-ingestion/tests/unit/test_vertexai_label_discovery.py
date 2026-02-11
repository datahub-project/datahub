from unittest.mock import MagicMock, patch

import pytest

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
