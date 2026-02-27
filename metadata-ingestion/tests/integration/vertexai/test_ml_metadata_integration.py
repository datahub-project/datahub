from unittest.mock import Mock, patch

import pytest

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai.vertexai import (
    VertexAIConfig,
    VertexAISource,
)
from datahub.ingestion.source.vertexai.vertexai_models import TrainingJobMetadata
from datahub.ingestion.source.vertexai.vertexai_utils import get_actor_from_labels
from datahub.metadata.schema_classes import (
    DataProcessInstancePropertiesClass,
    MLModelDeploymentPropertiesClass,
)
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_model,
    gen_mock_model_evaluation,
    gen_mock_training_custom_job,
)

PROJECT_ID = "test-project"
REGION = "us-west1"


@pytest.fixture
def source_with_ml_metadata() -> VertexAISource:
    """Create source with ML Metadata features enabled."""
    return VertexAISource(
        ctx=PipelineContext(run_id="ml-metadata-test"),
        config=VertexAIConfig(
            project_id=PROJECT_ID,
            region=REGION,
            use_ml_metadata_for_lineage=True,
            extract_execution_metrics=True,
            include_evaluations=True,
        ),
    )


class TestActorExtraction:
    """Test actor extraction from resource labels."""

    def test_actor_from_created_by_label(self, source_with_ml_metadata):
        labels = {"created_by": "data-scientist-1", "team": "ml-team"}

        actor = get_actor_from_labels(labels)

        assert actor == builder.make_user_urn("data-scientist-1")

    def test_actor_from_creator_label(self, source_with_ml_metadata):
        labels = {"creator": "john-doe", "env": "prod"}

        actor = get_actor_from_labels(labels)

        assert actor == builder.make_user_urn("john-doe")

    def test_actor_from_owner_label(self, source_with_ml_metadata):
        labels = {"owner": "ml-ops-team"}

        actor = get_actor_from_labels(labels)

        assert actor == builder.make_user_urn("ml-ops-team")

    def test_no_actor_when_no_labels(self, source_with_ml_metadata):
        assert get_actor_from_labels(None) is None
        assert get_actor_from_labels({}) is None

    def test_no_actor_when_no_matching_keys(self, source_with_ml_metadata):
        labels = {"team": "ml-team", "env": "prod"}

        assert get_actor_from_labels(labels) is None

    def test_priority_order(self, source_with_ml_metadata):
        labels = {
            "created_by": "user1",
            "creator": "user2",
            "owner": "user3",
        }

        actor = get_actor_from_labels(labels)

        assert actor == builder.make_user_urn("user1")


class TestModelEvaluationIngestion:
    """Test ModelEvaluation ingestion."""

    def test_model_evaluation_mcp_generation(self, source_with_ml_metadata):
        """Test MCP generation for ModelEvaluation."""
        source = source_with_ml_metadata
        mock_model = gen_mock_model(labels={"created_by": "ml-engineer"})

        mock_evaluation = gen_mock_model_evaluation()

        mcps = list(
            source.model_extractor._gen_model_evaluation_mcps(
                mock_model, mock_evaluation
            )
        )

        assert len(mcps) > 0

        deployment_props_mcps = [
            mcp
            for mcp in mcps
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, MLModelDeploymentPropertiesClass)
        ]
        assert len(deployment_props_mcps) > 0

        assert isinstance(
            deployment_props_mcps[0].metadata, MetadataChangeProposalWrapper
        )
        deployment_props = deployment_props_mcps[0].metadata.aspect
        assert isinstance(deployment_props, MLModelDeploymentPropertiesClass)
        assert deployment_props.createdAt is not None

    def test_model_evaluations_filtering(self, source_with_ml_metadata):
        """Test that max_evaluations_per_model limit is applied."""
        source = source_with_ml_metadata
        source.config.max_evaluations_per_model = 2

        mock_evals = [gen_mock_model_evaluation(i) for i in range(5)]

        mock_model = gen_mock_model(list_model_evaluations_return=mock_evals)

        with patch("google.cloud.aiplatform.Model.list") as mock_list:
            mock_list.return_value = [mock_model]

            mcps = list(source.model_extractor.get_evaluation_workunits())

            deployment_mcps = [
                mcp
                for mcp in mcps
                if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
                and isinstance(mcp.metadata.aspect, MLModelDeploymentPropertiesClass)
            ]
            assert len(deployment_mcps) == 2


class TestMLMetadataIntegration:
    """Integration tests for ML Metadata functionality."""

    @patch("google.cloud.aiplatform.TabularDataset.list")
    @patch("google.cloud.aiplatform.ImageDataset.list")
    @patch("google.cloud.aiplatform.TextDataset.list")
    @patch("google.cloud.aiplatform.VideoDataset.list")
    @patch("google.cloud.aiplatform.TimeSeriesDataset.list")
    def test_complete_customjob_workflow(
        self,
        mock_timeseries_list,
        mock_video_list,
        mock_text_list,
        mock_image_list,
        mock_tabular_list,
        source_with_ml_metadata,
    ):
        """Test complete workflow: CustomJob with actor attribution."""
        # Mock dataset list calls to return empty lists (no datasets)
        mock_tabular_list.return_value = []
        mock_image_list.return_value = []
        mock_text_list.return_value = []
        mock_video_list.return_value = []
        mock_timeseries_list.return_value = []

        source = source_with_ml_metadata
        mock_job = gen_mock_training_custom_job(
            labels={"created_by": "data-scientist", "team": "research"}
        )

        job_metadata = TrainingJobMetadata(job=mock_job)
        mcps = list(source.training_extractor._gen_training_job_mcps(job_metadata))

        assert len(mcps) > 0

        props_mcps = [
            mcp
            for mcp in mcps
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, DataProcessInstancePropertiesClass)
        ]
        assert len(props_mcps) > 0

        assert isinstance(props_mcps[0].metadata, MetadataChangeProposalWrapper)
        props = props_mcps[0].metadata.aspect
        assert isinstance(props, DataProcessInstancePropertiesClass)
        assert props.created.actor == builder.make_user_urn("data-scientist")

    def test_model_evaluation_complete_flow(self, source_with_ml_metadata):
        """Test complete ModelEvaluation ingestion flow."""
        source = source_with_ml_metadata
        mock_model = gen_mock_model(
            labels={
                "created_by": "ml-engineer",
                "project": "fraud-detection",
            }
        )

        mock_evaluation = gen_mock_model_evaluation(
            display_name="Production Validation"
        )

        mcps = list(
            source.model_extractor._gen_model_evaluation_mcps(
                mock_model, mock_evaluation
            )
        )

        assert len(mcps) > 0

        props_mcps = [
            mcp
            for mcp in mcps
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, MLModelDeploymentPropertiesClass)
        ]
        assert len(props_mcps) > 0

        assert isinstance(props_mcps[0].metadata, MetadataChangeProposalWrapper)
        props = props_mcps[0].metadata.aspect
        assert isinstance(props, MLModelDeploymentPropertiesClass)
        assert props.createdAt is not None

    def test_ml_metadata_disabled(self):
        """Test that ML Metadata is not used when disabled in config."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="no-ml-metadata-test"),
            config=VertexAIConfig(
                project_id=PROJECT_ID,
                region=REGION,
                use_ml_metadata_for_lineage=False,
                extract_execution_metrics=False,
            ),
        )

        mock_job = gen_mock_training_custom_job()

        lineage = source._get_job_lineage_from_ml_metadata(mock_job)

        assert lineage is None

    def test_execution_matching(self, source_with_ml_metadata):
        """Test execution to job matching logic using direct method calls."""
        source = source_with_ml_metadata

        source.get_workunits()

        if source._ml_metadata_helper:
            mock_execution_match = Mock()
            mock_execution_match.metadata = {
                "job_resource_name": "projects/test/jobs/my-training-job"
            }

            mock_execution_no_match = Mock()
            mock_execution_no_match.metadata = {"some_field": "unrelated"}

            assert source._ml_metadata_helper._execution_matches_job(
                mock_execution_match,
                "projects/test/jobs/my-training-job",
                "my-training-job",
            )

            assert not source._ml_metadata_helper._execution_matches_job(
                mock_execution_no_match,
                "projects/test/jobs/my-training-job",
                "my-training-job",
            )
