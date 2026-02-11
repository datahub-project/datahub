"""Integration tests for Vertex AI ML Metadata with mocked APIs."""

from unittest.mock import Mock, patch

import pytest

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai.vertexai import (
    VertexAIConfig,
    VertexAISource,
)
from datahub.metadata.schema_classes import (
    DataProcessInstancePropertiesClass,
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
        source = source_with_ml_metadata
        labels = {"created_by": "data-scientist-1", "team": "ml-team"}

        actor = source._get_actor_from_labels(labels)

        assert actor == builder.make_user_urn("data-scientist-1")

    def test_actor_from_creator_label(self, source_with_ml_metadata):
        source = source_with_ml_metadata
        labels = {"creator": "john-doe", "env": "prod"}

        actor = source._get_actor_from_labels(labels)

        assert actor == builder.make_user_urn("john-doe")

    def test_actor_from_owner_label(self, source_with_ml_metadata):
        source = source_with_ml_metadata
        labels = {"owner": "ml-ops-team"}

        actor = source._get_actor_from_labels(labels)

        assert actor == builder.make_user_urn("ml-ops-team")

    def test_no_actor_when_no_labels(self, source_with_ml_metadata):
        source = source_with_ml_metadata

        assert source._get_actor_from_labels(None) is None
        assert source._get_actor_from_labels({}) is None

    def test_no_actor_when_no_matching_keys(self, source_with_ml_metadata):
        source = source_with_ml_metadata
        labels = {"team": "ml-team", "env": "prod"}

        assert source._get_actor_from_labels(labels) is None

    def test_priority_order(self, source_with_ml_metadata):
        source = source_with_ml_metadata
        labels = {
            "created_by": "user1",
            "creator": "user2",
            "owner": "user3",
        }

        actor = source._get_actor_from_labels(labels)

        assert actor == builder.make_user_urn("user1")


class TestModelEvaluationIngestion:
    """Test ModelEvaluation ingestion."""

    def test_model_evaluation_mcp_generation(self, source_with_ml_metadata):
        """Test MCP generation for ModelEvaluation."""
        source = source_with_ml_metadata
        mock_model = gen_mock_model(labels={"created_by": "ml-engineer"})

        mock_evaluation = gen_mock_model_evaluation()

        mcps = list(source._gen_model_evaluation_mcps(mock_model, mock_evaluation))

        assert len(mcps) > 0

        dpi_props_mcps = [
            mcp
            for mcp in mcps
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, DataProcessInstancePropertiesClass)
        ]
        assert len(dpi_props_mcps) > 0

        assert isinstance(dpi_props_mcps[0].metadata, MetadataChangeProposalWrapper)
        dpi_props = dpi_props_mcps[0].metadata.aspect
        assert isinstance(dpi_props, DataProcessInstancePropertiesClass)
        assert "Evaluation" in dpi_props.name
        assert dpi_props.created is not None

    def test_model_evaluations_filtering(self, source_with_ml_metadata):
        """Test that max_evaluations_per_model limit is applied."""
        source = source_with_ml_metadata
        source.config.max_evaluations_per_model = 2

        mock_evals = [gen_mock_model_evaluation(i) for i in range(5)]

        mock_model = gen_mock_model(list_model_evaluations_return=mock_evals)

        with patch("google.cloud.aiplatform.Model.list") as mock_list:
            mock_list.return_value = [mock_model]

            mcps = list(source._get_model_evaluations_mcps())

            dpi_mcps = [
                mcp
                for mcp in mcps
                if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
                and isinstance(mcp.metadata.aspect, DataProcessInstancePropertiesClass)
            ]
            assert len(dpi_mcps) == 2


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

        mcps = list(source._get_training_job_mcps(mock_job))

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

        mcps = list(source._gen_model_evaluation_mcps(mock_model, mock_evaluation))

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
        assert "Evaluation" in props.name
        assert props.created is not None
        assert props.created.actor == builder.make_user_urn("ml-engineer")

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

    def test_hyperparameter_extraction_patterns(self, source_with_ml_metadata):
        """Test various hyperparam naming patterns are recognized."""
        source = source_with_ml_metadata

        mock_execution = Mock()
        mock_execution.name = "test-execution"
        mock_execution.metadata = {
            "learning_rate": Mock(string_value="0.001"),
            "param_custom_hp": Mock(string_value="custom_value"),
            "hp_dropout": Mock(number_value=0.2),
            "hyperparameter_momentum": Mock(number_value=0.9),
            "random_field": Mock(string_value="ignored"),
        }

        hyperparams = source._get_execution_hyperparams(mock_execution)

        assert len(hyperparams) == 4

        param_names = {hp.name for hp in hyperparams}
        assert "learning_rate" in param_names
        assert "param_custom_hp" in param_names
        assert "hp_dropout" in param_names
        assert "hyperparameter_momentum" in param_names
        assert "random_field" not in param_names

    def test_metric_extraction_patterns(self, source_with_ml_metadata):
        """Test various metric naming patterns are recognized."""
        source = source_with_ml_metadata

        mock_execution = Mock()
        mock_execution.name = "test-execution"
        mock_execution.metadata = {
            "accuracy": Mock(number_value=0.95),
            "metric_custom": Mock(number_value=0.8),
            "eval_precision": Mock(number_value=0.93),
            "custom_score": Mock(number_value=0.9),
            "val_loss": Mock(number_value=0.1),
            "train_auc": Mock(number_value=0.92),
            "random_field": Mock(string_value="ignored"),
        }

        metrics = source._get_execution_metrics(mock_execution)

        assert len(metrics) == 6

        metric_names = {m.name for m in metrics}
        assert "accuracy" in metric_names
        assert "metric_custom" in metric_names
        assert "eval_precision" in metric_names
        assert "custom_score" in metric_names
        assert "val_loss" in metric_names
        assert "train_auc" in metric_names
        assert "random_field" not in metric_names

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
