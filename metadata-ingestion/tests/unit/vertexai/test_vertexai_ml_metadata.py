from typing import Any, List
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.vertexai.ml_metadata_helper import MLMetadataHelper
from datahub.ingestion.source.vertexai.vertexai_constants import (
    HyperparameterPatterns,
    IngestionLimits,
    MetricPatterns,
    MLMetadataDefaults,
    URIPatterns,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    ExecutionMetadata,
    LineageMetadata,
    MLMetadataConfig,
)
from datahub.metadata.schema_classes import MLHyperParamClass, MLMetricClass


class TestHyperparameterPatterns:
    @pytest.mark.parametrize(
        "param_name",
        [
            "learning_rate",
            "LEARNING_RATE",
            "batch_size",
            "epochs",
            "optimizer",
            "dropout",
            "param_custom_value",
            "hp_learning_rate",
            "hyperparameter_epochs",
            "hyper_param_batch_size",
            "BATCH_SIZE",
            "Learning_Rate",
            "PARAM_CUSTOM",
        ],
    )
    def test_hyperparameter_matches(self, param_name):
        assert HyperparameterPatterns.is_hyperparam(param_name)

    @pytest.mark.parametrize(
        "param_name",
        [
            "accuracy",
            "loss",
            "random_field",
            "",
        ],
    )
    def test_non_hyperparameter_matches(self, param_name):
        assert not HyperparameterPatterns.is_hyperparam(param_name)


class TestMetricPatterns:
    @pytest.mark.parametrize(
        "metric_name",
        [
            "accuracy",
            "loss",
            "precision",
            "recall",
            "f1_score",
            "auc",
            "rmse",
            "metric_custom",
            "eval_accuracy",
            "test_precision",
            "train_loss",
            "val_accuracy",
            "custom_score",
            "my_metric",
            "cross_entropy_loss",
            "model_accuracy",
            "ACCURACY",
            "Val_Loss",
            "METRIC_CUSTOM",
        ],
    )
    def test_metric_matches(self, metric_name):
        assert MetricPatterns.is_metric(metric_name)

    @pytest.mark.parametrize(
        "metric_name",
        [
            "learning_rate",
            "batch_size",
            "random_field",
            "",
        ],
    )
    def test_non_metric_matches(self, metric_name):
        assert not MetricPatterns.is_metric(metric_name)


class TestURIPatterns:
    @pytest.mark.parametrize(
        "uri",
        [
            "gs://bucket/path/to/data",
            "gs://my-bucket/file.csv",
            "bq://project.dataset.table",
            "bq://my-project.my_dataset.my_table",
            "projects/123/locations/us/datasets/456",
        ],
    )
    def test_valid_uri_patterns(self, uri):
        assert URIPatterns.looks_like_uri(uri)

    @pytest.mark.parametrize(
        "uri",
        [
            "some_string",
            "http://example.com",
            "",
        ],
    )
    def test_invalid_uri_patterns(self, uri):
        assert not URIPatterns.looks_like_uri(uri)

    @pytest.mark.parametrize(
        "field_name",
        [
            "input_data",
            "source_path",
            "training_data",
            "train_data",
        ],
    )
    def test_input_field_patterns(self, field_name):
        assert URIPatterns.is_input_like(field_name)

    @pytest.mark.parametrize(
        "field_name",
        [
            "output_path",
            "destination",
            "model_output",
            "artifact_path",
        ],
    )
    def test_output_field_patterns(self, field_name):
        assert URIPatterns.is_output_like(field_name)


class TestIngestionLimits:
    @pytest.mark.parametrize(
        "default_limit",
        [
            IngestionLimits.DEFAULT_MAX_MODELS,
            IngestionLimits.DEFAULT_MAX_TRAINING_JOBS_PER_TYPE,
            IngestionLimits.DEFAULT_MAX_EXPERIMENTS,
            IngestionLimits.DEFAULT_MAX_RUNS_PER_EXPERIMENT,
            IngestionLimits.DEFAULT_MAX_EVALUATIONS_PER_MODEL,
        ],
    )
    def test_defaults_are_positive(self, default_limit):
        assert default_limit > 0

    @pytest.mark.parametrize(
        "absolute_max,default",
        [
            (IngestionLimits.ABSOLUTE_MAX_MODELS, IngestionLimits.DEFAULT_MAX_MODELS),
            (
                IngestionLimits.ABSOLUTE_MAX_TRAINING_JOBS_PER_TYPE,
                IngestionLimits.DEFAULT_MAX_TRAINING_JOBS_PER_TYPE,
            ),
            (
                IngestionLimits.ABSOLUTE_MAX_EXPERIMENTS,
                IngestionLimits.DEFAULT_MAX_EXPERIMENTS,
            ),
            (
                IngestionLimits.ABSOLUTE_MAX_RUNS_PER_EXPERIMENT,
                IngestionLimits.DEFAULT_MAX_RUNS_PER_EXPERIMENT,
            ),
            (
                IngestionLimits.ABSOLUTE_MAX_EVALUATIONS_PER_MODEL,
                IngestionLimits.DEFAULT_MAX_EVALUATIONS_PER_MODEL,
            ),
        ],
    )
    def test_absolute_max_greater_than_or_equal_to_defaults(
        self, absolute_max, default
    ):
        assert absolute_max >= default


class TestLineageMetadata:
    """Test LineageMetadata model."""

    def test_initialization(self):
        lineage = LineageMetadata()
        assert lineage.input_urns == []
        assert lineage.output_urns == []
        assert lineage.hyperparams == []
        assert lineage.metrics == []

    def test_merge(self):
        lineage1 = LineageMetadata(
            input_urns=["urn1", "urn2"],
            output_urns=["urn3"],
            hyperparams=[MLHyperParamClass(name="lr", value="0.001")],
            metrics=[MLMetricClass(name="accuracy", value="0.95")],
        )
        lineage2 = LineageMetadata(
            input_urns=["urn4"],
            output_urns=["urn5", "urn6"],
            hyperparams=[MLHyperParamClass(name="batch_size", value="32")],
            metrics=[MLMetricClass(name="loss", value="0.1")],
        )

        lineage1.merge(lineage2)

        assert len(lineage1.input_urns) == 3
        assert len(lineage1.output_urns) == 3
        assert len(lineage1.hyperparams) == 2
        assert len(lineage1.metrics) == 2

    def test_deduplicate(self):
        lineage = LineageMetadata(
            input_urns=["urn1", "urn1", "urn2"],
            output_urns=["urn3", "urn3"],
            hyperparams=[
                MLHyperParamClass(name="lr", value="0.001"),
                MLHyperParamClass(name="lr", value="0.002"),
            ],
            metrics=[
                MLMetricClass(name="accuracy", value="0.95"),
                MLMetricClass(name="accuracy", value="0.96"),
            ],
        )

        lineage.deduplicate()

        assert set(lineage.input_urns) == {"urn1", "urn2"}
        assert set(lineage.output_urns) == {"urn3"}
        assert len(lineage.hyperparams) == 1
        assert lineage.hyperparams[0].value == "0.002"
        assert len(lineage.metrics) == 1
        assert lineage.metrics[0].value == "0.96"


class TestExecutionMetadata:
    """Test ExecutionMetadata model."""

    def test_initialization(self):
        exec_meta = ExecutionMetadata(execution_name="test-execution")
        assert exec_meta.execution_name == "test-execution"
        assert exec_meta.hyperparams == []
        assert exec_meta.metrics == []
        assert exec_meta.input_artifact_urns == []
        assert exec_meta.output_artifact_urns == []
        assert exec_meta.custom_properties == {}


class TestMLMetadataConfig:
    """Test MLMetadataConfig model."""

    def test_initialization(self):
        config = MLMetadataConfig(project_id="test-project", region="us-west1")
        assert config.project_id == "test-project"
        assert config.region == "us-west1"
        assert config.metadata_store == "default"
        assert config.enable_lineage_extraction
        assert config.enable_metrics_extraction

    def test_get_parent_path(self):
        config = MLMetadataConfig(project_id="test-project", region="us-west1")
        expected = "projects/test-project/locations/us-west1/metadataStores/default"
        assert config.get_parent_path() == expected

    def test_custom_metadata_store(self):
        config = MLMetadataConfig(
            project_id="test-project",
            region="us-west1",
            metadata_store="custom-store",
        )
        expected = (
            "projects/test-project/locations/us-west1/metadataStores/custom-store"
        )
        assert config.get_parent_path() == expected

    def test_path_template_constant_format(self):
        template = MLMetadataDefaults.METADATA_STORE_PATH_TEMPLATE
        assert "{project_id}" in template
        assert "{region}" in template
        assert "{metadata_store}" in template
        assert template.startswith("projects/")
        assert "/locations/" in template
        assert "/metadataStores/" in template


class TestMLMetadataHelper:
    """Test MLMetadataHelper class."""

    @pytest.fixture
    def mock_metadata_client(self):
        return Mock()

    @pytest.fixture
    def ml_metadata_config(self):
        return MLMetadataConfig(project_id="test-project", region="us-west1")

    @pytest.fixture
    def mock_dataset_converter(self):
        def converter(uri: str) -> List[str]:
            if uri.startswith("gs://"):
                return [f"urn:li:dataset:(urn:li:dataPlatform:gcs,{uri},PROD)"]
            return []

        return converter

    @pytest.fixture
    def helper(self, mock_metadata_client, ml_metadata_config, mock_dataset_converter):
        return MLMetadataHelper(
            metadata_client=mock_metadata_client,
            config=ml_metadata_config,
            dataset_urn_converter=mock_dataset_converter,
        )

    def test_initialization(self, helper, mock_metadata_client, ml_metadata_config):
        assert helper.client == mock_metadata_client
        assert helper.config == ml_metadata_config

    def test_extract_hyperparams(self, helper):
        mock_execution = Mock()
        mock_execution.name = "test-execution"

        mock_value_lr = Mock()
        mock_value_lr.string_value = "0.001"

        mock_value_batch = Mock()
        mock_value_batch.number_value = 32

        mock_execution.metadata = {
            "learning_rate": mock_value_lr,
            "batch_size": mock_value_batch,
            "some_other_field": Mock(),
        }

        hyperparams = helper._extract_hyperparams(mock_execution)

        assert len(hyperparams) == 2
        param_names = {hp.name for hp in hyperparams}
        assert "learning_rate" in param_names
        assert "batch_size" in param_names

    def test_extract_metrics(self, helper):
        mock_execution = Mock()
        mock_execution.name = "test-execution"

        mock_value_acc = Mock()
        mock_value_acc.number_value = 0.95

        mock_value_loss = Mock()
        mock_value_loss.number_value = 0.1

        mock_execution.metadata = {
            "accuracy": mock_value_acc,
            "loss": mock_value_loss,
            "some_other_field": Mock(),
        }

        metrics = helper._extract_metrics(mock_execution)

        assert len(metrics) == 2
        metric_names = {m.name for m in metrics}
        assert "accuracy" in metric_names
        assert "loss" in metric_names

    def test_extract_artifact_lineage(
        self, helper, mock_metadata_client, mock_dataset_converter
    ):
        execution_name = "test-execution"

        mock_artifact_input = Mock()
        mock_artifact_input.name = "input-artifact"
        mock_artifact_input.uri = "gs://bucket/input/data"

        mock_artifact_output = Mock()
        mock_artifact_output.name = "output-artifact"
        mock_artifact_output.uri = "gs://bucket/output/model"

        mock_event_input = Mock()
        mock_event_input.artifact = "input-artifact"
        mock_event_input.type_.name = "INPUT"

        mock_event_output = Mock()
        mock_event_output.artifact = "output-artifact"
        mock_event_output.type_.name = "OUTPUT"

        mock_response = Mock()
        mock_response.artifacts = [mock_artifact_input, mock_artifact_output]
        mock_response.events = [mock_event_input, mock_event_output]

        def mock_query_side_effect(request=None, **kwargs):
            return mock_response

        mock_metadata_client.query_execution_inputs_and_outputs.side_effect = (
            mock_query_side_effect
        )

        input_urns, output_urns = helper._extract_artifact_lineage(execution_name)

        assert len(input_urns) == 1
        assert "gs://bucket/input/data" in input_urns[0]

        assert len(output_urns) == 1
        assert "gs://bucket/output/model" in output_urns[0]

    def test_execution_matches_job(self, helper):
        mock_execution = Mock()
        mock_execution.metadata = {"job_name": "my-training-job"}

        assert helper._execution_matches_job(
            mock_execution, "projects/123/my-training-job", "my-training-job"
        )

        assert not helper._execution_matches_job(
            mock_execution,
            "projects/123/different-job",
            "different-job",
        )

    def test_get_job_lineage_metadata_no_executions(self, helper, mock_metadata_client):
        mock_job = Mock()
        mock_job.display_name = "test-job"
        mock_job.name = "projects/123/jobs/456"

        mock_metadata_client.list_executions.return_value = []

        lineage = helper.get_job_lineage_metadata(mock_job)

        assert lineage is None

    def test_get_job_lineage_metadata_with_executions(
        self, helper, mock_metadata_client
    ):
        mock_job = Mock()
        mock_job.display_name = "test-job"
        mock_job.name = "projects/123/jobs/456"

        mock_execution = Mock()
        mock_execution.name = "test-execution"
        mock_execution.metadata = {
            "learning_rate": Mock(string_value="0.001"),
            "accuracy": Mock(number_value=0.95),
        }

        mock_artifact = Mock()
        mock_artifact.name = "input-artifact"
        mock_artifact.uri = "gs://bucket/data"

        mock_event = Mock()
        mock_event.artifact = "input-artifact"
        mock_event.type_.name = "INPUT"

        mock_response = Mock()
        mock_response.artifacts = [mock_artifact]
        mock_response.events = [mock_event]

        def mock_query_side_effect(request=None, **kwargs):
            return mock_response

        mock_metadata_client.list_executions.return_value = [mock_execution]
        mock_metadata_client.query_execution_inputs_and_outputs.side_effect = (
            mock_query_side_effect
        )

        lineage = helper.get_job_lineage_metadata(mock_job)

        assert lineage is not None
        assert len(lineage.input_urns) > 0
        assert len(lineage.hyperparams) == 1
        assert len(lineage.metrics) == 1


@pytest.mark.parametrize(
    "max_limit,expected_limit",
    [
        (None, MLMetadataDefaults.MAX_EXECUTION_SEARCH_RESULTS),
        (50, 50),
        (200, 200),
    ],
)
def test_ml_metadata_config_execution_limits(
    max_limit: int | None,
    expected_limit: int,
) -> None:
    """Test execution search limits configuration"""
    kwargs: dict[str, Any] = {"project_id": "test-project", "region": "us-central1"}
    if max_limit is not None:
        kwargs["max_execution_search_limit"] = max_limit

    config = MLMetadataConfig(**kwargs)
    assert config.max_execution_search_limit == expected_limit
