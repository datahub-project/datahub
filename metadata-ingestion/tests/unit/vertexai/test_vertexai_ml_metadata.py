from unittest.mock import MagicMock, Mock

import pytest

from datahub.ingestion.source.vertexai.vertexai_ml_metadata_helper import (
    MLMetadataHelper,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    LineageMetadata,
    MLMetadataConfig,
)
from datahub.metadata.schema_classes import MLHyperParamClass, MLMetricClass


class TestLineageMetadata:
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


class TestMLMetadataHelper:
    @pytest.fixture
    def mock_metadata_client(self):
        return Mock()

    @pytest.fixture
    def ml_metadata_config(self):
        return MLMetadataConfig(project_id="test-project", region="us-west1")

    @pytest.fixture
    def mock_uri_parser(self):
        mock_parser = MagicMock()
        mock_parser.dataset_urns_from_artifact_uri.side_effect = lambda uri: (
            [f"urn:li:dataset:(urn:li:dataPlatform:gcs,{uri},PROD)"]
            if uri.startswith("gs://")
            else []
        )
        return mock_parser

    @pytest.fixture
    def helper(self, mock_metadata_client, ml_metadata_config, mock_uri_parser):
        return MLMetadataHelper(
            metadata_client=mock_metadata_client,
            config=ml_metadata_config,
            uri_parser=mock_uri_parser,
        )

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
        self, helper, mock_metadata_client, mock_uri_parser
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

        artifact_urns = helper._extract_artifact_lineage(execution_name)

        assert len(artifact_urns.input_urns) == 1
        assert "gs://bucket/input/data" in artifact_urns.input_urns[0]

        assert len(artifact_urns.output_urns) == 1
        assert "gs://bucket/output/model" in artifact_urns.output_urns[0]

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

    def test_execution_cache_loaded_once_for_multiple_jobs(
        self, helper, mock_metadata_client
    ):
        """Bulk execution fetch must happen at most once regardless of job count.

        Without caching, N jobs that miss the targeted display_name filter each
        trigger a full paginated fetch — easily exhausting the 600 RPM quota.
        """
        # Targeted display_name filter returns nothing for both jobs,
        # forcing the fallback to the schema-based bulk fetch.
        schema_execution = Mock()
        schema_execution.name = "exec-1"
        schema_execution.metadata = {"job_name": "job-a"}

        def list_executions_side_effect(request=None, **kwargs):
            filter_str = getattr(request, "filter", "")
            if "display_name=" in filter_str:
                return []
            # Schema-based bulk fetch
            return [schema_execution]

        mock_metadata_client.list_executions.side_effect = list_executions_side_effect

        job_a = Mock()
        job_a.display_name = "job-a"
        job_a.name = "projects/123/jobs/job-a"

        job_b = Mock()
        job_b.display_name = "job-b"
        job_b.name = "projects/123/jobs/job-b"

        helper._find_executions_for_job(job_a)
        helper._find_executions_for_job(job_b)

        # list_executions called twice for targeted display_name filters (one per
        # job) plus once for the schema-based cache load — never more than 3 total.
        assert mock_metadata_client.list_executions.call_count == 3
