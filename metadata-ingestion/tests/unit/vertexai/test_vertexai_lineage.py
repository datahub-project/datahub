from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.aiplatform import PipelineJob
from google.cloud.aiplatform_v1 import PipelineTaskDetail
from google.cloud.aiplatform_v1.types import Execution, PipelineJob as PipelineJobType

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai.vertexai import VertexAIConfig, VertexAISource
from datahub.ingestion.source.vertexai.vertexai_models import (
    ExperimentMetadata,
    ExperimentRunMetadata,
    ModelMetadata,
)
from datahub.metadata.schema_classes import (
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    TrainingDataClass,
)
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_dataset,
    gen_mock_experiment,
    gen_mock_experiment_run,
    gen_mock_model,
    gen_mock_model_version,
)

PROJECT_ID = "acryl-poc"
REGION = "us-west2"


@pytest.fixture
def source() -> VertexAISource:
    return VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id=PROJECT_ID, region=REGION),
    )


def test_gen_run_execution_edges() -> None:
    """Test that experiment run executions generate correct input/output lineage edges."""

    class _Art:
        def __init__(self, uri: str):
            self.uri = uri

    class _Exec:
        def __init__(self):
            self.create_time = datetime.utcnow()
            self.update_time = self.create_time + timedelta(seconds=5)
            self.state = "COMPLETE"
            self.name = "exec1"

        def get_input_artifacts(self):
            return [_Art("gs://bucket/file"), _Art("bq://p.d.t")]

        def get_output_artifacts(self):
            return [_Art("projects/p/datasets/d/tables/t2")]

    class _Run:
        name = "run1"

        def get_executions(self):
            return []

    class _Exp:
        name = "exp1"
        resource_name = "res"
        dashboard_url = None

    source = VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id="p", region="r"),
    )

    mcps = list(
        source.experiment_extractor._gen_run_execution(
            cast(Any, _Exec()), cast(Any, _Run()), cast(Any, _Exp())
        )
    )

    has_inputs = any(
        getattr(m.metadata.aspect, "inputEdges", None)
        for m in mcps
        if hasattr(m.metadata, "aspect")
        and m.metadata.aspect.__class__.__name__ == "DataProcessInstanceInputClass"
    )
    has_outputs = any(
        getattr(m.metadata.aspect, "outputEdges", None)
        for m in mcps
        if hasattr(m.metadata, "aspect")
        and m.metadata.aspect.__class__.__name__ == "DataProcessInstanceOutputClass"
    )
    assert has_inputs and has_outputs


def test_training_job_external_lineage_edges() -> None:
    """Test that training jobs generate correct external dataset lineage edges."""

    class _Job:
        def __init__(self):
            self.name = "jobs/123"
            self.display_name = "my-job"
            self.create_time = datetime.utcnow()
            self.end_time = self.create_time + timedelta(seconds=10)

        def to_dict(self):
            return {
                "task": {
                    "gcsDestination": {"outputUriPrefix": "gs://bucket/out/"},
                    "bigqueryDestination": {"outputTable": "bq://proj.ds.tbl"},
                    "inputs": {"gcsSource": ["gs://bucket/in/file1"]},
                }
            }

    source = VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id="p", region="r"),
    )

    job = _Job()
    job_meta = source.training_extractor._get_training_job_metadata(cast(Any, job))
    uris = source.uri_parser.extract_external_uris_from_job(cast(Any, job))
    job_meta.external_input_urns = uris.input_urns
    job_meta.external_output_urns = uris.output_urns
    mcps = list(source.training_extractor._gen_training_job_mcps(job_meta))
    has_ext_inputs = any(
        getattr(m.metadata.aspect, "inputEdges", None)
        and any(
            "gcs" in e.destinationUrn or "bigquery" in e.destinationUrn
            for e in cast(Any, m.metadata.aspect).inputEdges
        )
        for m in mcps
        if hasattr(m.metadata, "aspect")
        and m.metadata.aspect.__class__.__name__ == "DataProcessInstanceInputClass"
    )
    has_ext_outputs = any(
        getattr(m.metadata.aspect, "outputEdges", None)
        and any(
            "gcs" in e.destinationUrn or "bigquery" in e.destinationUrn
            for e in cast(Any, m.metadata.aspect).outputEdges
        )
        for m in mcps
        if hasattr(m.metadata, "aspect")
        and m.metadata.aspect.__class__.__name__ == "DataProcessInstanceOutputClass"
    )
    assert has_ext_inputs and has_ext_outputs


def test_model_training_data_lineage(source: VertexAISource) -> None:
    """Test that models have TrainingDataClass aspect with dataset URNs."""

    mock_model = gen_mock_model()
    model_version = gen_mock_model_version(mock_model)
    mock_dataset = gen_mock_dataset()

    training_dataset_urn = builder.make_dataset_urn_with_platform_instance(
        platform=source.platform,
        name=source.name_formatter.format_dataset_name(entity_id=mock_dataset.name),
        platform_instance=source.config.platform_instance,
        env=source.config.env,
    )

    model_meta = ModelMetadata(
        model=mock_model,
        model_version=model_version,
        training_data_urns=[training_dataset_urn],
    )

    actual_mcps = list(source._gen_ml_model_mcps(model_meta))

    training_data_mcps = [
        mcp
        for mcp in actual_mcps
        if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
        and isinstance(mcp.metadata.aspect, TrainingDataClass)
    ]

    assert len(training_data_mcps) == 1
    training_data_mcp = training_data_mcps[0].metadata
    assert isinstance(training_data_mcp, MetadataChangeProposalWrapper)
    training_data_aspect = training_data_mcp.aspect
    assert isinstance(training_data_aspect, TrainingDataClass)
    assert len(training_data_aspect.trainingData) == 1
    assert training_data_aspect.trainingData[0].dataset == training_dataset_urn


def test_model_downstream_lineage_from_pipeline_tasks(source: VertexAISource) -> None:
    """Test that pipeline tasks using models as inputs create downstream lineage."""
    mock_pipeline = MagicMock(spec=PipelineJob)
    mock_pipeline.name = "test_pipeline_with_model_input"
    mock_pipeline.display_name = "stable_pipeline_with_model_input"
    mock_pipeline.resource_name = "projects/123/locations/us-central1/pipelineJobs/999"
    mock_pipeline.labels = {}
    mock_pipeline.create_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_pipeline.update_time = datetime(2024, 1, 2, tzinfo=timezone.utc)
    mock_pipeline.location = "us-central1"

    gca_resource = MagicMock(spec=PipelineJobType)
    mock_pipeline.gca_resource = gca_resource

    task_detail = MagicMock(spec=PipelineTaskDetail)
    task_detail.task_name = "prediction_task"
    task_detail.task_id = 1
    task_detail.state = MagicMock()
    task_detail.start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    task_detail.create_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    task_detail.end_time = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)

    model_artifact = MagicMock()
    model_artifact.uri = "projects/test-project/locations/us-central1/models/123456"

    model_input = MagicMock()
    model_input.artifacts = [model_artifact]

    task_detail.inputs = {"model": model_input}
    task_detail.outputs = {}

    mock_pipeline.task_details = [task_detail]
    gca_resource.pipeline_spec = {
        "root": {
            "dag": {
                "tasks": {
                    "prediction_task": {
                        "componentRef": {"name": "comp-prediction"},
                    }
                }
            }
        }
    }

    # Step 1: Process pipeline - this tracks the resource usage by artifact URI
    with patch("google.cloud.aiplatform.PipelineJob.list") as mock_list:
        mock_list.return_value = [mock_pipeline]
        list(source.pipeline_extractor.get_workunits())

    # Step 2: Simulate model processing to resolve resource name to URN
    resource_name = "projects/test-project/locations/us-central1/models/123456"
    mock_model = MagicMock()
    mock_model.name = "test_model"
    mock_model.resource_name = resource_name

    mock_version = MagicMock()
    mock_version.version_id = "1"

    model_name = source.name_formatter.format_model_name(mock_model.name)
    model_urn = source.urn_builder.make_ml_model_urn(mock_version, model_name)
    model_group_urn = source.urn_builder.make_ml_model_group_urn(mock_model)

    # This is what happens during model processing
    source.model_usage_tracker.resolve_and_track_resource(
        resource_name, model_urn, model_group_urn
    )

    # Step 3: Verify downstream lineage
    downstream_urns = source.model_usage_tracker.get_model_usage(model_urn)
    assert len(downstream_urns) == 1
    assert any("prediction_task" in urn for urn in downstream_urns)


def test_model_downstream_lineage_from_experiment_executions(
    source: VertexAISource,
) -> None:
    """Test that experiment executions using models as inputs create downstream lineage."""
    mock_experiment = gen_mock_experiment()
    mock_run = gen_mock_experiment_run()

    mock_execution = MagicMock(spec=Execution)
    mock_execution.name = "test_execution_with_model"
    mock_execution.state = MagicMock()
    mock_execution.state.name = "COMPLETE"
    mock_execution.create_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_execution.update_time = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)

    model_artifact = MagicMock()
    model_artifact.uri = "projects/test-project/locations/us-central1/models/789"

    dataset_artifact = MagicMock()
    dataset_artifact.uri = "bq://project.dataset.table"

    mock_execution.get_input_artifacts = MagicMock(
        return_value=[model_artifact, dataset_artifact]
    )
    mock_execution.get_output_artifacts = MagicMock(return_value=[])

    with patch.object(mock_run, "get_executions", return_value=[mock_execution]):
        list(
            source.experiment_extractor._gen_experiment_run_mcps(
                ExperimentMetadata(
                    experiment=mock_experiment, name=mock_experiment.name
                ),
                ExperimentRunMetadata(
                    run=mock_run,
                    name=mock_run.name,
                    experiment_name=mock_experiment.name,
                ),
            )
        )

    expected_model_urn = "urn:li:mlModel:(urn:li:dataPlatform:vertexai,789,PROD)"
    downstream_urns = source.model_usage_tracker.get_model_usage(expected_model_urn)
    assert len(downstream_urns) == 1


def test_model_includes_downstream_jobs_in_properties(source: VertexAISource) -> None:
    """Test that models include downstream jobs in their properties."""
    mock_model = gen_mock_model()
    mock_version = MagicMock()
    mock_version.version_id = "1"
    mock_version.version_description = "Test version"
    mock_version.version_create_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_version.version_update_time = datetime(2024, 1, 2, tzinfo=timezone.utc)

    model_name = source.name_formatter.format_model_name(mock_model.name)
    model_urn = source.urn_builder.make_ml_model_urn(mock_version, model_name)

    downstream_job_urn = "urn:li:dataProcessInstance:test_job"
    source.model_usage_tracker.track_model_usage(model_urn, downstream_job_urn)

    model_metadata = ModelMetadata(
        model=mock_model, model_version=mock_version, endpoints=[]
    )

    mcps = list(source._gen_ml_model_mcps(model_metadata))

    model_props_mcp = next(
        (
            mcp
            for mcp in mcps
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, MLModelPropertiesClass)
        ),
        None,
    )

    assert model_props_mcp is not None
    assert isinstance(model_props_mcp.metadata, MetadataChangeProposalWrapper)
    assert isinstance(model_props_mcp.metadata.aspect, MLModelPropertiesClass)

    assert model_props_mcp.metadata.aspect.downstreamJobs is not None
    assert len(model_props_mcp.metadata.aspect.downstreamJobs) > 0
    assert downstream_job_urn in model_props_mcp.metadata.aspect.downstreamJobs


def test_model_group_downstream_lineage_from_pipeline_tasks(
    source: VertexAISource,
) -> None:
    """Test that pipeline tasks using models as inputs create downstream lineage for model groups."""
    mock_pipeline = MagicMock(spec=PipelineJob)
    mock_pipeline.name = "test-pipeline"
    mock_pipeline.display_name = "test-pipeline-display"
    mock_pipeline.resource_name = "projects/test/locations/us-west2/pipelineJobs/12345"
    mock_pipeline.location = "us-west2"
    mock_pipeline.labels = {}
    mock_pipeline.state = MagicMock()
    mock_pipeline.state.value = 3
    mock_pipeline.create_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_pipeline.update_time = datetime(2024, 1, 2, tzinfo=timezone.utc)

    mock_task_detail = MagicMock(spec=PipelineTaskDetail)
    mock_task_detail.task_name = "prediction_task"
    mock_task_detail.task_id = 1
    mock_task_detail.state = PipelineTaskDetail.State.SUCCEEDED
    mock_task_detail.start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_task_detail.create_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_task_detail.end_time = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)

    mock_artifact = MagicMock()
    mock_artifact.uri = "projects/test-project/locations/us-central1/models/123456789"

    mock_input_entry = MagicMock()
    mock_input_entry.artifacts = [mock_artifact]

    mock_task_detail.inputs = {"model": mock_input_entry}
    mock_task_detail.outputs = {}

    # Set up the pipeline spec
    gca_resource = MagicMock(spec=PipelineJobType)
    mock_pipeline.gca_resource = gca_resource
    mock_pipeline.task_details = [mock_task_detail]
    gca_resource.pipeline_spec = {
        "root": {
            "dag": {
                "tasks": {
                    "prediction_task": {
                        "componentRef": {"name": "comp-prediction"},
                    }
                }
            }
        }
    }

    # Step 1: Process pipeline - this tracks resource usage by artifact URI
    with patch("google.cloud.aiplatform.PipelineJob.list") as mock_list:
        mock_list.return_value = [mock_pipeline]
        list(source.pipeline_extractor.get_workunits())

    # Step 2: Simulate model processing to resolve resource name to URNs
    resource_name = "projects/test-project/locations/us-central1/models/123456789"
    mock_model = MagicMock()
    mock_model.name = "test_model_group"
    mock_model.resource_name = resource_name

    mock_version = MagicMock()
    mock_version.version_id = "1"

    model_name = source.name_formatter.format_model_name(mock_model.name)
    model_urn = source.urn_builder.make_ml_model_urn(mock_version, model_name)
    model_group_urn = source.urn_builder.make_ml_model_group_urn(mock_model)

    # This is what happens during model processing
    source.model_usage_tracker.resolve_and_track_resource(
        resource_name, model_urn, model_group_urn
    )

    # Step 3: Verify downstream lineage for model group
    downstream_urns = source.model_usage_tracker.get_model_group_usage(model_group_urn)
    assert len(downstream_urns) == 1
    assert any("prediction_task" in urn for urn in downstream_urns)


def test_model_group_training_jobs_tracked(source: VertexAISource) -> None:
    """Test that model groups track training jobs from model versions."""
    mock_model = gen_mock_model()
    mock_version = MagicMock()
    mock_version.version_id = "1"
    mock_version.version_description = "Test version"
    mock_version.version_create_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_version.version_update_time = datetime(2024, 1, 2, tzinfo=timezone.utc)

    training_job_urn = "urn:li:dataProcessInstance:training_job_123"

    model_metadata = ModelMetadata(
        model=mock_model,
        model_version=mock_version,
        endpoints=[],
        training_job_urn=training_job_urn,
    )

    list(source._gen_ml_model_mcps(model_metadata))

    model_group_urn = source.urn_builder.make_ml_model_group_urn(mock_model)
    tracked_training_jobs = source.model_usage_tracker.get_model_group_training_jobs(
        model_group_urn
    )

    assert len(tracked_training_jobs) == 1
    assert training_job_urn in tracked_training_jobs


def test_model_group_includes_lineage_in_properties(source: VertexAISource) -> None:
    """Test that model groups include training jobs and downstream jobs in their properties."""
    mock_model = gen_mock_model()

    model_group_urn = source.urn_builder.make_ml_model_group_urn(mock_model)
    training_job_urn = "urn:li:dataProcessInstance:training_job_123"
    downstream_job_urn = "urn:li:dataJob:downstream_pipeline.task"

    source.model_usage_tracker.track_model_group_training_job(
        model_group_urn, training_job_urn
    )
    source.model_usage_tracker.track_model_usage(model_group_urn, downstream_job_urn)

    container_mcps = list(source.model_extractor._gen_ml_group_container(mock_model))
    properties_mcps = list(source.model_extractor._gen_ml_group_properties(mock_model))
    mcps = container_mcps + properties_mcps

    model_group_props_mcp = next(
        (
            mcp
            for mcp in mcps
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, MLModelGroupPropertiesClass)
        ),
        None,
    )

    assert model_group_props_mcp is not None
    assert isinstance(model_group_props_mcp.metadata, MetadataChangeProposalWrapper)
    assert isinstance(
        model_group_props_mcp.metadata.aspect, MLModelGroupPropertiesClass
    )

    assert model_group_props_mcp.metadata.aspect.trainingJobs is not None
    assert len(model_group_props_mcp.metadata.aspect.trainingJobs) == 1
    assert training_job_urn in model_group_props_mcp.metadata.aspect.trainingJobs

    assert model_group_props_mcp.metadata.aspect.downstreamJobs is not None
    assert len(model_group_props_mcp.metadata.aspect.downstreamJobs) == 1
    assert downstream_job_urn in model_group_props_mcp.metadata.aspect.downstreamJobs
