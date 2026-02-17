import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any, List, cast
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.aiplatform import Experiment, ExperimentRun, PipelineJob
from google.cloud.aiplatform_v1 import PipelineTaskDetail
from google.cloud.aiplatform_v1.types import Execution, PipelineJob as PipelineJobType

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ExperimentKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.gcp_project_filter import GcpProject
from datahub.ingestion.source.vertexai.vertexai import VertexAIConfig, VertexAISource
from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
)
from datahub.ingestion.source.vertexai.vertexai_constants import (
    ResourceCategory,
    VertexAISubTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    ModelMetadata,
    TrainingJobMetadata,
    VertexAIResourceCategoryKey,
)
from datahub.ingestion.source.vertexai.vertexai_utils import get_actor_from_labels
from datahub.metadata.com.linkedin.pegasus2avro.ml.metadata import (
    MLModelGroupProperties,
    MLModelProperties,
    MLTrainingRunProperties,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    ContainerPropertiesClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataPlatformInstanceClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DatasetPropertiesClass,
    EdgeClass,
    MLModelDeploymentPropertiesClass,
    MLModelPropertiesClass,
    StatusClass,
    SubTypesClass,
    TimeStampClass,
    TrainingDataClass,
    VersionPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import DataPlatformUrn
from datahub.utilities.time import datetime_to_ts_millis
from datahub.utilities.urns.data_job_urn import DataJobUrn
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_dataset,
    gen_mock_endpoint,
    gen_mock_experiment,
    gen_mock_experiment_run,
    gen_mock_model,
    gen_mock_model_version,
    gen_mock_training_custom_job,
    get_mock_pipeline_job,
)

PROJECT_ID = "acryl-poc"
REGION = "us-west2"


def get_resource_category_container_urn(source: VertexAISource, category: str) -> str:
    return VertexAIResourceCategoryKey(
        project_id=source._get_project_id(),
        platform=source.platform,
        instance=source.config.platform_instance,
        env=source.config.env,
        category=category,
    ).as_urn()


@pytest.fixture
def source() -> VertexAISource:
    return VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id=PROJECT_ID, region=REGION),
    )


def test_get_ml_model_mcps(source: VertexAISource) -> None:
    mock_model = gen_mock_model()
    with contextlib.ExitStack() as exit_stack:
        mock_model_list = exit_stack.enter_context(
            patch("google.cloud.aiplatform.Model.list")
        )
        mock_model_list.return_value = [mock_model]

        mock_endpoint_list = exit_stack.enter_context(
            patch("google.cloud.aiplatform.Endpoint.list")
        )
        mock_endpoint_list.return_value = []

        actual_mcps = [mcp for mcp in source.model_extractor.get_model_workunits()]

        expected_urn = builder.make_ml_model_group_urn(
            platform=source.platform,
            group_name=source.name_formatter.format_model_group_name(mock_model.name),
            env=source.config.env,
        )

        mcp_mlgroup = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=MLModelGroupProperties(
                name=mock_model.display_name,
                description=mock_model.description,
                created=TimeStampClass(
                    time=datetime_to_ts_millis(mock_model.create_time),
                    actor=None,
                ),
                lastModified=TimeStampClass(
                    time=datetime_to_ts_millis(mock_model.update_time),
                    actor=None,
                ),
                externalUrl=source.url_builder.make_model_url(mock_model.name),
            ),
        )

        mcp_mlgroup_container = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=ContainerClass(
                container=get_resource_category_container_urn(
                    source, ResourceCategory.MODELS
                )
            ),
        )
        mcp_subtype = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=SubTypesClass(typeNames=[VertexAISubTypes.MODEL_GROUP]),
        )

        mcp_dataplatform = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=DataPlatformInstanceClass(
                platform=DataPlatformUrn(source.platform).urn(), instance=None
            ),
        )

        assert len(actual_mcps) == 14
        assert any(mcp_mlgroup_container == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_subtype == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_mlgroup == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_dataplatform == mcp.metadata for mcp in actual_mcps)


def test_get_ml_model_properties_mcps(source: VertexAISource) -> None:
    mock_model = gen_mock_model()
    model_version = gen_mock_model_version(mock_model)
    model_meta = ModelMetadata(model=mock_model, model_version=model_version)

    actual_mcps = list(source.model_extractor._gen_ml_model_mcps(model_meta))
    expected_urn = source.urn_builder.make_ml_model_urn(
        model_version, source.name_formatter.format_model_name(mock_model.name)
    )

    mcp_mlmodel = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=MLModelProperties(
            name=f"{mock_model.display_name}_{model_version.version_id}",
            description=model_version.version_description,
            created=TimeStampClass(
                time=datetime_to_ts_millis(mock_model.create_time),
                actor=None,
            ),
            lastModified=TimeStampClass(
                time=datetime_to_ts_millis(mock_model.update_time),
                actor=None,
            ),
            customProperties={
                "versionId": model_version.version_id,
                "resourceName": mock_model.resource_name,
            },
            externalUrl=source.url_builder.make_model_version_url(
                mock_model.name, mock_model.version_id
            ),
            version=VersionTagClass(
                versionTag=model_version.version_id, metadataAttribution=None
            ),
            groups=[source.urn_builder.make_ml_model_group_urn(mock_model)],
            type=VertexAISubTypes.MODEL,
            deployments=[],
        ),
    )

    model_group_container_urn = source.model_extractor._get_model_group_container(
        mock_model
    ).as_urn()
    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(container=model_group_container_urn),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[VertexAISubTypes.MODEL]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=DataPlatformUrn(source.platform).urn()
        ),
    )

    assert len(actual_mcps) == 5
    assert any(mcp_container == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_subtype == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_mlmodel == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp.metadata for mcp in actual_mcps)

    # Version Tag has GUID generated by the system,  so we need to check it separately
    version_tag_mcps = [
        mcp
        for mcp in actual_mcps
        if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
        and isinstance(mcp.metadata.aspect, VersionPropertiesClass)
    ]
    assert len(version_tag_mcps) == 1
    version_tag_mcp = version_tag_mcps[0]
    assert isinstance(version_tag_mcp.metadata, MetadataChangeProposalWrapper)
    assert isinstance(version_tag_mcp.metadata.aspect, VersionPropertiesClass)


def test_get_endpoint_mcps(
    source: VertexAISource,
) -> None:
    mock_model = gen_mock_model()
    model_version = gen_mock_model_version(mock_model)
    mock_endpoint = gen_mock_endpoint()
    model_meta = ModelMetadata(
        model=mock_model, model_version=model_version, endpoints=[mock_endpoint]
    )

    actual_mcps = list(source.model_extractor._gen_endpoints_mcps(model_meta))
    expected_urn = builder.make_ml_model_deployment_urn(
        platform=source.platform,
        deployment_name=source.name_formatter.format_endpoint_name(
            entity_id=mock_endpoint.name
        ),
        env=source.config.env,
    )

    mcp_endpoint = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=MLModelDeploymentPropertiesClass(
            customProperties={"displayName": mock_endpoint.display_name},
            description=mock_model.description,
            createdAt=int(datetime_to_ts_millis(mock_endpoint.create_time)),
            version=VersionTagClass(
                versionTag=model_version.version_id, metadataAttribution=None
            ),
        ),
    )

    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(
            container=get_resource_category_container_urn(
                source, ResourceCategory.ENDPOINTS
            )
        ),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=DataPlatformUrn(source.platform).urn(), instance=None
        ),
    )

    assert len(actual_mcps) == 3
    assert any(mcp_endpoint == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_container == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp.metadata for mcp in actual_mcps)


def test_get_training_jobs_mcps(
    source: VertexAISource,
) -> None:
    mock_training_job = gen_mock_training_custom_job()
    with contextlib.ExitStack() as exit_stack:
        for func_to_mock in [
            "google.cloud.aiplatform.init",
            "google.cloud.aiplatform.CustomJob.list",
            "google.cloud.aiplatform.CustomTrainingJob.list",
            "google.cloud.aiplatform.CustomContainerTrainingJob.list",
            "google.cloud.aiplatform.CustomPythonPackageTrainingJob.list",
            "google.cloud.aiplatform.AutoMLTabularTrainingJob.list",
            "google.cloud.aiplatform.AutoMLImageTrainingJob.list",
            "google.cloud.aiplatform.AutoMLTextTrainingJob.list",
            "google.cloud.aiplatform.AutoMLVideoTrainingJob.list",
            "google.cloud.aiplatform.AutoMLForecastingTrainingJob.list",
            "google.cloud.aiplatform.TabularDataset.list",
            "google.cloud.aiplatform.ImageDataset.list",
            "google.cloud.aiplatform.TextDataset.list",
            "google.cloud.aiplatform.VideoDataset.list",
            "google.cloud.aiplatform.TimeSeriesDataset.list",
        ]:
            mock = exit_stack.enter_context(patch(func_to_mock))
            if func_to_mock == "google.cloud.aiplatform.CustomJob.list":
                mock.return_value = [mock_training_job]
            else:
                mock.return_value = []

        actual_mcps = [mcp for mcp in source.training_extractor.get_workunits()]

        expected_urn = builder.make_data_process_instance_urn(
            source.name_formatter.format_job_name(mock_training_job.name)
        )

        mcp_dpi = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=mock_training_job.display_name,
                externalUrl=source.url_builder.make_job_url(mock_training_job.name),
                customProperties={"jobType": "CustomJob"},
                created=AuditStampClass(
                    time=datetime_to_ts_millis(mock_training_job.create_time),
                    actor="urn:li:corpuser:unknown",
                ),
            ),
        )

        mcp_ml_props = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=MLTrainingRunProperties(
                id=mock_training_job.name,
                externalUrl=source.url_builder.make_job_url(mock_training_job.name),
            ),
        )

        mcp_container = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=ContainerClass(
                container=get_resource_category_container_urn(
                    source, ResourceCategory.TRAINING_JOBS
                )
            ),
        )
        mcp_subtype = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=SubTypesClass(typeNames=[VertexAISubTypes.TRAINING_JOB]),
        )

        mcp_dataplatform = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=DataPlatformInstanceClass(
                platform=DataPlatformUrn(source.platform).urn()
            ),
        )

        assert len(actual_mcps) == 5
        assert any(mcp_dpi == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_ml_props == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_subtype == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_container == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_dataplatform == mcp.metadata for mcp in actual_mcps)


def test_gen_training_job_mcps(source: VertexAISource) -> None:
    mock_training_job = gen_mock_training_custom_job()
    mock_dataset = gen_mock_dataset()
    job_meta = TrainingJobMetadata(job=mock_training_job, input_dataset=mock_dataset)

    actual_mcps = [
        mcp for mcp in source.training_extractor._gen_training_job_mcps(job_meta)
    ]

    dataset_name = source.name_formatter.format_dataset_name(
        entity_id=mock_dataset.name
    )
    dataset_urn = builder.make_dataset_urn(
        platform=source.platform,
        name=dataset_name,
        env=source.config.env,
    )

    expected_urn = builder.make_data_process_instance_urn(
        source.name_formatter.format_job_name(mock_training_job.name)
    )

    mcp_dpi = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataProcessInstancePropertiesClass(
            name=mock_training_job.display_name,
            externalUrl=source.url_builder.make_job_url(mock_training_job.name),
            customProperties={"jobType": "CustomJob"},
            created=AuditStampClass(
                time=datetime_to_ts_millis(mock_training_job.create_time),
                actor="urn:li:corpuser:unknown",
            ),
        ),
    )

    mcp_ml_props = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=MLTrainingRunProperties(
            id=mock_training_job.name,
            externalUrl=source.url_builder.make_job_url(mock_training_job.name),
        ),
    )

    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(
            container=get_resource_category_container_urn(
                source, ResourceCategory.TRAINING_JOBS
            )
        ),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[VertexAISubTypes.TRAINING_JOB]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=DataPlatformUrn(source.platform).urn()
        ),
    )

    mcp_dpi_input = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataProcessInstanceInputClass(
            inputs=[], inputEdges=[EdgeClass(destinationUrn=dataset_urn)]
        ),
    )

    assert len(actual_mcps) == 6
    assert any(mcp_dpi == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_ml_props == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_subtype == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_container == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_dpi_input == mcp.metadata for mcp in actual_mcps)


def test_get_input_dataset_mcps(source: VertexAISource) -> None:
    mock_dataset = gen_mock_dataset()
    mock_job = gen_mock_training_custom_job()
    job_meta = TrainingJobMetadata(job=mock_job, input_dataset=mock_dataset)

    actual_mcps: List[MetadataWorkUnit] = list(
        source.training_extractor._get_dataset_workunits_from_job_metadata(job_meta)
    )

    assert job_meta.input_dataset is not None
    expected_urn = builder.make_dataset_urn(
        platform=source.platform,
        name=source.name_formatter.format_dataset_name(
            entity_id=job_meta.input_dataset.name
        ),
        env=source.config.env,
    )

    mcp_ds = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DatasetPropertiesClass(
            name=mock_dataset.display_name,
            description=mock_dataset.display_name,
            qualifiedName=mock_dataset.resource_name,
            customProperties={"resourceName": mock_dataset.resource_name},
            created=TimeStampClass(
                time=datetime_to_ts_millis(mock_dataset.create_time)
            ),
        ),
    )
    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(
            container=get_resource_category_container_urn(
                source, ResourceCategory.DATASETS
            )
        ),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[VertexAISubTypes.DATASET]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=DataPlatformUrn(source.platform).urn()
        ),
    )

    assert len(actual_mcps) == 4
    assert any(mcp_ds == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_subtype == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_container == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp.metadata for mcp in actual_mcps)


@patch("google.cloud.aiplatform.Experiment.list")
def test_get_experiment_mcps(
    mock_list: List[Experiment], source: VertexAISource
) -> None:
    mock_experiment = gen_mock_experiment()
    assert hasattr(mock_list, "return_value")
    mock_list.return_value = [mock_experiment]
    actual_wus: List[MetadataWorkUnit] = list(
        source.experiment_extractor.get_experiment_workunits()
    )
    actual_mcps = [
        mcp.metadata
        for mcp in actual_wus
        if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
    ]

    expected_urn = ExperimentKey(
        platform=source.platform,
        instance=source.config.platform_instance,
        env=source.config.env,
        id=source.name_formatter.format_experiment_id(mock_experiment.name),
    ).as_urn()

    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[VertexAISubTypes.EXPERIMENT]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=DataPlatformUrn(source.platform).urn(), instance=None
        ),
    )

    mcp_container_props = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerPropertiesClass(
            name=mock_experiment.name,
            customProperties={
                "platform": source.platform,
                "env": source.config.env,
                "id": source.name_formatter.format_experiment_id(mock_experiment.name),
                "name": mock_experiment.name,
                "resourceName": mock_experiment.resource_name,
                "dashboardURL": mock_experiment.dashboard_url
                if mock_experiment.dashboard_url
                else "",
            },
            env=source.config.env,
            externalUrl=source.url_builder.make_experiment_url(mock_experiment.name),
        ),
    )

    mcp_status = MetadataChangeProposalWrapper(
        entityUrn=expected_urn, aspect=StatusClass(removed=False)
    )

    assert len(actual_wus) == 5
    assert len(actual_mcps) == 5

    assert any(mcp_container_props == mcp for mcp in actual_mcps)
    assert any(mcp_subtype == mcp for mcp in actual_mcps)
    assert any(mcp_container == mcp for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp for mcp in actual_mcps)
    assert any(mcp_status == mcp for mcp in actual_mcps)


@patch("google.cloud.aiplatform.ExperimentRun.list")
def test_gen_experiment_run_mcps(
    mock_list: List[ExperimentRun], source: VertexAISource
) -> None:
    mock_exp = gen_mock_experiment()
    source.experiment_extractor.experiments = [mock_exp]
    mock_exp_run = gen_mock_experiment_run()
    assert hasattr(mock_list, "return_value")
    mock_list.return_value = [mock_exp_run]

    expected_exp_urn = ExperimentKey(
        platform=source.platform,
        instance=source.config.platform_instance,
        env=source.config.env,
        id=source.name_formatter.format_experiment_id(mock_exp.name),
    ).as_urn()

    expected_urn = source.urn_builder.make_experiment_run_urn(mock_exp, mock_exp_run)
    actual_mcps: List[MetadataWorkUnit] = list(
        source.experiment_extractor.get_experiment_run_workunits()
    )

    mcp_dpi = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataProcessInstancePropertiesClass(
            name=mock_exp_run.name,
            externalUrl=source.url_builder.make_experiment_run_url(
                mock_exp, mock_exp_run
            ),
            customProperties={
                "externalUrl": source.url_builder.make_experiment_run_url(
                    mock_exp, mock_exp_run
                )
            },
            created=AuditStampClass(time=0, actor="urn:li:corpuser:unknown"),
        ),
    )

    mcp_ml_props = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=MLTrainingRunProperties(
            id=f"{mock_exp.name}-{mock_exp_run.name}",
            externalUrl=source.url_builder.make_experiment_run_url(
                mock_exp, mock_exp_run
            ),
            hyperParams=[],
            trainingMetrics=[],
        ),
    )

    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(container=expected_exp_urn),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[VertexAISubTypes.EXPERIMENT_RUN]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=DataPlatformUrn(source.platform).urn()
        ),
    )

    assert len(actual_mcps) == 5
    assert any(mcp_dpi == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_ml_props == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_subtype == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_container == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp.metadata for mcp in actual_mcps)


def test_get_pipeline_mcps(
    source: VertexAISource,
) -> None:
    mock_pipeline = get_mock_pipeline_job()
    with contextlib.ExitStack() as exit_stack:
        mock = exit_stack.enter_context(
            patch("google.cloud.aiplatform.PipelineJob.list")
        )
        mock.return_value = [mock_pipeline]

        actual_mcps = [mcp for mcp in source.pipeline_extractor.get_workunits()]

        # Get the actual pipeline URN from the metadata
        mock_pipeline_obj = get_mock_pipeline_job()
        pipeline_meta = source.pipeline_extractor._get_pipeline_metadata(
            mock_pipeline_obj
        )
        expected_pipeline_urn = str(pipeline_meta.urn)
        expected_task_urn = str(
            DataJobUrn.create_from_ids(
                data_flow_urn=expected_pipeline_urn,
                job_id=source.name_formatter.format_pipeline_task_id("reverse"),
            )
        )

    # Task run URN now includes pipeline name to make it unique per execution
    task_run_id = f"{mock_pipeline.name}_reverse"
    dpi_urn = f"urn:li:dataProcessInstance:{source.name_formatter.format_pipeline_task_run_id(task_run_id)}"

    # Expected MCPs: 7 (dataflow w/ container) + 4 (pipeline run) + 6 (datajob) + 4 (task run) = 21
    assert len(actual_mcps) == 21, f"Expected 21 MCPs, got {len(actual_mcps)}"

    dataflow_info_mcps = [
        mcp
        for mcp in actual_mcps
        if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
        and isinstance(mcp.metadata.aspect, DataFlowInfoClass)
        and mcp.metadata.entityUrn == expected_pipeline_urn
    ]
    assert len(dataflow_info_mcps) == 1, "Expected exactly one DataFlowInfoClass"

    # Type narrow for mypy
    dataflow_wu = dataflow_info_mcps[0]
    assert isinstance(dataflow_wu.metadata, MetadataChangeProposalWrapper)
    actual_df_info = dataflow_wu.metadata.aspect
    assert isinstance(actual_df_info, DataFlowInfoClass)

    assert actual_df_info.name == mock_pipeline.name
    assert actual_df_info.env == source.config.env
    assert actual_df_info.externalUrl == source.url_builder.make_pipeline_url(
        mock_pipeline.name
    )
    assert "resource_name" in actual_df_info.customProperties
    assert "create_time" in actual_df_info.customProperties
    assert "update_time" in actual_df_info.customProperties
    assert "duration" in actual_df_info.customProperties
    assert "location" in actual_df_info.customProperties
    assert "labels" in actual_df_info.customProperties

    assert any(
        isinstance(mcp.metadata, MetadataChangeProposalWrapper)
        and isinstance(mcp.metadata.aspect, StatusClass)
        and mcp.metadata.entityUrn == expected_pipeline_urn
        for mcp in actual_mcps
    ), "DataFlow StatusClass not found"

    assert any(
        isinstance(mcp.metadata, MetadataChangeProposalWrapper)
        and isinstance(mcp.metadata.aspect, DataJobInfoClass)
        and mcp.metadata.entityUrn == expected_task_urn
        for mcp in actual_mcps
    ), "DataJob DataJobInfoClass not found"

    # Verify DataJob does NOT have container aspect
    # (DataFlow entities cannot be containers for DataJob entities in DataHub's entity model)
    datajob_container_mcps = [
        mcp
        for mcp in actual_mcps
        if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
        and isinstance(mcp.metadata.aspect, ContainerClass)
        and mcp.metadata.entityUrn == expected_task_urn
    ]
    assert len(datajob_container_mcps) == 0, (
        "DataJob should NOT have ContainerClass aspect. "
        "The relationship to DataFlow is established via the flow_urn in the DataJobUrn."
    )

    assert any(
        isinstance(mcp.metadata, MetadataChangeProposalWrapper)
        and isinstance(mcp.metadata.aspect, DataProcessInstancePropertiesClass)
        and mcp.metadata.entityUrn == dpi_urn
        for mcp in actual_mcps
    ), "DPI PropertiesClass not found"


def test_vertexai_multi_project_context_naming() -> None:
    multi_source = VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id="fallback", region="us-central1"),
    )
    multi_source._current_project_id = "p2"
    multi_source._current_region = "r2"

    multi_source.name_formatter = VertexAINameFormatter(project_id="p2")
    multi_source.url_builder = VertexAIExternalURLBuilder(
        base_url=multi_source.config.vertexai_url
        or "https://console.cloud.google.com/vertex-ai",
        project_id="p2",
        region="r2",
    )

    assert (
        multi_source.name_formatter.format_model_group_name("m") == "p2.model_group.m"
    )
    assert multi_source.name_formatter.format_dataset_name("d") == "p2.dataset.d"
    assert (
        multi_source.name_formatter.format_pipeline_task_id("t") == "p2.pipeline_task.t"
    )

    assert (
        multi_source.url_builder.make_model_url("xyz")
        == f"{multi_source.config.vertexai_url}/models/locations/r2/models/xyz?project=p2"
    )


def test_dataset_urns_from_artifact_uri() -> None:
    source = VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id="p", region="r"),
    )
    gcs = source.uri_parser.dataset_urns_from_artifact_uri("gs://bucket/path/file")
    assert gcs and gcs[0].startswith("urn:li:dataset:(urn:li:dataPlatform:gcs,")

    bq = source.uri_parser.dataset_urns_from_artifact_uri("bq://proj.ds.tbl")
    assert bq and ",proj.ds.tbl," in bq[0]

    api = source.uri_parser.dataset_urns_from_artifact_uri(
        "projects/p/datasets/ds/tables/tbl"
    )
    assert api and ",p.ds.tbl," in api[0]


def test_gen_run_execution_edges() -> None:
    # Build a minimal dummy Execution-like object
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
    # Ensure at least one DataProcessInstanceInputClass and OutputClass exists with edges
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


@pytest.mark.parametrize(
    "start_time,end_time,task_name",
    [
        (
            None,
            datetime.now(timezone.utc) - timedelta(days=3, hours=1),
            "incomplete_task",
        ),
        (datetime.now(timezone.utc) - timedelta(days=3), None, "running_task"),
    ],
)
def test_pipeline_task_with_none_timestamps(
    source: VertexAISource,
    start_time: datetime | None,
    end_time: datetime | None,
    task_name: str,
) -> None:
    """Test that pipeline tasks with None start_time or end_time don't crash the ingestion."""
    mock_pipeline_job = MagicMock(spec=PipelineJob)
    mock_pipeline_job.name = f"test_pipeline_{task_name}"
    mock_pipeline_job.resource_name = (
        "projects/123/locations/us-central1/pipelineJobs/789"
    )
    mock_pipeline_job.labels = {}
    mock_pipeline_job.create_time = datetime.now(timezone.utc) - timedelta(days=3)
    mock_pipeline_job.update_time = datetime.now(timezone.utc) - timedelta(days=2)
    mock_pipeline_job.location = "us-west2"

    gca_resource = MagicMock(spec=PipelineJobType)
    mock_pipeline_job.gca_resource = gca_resource

    task_detail = MagicMock(spec=PipelineTaskDetail)
    task_detail.task_name = task_name
    task_detail.task_id = 123
    task_detail.state = MagicMock()
    task_detail.start_time = start_time
    task_detail.create_time = datetime.now(timezone.utc) - timedelta(days=3)
    task_detail.end_time = end_time
    task_detail.inputs = {}
    task_detail.outputs = {}

    mock_pipeline_job.task_details = [task_detail]
    gca_resource.pipeline_spec = {
        "root": {
            "dag": {
                "tasks": {
                    task_name: {
                        "componentRef": {"name": f"comp-{task_name}"},
                        "taskInfo": {"name": task_name},
                    }
                }
            }
        }
    }

    with contextlib.ExitStack() as exit_stack:
        mock = exit_stack.enter_context(
            patch("google.cloud.aiplatform.PipelineJob.list")
        )
        mock.return_value = [mock_pipeline_job]

        actual_mcps = list(source.pipeline_extractor.get_workunits())

        task_run_mcps = [
            mcp
            for mcp in actual_mcps
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, DataProcessInstancePropertiesClass)
            and task_name in mcp.metadata.aspect.name
        ]

        assert len(task_run_mcps) > 0


def test_experiment_run_with_none_timestamps(source: VertexAISource) -> None:
    """Test that experiment runs with None create_time/update_time don't crash."""
    mock_exp = gen_mock_experiment()
    source.experiment_extractor.experiments = [mock_exp]

    mock_exp_run = MagicMock(spec=ExperimentRun)
    mock_exp_run.name = "test_run_none_timestamps"
    mock_exp_run.create_time = datetime(2022, 3, 21, 10, 0, 0, tzinfo=timezone.utc)
    mock_exp_run.update_time = datetime(2022, 3, 21, 10, 0, 0, tzinfo=timezone.utc)
    mock_exp_run.get_state.return_value = "COMPLETE"
    mock_exp_run.get_params.return_value = {}
    mock_exp_run.get_metrics.return_value = {}

    mock_execution = MagicMock()
    mock_execution.name = "test_execution"
    mock_execution.create_time = None
    mock_execution.update_time = None
    mock_execution.state = "COMPLETE"
    mock_execution.get_input_artifacts.return_value = []
    mock_execution.get_output_artifacts.return_value = []

    mock_exp_run.get_executions.return_value = [mock_execution]

    with patch("google.cloud.aiplatform.ExperimentRun.list") as mock_list:
        mock_list.return_value = [mock_exp_run]

        actual_mcps = list(source.experiment_extractor.get_experiment_run_workunits())

        run_mcps = [
            mcp
            for mcp in actual_mcps
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, DataProcessInstancePropertiesClass)
            and "test_run_none_timestamps" in mcp.metadata.aspect.name
        ]

        assert len(run_mcps) > 0


@patch("datahub.ingestion.source.vertexai.vertexai.resolve_gcp_projects")
def test_multi_project_initialization_with_explicit_ids(
    mock_resolve: MagicMock,
) -> None:
    """Test initialization with explicit project_ids"""
    mock_resolve.return_value = [
        GcpProject(id="project-1", name="Project 1"),
        GcpProject(id="project-2", name="Project 2"),
    ]

    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "project_ids": ["project-1", "project-2"],
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._projects == ["project-1", "project-2"]
    mock_resolve.assert_called_once()


@patch("datahub.ingestion.source.vertexai.vertexai.resolve_gcp_projects")
def test_multi_project_initialization_with_labels(mock_resolve: MagicMock) -> None:
    """Test initialization with project labels"""
    mock_resolve.return_value = [
        GcpProject(id="dev-project", name="Development"),
        GcpProject(id="prod-project", name="Production"),
    ]

    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "project_labels": ["env:prod", "team:ml"],
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._projects == ["dev-project", "prod-project"]


@patch("datahub.ingestion.source.vertexai.vertexai.resolve_gcp_projects")
def test_multi_project_fallback_to_project_id_when_resolution_fails(
    mock_resolve: MagicMock,
) -> None:
    """Test fallback to project_id when multi-project resolution returns empty"""
    mock_resolve.return_value = []

    config = VertexAIConfig.model_validate(
        {
            "project_id": "fallback-project",
            "region": "us-central1",
            "project_ids": ["invalid-project"],
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._projects == ["fallback-project"]


@pytest.mark.parametrize(
    "duration,expected",
    [
        (timedelta(seconds=45), "45s"),
        (timedelta(minutes=5, seconds=30), "5m 30s"),
        (timedelta(hours=2, minutes=15), "2h 15m"),
        (timedelta(days=1, hours=3, minutes=20), "1d 3h 20m"),
        (timedelta(seconds=0), "0s"),
    ],
)
def test_format_pipeline_duration(
    source: VertexAISource, duration: timedelta, expected: str
) -> None:
    """Test various duration formatting scenarios"""
    formatted = source.pipeline_extractor._format_pipeline_duration(duration)
    assert formatted == expected


def test_ml_metadata_helper_disabled_by_config() -> None:
    """Test that ML Metadata helper is not initialized when disabled"""
    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "use_ml_metadata_for_lineage": False,
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._ml_metadata_helper is None


@patch("datahub.ingestion.source.vertexai.ml_metadata_helper.MetadataServiceClient")
def test_ml_metadata_helper_handles_init_errors(mock_client: MagicMock) -> None:
    """Test ML Metadata helper gracefully handles initialization errors"""
    mock_client.side_effect = Exception("Auth error")

    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "use_ml_metadata_for_lineage": True,
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._ml_metadata_helper is None


def test_model_training_data_lineage(source: VertexAISource) -> None:
    """Test that models have TrainingDataClass aspect with dataset URNs"""

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


def test_dataset_urns_include_platform_instance() -> None:
    """Test that dataset URNs include platform_instance when configured"""
    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "platform_instance": "prod-vertexai",
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    dataset_name = "test-dataset"
    dataset_urn = source.urn_builder.make_dataset_urn(dataset_name)

    assert "prod-vertexai" in dataset_urn
    assert dataset_urn.startswith("urn:li:dataset:")


@pytest.mark.parametrize(
    "artifact_uri,platform",
    [
        ("gs://bucket/path", "gcs"),
        ("bq://proj.ds.tbl", "bigquery"),
    ],
)
def test_uri_parser_includes_platform_instance(
    artifact_uri: str, platform: str
) -> None:
    """Test that URI parser includes platform_instance in generated URNs for external platforms"""
    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "platform_to_instance_map": {
                platform: {
                    "platform_instance": "prod-instance",
                    "env": "PROD",
                }
            },
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    urns = source.uri_parser.dataset_urns_from_artifact_uri(artifact_uri)
    assert len(urns) == 1
    assert "prod-instance" in urns[0]


@pytest.mark.parametrize(
    "labels,expected_user",
    [
        ({"created_by": "john.doe"}, "john.doe"),
        ({"creator": "jane.smith"}, "jane.smith"),
        ({"owner": "team-lead"}, "team-lead"),
        ({"env": "prod", "version": "1.0"}, None),
        (None, None),
    ],
)
def test_owner_extraction_from_labels(
    source: VertexAISource, labels: Any, expected_user: str | None
) -> None:
    """Test owner extraction from various label keys"""
    owner = get_actor_from_labels(labels)
    if expected_user:
        assert owner == builder.make_user_urn(expected_user)
    else:
        assert owner is None


@pytest.mark.parametrize(
    "uri,should_parse,expected_content",
    [
        (
            "projects/test-project/locations/us-central1/models/123456",
            True,
            ["mlModel", "123456"],
        ),
        ("gs://bucket/path/file", False, None),
        (None, False, None),
    ],
)
def test_model_urn_from_artifact_uri(
    uri: str | None, should_parse: bool, expected_content: list[str] | None
) -> None:
    """Test parsing model URNs from ML Metadata artifact URIs"""
    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    model_urn = source.uri_parser.model_urn_from_artifact_uri(uri)
    if should_parse:
        assert model_urn is not None
        assert expected_content is not None
        for content in expected_content:
            assert content in model_urn
    else:
        assert model_urn is None


def test_pipeline_task_run_lineage_edges() -> None:
    """Test that DataProcessInstance aspects support both dataset and model URNs"""
    # This is a structure verification test to ensure we're using the correct
    # aspects for pipeline task run lineage.

    # DataProcessInstanceInput and Output aspects support dataset and mlModel entity types
    # This is verified in entity-registry.yml and the PDL definitions

    dataset_edge = EdgeClass(
        destinationUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.ds.table,PROD)"
    )
    model_edge = EdgeClass(
        destinationUrn="urn:li:mlModel:(urn:li:dataPlatform:vertexai,test-model,PROD)"
    )

    input_aspect = DataProcessInstanceInputClass(
        inputs=[],
        inputEdges=[dataset_edge],
    )
    output_aspect = DataProcessInstanceOutputClass(
        outputs=[],
        outputEdges=[dataset_edge, model_edge],
    )

    assert input_aspect.inputEdges is not None
    assert len(input_aspect.inputEdges) == 1
    assert output_aspect.outputEdges is not None
    assert len(output_aspect.outputEdges) == 2
    assert "dataset" in output_aspect.outputEdges[0].destinationUrn
    assert "mlModel" in output_aspect.outputEdges[1].destinationUrn


def test_model_downstream_lineage_from_pipeline_tasks(source: VertexAISource) -> None:
    """Test that pipeline tasks using models as inputs create downstream lineage."""
    mock_pipeline = MagicMock(spec=PipelineJob)
    mock_pipeline.name = "test_pipeline_with_model_input"
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

    with patch("google.cloud.aiplatform.PipelineJob.list") as mock_list:
        mock_list.return_value = [mock_pipeline]
        list(source.pipeline_extractor.get_workunits())

    expected_model_urn = "urn:li:mlModel:(urn:li:dataPlatform:vertexai,123456,PROD)"

    downstream_urns = source.model_usage_tracker.get_downstream_jobs(expected_model_urn)
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
                mock_experiment, mock_run
            )
        )

    expected_model_urn = "urn:li:mlModel:(urn:li:dataPlatform:vertexai,789,PROD)"
    downstream_urns = source.model_usage_tracker.get_downstream_jobs(expected_model_urn)
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
    source.model_usage_tracker.track_usage(model_urn, downstream_job_urn)

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
