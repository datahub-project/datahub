import contextlib
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import (
    DeadlineExceeded,
    FailedPrecondition,
    GoogleAPICallError,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)
from google.cloud.aiplatform import Endpoint, Experiment, ExperimentRun, PipelineJob
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform_v1 import PipelineTaskDetail
from google.cloud.aiplatform_v1.types import PipelineJob as PipelineJobType

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ExperimentKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.common.gcp_project_utils import (
    GCPProject,
    GCPProjectDiscoveryError,
    _filter_projects_by_pattern,
    _is_rate_limit_error,
    _search_projects_with_retry,
    _validate_and_filter_projects,
    get_projects,
    get_projects_by_labels,
    get_projects_from_explicit_list,
    list_all_accessible_projects,
)
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.vertexai.vertexai import (
    ModelMetadata,
    TrainingJobMetadata,
    VertexAIConfig,
    VertexAISource,
    _cleanup_credentials,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceRelationships,
)
from datahub.metadata.com.linkedin.pegasus2avro.ml.metadata import (
    MLModelGroupProperties,
    MLModelProperties,
    MLTrainingRunProperties,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DataProcessInstanceInputClass,
    DataProcessInstancePropertiesClass,
    DatasetPropertiesClass,
    EdgeClass,
    GlobalTagsClass,
    MLModelDeploymentPropertiesClass,
    StatusClass,
    SubTypesClass,
    TimeStampClass,
    VersionPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import DataPlatformUrn
from datahub.utilities.time import datetime_to_ts_millis
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_dataset,
    gen_mock_endpoint,
    gen_mock_experiment,
    gen_mock_experiment_run,
    gen_mock_model,
    gen_mock_model_version,
    gen_mock_training_automl_job,
    gen_mock_training_custom_job,
    get_mock_pipeline_job,
)

PROJECT_ID = "test-gcp-project"
REGION = "us-west2"


@pytest.fixture
def source() -> VertexAISource:
    with patch("google.cloud.aiplatform.init"):
        source = VertexAISource(
            ctx=PipelineContext(run_id="vertexai-source-test"),
            config=VertexAIConfig(project_ids=[PROJECT_ID], region=REGION),
        )
        source._current_project_id = PROJECT_ID
        return source


def test_get_ml_model_mcps(source: VertexAISource) -> None:
    mock_model = gen_mock_model()
    with contextlib.ExitStack() as exit_stack:
        mock = exit_stack.enter_context(patch("google.cloud.aiplatform.Model.list"))
        mock.return_value = [mock_model]

        actual_mcps = [mcp for mcp in source._get_ml_models_mcps()]

        expected_urn = builder.make_ml_model_group_urn(
            platform=source.platform,
            group_name=source._make_vertexai_model_group_name(mock_model.name),
            env=source.config.env,
        )

        mcp_mlgroup = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=MLModelGroupProperties(
                name=mock_model.display_name,
                description=mock_model.description,
                created=TimeStampClass(
                    time=datetime_to_ts_millis(mock_model.create_time),
                    actor="urn:li:corpuser:datahub",
                ),
                lastModified=TimeStampClass(
                    time=datetime_to_ts_millis(mock_model.update_time),
                    actor="urn:li:corpuser:datahub",
                ),
                externalUrl=source._make_model_external_url(mock_model),
            ),
        )

        mcp_container = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=ContainerClass(container=source._get_project_container().as_urn()),
        )
        mcp_subtype = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_MODEL_GROUP]),
        )

        mcp_dataplatform = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=DataPlatformInstanceClass(
                platform=str(DataPlatformUrn(source.platform))
            ),
        )

        assert len(actual_mcps) == 4
        assert any(mcp_container == mcp for mcp in actual_mcps)
        assert any(mcp_subtype == mcp for mcp in actual_mcps)
        assert any(mcp_mlgroup == mcp for mcp in actual_mcps)
        assert any(mcp_dataplatform == mcp for mcp in actual_mcps)


def test_get_ml_model_properties_mcps(source: VertexAISource) -> None:
    mock_model = gen_mock_model()
    model_version = gen_mock_model_version(mock_model)
    model_meta = ModelMetadata(mock_model, model_version)

    actual_mcps = list(source._gen_ml_model_mcps(model_meta))
    expected_urn = source._make_ml_model_urn(
        model_version, source._make_vertexai_model_name(mock_model.name)
    )

    mcp_mlmodel = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=MLModelProperties(
            name=f"{mock_model.display_name}_{model_version.version_id}",
            description=model_version.version_description,
            created=TimeStampClass(
                time=datetime_to_ts_millis(mock_model.create_time),
                actor="urn:li:corpuser:datahub",
            ),
            lastModified=TimeStampClass(
                time=datetime_to_ts_millis(mock_model.update_time),
                actor="urn:li:corpuser:datahub",
            ),
            customProperties={
                "versionId": model_version.version_id,
                "resourceName": mock_model.resource_name,
            },
            externalUrl=source._make_model_version_external_url(mock_model),
            version=VersionTagClass(
                versionTag=model_version.version_id, metadataAttribution=None
            ),
            groups=[source._make_ml_model_group_urn(mock_model)],
            type=MLAssetSubTypes.VERTEX_MODEL,
            deployments=[],
        ),
    )

    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_MODEL]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=str(DataPlatformUrn(source.platform))
        ),
    )

    assert len(actual_mcps) == 5
    assert any(mcp_container == mcp for mcp in actual_mcps)
    assert any(mcp_subtype == mcp for mcp in actual_mcps)
    assert any(mcp_mlmodel == mcp for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp for mcp in actual_mcps)

    # Version Tag has GUID generated by the system,  so we need to check it separately
    version_tag_mcps = [
        mcp for mcp in actual_mcps if isinstance(mcp.aspect, VersionPropertiesClass)
    ]
    assert len(version_tag_mcps) == 1
    version_tag_mcp = version_tag_mcps[0]
    assert isinstance(version_tag_mcp.aspect, VersionPropertiesClass)


def test_get_endpoint_mcps(
    source: VertexAISource,
) -> None:
    mock_model = gen_mock_model()
    model_version = gen_mock_model_version(mock_model)
    mock_endpoint = gen_mock_endpoint()
    model_meta = ModelMetadata(
        model=mock_model, model_version=model_version, endpoints=[mock_endpoint]
    )

    actual_mcps = list(source._gen_endpoints_mcps(model_meta))
    expected_urn = builder.make_ml_model_deployment_urn(
        platform=source.platform,
        deployment_name=source._make_vertexai_endpoint_name(
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
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )

    assert len(actual_mcps) == 2
    assert any(mcp_endpoint == mcp for mcp in actual_mcps)
    assert any(mcp_container == mcp for mcp in actual_mcps)


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
        ]:
            mock = exit_stack.enter_context(patch(func_to_mock))
            if func_to_mock == "google.cloud.aiplatform.CustomJob.list":
                mock.return_value = [mock_training_job]
            else:
                mock.return_value = []

        actual_mcps = [mcp for mcp in source._get_training_jobs_mcps()]

        expected_urn = builder.make_data_process_instance_urn(
            source._make_vertexai_job_name(mock_training_job.name)
        )

        mcp_dpi = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=mock_training_job.display_name,
                externalUrl=source._make_job_external_url(mock_training_job),
                customProperties={"jobType": "CustomJob"},
                created=AuditStamp(
                    time=datetime_to_ts_millis(mock_training_job.create_time),
                    actor="urn:li:corpuser:datahub",
                ),
            ),
        )

        mcp_ml_props = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=MLTrainingRunProperties(
                id=mock_training_job.name,
                externalUrl=source._make_job_external_url(mock_training_job),
            ),
        )

        mcp_container = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=ContainerClass(container=source._get_project_container().as_urn()),
        )
        mcp_subtype = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_TRAINING_JOB]),
        )

        mcp_dataplatform = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=DataPlatformInstanceClass(
                platform=str(DataPlatformUrn(source.platform))
            ),
        )

        assert len(actual_mcps) == 5
        assert any(mcp_dpi == mcp for mcp in actual_mcps)
        assert any(mcp_ml_props == mcp for mcp in actual_mcps)
        assert any(mcp_subtype == mcp for mcp in actual_mcps)
        assert any(mcp_container == mcp for mcp in actual_mcps)
        assert any(mcp_dataplatform == mcp for mcp in actual_mcps)


def test_gen_training_job_mcps(source: VertexAISource) -> None:
    mock_training_job = gen_mock_training_custom_job()
    mock_dataset = gen_mock_dataset()
    job_meta = TrainingJobMetadata(mock_training_job, input_dataset=mock_dataset)

    actual_mcps = [mcp for mcp in source._gen_training_job_mcps(job_meta)]

    dataset_name = source._make_vertexai_dataset_name(entity_id=mock_dataset.name)
    dataset_urn = builder.make_dataset_urn(
        platform=source.platform,
        name=dataset_name,
        env=source.config.env,
    )

    expected_urn = builder.make_data_process_instance_urn(
        source._make_vertexai_job_name(mock_training_job.name)
    )

    mcp_dpi = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataProcessInstancePropertiesClass(
            name=mock_training_job.display_name,
            externalUrl=source._make_job_external_url(mock_training_job),
            customProperties={"jobType": "CustomJob"},
            created=AuditStamp(
                time=datetime_to_ts_millis(mock_training_job.create_time),
                actor="urn:li:corpuser:datahub",
            ),
        ),
    )

    mcp_ml_props = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=MLTrainingRunProperties(
            id=mock_training_job.name,
            externalUrl=source._make_job_external_url(mock_training_job),
        ),
    )

    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_TRAINING_JOB]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=str(DataPlatformUrn(source.platform))
        ),
    )

    mcp_dpi_input = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataProcessInstanceInputClass(
            inputs=[], inputEdges=[EdgeClass(destinationUrn=dataset_urn)]
        ),
    )

    assert len(actual_mcps) == 6
    assert any(mcp_dpi == mcp for mcp in actual_mcps)
    assert any(mcp_ml_props == mcp for mcp in actual_mcps)
    assert any(mcp_subtype == mcp for mcp in actual_mcps)
    assert any(mcp_container == mcp for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp for mcp in actual_mcps)
    assert any(mcp_dpi_input == mcp for mcp in actual_mcps)


def test_vertexai_config_init():
    config_data = {
        "project_id": "test-project",
        "region": "us-central1",
        "bucket_uri": "gs://test-bucket",
        "vertexai_url": "https://console.cloud.google.com/vertex-ai",
        "credential": {
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\ntest-private-key\n-----END PRIVATE KEY-----\n",
            "client_email": "test-email@test-project.iam.gserviceaccount.com",
            "client_id": "test-client-id",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "type": "service_account",
        },
    }

    config = VertexAIConfig(**config_data)

    assert config.project_ids == ["test-project"]
    assert config.region == "us-central1"
    assert config.bucket_uri == "gs://test-bucket"
    assert config.vertexai_url == "https://console.cloud.google.com/vertex-ai"
    assert config.credential is not None
    assert config.credential.private_key_id == "test-key-id"
    assert (
        config.credential.private_key
        == "-----BEGIN PRIVATE KEY-----\ntest-private-key\n-----END PRIVATE KEY-----\n"
    )
    assert (
        config.credential.client_email
        == "test-email@test-project.iam.gserviceaccount.com"
    )
    assert config.credential.client_id == "test-client-id"
    assert config.credential.auth_uri == "https://accounts.google.com/o/oauth2/auth"
    assert config.credential.token_uri == "https://oauth2.googleapis.com/token"
    assert (
        config.credential.auth_provider_x509_cert_url
        == "https://www.googleapis.com/oauth2/v1/certs"
    )

    assert config._credentials_path is not None


def test_get_input_dataset_mcps(source: VertexAISource) -> None:
    mock_dataset = gen_mock_dataset()
    mock_job = gen_mock_training_custom_job()
    job_meta = TrainingJobMetadata(mock_job, input_dataset=mock_dataset)

    actual_mcps: List[MetadataChangeProposalWrapper] = list(
        source._get_input_dataset_mcps(job_meta)
    )

    assert job_meta.input_dataset is not None
    expected_urn = builder.make_dataset_urn(
        platform=source.platform,
        name=source._make_vertexai_dataset_name(entity_id=job_meta.input_dataset.name),
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
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_DATASET]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=str(DataPlatformUrn(source.platform))
        ),
    )

    assert len(actual_mcps) == 4
    assert any(mcp_ds == mcp for mcp in actual_mcps)
    assert any(mcp_subtype == mcp for mcp in actual_mcps)
    assert any(mcp_container == mcp for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp for mcp in actual_mcps)


@patch("google.cloud.aiplatform.Experiment.list")
def test_get_experiment_mcps(
    mock_list: List[Experiment], source: VertexAISource
) -> None:
    mock_experiment = gen_mock_experiment()
    assert hasattr(mock_list, "return_value")  # this check needed to go ground lint
    mock_list.return_value = [mock_experiment]
    actual_wus: List[MetadataWorkUnit] = list(source._get_experiments_workunits())
    actual_mcps = [
        mcp.metadata
        for mcp in actual_wus
        if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
    ]

    expected_urn = ExperimentKey(
        platform=source.platform,
        id=source._make_vertexai_experiment_id(mock_experiment.name),
    ).as_urn()

    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_EXPERIMENT]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=str(DataPlatformUrn(source.platform))
        ),
    )

    mcp_container_props = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerPropertiesClass(
            name=mock_experiment.name,
            customProperties={
                "platform": source.platform,
                "id": source._make_vertexai_experiment_id(mock_experiment.name),
                "name": mock_experiment.name,
                "resourceName": mock_experiment.resource_name,
                "dashboardURL": mock_experiment.dashboard_url
                if mock_experiment.dashboard_url
                else "",
            },
            externalUrl=source._make_experiment_external_url(mock_experiment),
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
    source.experiments = [mock_exp]
    mock_exp_run = gen_mock_experiment_run()
    assert hasattr(mock_list, "return_value")  # this check needed to go ground lint
    mock_list.return_value = [mock_exp_run]

    expected_exp_urn = ExperimentKey(
        platform=source.platform,
        id=source._make_vertexai_experiment_id(mock_exp.name),
    ).as_urn()

    expected_urn = source._make_experiment_run_urn(mock_exp, mock_exp_run)
    actual_mcps: List[MetadataChangeProposalWrapper] = list(
        source._get_experiment_runs_mcps()
    )

    mcp_dpi = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataProcessInstancePropertiesClass(
            name=mock_exp_run.name,
            externalUrl=source._make_experiment_run_external_url(
                mock_exp, mock_exp_run
            ),
            customProperties={
                "externalUrl": source._make_experiment_run_external_url(
                    mock_exp, mock_exp_run
                )
            },
            created=AuditStamp(time=0, actor="urn:li:corpuser:datahub"),
        ),
    )

    mcp_ml_props = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=MLTrainingRunProperties(
            id=f"{mock_exp.name}-{mock_exp_run.name}",
            externalUrl=source._make_experiment_run_external_url(
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
        aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_EXPERIMENT_RUN]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=str(DataPlatformUrn(source.platform))
        ),
    )

    assert len(actual_mcps) == 5
    assert any(mcp_dpi == mcp for mcp in actual_mcps)
    assert any(mcp_ml_props == mcp for mcp in actual_mcps)
    assert any(mcp_subtype == mcp for mcp in actual_mcps)
    assert any(mcp_container == mcp for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp for mcp in actual_mcps)


def test_get_pipeline_mcps(
    source: VertexAISource,
) -> None:
    mock_pipeline = get_mock_pipeline_job()
    with contextlib.ExitStack() as exit_stack:
        mock = exit_stack.enter_context(
            patch("google.cloud.aiplatform.PipelineJob.list")
        )
        mock.return_value = [mock_pipeline]

        actual_mcps = [mcp for mcp in source._get_pipelines_mcps()]

        expected_pipeline_urn = "urn:li:dataFlow:(vertexai,vertexai.test-gcp-project.pipeline.mock_pipeline_job,PROD)"

        expected_task_urn = "urn:li:dataJob:(urn:li:dataFlow:(vertexai,vertexai.test-gcp-project.pipeline.mock_pipeline_job,PROD),test-gcp-project.pipeline_task.reverse)"

        duration = timedelta(
            milliseconds=datetime_to_ts_millis(mock_pipeline.update_time)
            - datetime_to_ts_millis(mock_pipeline.create_time)
        )

        mcp_pipe_df_info = MetadataChangeProposalWrapper(
            entityUrn=expected_pipeline_urn,
            aspect=DataFlowInfoClass(
                env=source.config.env,
                name=mock_pipeline.name,
                customProperties={
                    "resource_name": mock_pipeline.resource_name,
                    "create_time": mock_pipeline.create_time.isoformat(),
                    "update_time": mock_pipeline.update_time.isoformat(),
                    "duration": source._format_pipeline_duration(duration),
                    "location": mock_pipeline.location,
                    "labels": ",".join(
                        [f"{k}:{v}" for k, v in mock_pipeline.labels.items()]
                    ),
                },
                externalUrl=source._make_pipeline_external_url(mock_pipeline.name),
            ),
        )
        mcp_pipe_df_status = MetadataChangeProposalWrapper(
            entityUrn=expected_pipeline_urn,
            aspect=StatusClass(removed=False),
        )
        mcp_pipe_container = MetadataChangeProposalWrapper(
            entityUrn=expected_pipeline_urn,
            aspect=ContainerClass(container=source._get_project_container().as_urn()),
        )
        mcp_pipe_subtype = MetadataChangeProposalWrapper(
            entityUrn=expected_pipeline_urn,
            aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_PIPELINE]),
        )

        mcp_pipeline_tag = MetadataChangeProposalWrapper(
            entityUrn=expected_pipeline_urn,
            aspect=GlobalTagsClass(tags=[]),
        )

        mcp_task_input = MetadataChangeProposalWrapper(
            entityUrn=expected_task_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=[],
                outputDatasets=[],
                inputDatajobs=[
                    "urn:li:dataJob:(urn:li:dataFlow:(vertexai,vertexai.test-gcp-project.pipeline.mock_pipeline_job,PROD),test-gcp-project.pipeline_task.concat)"
                ],
                fineGrainedLineages=[],
            ),
        )

        mcp_task_info = MetadataChangeProposalWrapper(
            entityUrn=expected_task_urn,
            aspect=DataJobInfoClass(
                name="reverse",
                customProperties={},
                type="COMMAND",
                externalUrl="https://console.cloud.google.com/vertex-ai/pipelines/locations/us-west2/runs/mock_pipeline_job?project=test-gcp-project",
                env=source.config.env,
            ),
        )

        mcp_task_container = MetadataChangeProposalWrapper(
            entityUrn=expected_task_urn,
            aspect=ContainerClass(container=source._get_project_container().as_urn()),
        )
        mcp_task_subtype = MetadataChangeProposalWrapper(
            entityUrn=expected_task_urn,
            aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_PIPELINE_TASK]),
        )

        mcp_task_status = MetadataChangeProposalWrapper(
            entityUrn=expected_pipeline_urn,
            aspect=StatusClass(removed=False),
        )

        mcp_task_tag = MetadataChangeProposalWrapper(
            entityUrn=expected_task_urn,
            aspect=GlobalTagsClass(tags=[]),
        )

        dpi_urn = (
            "urn:li:dataProcessInstance:test-gcp-project.pipeline_task_run.reverse"
        )

        mcp_task_run_dpi = MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstancePropertiesClass(
                name="reverse",
                externalUrl="https://console.cloud.google.com/vertex-ai/pipelines/locations/us-west2/runs/mock_pipeline_job?project=test-gcp-project",
                customProperties={},
                created=AuditStamp(
                    time=0,
                    actor="urn:li:corpuser:datahub",
                ),
            ),
        )

        mcp_task_run_container = MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=ContainerClass(container=source._get_project_container().as_urn()),
        )
        mcp_task_run_subtype = MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=SubTypesClass(typeNames=[MLAssetSubTypes.VERTEX_PIPELINE_TASK_RUN]),
        )

        mcp_task_run_dataplatform = MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataPlatformInstanceClass(
                platform=str(DataPlatformUrn(source.platform))
            ),
        )

        mcp_task_run_relationship = MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstanceRelationships(
                upstreamInstances=[], parentTemplate=expected_task_urn
            ),
        )

        assert len(actual_mcps) == 19
        assert any(mcp_pipe_df_info == mcp for mcp in actual_mcps)
        assert any(mcp_pipe_df_status == mcp for mcp in actual_mcps)
        assert any(mcp_pipe_subtype == mcp for mcp in actual_mcps)
        assert any(mcp_pipe_container == mcp for mcp in actual_mcps)
        assert any(mcp_pipeline_tag == mcp for mcp in actual_mcps)
        assert any(mcp_task_input == mcp for mcp in actual_mcps)
        assert any(mcp_task_info == mcp for mcp in actual_mcps)
        assert any(mcp_task_container == mcp for mcp in actual_mcps)
        assert any(mcp_task_subtype == mcp for mcp in actual_mcps)
        assert any(mcp_task_status == mcp for mcp in actual_mcps)
        assert any(mcp_task_tag == mcp for mcp in actual_mcps)

        assert any(mcp_task_run_dpi == mcp for mcp in actual_mcps)
        assert any(mcp_task_run_container == mcp for mcp in actual_mcps)
        assert any(mcp_task_run_subtype == mcp for mcp in actual_mcps)
        assert any(mcp_task_run_dataplatform == mcp for mcp in actual_mcps)
        assert any(mcp_task_run_dataplatform == mcp for mcp in actual_mcps)
        assert any(mcp_task_run_relationship == mcp for mcp in actual_mcps)


def test_make_model_external_url(source: VertexAISource) -> None:
    mock_model = gen_mock_model()
    assert (
        source._make_model_external_url(mock_model)
        == f"{source.config.vertexai_url}/models/locations/{source.config.region}/models/{mock_model.name}"
        f"?project={source._get_current_project_id()}"
    )


def test_make_job_urn(source: VertexAISource) -> None:
    mock_training_job = gen_mock_training_automl_job()
    assert (
        source._make_training_job_urn(mock_training_job)
        == f"{builder.make_data_process_instance_urn(source._make_vertexai_job_name(mock_training_job.name))}"
    )


@pytest.mark.parametrize(
    "start_time,end_time,task_name",
    [
        (None, datetime.fromtimestamp(1647878600, tz=timezone.utc), "incomplete_task"),
        (datetime.fromtimestamp(1647878400, tz=timezone.utc), None, "running_task"),
    ],
)
def test_pipeline_task_with_none_timestamps(
    source: VertexAISource,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    task_name: str,
) -> None:
    mock_pipeline_job = MagicMock(spec=PipelineJob)
    mock_pipeline_job.name = f"test_pipeline_{task_name}"
    mock_pipeline_job.resource_name = (
        "projects/123/locations/us-central1/pipelineJobs/789"
    )
    mock_pipeline_job.labels = {}
    mock_pipeline_job.create_time = datetime.fromtimestamp(1647878400, tz=timezone.utc)
    mock_pipeline_job.update_time = datetime.fromtimestamp(1647878500, tz=timezone.utc)
    mock_pipeline_job.location = "us-west2"

    gca_resource = MagicMock(spec=PipelineJobType)
    mock_pipeline_job.gca_resource = gca_resource

    task_detail = MagicMock(spec=PipelineTaskDetail)
    task_detail.task_name = task_name
    task_detail.task_id = 123
    task_detail.state = MagicMock()
    task_detail.start_time = start_time
    task_detail.create_time = datetime.fromtimestamp(1647878400, tz=timezone.utc)
    task_detail.end_time = end_time

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

        actual_mcps = list(source._get_pipelines_mcps())

        task_run_mcps = [
            mcp
            for mcp in actual_mcps
            if isinstance(mcp.aspect, DataProcessInstancePropertiesClass)
            and task_name in mcp.aspect.name
        ]

        assert len(task_run_mcps) > 0


def test_experiment_run_with_none_timestamps(source: VertexAISource) -> None:
    mock_exp = gen_mock_experiment()
    source.experiments = [mock_exp]

    mock_exp_run = MagicMock(spec=ExperimentRun)
    mock_exp_run.name = "test_run_none_timestamps"
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

        actual_mcps = list(source._get_experiment_runs_mcps())

        run_mcps = [
            mcp
            for mcp in actual_mcps
            if isinstance(mcp.aspect, DataProcessInstancePropertiesClass)
            and "test_run_none_timestamps" in mcp.aspect.name
        ]

        assert len(run_mcps) > 0


class TestVertexAIConfigMultiProject:
    def test_config_backward_compatibility_single_project_id(self) -> None:
        config = VertexAIConfig(project_id="my-project", region="us-central1")

        assert config.project_ids == ["my-project"]

    def test_config_project_ids_takes_precedence(self) -> None:
        config = VertexAIConfig(
            project_id="old-project",
            project_ids=["new-project-1", "new-project-2"],
            region="us-central1",
        )

        assert config.project_ids == ["new-project-1", "new-project-2"]

    def test_config_with_only_project_ids(self) -> None:
        config = VertexAIConfig(
            project_ids=["project-a", "project-b", "project-c"],
            region="us-west2",
        )

        assert config.project_ids == ["project-a", "project-b", "project-c"]

    def test_config_with_project_id_pattern(self) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["prod-project", "dev-project", "test-project"],
                project_id_pattern=AllowDenyPattern(allow=["prod-.*"]),
                region="us-central1",
            ),
        )

        projects = source._get_projects_to_process()
        assert len(projects) == 1
        assert projects[0].id == "prod-project"

    def test_config_pattern_deny(self) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-1", "project-2-test", "project-3"],
                project_id_pattern=AllowDenyPattern(deny=[".*-test$"]),
                region="us-central1",
            ),
        )

        projects = source._get_projects_to_process()
        assert len(projects) == 2
        assert [p.id for p in projects] == ["project-1", "project-3"]

    def test_config_empty_project_ids_allowed(self) -> None:
        config = VertexAIConfig(region="us-central1")
        assert config.project_ids == []


class TestVertexAISourceMultiProject:
    @patch("google.cloud.aiplatform.init")
    def test_source_with_single_project(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_id="single-project", region="us-west2"),
        )

        projects = source._get_projects_to_process()
        assert len(projects) == 1
        assert projects[0].id == "single-project"

    @patch("google.cloud.aiplatform.init")
    def test_source_with_multiple_projects(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-a", "project-b"],
                region="us-central1",
            ),
        )

        projects = source._get_projects_to_process()
        assert len(projects) == 2
        assert projects[0].id == "project-a"
        assert projects[1].id == "project-b"

    @patch("google.cloud.aiplatform.init")
    def test_source_with_pattern_filtering(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["prod-project-1", "dev-project", "prod-project-2"],
                project_id_pattern=AllowDenyPattern(allow=["prod-.*"]),
                region="us-central1",
            ),
        )

        projects = source._get_projects_to_process()
        assert len(projects) == 2
        assert all(p.id.startswith("prod-") for p in projects)

    @patch("google.cloud.aiplatform.init")
    def test_init_for_project_resets_caches(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_id="test-project", region="us-west2"),
        )

        source.endpoints = {"key": [MagicMock(spec=Endpoint), MagicMock(spec=Endpoint)]}
        source.datasets = {"key": MagicMock(spec=VertexAiResourceNoun)}
        source.experiments = [MagicMock(spec=Experiment), MagicMock(spec=Experiment)]

        source._init_for_project(GCPProject(id="new-project", name="New Project"))

        assert source.endpoints is None
        assert source.datasets is None
        assert source.experiments is None
        assert source._current_project_id == "new-project"
        mock_init.assert_called_with(
            project="new-project",
            location="us-west2",
        )

    @patch("google.cloud.aiplatform.init")
    def test_entity_names_include_current_project(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_id="my-project", region="us-west2"),
        )
        source._current_project_id = "my-project"

        assert source._make_vertexai_model_name("123") == "my-project.model.123"
        assert source._make_vertexai_job_name("456") == "my-project.job.456"
        assert (
            source._make_vertexai_experiment_id("exp1") == "my-project.experiment.exp1"
        )
        assert source._make_vertexai_pipeline_id("pipe1") == "my-project.pipeline.pipe1"

    @patch("google.cloud.aiplatform.init")
    def test_project_container_uses_current_project(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_id="container-project", region="us-west2"),
        )
        source._current_project_id = "container-project"

        container = source._get_project_container()
        assert container.project_id == "container-project"


class TestGCPProjectUtils:
    def test_get_projects_from_explicit_list(self) -> None:
        projects = get_projects_from_explicit_list(
            project_ids=["project-1", "project-2", "project-3"]
        )

        assert len(projects) == 3
        assert projects[0].id == "project-1"
        assert projects[1].id == "project-2"
        assert projects[2].id == "project-3"

    def test_get_projects_from_explicit_list_with_pattern(self) -> None:
        pattern = AllowDenyPattern(allow=[".*-prod$"])
        projects = get_projects_from_explicit_list(
            project_ids=["app-prod", "app-dev", "api-prod"],
            project_id_pattern=pattern,
        )

        assert len(projects) == 2
        assert all(p.id.endswith("-prod") for p in projects)

    def test_get_projects_with_explicit_ids(self) -> None:
        projects = get_projects(
            project_ids=["explicit-project-1", "explicit-project-2"],
        )

        assert len(projects) == 2
        assert projects[0].id == "explicit-project-1"
        assert projects[1].id == "explicit-project-2"

    @pytest.mark.parametrize(
        "side_effect,error_match",
        [
            (PermissionDenied("No access"), "Permission denied"),
            (GoogleAPICallError("API error"), "GCP API error"),
        ],
    )
    def test_list_all_accessible_projects_error_handling(
        self, side_effect: Exception, error_match: str
    ) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.side_effect = side_effect
        with pytest.raises(GCPProjectDiscoveryError, match=error_match):
            list(list_all_accessible_projects(mock_client))

    def test_get_projects_auto_discovery_empty_raises_error(self) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.return_value = iter([])

        with pytest.raises(GCPProjectDiscoveryError, match="No projects discovered"):
            get_projects(client=mock_client)

    def test_get_projects_all_filtered_out_raises_error(self) -> None:
        mock_client = MagicMock()
        mock_project = MagicMock()
        mock_project.project_id = "dev-project"
        mock_project.display_name = "Dev Project"
        mock_client.search_projects.return_value = iter([mock_project])

        pattern = AllowDenyPattern(allow=["prod-.*"])

        with pytest.raises(
            GCPProjectDiscoveryError, match="excluded by project_id_pattern"
        ):
            get_projects(client=mock_client, project_id_pattern=pattern)

    @patch("datahub.ingestion.source.common.gcp_project_utils.get_projects_client")
    def test_auto_discovery_all_filtered_by_pattern_at_runtime(
        self, mock_get_client: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_project1 = MagicMock()
        mock_project1.project_id = "dev-project-1"
        mock_project1.display_name = "Dev Project 1"
        mock_project2 = MagicMock()
        mock_project2.project_id = "dev-project-2"
        mock_project2.display_name = "Dev Project 2"
        mock_client.search_projects.return_value = iter([mock_project1, mock_project2])

        pattern = AllowDenyPattern(allow=["prod-.*"])

        with pytest.raises(
            GCPProjectDiscoveryError, match="excluded by project_id_pattern"
        ):
            get_projects(project_id_pattern=pattern)

    @pytest.mark.parametrize(
        "exception,expected",
        [
            (ResourceExhausted("Quota exceeded"), True),
            (GoogleAPICallError("quota limits"), True),
            (GoogleAPICallError("rate limit exceeded"), True),
            (GoogleAPICallError("Invalid argument"), False),
            (PermissionDenied("Access denied"), False),
            (ValueError("Some error"), False),
        ],
    )
    def test_is_rate_limit_error(self, exception: Exception, expected: bool) -> None:
        assert _is_rate_limit_error(exception) == expected

    @pytest.mark.parametrize(
        "side_effect,error_match",
        [
            (PermissionDenied("No access"), "Permission denied"),
            (GoogleAPICallError("API failure"), "GCP API error"),
        ],
    )
    def test_get_projects_by_labels_error_handling(
        self, side_effect: Exception, error_match: str
    ) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.side_effect = side_effect
        with pytest.raises(GCPProjectDiscoveryError, match=error_match):
            get_projects_by_labels(["env:prod"], mock_client)

    def test_get_projects_by_labels_empty_result(self) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.return_value = iter([])
        with pytest.raises(GCPProjectDiscoveryError, match="No projects match labels"):
            get_projects_by_labels(["env:prod"], mock_client)

    def test_get_projects_by_labels_all_filtered_out(self) -> None:
        mock_client = MagicMock()
        mock_project = MagicMock()
        mock_project.project_id = "dev-project"
        mock_project.display_name = "Dev Project"
        mock_client.search_projects.return_value = iter([mock_project])
        with pytest.raises(
            GCPProjectDiscoveryError, match="excluded by project_id_pattern"
        ):
            get_projects_by_labels(
                ["env:prod"], mock_client, AllowDenyPattern(allow=["prod-.*"])
            )

    def test_get_projects_by_labels_success(self) -> None:
        mock_client = MagicMock()
        mock_project = MagicMock()
        mock_project.project_id = "prod-project"
        mock_project.display_name = "Prod Project"
        mock_client.search_projects.return_value = iter([mock_project])
        projects = get_projects_by_labels(["env:prod"], mock_client)
        assert len(projects) == 1
        assert projects[0].id == "prod-project"

    def test_get_projects_by_labels_empty_list_raises_error(self) -> None:
        mock_client = MagicMock()
        with pytest.raises(GCPProjectDiscoveryError, match="cannot be empty"):
            get_projects_by_labels([], mock_client)

    def test_search_projects_logs_error_for_missing_project_id(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_client = MagicMock()
        mock_project_valid = MagicMock()
        mock_project_valid.project_id = "valid-project"
        mock_project_valid.display_name = "Valid Project"
        mock_project_invalid = MagicMock()
        mock_project_invalid.project_id = None
        mock_project_invalid.display_name = "Invalid Project"
        mock_client.search_projects.return_value = iter(
            [mock_project_valid, mock_project_invalid]
        )

        with caplog.at_level(logging.ERROR):
            projects = list(_search_projects_with_retry(mock_client, "state:ACTIVE"))

        assert len(projects) == 1
        assert projects[0].id == "valid-project"
        assert "GCP returned project without project_id" in caplog.text

    @pytest.mark.parametrize(
        "projects,pattern,expected_ids",
        [
            ([], AllowDenyPattern.allow_all(), []),
            (
                [GCPProject(id="p1", name="P1"), GCPProject(id="p2", name="P2")],
                AllowDenyPattern.allow_all(),
                ["p1", "p2"],
            ),
            (
                [GCPProject(id="dev", name="Dev"), GCPProject(id="test", name="Test")],
                AllowDenyPattern(allow=["prod-.*"]),
                [],
            ),
            (
                [
                    GCPProject(id="prod-app", name="Prod"),
                    GCPProject(id="dev", name="Dev"),
                ],
                AllowDenyPattern(allow=["prod-.*"]),
                ["prod-app"],
            ),
        ],
    )
    def test_filter_projects_by_pattern(
        self,
        projects: List[GCPProject],
        pattern: AllowDenyPattern,
        expected_ids: List[str],
    ) -> None:
        result = _filter_projects_by_pattern(projects, pattern)
        assert [p.id for p in result] == expected_ids

    def test_validate_and_filter_projects_success(self) -> None:
        projects = [
            GCPProject(id="project-1", name="Project 1"),
            GCPProject(id="project-2", name="Project 2"),
        ]
        pattern = AllowDenyPattern.allow_all()
        result = _validate_and_filter_projects(projects, pattern, "test source")
        assert len(result) == 2

    def test_validate_and_filter_projects_all_filtered_raises_error(self) -> None:
        projects = [
            GCPProject(id="dev-project", name="Dev Project"),
        ]
        pattern = AllowDenyPattern(allow=["prod-.*"])
        with pytest.raises(
            GCPProjectDiscoveryError, match="excluded by project_id_pattern"
        ):
            _validate_and_filter_projects(projects, pattern, "test source")

    @pytest.mark.parametrize(
        "exception,expected_match",
        [
            (DeadlineExceeded("Request timed out"), "timed out"),
            (
                ServiceUnavailable("Service temporarily unavailable"),
                "temporarily unavailable",
            ),
        ],
    )
    def test_get_projects_by_labels_handles_transient_errors(
        self, exception: Exception, expected_match: str
    ) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.side_effect = exception

        with pytest.raises(GCPProjectDiscoveryError, match=expected_match):
            get_projects_by_labels(["env:prod"], mock_client)


class TestPartialFailureHandling:
    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_discovery_error_raises_not_swallowed(
        self,
        mock_get_projects: MagicMock,
        mock_get_projects_client: MagicMock,
        mock_init: MagicMock,
    ) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(region="us-west2"),
        )

        mock_get_projects.side_effect = GCPProjectDiscoveryError("Permission denied")

        with pytest.raises(GCPProjectDiscoveryError):
            list(source.get_workunits_internal())

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_partial_failure_continues_processing(
        self,
        mock_get_projects: MagicMock,
        mock_get_client: MagicMock,
        mock_init: MagicMock,
    ) -> None:
        # Use discovered projects (not explicit) so PermissionDenied doesn't fail fast
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_labels=["env:prod"],
                region="us-west2",
            ),
        )

        mock_get_projects.return_value = [
            GCPProject(id="project-1", name="Project 1"),
            GCPProject(id="project-2", name="Project 2"),
            GCPProject(id="project-3", name="Project 3"),
        ]

        call_count = 0

        def mock_process():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise PermissionDenied("Access denied to project")
            yield from []

        with patch.object(source, "_process_current_project", side_effect=mock_process):
            list(source.get_workunits_internal())

        assert len(source.report.failures) == 1

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_partial_success_processes_remaining_projects(
        self,
        mock_get_projects: MagicMock,
        mock_get_client: MagicMock,
        mock_init: MagicMock,
    ) -> None:
        # Use discovered projects (not explicit) so PermissionDenied doesn't fail fast
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_labels=["env:prod"],
                region="us-west2",
            ),
        )

        mock_get_projects.return_value = [
            GCPProject(id="project-1", name="Project 1"),
            GCPProject(id="project-2", name="Project 2"),
            GCPProject(id="project-3", name="Project 3"),
        ]

        processed_projects: List[str] = []

        def mock_process():
            project_id = source._current_project_id
            assert project_id is not None
            if project_id == "project-2":
                raise PermissionDenied("Access denied")
            processed_projects.append(project_id)
            yield from []

        with patch.object(source, "_process_current_project", side_effect=mock_process):
            list(source.get_workunits_internal())

        assert len(source.report.failures) == 1
        assert len(processed_projects) == 2
        assert "project-1" in processed_projects
        assert "project-3" in processed_projects
        assert "project-2" not in processed_projects

        assert len(source.report.warnings) == 1

    @patch("google.cloud.aiplatform.init")
    def test_permission_denied_fails_fast_for_explicit_ids(
        self, mock_init: MagicMock
    ) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-1", "project-2"],
                region="us-west2",
            ),
        )

        source._projects = [
            GCPProject(id="project-1", name="Project 1"),
            GCPProject(id="project-2", name="Project 2"),
        ]

        def mock_process():
            raise PermissionDenied("Access denied")
            yield

        with (
            patch.object(source, "_process_current_project", side_effect=mock_process),
            pytest.raises(RuntimeError, match="Permission denied for project"),
        ):
            list(source.get_workunits_internal())

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_all_projects_fail_raises_error(
        self,
        mock_get_projects: MagicMock,
        mock_get_client: MagicMock,
        mock_init: MagicMock,
    ) -> None:
        # Use discovered projects so we test partial failure path, not fail-fast
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_labels=["env:prod"],
                region="us-west2",
            ),
        )

        mock_get_projects.return_value = [
            GCPProject(id="project-1", name="Project 1"),
            GCPProject(id="project-2", name="Project 2"),
        ]

        def mock_process():
            raise GoogleAPICallError("API error")
            yield

        with (
            patch.object(source, "_process_current_project", side_effect=mock_process),
            pytest.raises(RuntimeError, match="All .* projects failed"),
        ):
            list(source.get_workunits_internal())

    @patch("google.cloud.aiplatform.init")
    def test_api_error_continues_with_partial_failure(
        self, mock_init: MagicMock
    ) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-1", "project-2", "project-3"],
                region="us-west2",
            ),
        )

        source._projects = [
            GCPProject(id="project-1", name="Project 1"),
            GCPProject(id="project-2", name="Project 2"),
            GCPProject(id="project-3", name="Project 3"),
        ]

        def mock_process():
            raise GoogleAPICallError("Invalid argument: malformed request")
            yield

        with (
            patch.object(source, "_process_current_project", side_effect=mock_process),
            pytest.raises(RuntimeError, match="All .* projects failed"),
        ):
            list(source.get_workunits_internal())

        assert len(source.report.failures) == 3

    @patch("google.cloud.aiplatform.init")
    def test_not_found_error_fails_fast_for_explicit_ids(
        self, mock_init: MagicMock
    ) -> None:
        """NotFound on explicit project_ids should fail fast (likely a typo)."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["missing-project", "project-2"],
                region="us-west2",
            ),
        )

        source._projects = [
            GCPProject(id="missing-project", name="Missing Project"),
            GCPProject(id="project-2", name="Project 2"),
        ]

        def mock_process():
            raise NotFound("Project not found")
            yield

        with (
            patch.object(source, "_process_current_project", side_effect=mock_process),
            pytest.raises(RuntimeError, match="not found.*explicitly configured"),
        ):
            list(source.get_workunits_internal())

    @patch("google.cloud.aiplatform.init")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_not_found_error_continues_for_discovered_projects(
        self,
        mock_get_projects: MagicMock,
        mock_get_client: MagicMock,
        mock_init: MagicMock,
    ) -> None:
        """NotFound on discovered projects should continue (project may have been deleted)."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(region="us-west2"),
        )

        mock_get_projects.return_value = [
            GCPProject(id="deleted-project", name="Deleted Project"),
            GCPProject(id="project-2", name="Project 2"),
        ]

        call_count = 0

        def mock_process():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise NotFound("Project not found")
            yield from []

        with patch.object(source, "_process_current_project", side_effect=mock_process):
            list(source.get_workunits_internal())

        assert len(source.report.failures) == 1

    @pytest.mark.parametrize(
        "exception",
        [
            InvalidArgument("Invalid region specified"),
            FailedPrecondition("Vertex AI API not enabled"),
        ],
    )
    @patch("google.cloud.aiplatform.init")
    def test_config_error_fails_fast(
        self, mock_init: MagicMock, exception: Exception
    ) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-1", "project-2"],
                region="invalid-region",
            ),
        )

        source._projects = [
            GCPProject(id="project-1", name="Project 1"),
            GCPProject(id="project-2", name="Project 2"),
        ]

        def mock_process():
            raise exception
            yield

        with (
            patch.object(source, "_process_current_project", side_effect=mock_process),
            pytest.raises(RuntimeError, match="Configuration error"),
        ):
            list(source.get_workunits_internal())

    @pytest.mark.parametrize(
        "project_id,exception,expected_log_content",
        [
            (
                "test-project",
                PermissionDenied("Access denied"),
                "gcloud projects get-iam-policy",
            ),
            (
                "missing-project",
                NotFound("Project not found"),
                "gcloud projects describe",
            ),
            ("quota-project", GoogleAPICallError("Quota exceeded"), "GCP API error"),
        ],
    )
    @patch("google.cloud.aiplatform.init")
    def test_handle_project_error(
        self,
        mock_init: MagicMock,
        project_id: str,
        exception: Exception,
        expected_log_content: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_ids=["test-project"], region="us-west2"),
        )
        with caplog.at_level(logging.WARNING):
            source._handle_project_error(project_id, exception)
        assert len(source.report.failures) == 1
        assert expected_log_content in caplog.text
        assert project_id in caplog.text


class TestConfigValidation:
    @pytest.mark.parametrize(
        "project_ids,pattern",
        [
            (["dev-project", "test-project"], AllowDenyPattern(allow=["prod-.*"])),
            (["test-project"], AllowDenyPattern(deny=[".*"])),
        ],
    )
    def test_all_projects_filtered_out_raises_value_error(
        self, project_ids: List[str], pattern: AllowDenyPattern
    ) -> None:
        with pytest.raises(ValueError, match="filtered out"):
            VertexAIConfig(
                project_ids=project_ids,
                project_id_pattern=pattern,
                region="us-central1",
            )

    def test_empty_string_in_project_ids_raises_error(self) -> None:
        with pytest.raises(ValueError, match="empty or whitespace-only"):
            VertexAIConfig(
                project_ids=["valid-project", "", "another-project"],
                region="us-central1",
            )

    def test_whitespace_only_project_id_raises_error(self) -> None:
        with pytest.raises(ValueError, match="empty or whitespace-only"):
            VertexAIConfig(
                project_ids=["valid-project", "   ", "another-project"],
                region="us-central1",
            )

    def test_duplicate_project_ids_raises_error(self) -> None:
        with pytest.raises(ValueError, match="duplicates"):
            VertexAIConfig(
                project_ids=["project-a", "project-a", "project-b"],
                region="us-central1",
            )

    def test_empty_explicit_project_ids_allowed(self) -> None:
        config = VertexAIConfig(
            project_ids=[],
            region="us-central1",
        )
        assert config.project_ids == []

    def test_restrictive_allow_pattern_with_auto_discovery_raises_error(self) -> None:
        with pytest.raises(
            ValueError, match="Auto-discovery with restrictive allow patterns"
        ):
            VertexAIConfig(
                project_id_pattern=AllowDenyPattern(allow=["prod-.*"]),
                region="us-central1",
            )

    def test_deny_only_pattern_with_auto_discovery_allowed(self) -> None:
        config = VertexAIConfig(
            project_id_pattern=AllowDenyPattern(deny=["dev-.*"]),
            region="us-central1",
        )
        assert config.project_id_pattern.deny == ["dev-.*"]


class TestCacheInvalidation:
    @patch("google.cloud.aiplatform.init")
    def test_caches_cleared_between_projects(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-1", "project-2"],
                region="us-central1",
            ),
        )

        source.endpoints = {"endpoint-1": MagicMock()}
        source.datasets = {"dataset-1": MagicMock()}
        source.experiments = [MagicMock()]

        project = GCPProject(id="project-2", name="Project 2")
        source._init_for_project(project)

        assert source.endpoints is None
        assert source.datasets is None
        assert source.experiments is None
        assert source._current_project_id == "project-2"

    @patch("google.cloud.aiplatform.init")
    def test_aiplatform_init_called_per_project(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-1", "project-2"],
                region="us-central1",
            ),
        )

        source._init_for_project(GCPProject(id="project-1", name="Project 1"))
        source._init_for_project(GCPProject(id="project-2", name="Project 2"))

        assert mock_init.call_count == 2
        mock_init.assert_any_call(project="project-1", location="us-central1")
        mock_init.assert_any_call(project="project-2", location="us-central1")

    @patch("google.cloud.aiplatform.init")
    def test_no_cross_project_data_leakage(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-a", "project-b"],
                region="us-central1",
            ),
        )

        project_a_endpoint = MagicMock()
        project_a_endpoint.name = "project-a-endpoint"
        project_a_dataset = MagicMock()
        project_a_dataset.name = "project-a-dataset"
        project_a_experiment = MagicMock()
        project_a_experiment.name = "project-a-experiment"

        source._init_for_project(GCPProject(id="project-a", name="Project A"))
        source.endpoints = {"endpoint-1": [project_a_endpoint]}
        source.datasets = {"dataset-1": project_a_dataset}
        source.experiments = [project_a_experiment]

        assert source.endpoints is not None
        assert "endpoint-1" in source.endpoints

        source._init_for_project(GCPProject(id="project-b", name="Project B"))

        assert source.endpoints is None, "Project A's endpoints leaked to Project B"
        assert source.datasets is None, "Project A's datasets leaked to Project B"
        assert source.experiments is None, "Project A's experiments leaked to Project B"
        assert source._current_project_id == "project-b"


class TestCredentialsBehavior:
    def test_credentials_set_env_var(self) -> None:
        config = VertexAIConfig(
            project_ids=["project-1", "project-2"],
            region="us-central1",
            credential=GCPCredential(
                type="service_account",
                project_id="credential-project",
                private_key_id="key-id",
                private_key="-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----\n",
                client_email="sa@project.iam.gserviceaccount.com",
                client_id="123",
                auth_uri="https://accounts.google.com/o/oauth2/auth",
                token_uri="https://oauth2.googleapis.com/token",
                auth_provider_x509_cert_url="https://www.googleapis.com/oauth2/v1/certs",
                client_x509_cert_url="https://www.googleapis.com/cert",
            ),
        )

        assert config._credentials_path is not None
        assert (
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == config._credentials_path
        )

    def test_no_credentials_no_env_var_set(self) -> None:
        original_val = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if original_val:
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

        try:
            config = VertexAIConfig(
                project_ids=["project-1"],
                region="us-central1",
            )
            assert config._credentials_path is None
        finally:
            if original_val:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original_val


class TestProjectLabelsSupport:
    def test_config_with_project_labels(self) -> None:
        config = VertexAIConfig(
            project_labels=["env:prod", "team:ml"],
            region="us-central1",
        )
        assert config.project_labels == ["env:prod", "team:ml"]
        assert config.project_ids == []

    def test_valid_label_formats(self) -> None:
        config = VertexAIConfig(
            project_labels=["env:prod", "team", "cost_center:123", "my-label:value-1"],
            region="us-central1",
        )
        assert len(config.project_labels) == 4

    @pytest.mark.parametrize(
        "invalid_label",
        ["env=prod", "Env:prod", "123env:prod", "env:"],
    )
    def test_invalid_label_format(self, invalid_label: str) -> None:
        with pytest.raises(ValueError, match="Invalid project_labels format"):
            VertexAIConfig(project_labels=[invalid_label], region="us-central1")

    def test_project_ids_takes_precedence_over_labels(self) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["explicit-project"],
                project_labels=["env:prod"],
                region="us-central1",
            ),
        )

        with patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client"):
            projects = source._get_projects_to_process()

        assert len(projects) == 1
        assert projects[0].id == "explicit-project"

    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects_client")
    @patch("datahub.ingestion.source.vertexai.vertexai.get_projects")
    def test_labels_discovery_called_when_no_project_ids(
        self, mock_get_projects: MagicMock, mock_client: MagicMock
    ) -> None:
        mock_get_projects.return_value = [
            GCPProject(id="labeled-project", name="Labeled Project")
        ]

        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_labels=["env:prod"],
                region="us-central1",
            ),
        )

        source._get_projects_to_process()

        mock_get_projects.assert_called_once()
        call_kwargs = mock_get_projects.call_args.kwargs
        assert call_kwargs["project_ids"] is None
        assert call_kwargs["project_labels"] == ["env:prod"]


class TestCredentialCleanup:
    @patch("google.cloud.aiplatform.init")
    def test_close_cleans_up_credentials(self, mock_init: MagicMock) -> None:
        config = VertexAIConfig(
            project_ids=["test-project"],
            region="us-central1",
            credential=GCPCredential(
                private_key_id="test-key-id",
                private_key="-----BEGIN PRIVATE KEY-----\nMIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAq7BFUpkGp3+LQmlQ\n-----END PRIVATE KEY-----\n",
                client_email="test@project.iam.gserviceaccount.com",
                client_id="123456789",
            ),
        )

        assert config._credentials_path is not None
        temp_path = config._credentials_path
        assert os.path.exists(temp_path)

        source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)
        source.close()

        assert not os.path.exists(temp_path)

    def test_cleanup_credentials_silently_ignores_missing_file(self) -> None:
        _cleanup_credentials("/nonexistent/path/to/credentials.json")

    def test_cleanup_credentials_handles_none_path(self) -> None:
        _cleanup_credentials(None)
