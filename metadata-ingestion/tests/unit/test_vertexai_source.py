import contextlib
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import (
    DeadlineExceeded,
    FailedPrecondition,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)
from google.cloud.aiplatform import Endpoint, Experiment, ExperimentRun, PipelineJob
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
    _validate_pattern_before_discovery,
    gcp_credentials_context,
    get_projects_from_explicit_list,
    is_gcp_transient_error,
)
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.vertexai.vertexai import (
    ModelMetadata,
    TrainingJobMetadata,
    VertexAIConfig,
    VertexAISource,
    _call_with_retry,
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

    creds_dict = config.get_credentials_dict()
    assert creds_dict is not None
    assert creds_dict["private_key_id"] == "test-key-id"


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


class TestNoneTimestampHandling:
    """Verify ingestion handles None timestamps gracefully across different entity types."""

    @pytest.mark.parametrize(
        "start_time,end_time",
        [
            (None, datetime.fromtimestamp(1647878600, tz=timezone.utc)),
            (datetime.fromtimestamp(1647878400, tz=timezone.utc), None),
        ],
    )
    def test_pipeline_task_with_none_timestamps(
        self,
        source: VertexAISource,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> None:
        """Pipeline tasks with None start_time or end_time should not crash."""
        mock_pipeline_job = MagicMock(spec=PipelineJob)
        mock_pipeline_job.name = "test_pipeline"
        mock_pipeline_job.resource_name = (
            "projects/123/locations/us-central1/pipelineJobs/789"
        )
        mock_pipeline_job.labels = {}
        mock_pipeline_job.create_time = datetime.fromtimestamp(
            1647878400, tz=timezone.utc
        )
        mock_pipeline_job.update_time = datetime.fromtimestamp(
            1647878500, tz=timezone.utc
        )
        mock_pipeline_job.location = "us-west2"

        gca_resource = MagicMock(spec=PipelineJobType)
        mock_pipeline_job.gca_resource = gca_resource

        task_detail = MagicMock(spec=PipelineTaskDetail)
        task_detail.task_name = "test_task"
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
                        "test_task": {
                            "componentRef": {"name": "comp-test_task"},
                            "taskInfo": {"name": "test_task"},
                        }
                    }
                }
            }
        }

        with patch("google.cloud.aiplatform.PipelineJob.list") as mock:
            mock.return_value = [mock_pipeline_job]
            actual_mcps = list(source._get_pipelines_mcps())
            assert len(actual_mcps) > 0

    def test_experiment_run_with_none_timestamps(self, source: VertexAISource) -> None:
        """Experiment runs with None create_time/update_time should not crash."""
        mock_exp = gen_mock_experiment()
        source.experiments = [mock_exp]

        mock_exp_run = MagicMock(spec=ExperimentRun)
        mock_exp_run.name = "test_run"
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
            assert len(actual_mcps) > 0


class TestMultiProjectConfig:
    def test_backward_compatibility_project_id(self) -> None:
        config = VertexAIConfig.model_validate(
            {"project_id": "my-project", "region": "us-central1"}
        )
        assert config.project_ids == ["my-project"]

    def test_project_ids_takes_precedence(self) -> None:
        config = VertexAIConfig.model_validate(
            {
                "project_id": "old-project",
                "project_ids": ["new-project-1", "new-project-2"],
                "region": "us-central1",
            }
        )
        assert config.project_ids == ["new-project-1", "new-project-2"]

    @pytest.mark.parametrize(
        "project_ids,pattern,expected_count",
        [
            (
                ["prod-project-1", "dev-project-1", "prod-project-2"],
                AllowDenyPattern(allow=["prod-.*"]),
                2,
            ),
            (
                ["project-alpha", "project-beta-test", "project-gamma"],
                AllowDenyPattern(deny=[".*-test$"]),
                2,
            ),
        ],
    )
    def test_pattern_filtering(
        self, project_ids: List[str], pattern: AllowDenyPattern, expected_count: int
    ) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=project_ids,
                project_id_pattern=pattern,
                region="us-central1",
            ),
        )
        assert len(source._get_projects_to_process()) == expected_count

    @pytest.mark.parametrize("invalid_label", ["env=prod", "Env:prod", "123env:prod"])
    def test_invalid_label_format_raises_error(self, invalid_label: str) -> None:
        with pytest.raises(ValueError, match="Invalid project_labels format"):
            VertexAIConfig(project_labels=[invalid_label], region="us-central1")

    @pytest.mark.parametrize(
        "config_kwargs,error_match",
        [
            (
                {
                    "project_ids": ["dev-project-1", "test-project-2"],
                    "project_id_pattern": AllowDenyPattern(allow=["prod-.*"]),
                },
                "filtered out",
            ),
            (
                {"project_ids": []},
                "cannot be an empty list",
            ),
            (
                {"project_ids": ["valid-project", "", "other-project"]},
                "empty values",
            ),
            (
                {"project_ids": ["project-one", "project-one", "project-two"]},
                "duplicates",
            ),
            (
                {"project_id_pattern": AllowDenyPattern(allow=["prod-.*"])},
                "Auto-discovery with restrictive",
            ),
            (
                {"project_ids": ["MY-PROJECT", "valid-project"]},
                "Invalid project_ids format",
            ),
            (
                {"project_ids": ["ab"]},
                "Invalid project_ids format",
            ),
            (
                {"project_id_pattern": AllowDenyPattern(allow=["[invalid"])},
                "Invalid regex",
            ),
        ],
    )
    def test_config_validation_errors(
        self, config_kwargs: dict, error_match: str
    ) -> None:
        with pytest.raises(ValueError, match=error_match):
            VertexAIConfig(region="us-central1", **config_kwargs)

    def test_deprecated_project_id_with_pattern_shows_correct_field_name(self) -> None:
        """When deprecated project_id is filtered out by pattern, error mentions project_ids."""
        with pytest.raises(ValueError, match=r"project_ids.*filtered out"):
            VertexAIConfig.model_validate(
                {
                    "project_id": "dev-project",
                    "project_id_pattern": {"allow": ["prod-.*"]},
                    "region": "us-central1",
                }
            )


class TestMultiProjectExecution:
    @patch("google.cloud.aiplatform.init")
    def test_init_for_project_resets_caches(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_ids=["test-project"], region="us-west2"),
        )
        source.endpoints = {"key": [MagicMock(spec=Endpoint)]}
        source.datasets = {"key": MagicMock()}
        source.experiments = [MagicMock()]

        source._init_for_project(GCPProject(id="new-project", name="New"))

        assert source.endpoints is None
        assert source.datasets is None
        assert source.experiments is None
        assert source._current_project_id == "new-project"

    @patch("google.cloud.aiplatform.init")
    def test_entity_names_include_project(self, mock_init: MagicMock) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_ids=["my-project"], region="us-west2"),
        )
        source._current_project_id = "my-project"

        assert source._make_vertexai_model_name("123") == "my-project.model.123"
        assert source._make_vertexai_job_name("456") == "my-project.job.456"


class TestErrorHandling:
    @pytest.mark.parametrize(
        "exception",
        [NotFound("Not found"), PermissionDenied("Access denied")],
    )
    @patch("google.cloud.aiplatform.init")
    def test_single_project_error_reports_failure(
        self, mock_init: MagicMock, exception: Exception
    ) -> None:
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_ids=["test-project"], region="us-west2"),
        )
        source._projects = [GCPProject(id="test-project", name="Test")]

        def mock_process():
            raise exception
            yield

        with (
            patch.object(source, "_process_current_project", side_effect=mock_process),
            pytest.raises(RuntimeError, match="All .* projects failed"),
        ):
            list(source.get_workunits_internal())

        assert len(source.report.failures) == 1
        assert "test-project" in str(source.report.failures)

    @pytest.mark.parametrize(
        "exception",
        [
            InvalidArgument("Invalid region"),
            InvalidArgument("Vertex AI API not enabled"),
            FailedPrecondition("API not enabled"),
            FailedPrecondition("Invalid region configuration"),
        ],
    )
    @patch("google.cloud.aiplatform.init")
    def test_config_error_reports_failure(
        self, mock_init: MagicMock, exception: Exception
    ) -> None:
        """Config errors (InvalidArgument, FailedPrecondition) are reported as failures."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_ids=["project-one"], region="bad-region"),
        )
        source._projects = [GCPProject(id="project-one", name="Project One")]

        def mock_process():
            raise exception
            yield

        with (
            patch.object(source, "_process_current_project", side_effect=mock_process),
            pytest.raises(RuntimeError, match="All .* projects failed"),
        ):
            list(source.get_workunits_internal())

        assert len(source.report.failures) >= 1
        assert "Configuration error" in str(source.report.failures)


class TestMixedProjectErrors:
    """Test partial success scenarios with multiple projects."""

    @patch("google.cloud.aiplatform.init")
    def test_partial_success_with_mixed_error_types(self, mock_init: MagicMock) -> None:
        """When some projects succeed and others fail with different errors."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-ok", "project-denied", "project-notfound"],
                region="us-west2",
            ),
        )
        source._projects = [
            GCPProject(id="project-ok", name="OK"),
            GCPProject(id="project-denied", name="Denied"),
            GCPProject(id="project-notfound", name="NotFound"),
        ]

        call_count = 0

        def mock_process():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return
                yield
            elif call_count == 2:
                raise PermissionDenied("Access denied")
            else:
                raise NotFound("Project not found")

        with patch.object(source, "_process_current_project", side_effect=mock_process):
            list(source.get_workunits_internal())

        assert len(source.report.failures) == 2
        assert len(source.report.warnings) >= 1
        warning_msg = str(source.report.warnings)
        assert "2" in warning_msg and "3" in warning_msg

    @patch("google.cloud.aiplatform.init")
    def test_all_projects_fail_with_different_errors(
        self, mock_init: MagicMock
    ) -> None:
        """When all projects fail with different error types, error message includes all types."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["proj-invalid", "proj-denied", "proj-notfound"],
                region="us-west2",
            ),
        )
        source._projects = [
            GCPProject(id="proj-invalid", name="Invalid"),
            GCPProject(id="proj-denied", name="Denied"),
            GCPProject(id="proj-notfound", name="NotFound"),
        ]

        call_count = 0

        def mock_process():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise InvalidArgument("Invalid config")
            elif call_count == 2:
                raise PermissionDenied("Access denied")
            else:
                raise NotFound("Not found")
            yield

        with (
            patch.object(source, "_process_current_project", side_effect=mock_process),
            pytest.raises(RuntimeError) as exc_info,
        ):
            list(source.get_workunits_internal())

        assert "3 projects failed" in str(exc_info.value)
        assert len(source.report.failures) == 3


class TestErrorContextBuilding:
    """Tests for _build_project_error_context() method."""

    @patch("google.cloud.aiplatform.init")
    def test_known_exception_types_return_specific_context(
        self, mock_init: MagicMock
    ) -> None:
        """Each known GCP exception type should return appropriate context."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_ids=["test-project-123"], region="us-west2"),
        )

        debug_cmd, tips = source._build_project_error_context(
            "my-project", PermissionDenied("Access denied")
        )
        assert "get-iam-policy" in debug_cmd
        assert "Permission denied" in tips
        assert "Vertex AI User" in tips

        debug_cmd, tips = source._build_project_error_context(
            "my-project", NotFound("Not found")
        )
        assert "describe" in debug_cmd
        assert "not found" in tips.lower()

        debug_cmd, tips = source._build_project_error_context(
            "my-project", InvalidArgument("API not enabled")
        )
        assert "enable aiplatform" in debug_cmd
        assert "not enabled" in tips.lower()

    @patch("google.cloud.aiplatform.init")
    def test_unknown_exception_falls_back_to_default(
        self, mock_init: MagicMock
    ) -> None:
        """Unknown exception types should use default context with error type prefix."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_ids=["test-project-123"], region="us-west2"),
        )

        debug_cmd, tips = source._build_project_error_context(
            "my-project", ResourceExhausted("Quota exceeded")
        )
        assert "my-project" in debug_cmd
        assert "Rate limit exceeded" in tips

    @patch("google.cloud.aiplatform.init")
    def test_project_id_substituted_in_debug_command(
        self, mock_init: MagicMock
    ) -> None:
        """Project ID should be substituted in debug command template."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(project_ids=["test-project-123"], region="us-west2"),
        )

        debug_cmd, _ = source._build_project_error_context(
            "specific-project-id", PermissionDenied("Denied")
        )
        assert "specific-project-id" in debug_cmd


class TestDeprecationWarnings:
    """Tests for backward compatibility deprecation warnings."""

    def test_project_id_emits_deprecation_warning(self, caplog: Any) -> None:
        """Using deprecated project_id should emit a warning."""
        with caplog.at_level(logging.WARNING):
            config = VertexAIConfig.model_validate(
                {"project_id": "legacy-project", "region": "us-central1"}
            )

        assert config.project_ids == ["legacy-project"]
        assert any("deprecated" in record.message.lower() for record in caplog.records)

    def test_both_fields_prefers_project_ids_with_warning(self, caplog: Any) -> None:
        """When both fields provided, project_ids wins with a warning."""
        with caplog.at_level(logging.WARNING):
            config = VertexAIConfig.model_validate(
                {
                    "project_id": "old-project",
                    "project_ids": ["new-project-one", "new-project-two"],
                    "region": "us-central1",
                }
            )

        assert config.project_ids == ["new-project-one", "new-project-two"]
        assert any("ignoring" in record.message.lower() for record in caplog.records)


class TestCredentialManagement:
    def test_get_credentials_dict_returns_dict_when_credential_set(self) -> None:
        config = VertexAIConfig(
            project_ids=["test-project-123"],
            region="us-central1",
            credential=GCPCredential(
                private_key_id="key-id",
                private_key="-----BEGIN PRIVATE KEY-----\nMIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAq7BFUpkGp3+LQmlQ\n-----END PRIVATE KEY-----\n",
                client_email="sa@project.iam.gserviceaccount.com",
                client_id="123",
            ),
        )
        creds_dict = config.get_credentials_dict()
        assert creds_dict is not None
        assert creds_dict["private_key_id"] == "key-id"
        assert creds_dict["client_email"] == "sa@project.iam.gserviceaccount.com"
        assert creds_dict["type"] == "service_account"

    def test_get_credentials_dict_returns_none_when_no_credential(self) -> None:
        config = VertexAIConfig(project_ids=["test-project-123"], region="us-central1")
        assert config.get_credentials_dict() is None


class TestRetryLogic:
    @pytest.mark.parametrize(
        "exception,expected",
        [
            (ResourceExhausted("Quota exceeded"), True),
            (ServiceUnavailable("Service unavailable"), True),
            (DeadlineExceeded("Timeout"), True),
            (PermissionDenied("Access denied"), False),
            (NotFound("Not found"), False),
            (InvalidArgument("Invalid argument"), False),
        ],
    )
    def test_is_retryable_error(self, exception: Exception, expected: bool) -> None:
        assert is_gcp_transient_error(exception) == expected

    def test_retry_on_rate_limit(self) -> None:
        with patch(
            "datahub.ingestion.source.common.gcp_project_utils.gcp_api_retry"
        ) as mock_retry:
            mock_retry.return_value = lambda f: f
            result = _call_with_retry(lambda: ["result"])
            assert result == ["result"]

    def test_no_retry_on_permission_denied(self) -> None:
        def mock_list():
            raise PermissionDenied("Access denied")

        with patch(
            "datahub.ingestion.source.common.gcp_project_utils.gcp_api_retry"
        ) as mock_retry:
            mock_retry.return_value = lambda f: f
            with pytest.raises(PermissionDenied):
                _call_with_retry(mock_list)


class TestLineageEdgeCases:
    def test_training_job_with_missing_output_model(
        self, source: VertexAISource
    ) -> None:
        """AutoML job that doesn't produce a model should still generate valid MCPs."""
        mock_training_job = gen_mock_training_automl_job()
        job_meta = TrainingJobMetadata(mock_training_job)

        actual_mcps = list(source._gen_training_job_mcps(job_meta))

        assert len(actual_mcps) >= 5
        for mcp in actual_mcps:
            if mcp.aspect is None:
                continue
            outputs = getattr(mcp.aspect, "outputs", None)
            if outputs is not None:
                assert outputs == []
            output_edges = getattr(mcp.aspect, "outputEdges", None)
            if output_edges is not None:
                assert output_edges == []

    def test_training_job_with_missing_dataset(self, source: VertexAISource) -> None:
        """Training job with no input dataset should generate valid MCPs."""
        mock_training_job = gen_mock_training_custom_job()
        job_meta = TrainingJobMetadata(mock_training_job)

        actual_mcps = list(source._gen_training_job_mcps(job_meta))

        assert len(actual_mcps) >= 5
        input_mcps = [
            mcp
            for mcp in actual_mcps
            if isinstance(mcp.aspect, DataProcessInstanceInputClass)
        ]
        for mcp in input_mcps:
            aspect = mcp.aspect
            assert isinstance(aspect, DataProcessInstanceInputClass)
            assert aspect.inputEdges == []

    def test_model_without_deployment_endpoint(self, source: VertexAISource) -> None:
        """Model that's never deployed should still generate valid MCPs."""
        mock_model = gen_mock_model()
        mock_model_version = MagicMock()
        mock_model_version.version_id = "1"
        mock_model_version.version_create_time = datetime.fromtimestamp(
            1647878400, tz=timezone.utc
        )
        mock_model_version.version_update_time = datetime.fromtimestamp(
            1647878500, tz=timezone.utc
        )

        model_metadata = ModelMetadata(
            model=mock_model, model_version=mock_model_version
        )

        with patch.object(source, "_search_endpoint", return_value=None):
            actual_mcps = list(source._gen_ml_model_mcps(model_metadata))

            assert len(actual_mcps) >= 5
            model_props_mcps = [
                mcp for mcp in actual_mcps if isinstance(mcp.aspect, MLModelProperties)
            ]
            assert len(model_props_mcps) == 1
            for mcp in actual_mcps:
                if mcp.aspect is None:
                    continue
                deployed_to = getattr(mcp.aspect, "deployedTo", None)
                if deployed_to is not None:
                    assert deployed_to == []


class TestRegexVsGlobPatternConfusion:
    """Tests for detecting common regex vs glob pattern confusion."""

    @pytest.mark.parametrize(
        "pattern,should_warn",
        [
            (AllowDenyPattern(allow=["prod-*"]), True),
            (AllowDenyPattern(allow=["prod-.*"]), False),
            (AllowDenyPattern(allow=["test-???"]), True),
            (AllowDenyPattern(allow=["test-.{3}"]), False),
            (AllowDenyPattern(allow=["^prod-.*$"]), False),
        ],
    )
    def test_glob_pattern_warning(
        self,
        caplog: pytest.LogCaptureFixture,
        pattern: AllowDenyPattern,
        should_warn: bool,
    ) -> None:
        """Warns when patterns look like glob syntax instead of regex."""
        with caplog.at_level(logging.WARNING):
            _validate_pattern_before_discovery(pattern, "auto-discovery")

        if should_warn:
            assert (
                "looks like glob syntax" in caplog.text or "Did you mean" in caplog.text
            )
        else:
            assert "looks like glob syntax" not in caplog.text

    def test_glob_pattern_warning_with_asterisk(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Specifically test prod-* pattern (common glob confusion)."""
        pattern = AllowDenyPattern(allow=["prod-*"])

        with caplog.at_level(logging.WARNING):
            _validate_pattern_before_discovery(pattern, "auto-discovery")

        assert "prod-*" in caplog.text
        assert "prod-.*" in caplog.text or "Did you mean" in caplog.text

    def test_glob_pattern_no_warning_for_valid_regex(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Valid regex patterns should not trigger false warnings."""
        valid_patterns = [
            AllowDenyPattern(allow=["^prod-.*"]),
            AllowDenyPattern(allow=["prod-.+"]),
            AllowDenyPattern(allow=["^ml-[0-9]+$"]),
        ]

        for pattern in valid_patterns:
            with caplog.at_level(logging.WARNING):
                _validate_pattern_before_discovery(pattern, "auto-discovery")
            assert "looks like glob syntax" not in caplog.text


class TestEmptyStringValidation:
    """Tests for empty string and whitespace validation in project configs."""

    def test_empty_string_in_project_ids_rejected(self) -> None:
        """Empty strings in project_ids should be rejected."""
        with pytest.raises(ValueError, match="empty"):
            VertexAIConfig(
                project_ids=["valid-project-123", "", "another-project-456"],
                region="us-west2",
            )

    def test_whitespace_only_in_project_ids_rejected(self) -> None:
        """Whitespace-only strings in project_ids should be rejected."""
        with pytest.raises(ValueError, match="empty"):
            VertexAIConfig(
                project_ids=["valid-project-123", "   ", "\t\n"],
                region="us-west2",
            )

    def test_empty_project_ids_list_is_valid(self) -> None:
        """Empty list [] is valid for auto-discovery."""
        config = VertexAIConfig(project_ids=[], region="us-west2")
        assert config.project_ids == []

    def test_empty_string_in_project_labels_rejected(self) -> None:
        """Empty strings in project_labels should be rejected."""
        with pytest.raises(ValueError, match="empty"):
            VertexAIConfig(
                project_labels=["env:prod", "", "team:ml"],
                region="us-west2",
            )

    def test_malformed_label_format_rejected(self) -> None:
        """Labels without proper key:value format should be rejected."""
        with pytest.raises(ValueError, match="Invalid project_labels format"):
            VertexAIConfig(
                project_labels=["env:prod", "invalid_label", "team:ml"],
                region="us-west2",
            )


class TestPatternsThatMatchNothing:
    """Tests for patterns that can never match or are contradictory."""

    def test_contradictory_allow_deny_patterns_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When deny-all pattern overrides allow patterns, should warn."""
        pattern = AllowDenyPattern(allow=["^prod-.*"], deny=[".*"])

        with caplog.at_level(logging.WARNING):
            _validate_pattern_before_discovery(pattern, "auto-discovery")

        assert (
            "deny pattern" in caplog.text.lower() or "override" in caplog.text.lower()
        )

    def test_deny_all_with_allow_all_raises_error(self) -> None:
        """Deny all with default allow should raise error."""
        pattern = AllowDenyPattern(allow=[".*"], deny=[".*"])

        with pytest.raises(GCPProjectDiscoveryError, match="blocks ALL"):
            _validate_pattern_before_discovery(pattern, "auto-discovery")

    @patch("google.cloud.aiplatform.init")
    def test_pattern_matching_zero_projects_raises_error(
        self, mock_init: MagicMock
    ) -> None:
        """When pattern filters out all projects, should raise informative error."""
        with pytest.raises(GCPProjectDiscoveryError, match="excludes ALL"):
            get_projects_from_explicit_list(
                ["dev-1", "dev-2"],
                AllowDenyPattern(allow=["^prod-.*"]),
            )


class TestConcurrentProjectProcessing:
    """Tests for concurrent/parallel project processing edge cases."""

    @patch("google.cloud.aiplatform.init")
    def test_concurrent_project_processing_isolation(
        self, mock_init: MagicMock
    ) -> None:
        """Project state should not leak between concurrent processing."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-a", "project-b"], region="us-west2"
            ),
        )

        source._projects = [
            GCPProject(id="project-a", name="Project A"),
            GCPProject(id="project-b", name="Project B"),
        ]

        project_a_id = None
        project_b_id = None

        def mock_process_a():
            source._init_for_project(GCPProject(id="project-a", name="Project A"))
            nonlocal project_a_id
            project_a_id = source._current_project_id
            yield MetadataWorkUnit(id="a", mcp=MagicMock())

        def mock_process_b():
            source._init_for_project(GCPProject(id="project-b", name="Project B"))
            nonlocal project_b_id
            project_b_id = source._current_project_id
            yield MetadataWorkUnit(id="b", mcp=MagicMock())

        with patch.object(
            source,
            "_process_current_project",
            side_effect=[mock_process_a(), mock_process_b()],
        ):
            list(source.get_workunits_internal())

        assert project_a_id == "project-a"
        assert project_b_id == "project-b"
        assert project_a_id != project_b_id

    @patch("google.cloud.aiplatform.init")
    def test_concurrent_project_processing_error_isolation(
        self, mock_init: MagicMock
    ) -> None:
        """Errors in one project should not affect other projects."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-a", "project-b"], region="us-west2"
            ),
        )

        source._projects = [
            GCPProject(id="project-a", name="Project A"),
            GCPProject(id="project-b", name="Project B"),
        ]

        def mock_process_a():
            raise PermissionDenied("Access denied for project-a")
            yield

        def mock_process_b():
            yield MetadataWorkUnit(id="b", mcp=MagicMock())

        with patch.object(
            source,
            "_process_current_project",
            side_effect=[mock_process_a(), mock_process_b()],
        ):
            workunits = list(source.get_workunits_internal())

        assert len(workunits) == 1
        assert workunits[0].id == "b"
        assert len(source.report.failures) == 1

    @patch("google.cloud.aiplatform.init")
    def test_shared_credential_state_thread_safety(self, mock_init: MagicMock) -> None:
        """Credentials should be thread-safe during concurrent access."""
        creds = {"type": "service_account", "project_id": "test"}
        env_vars = []

        def access_credentials():
            with gcp_credentials_context(creds):
                env_vars.append(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))

        threads = [threading.Thread(target=access_credentials) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(env_vars) == 3
        assert all(v is not None for v in env_vars)

    @patch("google.cloud.aiplatform.init")
    def test_project_processing_result_aggregation(self, mock_init: MagicMock) -> None:
        """MCPs from all projects should be correctly collected."""
        source = VertexAISource(
            ctx=PipelineContext(run_id="test"),
            config=VertexAIConfig(
                project_ids=["project-a", "project-b"], region="us-west2"
            ),
        )

        source._projects = [
            GCPProject(id="project-a", name="Project A"),
            GCPProject(id="project-b", name="Project B"),
        ]

        def mock_process_a():
            yield MetadataWorkUnit(id="a1", mcp=MagicMock())
            yield MetadataWorkUnit(id="a2", mcp=MagicMock())

        def mock_process_b():
            yield MetadataWorkUnit(id="b1", mcp=MagicMock())

        with patch.object(
            source,
            "_process_current_project",
            side_effect=[mock_process_a(), mock_process_b()],
        ):
            workunits = list(source.get_workunits_internal())

        assert len(workunits) == 3
        workunit_ids = [w.id for w in workunits]
        assert "a1" in workunit_ids
        assert "a2" in workunit_ids
        assert "b1" in workunit_ids

    @patch("google.cloud.aiplatform.init")
    def test_pattern_validation_with_real_projects(self, mock_init: MagicMock) -> None:
        """Integration test with realistic project filtering."""
        projects = get_projects_from_explicit_list(
            ["prod-ml-1", "prod-data-2", "dev-test-1"],
            AllowDenyPattern(allow=["^prod-.*"]),
        )

        assert len(projects) == 2
        project_ids = [p.id for p in projects]
        assert "prod-ml-1" in project_ids
        assert "prod-data-2" in project_ids
        assert "dev-test-1" not in project_ids
