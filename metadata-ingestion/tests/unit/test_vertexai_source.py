import contextlib
from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.aiplatform import Experiment, ExperimentRun, PipelineJob
from google.cloud.aiplatform_v1 import PipelineTaskDetail
from google.cloud.aiplatform_v1.types import PipelineJob as PipelineJobType

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ExperimentKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.vertexai.vertexai import (
    ModelMetadata,
    TrainingJobMetadata,
    VertexAIConfig,
    VertexAISource,
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

PROJECT_ID = "acryl-poc"
REGION = "us-west2"


@pytest.fixture
def source() -> VertexAISource:
    return VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id=PROJECT_ID, region=REGION),
    )


def test_get_ml_model_mcps(source: VertexAISource) -> None:
    mock_model = gen_mock_model()
    with contextlib.ExitStack() as exit_stack:
        mock = exit_stack.enter_context(patch("google.cloud.aiplatform.Model.list"))
        mock.return_value = [mock_model]

        # Running _get_ml_models_mcps
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

    # Run _gen_ml_model_mcps
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

    # Run _gen_endpoint_mcps
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

        """
        Test the retrieval of training jobs work units from Vertex AI.
        This function mocks customJob and AutoMLTabularTrainingJob, 
        and verifies the properties of the work units
        """

        # Run _get_training_jobs_mcps
        actual_mcps = [mcp for mcp in source._get_training_jobs_mcps()]

        # Assert Entity Urns
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

    # Assert Entity Urns
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

    assert config.project_id == "test-project"
    assert config.region == "us-central1"
    assert config.bucket_uri == "gs://test-bucket"
    assert config.vertexai_url == "https://console.cloud.google.com/vertex-ai"
    assert config.credential is not None
    assert config.credential.private_key_id == "test-key-id"
    assert (
        config.credential.private_key.get_secret_value()
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

    parsed_conf = config.get_credentials()
    assert parsed_conf is not None
    assert parsed_conf.get("project_id") == config_data["project_id"]
    assert "credential" in config_data
    assert parsed_conf.get("private_key_id", "") == "test-key-id"
    assert (
        parsed_conf.get("private_key", "")
        == "-----BEGIN PRIVATE KEY-----\ntest-private-key\n-----END PRIVATE KEY-----\n"
    )
    assert (
        parsed_conf.get("client_email")
        == "test-email@test-project.iam.gserviceaccount.com"
    )
    assert parsed_conf.get("client_id") == "test-client-id"
    assert parsed_conf.get("auth_uri") == "https://accounts.google.com/o/oauth2/auth"
    assert parsed_conf.get("token_uri") == "https://oauth2.googleapis.com/token"
    assert (
        parsed_conf.get("auth_provider_x509_cert_url")
        == "https://www.googleapis.com/oauth2/v1/certs"
    )
    assert parsed_conf.get("type") == "service_account"


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

        # Assert Entity Urns
        expected_pipeline_urn = "urn:li:dataFlow:(vertexai,vertexai.acryl-poc.pipeline.mock_pipeline_job,PROD)"

        expected_task_urn = "urn:li:dataJob:(urn:li:dataFlow:(vertexai,vertexai.acryl-poc.pipeline.mock_pipeline_job,PROD),acryl-poc.pipeline_task.reverse)"

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
                    "urn:li:dataJob:(urn:li:dataFlow:(vertexai,vertexai.acryl-poc.pipeline.mock_pipeline_job,PROD),acryl-poc.pipeline_task.concat)"
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
                externalUrl="https://console.cloud.google.com/vertex-ai/pipelines/locations/us-west2/runs/mock_pipeline_job?project=acryl-poc",
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

        dpi_urn = "urn:li:dataProcessInstance:acryl-poc.pipeline_task_run.reverse"

        mcp_task_run_dpi = MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstancePropertiesClass(
                name="reverse",
                externalUrl="https://console.cloud.google.com/vertex-ai/pipelines/locations/us-west2/runs/mock_pipeline_job?project=acryl-poc",
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
        f"?project={source.config.project_id}"
    )


def test_make_job_urn(source: VertexAISource) -> None:
    mock_training_job = gen_mock_training_automl_job()
    assert (
        source._make_training_job_urn(mock_training_job)
        == f"{builder.make_data_process_instance_urn(source._make_vertexai_job_name(mock_training_job.name))}"
    )


def test_pipeline_task_with_none_start_time(source: VertexAISource) -> None:
    """Test that pipeline tasks with None start_time don't crash the ingestion."""
    mock_pipeline_job = MagicMock(spec=PipelineJob)
    mock_pipeline_job.name = "test_pipeline_none_timestamps"
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
    task_detail.task_name = "incomplete_task"
    task_detail.task_id = 123
    task_detail.state = MagicMock()
    task_detail.start_time = None
    task_detail.create_time = datetime.fromtimestamp(1647878400, tz=timezone.utc)
    task_detail.end_time = datetime.fromtimestamp(1647878600, tz=timezone.utc)

    mock_pipeline_job.task_details = [task_detail]
    gca_resource.pipeline_spec = {
        "root": {
            "dag": {
                "tasks": {
                    "incomplete_task": {
                        "componentRef": {"name": "comp-incomplete"},
                        "taskInfo": {"name": "incomplete_task"},
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
            and "incomplete_task" in mcp.aspect.name
        ]

        assert len(task_run_mcps) > 0


def test_pipeline_task_with_none_end_time(source: VertexAISource) -> None:
    """Test that pipeline tasks with None end_time don't crash the ingestion."""
    mock_pipeline_job = MagicMock(spec=PipelineJob)
    mock_pipeline_job.name = "test_pipeline_no_end_time"
    mock_pipeline_job.resource_name = (
        "projects/123/locations/us-central1/pipelineJobs/790"
    )
    mock_pipeline_job.labels = {}
    mock_pipeline_job.create_time = datetime.fromtimestamp(1647878400, tz=timezone.utc)
    mock_pipeline_job.update_time = datetime.fromtimestamp(1647878500, tz=timezone.utc)
    mock_pipeline_job.location = "us-west2"

    gca_resource = MagicMock(spec=PipelineJobType)
    mock_pipeline_job.gca_resource = gca_resource

    task_detail = MagicMock(spec=PipelineTaskDetail)
    task_detail.task_name = "running_task"
    task_detail.task_id = 124
    task_detail.state = MagicMock()
    task_detail.start_time = datetime.fromtimestamp(1647878400, tz=timezone.utc)
    task_detail.create_time = datetime.fromtimestamp(1647878400, tz=timezone.utc)
    task_detail.end_time = None

    mock_pipeline_job.task_details = [task_detail]
    gca_resource.pipeline_spec = {
        "root": {
            "dag": {
                "tasks": {
                    "running_task": {
                        "componentRef": {"name": "comp-running"},
                        "taskInfo": {"name": "running_task"},
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
            and "running_task" in mcp.aspect.name
        ]

        assert len(task_run_mcps) > 0


def test_experiment_run_with_none_timestamps(source: VertexAISource) -> None:
    """Test that experiment runs with None create_time/update_time don't crash."""
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
