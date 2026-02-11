import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any, List, cast
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
from datahub.ingestion.source.vertexai.vertexai import (
    ModelMetadata,
    TrainingJobMetadata,
    VertexAIConfig,
    VertexAISource,
)
from datahub.ingestion.source.vertexai.vertexai_constants import VertexAISubTypes
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceRelationships,
)
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
                    actor=builder.make_user_urn("datahub"),
                ),
                lastModified=TimeStampClass(
                    time=datetime_to_ts_millis(mock_model.update_time),
                    actor=builder.make_user_urn("datahub"),
                ),
                externalUrl=source.url_builder.make_model_url(mock_model.name),
            ),
        )

        mcp_container = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=ContainerClass(container=source._get_project_container().as_urn()),
        )
        mcp_subtype = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=SubTypesClass(typeNames=[VertexAISubTypes.MODEL_GROUP]),
        )

        mcp_dataplatform = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=DataPlatformInstanceClass(
                platform=str(DataPlatformUrn(source.platform))
            ),
        )

        assert len(actual_mcps) == 4
        assert any(mcp_container == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_subtype == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_mlgroup == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_dataplatform == mcp.metadata for mcp in actual_mcps)


def test_get_ml_model_properties_mcps(source: VertexAISource) -> None:
    mock_model = gen_mock_model()
    model_version = gen_mock_model_version(mock_model)
    model_meta = ModelMetadata(mock_model, model_version)

    # Run _gen_ml_model_mcps
    actual_mcps = list(source._gen_ml_model_mcps(model_meta))
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
                actor=builder.make_user_urn("datahub"),
            ),
            lastModified=TimeStampClass(
                time=datetime_to_ts_millis(mock_model.update_time),
                actor=builder.make_user_urn("datahub"),
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

    mcp_container = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[VertexAISubTypes.MODEL]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=str(DataPlatformUrn(source.platform))
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

    # Run _gen_endpoint_mcps
    actual_mcps = list(source._gen_endpoints_mcps(model_meta))
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
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )

    assert len(actual_mcps) == 2
    assert any(mcp_endpoint == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_container == mcp.metadata for mcp in actual_mcps)


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

        """
        Test the retrieval of training jobs work units from Vertex AI.
        This function mocks customJob and AutoMLTabularTrainingJob, 
        and verifies the properties of the work units
        """

        # Run _get_training_jobs_mcps
        actual_mcps = [mcp for mcp in source._get_training_jobs_mcps()]

        # Assert Entity Urns
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
                    actor="urn:li:corpuser:datahub",
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
            aspect=ContainerClass(container=source._get_project_container().as_urn()),
        )
        mcp_subtype = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=SubTypesClass(typeNames=[VertexAISubTypes.TRAINING_JOB]),
        )

        mcp_dataplatform = MetadataChangeProposalWrapper(
            entityUrn=expected_urn,
            aspect=DataPlatformInstanceClass(
                platform=str(DataPlatformUrn(source.platform))
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
    job_meta = TrainingJobMetadata(mock_training_job, input_dataset=mock_dataset)

    actual_mcps = [mcp for mcp in source._gen_training_job_mcps(job_meta)]

    dataset_name = source.name_formatter.format_dataset_name(
        entity_id=mock_dataset.name
    )
    dataset_urn = builder.make_dataset_urn(
        platform=source.platform,
        name=dataset_name,
        env=source.config.env,
    )

    # Assert Entity Urns
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
                actor="urn:li:corpuser:datahub",
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
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[VertexAISubTypes.TRAINING_JOB]),
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
    assert any(mcp_dpi == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_ml_props == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_subtype == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_container == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_dataplatform == mcp.metadata for mcp in actual_mcps)
    assert any(mcp_dpi_input == mcp.metadata for mcp in actual_mcps)


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

    # This function actually generates the dataset MCPs
    actual_mcps: List[MetadataWorkUnit] = list(
        source._get_dataset_workunits_from_job_metadata(job_meta)
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
        aspect=ContainerClass(container=source._get_project_container().as_urn()),
    )
    mcp_subtype = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=SubTypesClass(typeNames=[VertexAISubTypes.DATASET]),
    )

    mcp_dataplatform = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=DataPlatformInstanceClass(
            platform=str(DataPlatformUrn(source.platform))
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
            platform=str(DataPlatformUrn(source.platform))
        ),
    )

    mcp_container_props = MetadataChangeProposalWrapper(
        entityUrn=expected_urn,
        aspect=ContainerPropertiesClass(
            name=mock_experiment.name,
            customProperties={
                "platform": source.platform,
                "id": source.name_formatter.format_experiment_id(mock_experiment.name),
                "name": mock_experiment.name,
                "resourceName": mock_experiment.resource_name,
                "dashboardURL": mock_experiment.dashboard_url
                if mock_experiment.dashboard_url
                else "",
            },
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
    source.experiments = [mock_exp]
    mock_exp_run = gen_mock_experiment_run()
    assert hasattr(mock_list, "return_value")  # this check needed to go ground lint
    mock_list.return_value = [mock_exp_run]

    expected_exp_urn = ExperimentKey(
        platform=source.platform,
        id=source.name_formatter.format_experiment_id(mock_exp.name),
    ).as_urn()

    expected_urn = source.urn_builder.make_experiment_run_urn(mock_exp, mock_exp_run)
    actual_mcps: List[MetadataWorkUnit] = list(source._get_experiment_runs_mcps())

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
            created=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
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
            platform=str(DataPlatformUrn(source.platform))
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
                externalUrl=source.url_builder.make_pipeline_url(mock_pipeline.name),
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
            aspect=SubTypesClass(typeNames=[VertexAISubTypes.PIPELINE]),
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
            aspect=SubTypesClass(typeNames=[VertexAISubTypes.PIPELINE_TASK]),
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
                created=AuditStampClass(
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
            aspect=SubTypesClass(typeNames=[VertexAISubTypes.PIPELINE_TASK_RUN]),
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
        assert any(mcp_pipe_df_info == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_pipe_df_status == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_pipe_subtype == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_pipe_container == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_pipeline_tag == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_input == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_info == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_container == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_subtype == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_status == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_tag == mcp.metadata for mcp in actual_mcps)

        assert any(mcp_task_run_dpi == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_run_container == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_run_subtype == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_run_dataplatform == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_run_dataplatform == mcp.metadata for mcp in actual_mcps)
        assert any(mcp_task_run_relationship == mcp.metadata for mcp in actual_mcps)


def test_make_model_external_url(source: VertexAISource) -> None:
    mock_model = gen_mock_model()
    assert (
        source.url_builder.make_model_url(mock_model.name)
        == f"{source.config.vertexai_url}/models/locations/{source.config.region}/models/{mock_model.name}"
        f"?project={source.config.project_id}"
    )


def test_make_job_urn(source: VertexAISource) -> None:
    mock_training_job = gen_mock_training_automl_job()
    assert source.urn_builder.make_training_job_urn(
        mock_training_job.name
    ) == builder.make_data_process_instance_urn(
        source.name_formatter.format_job_name(mock_training_job.name)
    )


def test_vertexai_multi_project_context_naming() -> None:
    # Use single-project config to avoid invoking project resolution in __init__
    multi_source = VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id="fallback", region="us-central1"),
    )
    # Simulate iteration context
    multi_source._current_project_id = "p2"
    multi_source._current_region = "r2"

    # Reinitialize formatters with new project context (as would happen in get_workunits)
    from datahub.ingestion.source.vertexai.vertexai_builder import (
        VertexAIExternalURLBuilder,
        VertexAINameFormatter,
    )

    multi_source.name_formatter = VertexAINameFormatter(project_id="p2")
    multi_source.url_builder = VertexAIExternalURLBuilder(
        base_url=multi_source.config.vertexai_url
        or "https://console.cloud.google.com/vertex-ai",
        project_id="p2",
        region="r2",
    )

    # Names and URLs should use current context
    assert (
        multi_source.name_formatter.format_model_group_name("m") == "p2.model_group.m"
    )
    assert multi_source.name_formatter.format_dataset_name("d") == "p2.dataset.d"
    assert (
        multi_source.name_formatter.format_pipeline_task_id("t") == "p2.pipeline_task.t"
    )

    # External URLs should embed current region and project
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
            from datetime import datetime, timedelta

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
        source._gen_run_execution(
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
            from datetime import datetime, timedelta

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
    # Generate mcps for job; we only need the DPI aspects for edges
    # Cast is necessary as _Job is a test mock, not a real VertexAiResourceNoun
    job_meta = source._get_training_job_metadata(cast(Any, job))
    # Attach extracted uris
    uris = source.uri_parser.extract_external_uris_from_job(cast(Any, job))
    job_meta.external_input_urns = uris.input_urns
    job_meta.external_output_urns = uris.output_urns
    mcps = list(source._gen_training_job_mcps(job_meta))
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
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, DataProcessInstancePropertiesClass)
            and "incomplete_task" in mcp.metadata.aspect.name
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
            if isinstance(mcp.metadata, MetadataChangeProposalWrapper)
            and isinstance(mcp.metadata.aspect, DataProcessInstancePropertiesClass)
            and "running_task" in mcp.metadata.aspect.name
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
    from datahub.ingestion.source.common.gcp_project_filter import GcpProject

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
    from datahub.ingestion.source.common.gcp_project_filter import GcpProject

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


@patch("google.cloud.aiplatform.aiplatform.list_models")
def test_region_discovery_for_project(mock_list_models: MagicMock) -> None:
    """Test automatic region discovery for projects"""
    mock_list_models.side_effect = [
        [],  # us-central1 - empty
        [MagicMock()],  # us-west1 - has models
        [],  # europe-west1 - empty
    ]

    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "discover_regions": True,
            "candidate_regions": ["us-central1", "us-west1", "europe-west1"],
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert "test-project" in source._project_to_regions
    assert "us-west1" in source._project_to_regions["test-project"]


@patch("google.cloud.aiplatform.aiplatform.list_models")
def test_region_discovery_handles_errors_gracefully(
    mock_list_models: MagicMock,
) -> None:
    """Test that region discovery continues on API errors"""
    from google.api_core.exceptions import GoogleAPICallError

    mock_list_models.side_effect = [
        GoogleAPICallError("Permission denied"),
        [MagicMock()],  # us-west1 succeeds
    ]

    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "discover_regions": True,
            "candidate_regions": ["us-central1", "us-west1"],
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert "us-west1" in source._project_to_regions.get("test-project", [])


@pytest.mark.parametrize(
    "duration,expected",
    [
        (timedelta(seconds=45), "45 seconds"),
        (timedelta(minutes=5, seconds=30), "5 minutes 30 seconds"),
        (timedelta(hours=2, minutes=15), "2 hours 15 minutes 0 seconds"),
        (timedelta(days=1, hours=3, minutes=20), "1 days 3 hours 20 minutes"),
        (timedelta(seconds=0), "0 seconds"),
    ],
)
def test_format_pipeline_duration(
    source: VertexAISource, duration: timedelta, expected: str
) -> None:
    """Test various duration formatting scenarios"""
    formatted = source._format_pipeline_duration(duration)
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
    assert len(source.report.failures) > 0
