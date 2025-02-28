from datetime import datetime
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import aiplatform
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import Endpoint, Model, VersionInfo
from google.cloud.aiplatform.training_jobs import _TrainingJob
from google.protobuf import timestamp_pb2

from datahub.emitter.mcp_builder import ProjectIdKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai import VertexAIConfig, VertexAISource
from datahub.metadata._schema_classes import ContainerClass
from datahub.metadata.com.linkedin.pegasus2avro.ml.metadata import (
    MLModelGroupProperties,
    MLModelProperties,
)
from datahub.metadata.schema_classes import (
    DataProcessInstanceInputClass,
    DataProcessInstancePropertiesClass,
    MLModelDeploymentPropertiesClass,
    SubTypesClass,
)


@pytest.fixture
def mock_model() -> Model:
    mock_model_1 = MagicMock(spec=Model)
    mock_model_1.name = "mock_prediction_model_1"
    mock_model_1.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.version_id = "1"
    mock_model_1.display_name = "mock_prediction_model_1_display_name"
    mock_model_1.resource_name = (
        "projects/872197881936/locations/us-west2/models/3583871344875405312"
    )
    return mock_model_1


@pytest.fixture
def mock_models() -> List[Model]:
    mock_model_1 = MagicMock(spec=Model)
    mock_model_1.name = "mock_prediction_model_1"
    mock_model_1.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.version_id = "1"
    mock_model_1.display_name = "mock_prediction_model_1_display_name"
    mock_model_1.description = "mock_prediction_model_1_description"

    mock_model_2 = MagicMock(spec=Model)
    mock_model_2.name = "mock_prediction_model_2"

    mock_model_2.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_2.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_2.version_id = "1"
    mock_model_2.display_name = "mock_prediction_model_2_display_name"
    mock_model_2.description = "mock_prediction_model_1_description"

    return [mock_model_1, mock_model_2]


@pytest.fixture
def mock_training_job() -> VertexAiResourceNoun:
    mock_training_job = MagicMock(spec=VertexAiResourceNoun)
    mock_training_job.name = "mock_training_job"
    mock_training_job.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_training_job.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_training_job.display_name = "mock_training_job_display_name"
    mock_training_job.description = "mock_training_job_description"
    return mock_training_job


@pytest.fixture
def mock_endpoint() -> Endpoint:
    mock_endpoint = MagicMock(spec=Endpoint)
    mock_endpoint.description = "test endpoint"
    mock_endpoint.create_time = datetime.now()
    mock_endpoint.display_name = "test endpoint display name"
    return mock_endpoint


@pytest.fixture
def project_id() -> str:
    """
    Replace with your GCP Project ID
    """
    return "acryl-poc"


@pytest.fixture
def region() -> str:
    """
    Replace with your GCP region s
    """
    return "us-west2"


@pytest.fixture
def source(project_id: str, region: str) -> VertexAISource:
    return VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id=project_id, region=region),
    )


@pytest.fixture
def real_model(source: VertexAISource) -> Model:
    """
    Fixture for the model that is actually registered in the Vertex AI Model Registry
    Use mock_model for local testing purpose, but this fixture is provided to use real model for debugging.
    Replace model name with your real model when using this fixture.
    """
    model_name = "projects/872197881936/locations/us-west2/models/3583871344875405312"
    return Model(model_name=model_name)


@pytest.fixture
def real_autoML_tabular_job(source: VertexAISource) -> _TrainingJob:
    """
    Fixture for the training job that is actually registered in the Vertex AI Model Registry
    Use mock_training_job for local testing purpose, but this fixture is provided to use real training job for debugging.
    Replace training job name with your real training job when using this fixture.
    """

    # Initialize the AI Platform client
    aiplatform.init(project=source.config.project_id, location=source.config.region)

    # Retrieve the custom training job by its resource name
    # resource_name format 'projects/your-project-id/locations/your-location/trainingPipelines/your-training-job-id')
    job = aiplatform.AutoMLTabularTrainingJob.get(
        resource_name="projects/872197881936/locations/us-west2/trainingPipelines/5401695018589093888"
    )
    return job


@pytest.fixture
def model_version(
    source: VertexAISource,
    mock_model: Model,
) -> VersionInfo:
    version = "1"
    return VersionInfo(
        version_id=version,
        version_description="test",
        version_create_time=timestamp_pb2.Timestamp().GetCurrentTime(),
        version_update_time=timestamp_pb2.Timestamp().GetCurrentTime(),
        model_display_name=mock_model.name,
        model_resource_name=mock_model.resource_name,
    )


@patch("google.cloud.aiplatform.Model.list")
def test_get_ml_model_workunits(
    mock_list: List[Model], source: VertexAISource, mock_models: List[Model]
) -> None:
    assert hasattr(mock_list, "return_value")  # this check needed to go ground lint
    mock_list.return_value = mock_models

    wcs = [wc for wc in source._get_ml_model_workunits()]
    assert len(wcs) == 2
    # aspect is MLModelGroupPropertiesClass

    assert hasattr(wcs[0].metadata, "aspect")
    aspect = wcs[0].metadata.aspect
    assert isinstance(aspect, MLModelGroupProperties)
    assert (
        aspect.name == f"{source._make_vertexai_model_group_name(mock_models[0].name)}"
    )
    assert aspect.description == mock_models[0].description

    assert hasattr(wcs[1].metadata, "aspect")
    aspect = wcs[1].metadata.aspect
    assert isinstance(aspect, MLModelGroupProperties)
    assert (
        aspect.name == f"{source._make_vertexai_model_group_name(mock_models[1].name)}"
    )
    assert aspect.description == mock_models[1].description


def test_get_ml_model_properties_workunit(
    source: VertexAISource, mock_model: Model, model_version: VersionInfo
) -> None:
    wu = [
        wu for wu in source._get_ml_model_properties_workunit(mock_model, model_version)
    ]
    assert len(wu) == 1
    assert hasattr(wu[0].metadata, "aspect")
    aspect = wu[0].metadata.aspect
    assert isinstance(aspect, MLModelProperties)
    assert (
        aspect.name
        == f"{source._make_vertexai_model_name(mock_model.name)}_{mock_model.version_id}"
    )
    assert aspect.description == model_version.version_description
    assert aspect.date == model_version.version_create_time
    assert aspect.hyperParams is None


def test_get_endpoint_workunit(
    source: VertexAISource,
    mock_endpoint: Endpoint,
    mock_model: Model,
    model_version: VersionInfo,
) -> None:
    for wu in source._get_endpoint_workunit(mock_endpoint, mock_model, model_version):
        assert hasattr(wu.metadata, "aspect")
        aspect = wu.metadata.aspect
        if isinstance(aspect, MLModelDeploymentPropertiesClass):
            assert aspect.description == mock_model.description
            assert aspect.customProperties == {
                "displayName": mock_endpoint.display_name
            }
            assert aspect.createdAt == int(mock_endpoint.create_time.timestamp() * 1000)


def test_get_data_process_properties_workunit(
    source: VertexAISource, mock_training_job: VertexAiResourceNoun
) -> None:
    for wu in source._get_data_process_properties_workunit(mock_training_job):
        assert hasattr(wu.metadata, "aspect")
        aspect = wu.metadata.aspect
        if isinstance(aspect, DataProcessInstancePropertiesClass):
            assert (
                aspect.name
                == f"{source._make_vertexai_job_name(mock_training_job.name)}"
            )
            assert aspect.externalUrl == source._make_job_external_url(
                mock_training_job
            )
        elif isinstance(aspect, SubTypesClass):
            assert "Training Job" in aspect.typeNames


@patch("google.cloud.aiplatform.datasets.TextDataset.list")
@patch("google.cloud.aiplatform.datasets.TabularDataset.list")
@patch("google.cloud.aiplatform.datasets.ImageDataset.list")
@patch("google.cloud.aiplatform.datasets.TimeSeriesDataset.list")
@patch("google.cloud.aiplatform.datasets.VideoDataset.list")
def test_get_data_process_input_workunit(
    mock_text_list: List[VertexAiResourceNoun],
    mock_tabular_list: List[VertexAiResourceNoun],
    mock_image_list: List[VertexAiResourceNoun],
    mock_time_series_list: List[VertexAiResourceNoun],
    mock_video_list: List[VertexAiResourceNoun],
    source: VertexAISource,
    mock_training_job: VertexAiResourceNoun,
) -> None:
    # Mocking all the dataset list
    assert hasattr(
        mock_text_list, "return_value"
    )  # this check needed to go ground lint
    mock_text_list.return_value = []
    assert hasattr(
        mock_tabular_list, "return_value"
    )  # this check needed to go ground lint
    mock_tabular_list.return_value = []
    assert hasattr(
        mock_video_list, "return_value"
    )  # this check needed to go ground lint
    mock_video_list.return_value = []
    assert hasattr(
        mock_time_series_list, "return_value"
    )  # this check needed to go ground lint
    mock_time_series_list.return_value = []
    assert hasattr(
        mock_image_list, "return_value"
    )  # this check needed to go ground lint
    mock_image_list.return_value = []

    for wu in source._get_data_process_input_workunit(mock_training_job, "12345"):
        assert hasattr(wu.metadata, "aspect")
        aspect = wu.metadata.aspect
        assert isinstance(aspect, DataProcessInstanceInputClass)
        assert len(aspect.inputs) == 1


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


@patch("google.cloud.aiplatform.CustomJob.list")
@patch("google.cloud.aiplatform.CustomTrainingJob.list")
@patch("google.cloud.aiplatform.CustomContainerTrainingJob.list")
@patch("google.cloud.aiplatform.CustomPythonPackageTrainingJob.list")
@patch("google.cloud.aiplatform.AutoMLTabularTrainingJob.list")
@patch("google.cloud.aiplatform.AutoMLTextTrainingJob.list")
@patch("google.cloud.aiplatform.AutoMLImageTrainingJob.list")
@patch("google.cloud.aiplatform.AutoMLVideoTrainingJob.list")
@patch("google.cloud.aiplatform.AutoMLForecastingTrainingJob.list")
def test_get_training_jobs_workunit(
    mock_automl_forecasting_job_list: List[VertexAiResourceNoun],
    mock_automl_video_job_list: List[VertexAiResourceNoun],
    mock_automl_image_list: List[VertexAiResourceNoun],
    mock_automl_text_job_list: List[VertexAiResourceNoun],
    mock_automl_tabular_job_list: List[VertexAiResourceNoun],
    mock_custom_python_job_list: List[VertexAiResourceNoun],
    mock_custom_container_job_list: List[VertexAiResourceNoun],
    mock_custom_training_job_list: List[VertexAiResourceNoun],
    mock_custom_job_list: List[VertexAiResourceNoun],
    source: VertexAISource,
    mock_training_job: VertexAiResourceNoun,
) -> None:
    assert hasattr(mock_custom_job_list, "return_value")
    mock_custom_job_list.return_value = [mock_training_job]
    assert hasattr(mock_custom_training_job_list, "return_value")
    mock_custom_training_job_list.return_value = []
    assert hasattr(mock_custom_container_job_list, "return_value")
    mock_custom_container_job_list.return_value = []
    assert hasattr(mock_custom_python_job_list, "return_value")
    mock_custom_python_job_list.return_value = []
    assert hasattr(mock_automl_tabular_job_list, "return_value")
    mock_automl_tabular_job_list.return_value = []
    assert hasattr(mock_automl_text_job_list, "return_value")
    mock_automl_text_job_list.return_value = []
    assert hasattr(mock_automl_image_list, "return_value")
    mock_automl_image_list.return_value = []
    assert hasattr(mock_automl_video_job_list, "return_value")
    mock_automl_video_job_list.return_value = []
    assert hasattr(mock_automl_forecasting_job_list, "return_value")
    mock_automl_forecasting_job_list.return_value = []

    container_key = ProjectIdKey(
        project_id=source.config.project_id, platform=source.platform
    )

    for wc in source._get_training_jobs_workunit():
        assert hasattr(wc.metadata, "aspect")
        aspect = wc.metadata.aspect
        if isinstance(aspect, DataProcessInstancePropertiesClass):
            assert (
                aspect.name
                == f"{source.config.project_id}.job.{mock_training_job.name}"
            )
            assert (
                aspect.customProperties["displayName"] == mock_training_job.display_name
            )
        if isinstance(aspect, SubTypesClass):
            assert aspect.typeNames == ["Training Job"]

        if isinstance(aspect, ContainerClass):
            assert aspect.container == container_key.as_urn()


def test_make_model_external_url(mock_model: Model, source: VertexAISource) -> None:
    assert (
        source._make_model_external_url(mock_model)
        == f"{source.config.vertexai_url}/models/locations/{source.config.region}/models/{mock_model.name}"
        f"?project={source.config.project_id}"
    )


@pytest.mark.skip(reason="Skipping, this is for debugging purpose")
def test_real_model_workunit(
    source: VertexAISource, real_model: Model, model_version: VersionInfo
) -> None:
    """
    Disabled as default
    Use real model registered in the Vertex AI Model Registry
    """
    for wu in source._get_ml_model_properties_workunit(
        model=real_model, model_version=model_version
    ):
        assert hasattr(wu.metadata, "aspect")
        aspect = wu.metadata.aspect
        assert isinstance(aspect, MLModelProperties)
        # aspect is MLModelPropertiesClass
        assert aspect.description == model_version.version_description
        assert aspect.date == model_version.version_create_time
        assert aspect.hyperParams is None
        assert aspect.trainingMetrics is None


@pytest.mark.skip(reason="Skipping, this is for debugging purpose")
def test_real_get_data_process_properties(
    source: VertexAISource, real_autoML_tabular_job: _TrainingJob
) -> None:
    for wu in source._get_data_process_properties_workunit(real_autoML_tabular_job):
        assert hasattr(wu.metadata, "aspect")
        aspect = wu.metadata.aspect
        if isinstance(aspect, DataProcessInstancePropertiesClass):
            # aspect is DataProcessInstancePropertiesClass
            assert aspect.externalUrl == source._make_job_external_url(
                real_autoML_tabular_job
            )
