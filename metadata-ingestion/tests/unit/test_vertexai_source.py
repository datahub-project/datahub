from typing import List
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import aiplatform
from google.cloud.aiplatform.models import Model, VersionInfo
from google.protobuf import timestamp_pb2

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai import VertexAISource, VertexAIConfig

@pytest.fixture
def project_id() -> str:
    return "acryl-poc"

@pytest.fixture
def region() -> str:
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
        use mock_models for local testing purpose
    """
    model_name = "projects/872197881936/locations/us-west2/models/3583871344875405312"
    return Model(model_name=model_name)


@pytest.fixture
def model_version(
    source: VertexAISource,
        real_model: Model,
) -> VersionInfo:
    version = "1"
    return VersionInfo(
        version_id=version,
        version_description="test",
        # how to create timestamp_pb2.Timestamp using current time?
        version_create_time=timestamp_pb2.Timestamp().GetCurrentTime(),
        version_update_time=timestamp_pb2.Timestamp().GetCurrentTime(),
        model_display_name=real_model.name,
        model_resource_name=real_model.resource_name,
    )
@pytest.fixture
def mock_models()-> List[Model]:
    mock_model_1 = MagicMock(spec=Model)
    mock_model_1.name = "mock_prediction_model_1"
    mock_model_1.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.version_id = "1"
    mock_model_1.display_name = "mock_prediction_model_1_display_name"

    mock_model_2 = MagicMock(spec=Model)
    mock_model_2.name = "mock_prediction_model_2"

    mock_model_2.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_2.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_2.version_id = "1"
    mock_model_2.display_name = "mock_prediction_model_2_display_name"

    return [mock_model_1, mock_model_2]

@pytest.fixture
def mock_model():
    mock_model_1 = MagicMock(spec=Model)
    mock_model_1.name = "mock_prediction_model_1"
    mock_model_1.create_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.update_time = timestamp_pb2.Timestamp().GetCurrentTime()
    mock_model_1.version_id = "1"
    mock_model_1.display_name = "mock_prediction_model_1_display_name"
    return mock_model_1

def test_mock_model_workunit(source, mock_model, model_version):
    wu = source._get_ml_model_properties_workunit(
        registered_model=mock_model,
        model_version=model_version,
    )
    aspect = wu.metadata.aspect
    # aspect is MLModelPropertiesClass
    print(aspect)
    assert aspect.description == model_version.version_description
    assert aspect.date == model_version.version_create_time

@pytest.mark.skip(reason="Skipping, this is for debugging purpose")
def test_real_model_workunit(source, real_model, model_version):
    """
        Disabled as default
        Use real model registered in the Vertex AI Model Registry
    """
    wu = source._get_ml_model_properties_workunit(
        registered_model=real_model,
        model_version=model_version,
    )
    aspect = wu.metadata.aspect
    # aspect is MLModelPropertiesClass
    assert aspect.description == model_version.version_description
    assert aspect.date == model_version.version_create_time
    assert aspect.hyperParams is None
    assert aspect.trainingMetrics is None


@patch('google.cloud.aiplatform.Model.list')
def test_mock_models_workunits(mock_list, source, real_model, model_version, mock_models):
    mock_list.return_value=mock_models
    wcs = [wc for wc in source._get_ml_model_workunits()]
    assert len(wcs) == 2
    # aspect is MLModelGroupPropertiesClass
    assert wcs[0].metadata.aspect.name == mock_models[0].name
    assert wcs[0].metadata.aspect.description == mock_models[0].description
    assert wcs[0].metadata.aspect.createdAt == mock_models[0].create_time
    assert wcs[1].metadata.aspect.name == mock_models[1].name
    assert wcs[1].metadata.aspect.description == mock_models[1].description
    assert wcs[1].metadata.aspect.createdAt == mock_models[1].create_time


def test_config_model_name_separator(source, model_version):
    name_version_sep = "+"
    source.config.model_name_separator = name_version_sep
    expected_model_name = (
        f"{model_version.model_display_name}{name_version_sep}{model_version.version_id}"
    )
    expected_urn = f"urn:li:mlModel:(urn:li:dataPlatform:vertexai,{expected_model_name},{source.config.env})"

    urn = source._make_ml_model_urn(model_version)

    assert urn == expected_urn
