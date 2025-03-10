import contextlib
import json
from typing import List
from unittest.mock import patch

import pytest

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai import (
    MLTypes,
    ModelMetadata,
    TrainingJobMetadata,
    VertexAIConfig,
    VertexAISource,
)
from datahub.metadata.com.linkedin.pegasus2avro.ml.metadata import (
    MLModelGroupProperties,
    MLModelProperties,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataProcessInstanceInputClass,
    DataProcessInstancePropertiesClass,
    DatasetPropertiesClass,
    MLModelDeploymentPropertiesClass,
    MLModelGroupPropertiesClass,
    MLTrainingRunPropertiesClass,
    SubTypesClass,
)
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_dataset,
    gen_mock_endpoint,
    gen_mock_model,
    gen_mock_model_version,
    gen_mock_models,
    gen_mock_training_automl_job,
    gen_mock_training_custom_job,
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
    mock_models = gen_mock_models()
    with contextlib.ExitStack() as exit_stack:
        mock = exit_stack.enter_context(patch("google.cloud.aiplatform.Model.list"))
        mock.return_value = mock_models

        # Running _get_ml_models_mcps
        actual_mcps = [mcp for mcp in source._get_ml_models_mcps()]
        actual_urns = [mcp.entityUrn for mcp in actual_mcps]
        expected_urns = [
            builder.make_ml_model_group_urn(
                platform=source.platform,
                group_name=source._make_vertexai_model_group_name(mock_model.name),
                env=source.config.env,
            )
            for mock_model in mock_models
        ]
        # expect 2 model groups
        assert actual_urns == expected_urns

        # assert expected aspect classes
        expected_classes = [MLModelGroupPropertiesClass] * 2
        actual_classes = [mcp.aspect.__class__ for mcp in actual_mcps]

        assert expected_classes == actual_classes

        for mcp in actual_mcps:
            assert isinstance(mcp, MetadataChangeProposalWrapper)
            aspect = mcp.aspect
            if isinstance(aspect, MLModelGroupProperties):
                assert (
                    aspect.name
                    == f"{source._make_vertexai_model_group_name(mock_models[0].name)}"
                    or aspect.name
                    == f"{source._make_vertexai_model_group_name(mock_models[1].name)}"
                )
                assert (
                    aspect.description == mock_models[0].description
                    or aspect.description == mock_models[1].description
                )


def test_get_ml_model_properties_mcps(
    source: VertexAISource,
) -> None:
    mock_model = gen_mock_model()
    model_version = gen_mock_model_version(mock_model)
    model_meta = ModelMetadata(mock_model, model_version)

    # Run _gen_ml_model_mcps
    mcps = list(source._gen_ml_model_mcps(model_meta))
    assert len(mcps) == 1
    mcp = mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)

    # Assert URN
    assert mcp.entityUrn == source._make_ml_model_urn(
        model_version, source._make_vertexai_model_name(mock_model.name)
    )
    aspect = mcp.aspect
    # Assert Aspect Class
    assert isinstance(aspect, MLModelProperties)
    assert (
        aspect.name
        == f"{source._make_vertexai_model_name(mock_model.name)}_{mock_model.version_id}"
    )
    assert aspect.description == model_version.version_description
    assert aspect.date == model_version.version_create_time
    assert aspect.hyperParams is None


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
    actual_urns = [mcp.entityUrn for mcp in actual_mcps]
    endpoint_urn = builder.make_ml_model_deployment_urn(
        platform=source.platform,
        deployment_name=source._make_vertexai_endpoint_name(
            entity_id=mock_endpoint.name
        ),
        env=source.config.env,
    )
    expected_urns = [endpoint_urn]
    # Assert URN,   expect 1 endpoint urn
    assert actual_urns == expected_urns

    # Assert Aspect Classes
    expected_classes = [MLModelDeploymentPropertiesClass]
    actual_classes = [mcp.aspect.__class__ for mcp in actual_mcps]
    assert actual_classes == expected_classes

    for mcp in source._gen_endpoints_mcps(model_meta):
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        aspect = mcp.aspect
        if isinstance(aspect, MLModelDeploymentPropertiesClass):
            assert aspect.description == mock_model.description
            assert aspect.customProperties == {
                "displayName": mock_endpoint.display_name
            }
            assert aspect.createdAt == int(mock_endpoint.create_time.timestamp() * 1000)
        # TODO: Add following when container/subtype supported
        # elif isinstance(aspect, ContainerClass):
        #     assert aspect.container == source._get_project_container().as_urn()
        # elif isinstance(aspect, SubTypesClass):
        #     assert aspect.typeNames == ["Endpoint"]


def test_get_training_jobs_mcps(
    source: VertexAISource,
) -> None:
    mock_training_job = gen_mock_training_custom_job()
    mock_training_automl_job = gen_mock_training_automl_job()
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
        actual_urns = [mcp.entityUrn for mcp in actual_mcps]
        expected_urns = [
            builder.make_data_process_instance_urn(
                source._make_vertexai_job_name(mock_training_job.name)
            )
        ] * 4  # expect 4 aspects
        assert actual_urns == expected_urns

        # Assert Aspect Classes
        expected_classes = [
            DataProcessInstancePropertiesClass,
            MLTrainingRunPropertiesClass,
            SubTypesClass,
            ContainerClass,
        ]
        actual_classes = [mcp.aspect.__class__ for mcp in actual_mcps]
        assert set(expected_classes) == set(actual_classes)

        # Assert Aspect Contents
        for mcp in actual_mcps:
            assert isinstance(mcp, MetadataChangeProposalWrapper)
            aspect = mcp.aspect
            if isinstance(aspect, DataProcessInstancePropertiesClass):
                assert (
                    aspect.name
                    == f"{source.config.project_id}.job.{mock_training_job.name}"
                    or f"{source.config.project_id}.job.{mock_training_automl_job.name}"
                )
                assert (
                    aspect.customProperties["displayName"]
                    == mock_training_job.display_name
                    or mock_training_automl_job.display_name
                )
            if isinstance(aspect, MLTrainingRunPropertiesClass):
                assert aspect.id == mock_training_job.name
                assert aspect.externalUrl == source._make_job_external_url(
                    mock_training_job
                )
            if isinstance(aspect, SubTypesClass):
                assert aspect.typeNames == [MLTypes.TRAINING_JOB]

            if isinstance(aspect, ContainerClass):
                assert aspect.container == source._get_project_container().as_urn()


def test_gen_training_job_mcps(source: VertexAISource) -> None:
    mock_training_job = gen_mock_training_custom_job()
    mock_dataset = gen_mock_dataset()
    job_meta = TrainingJobMetadata(mock_training_job, input_dataset=mock_dataset)

    actual_mcps = [mcp for mcp in source._gen_training_job_mcps(job_meta)]

    # Assert Entity Urns
    actual_urns = [mcp.entityUrn for mcp in actual_mcps]
    expected_urns = [
        builder.make_data_process_instance_urn(
            source._make_vertexai_job_name(mock_training_job.name)
        )
    ] * 5  # expect 5 aspects under the same urn for the job
    assert actual_urns == expected_urns

    # Assert Aspect Classes
    expected_classes = [
        DataProcessInstancePropertiesClass,
        MLTrainingRunPropertiesClass,
        SubTypesClass,
        ContainerClass,
        DataProcessInstanceInputClass,
    ]
    actual_classes = [mcp.aspect.__class__ for mcp in actual_mcps]
    assert set(expected_classes) == set(actual_classes)

    dataset_name = source._make_vertexai_dataset_name(entity_id=mock_dataset.name)
    dataset_urn = builder.make_dataset_urn(
        platform=source.platform,
        name=dataset_name,
        env=source.config.env,
    )

    for mcp in actual_mcps:
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        aspect = mcp.aspect
        if isinstance(aspect, DataProcessInstancePropertiesClass):
            assert (
                aspect.name
                == f"{source.config.project_id}.job.{mock_training_job.name}"
            )
            assert (
                aspect.customProperties["displayName"] == mock_training_job.display_name
            )
        if isinstance(aspect, MLTrainingRunPropertiesClass):
            assert aspect.id == mock_training_job.name
            assert aspect.externalUrl == source._make_job_external_url(
                mock_training_job
            )

        if isinstance(aspect, SubTypesClass):
            assert aspect.typeNames == [MLTypes.TRAINING_JOB]

        if isinstance(aspect, ContainerClass):
            assert aspect.container == source._get_project_container().as_urn()

        if isinstance(aspect, DataProcessInstanceInputClass):
            assert aspect.inputs == [dataset_urn]


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

    assert config._credentials_path is not None
    with open(config._credentials_path, "r") as file:
        content = json.loads(file.read())
        assert content["project_id"] == "test-project"
        assert content["private_key_id"] == "test-key-id"
        assert content["private_key_id"] == "test-key-id"
        assert (
            content["private_key"]
            == "-----BEGIN PRIVATE KEY-----\ntest-private-key\n-----END PRIVATE KEY-----\n"
        )
        assert (
            content["client_email"] == "test-email@test-project.iam.gserviceaccount.com"
        )
        assert content["client_id"] == "test-client-id"
        assert content["auth_uri"] == "https://accounts.google.com/o/oauth2/auth"
        assert content["token_uri"] == "https://oauth2.googleapis.com/token"
        assert (
            content["auth_provider_x509_cert_url"]
            == "https://www.googleapis.com/oauth2/v1/certs"
        )


def test_get_input_dataset_mcps(source: VertexAISource) -> None:
    mock_dataset = gen_mock_dataset()
    mock_job = gen_mock_training_custom_job()
    job_meta = TrainingJobMetadata(mock_job, input_dataset=mock_dataset)

    actual_mcps: List[MetadataChangeProposalWrapper] = list(
        source._get_input_dataset_mcps(job_meta)
    )

    actual_urns = [mcp.entityUrn for mcp in actual_mcps]
    assert job_meta.input_dataset is not None
    expected_urns = [
        builder.make_dataset_urn(
            platform=source.platform,
            name=source._make_vertexai_dataset_name(
                entity_id=job_meta.input_dataset.name
            ),
            env=source.config.env,
        )
    ] * 3  # expect 3 aspects
    assert actual_urns == expected_urns

    # Assert Aspect Classes
    actual_classes = [mcp.aspect.__class__ for mcp in actual_mcps]
    expected_classes = [DatasetPropertiesClass, ContainerClass, SubTypesClass]
    assert set(expected_classes) == set(actual_classes)

    # Run _get_input_dataset_mcps
    for mcp in actual_mcps:
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        aspect = mcp.aspect
        if isinstance(aspect, DataProcessInstancePropertiesClass):
            assert aspect.name == f"{source._make_vertexai_job_name(mock_dataset.name)}"
            assert aspect.customProperties["displayName"] == mock_dataset.display_name
        elif isinstance(aspect, ContainerClass):
            assert aspect.container == source._get_project_container().as_urn()
        elif isinstance(aspect, SubTypesClass):
            assert aspect.typeNames == ["Dataset"]


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
