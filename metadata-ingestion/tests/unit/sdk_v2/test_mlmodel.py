from __future__ import annotations

import re
from datetime import datetime, timezone
from unittest import mock

import pytest

from datahub.errors import ItemNotFoundError
from datahub.metadata.schema_classes import (
    MLHyperParamClass,
    MLMetricClass,
)
from datahub.metadata.urns import (
    DataPlatformUrn,
    DataProcessInstanceUrn,
    MlModelGroupUrn,
    MlModelUrn,
)
from datahub.sdk.mlmodel import MLModel
from datahub.utilities.urns.error import InvalidUrnError


def test_mlmodel() -> None:
    """Test MLModel functionality with all essential features."""
    # Create model with basic properties
    model = MLModel(
        id="test_model",
        platform="mlflow",
        name="test_model",
    )

    # Test basic properties
    assert model.urn == MlModelUrn("mlflow", "test_model")
    assert model.name == "test_model"
    assert model.platform == DataPlatformUrn("urn:li:dataPlatform:mlflow")

    # Test description and URL
    model.set_description("A test model")
    assert model.description == "A test model"
    model.set_external_url("https://example.com/model")
    assert model.external_url == "https://example.com/model"

    # Test dates
    test_date = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    model.set_created(test_date)
    model.set_last_modified(test_date)
    assert model.created == test_date
    assert model.last_modified == test_date

    # Test version and aliases
    model.set_version("1.0.0")
    assert model.version == "1.0.0"
    model.add_version_alias("stable")
    model.add_version_alias("production")
    aliases = model.version_aliases
    assert aliases is not None
    assert "stable" in aliases
    assert "production" in aliases
    model.remove_version_alias("stable")
    assert "stable" not in model.version_aliases

    # Test metrics - both individual and bulk operations
    model.set_training_metrics(
        {
            "precision": "0.92",
            "recall": "0.88",
        }
    )
    model.add_training_metrics([MLMetricClass(name="f1_score", value="0.90")])
    model.add_training_metrics([MLMetricClass(name="accuracy", value="0.95")])
    metrics = model.training_metrics
    assert metrics is not None
    assert len(metrics) == 4
    metric_values = {
        m.name: m.value for m in metrics if hasattr(m, "name") and hasattr(m, "value")
    }
    assert metric_values["precision"] == "0.92"
    assert metric_values["accuracy"] == "0.95"

    # Test hyperparameters
    model.set_hyper_params({"learning_rate": "0.001", "num_layers": "3"})
    model.add_hyper_params({"batch_size": "32"})
    model.add_hyper_params([MLHyperParamClass(name="num_epochs", value="10")])
    params = model.hyper_params
    assert params is not None
    assert len(params) == 4
    param_values = {
        p.name: p.value for p in params if hasattr(p, "name") and hasattr(p, "value")
    }
    assert param_values["learning_rate"] == "0.001"
    assert param_values["num_epochs"] == "10"

    # Test custom properties
    model.set_custom_properties(
        {
            "framework": "pytorch",
            "task": "classification",
        }
    )
    assert model.custom_properties == {
        "framework": "pytorch",
        "task": "classification",
    }

    # Test relationships
    # Model group
    group_urn = MlModelGroupUrn("mlflow", "test_group")
    model.set_model_group(group_urn)
    assert model.model_group is not None
    assert str(group_urn) == model.model_group

    # Training and downstream jobs
    job1 = DataProcessInstanceUrn("job1")
    job2 = DataProcessInstanceUrn("job2")

    # Add and remove jobs
    model.add_training_job(job1)
    assert model.training_jobs is not None
    assert str(job1) in model.training_jobs

    model.remove_training_job(job1)
    assert model.training_jobs is not None
    assert len(model.training_jobs) == 0

    # Test bulk job operations
    model.set_training_jobs([job1, job2])
    model.set_downstream_jobs([job1, job2])
    assert model.training_jobs is not None
    assert model.downstream_jobs is not None
    assert len(model.training_jobs) == 2
    assert len(model.downstream_jobs) == 2


def test_mlmodel_complex_initialization() -> None:
    """Test MLModel with initialization of all properties at once."""
    test_date = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    model = MLModel(
        id="complex_model",
        platform="mlflow",
        name="Complex Model",
        description="A model with all properties",
        external_url="https://example.com/model",
        version="2.0.0",
        created=test_date,
        last_modified=test_date,
        training_metrics=[
            MLMetricClass(name="accuracy", value="0.95"),
            MLMetricClass(name="loss", value="0.1"),
        ],
        hyper_params=[
            MLHyperParamClass(name="learning_rate", value="0.001"),
            MLHyperParamClass(name="batch_size", value="32"),
        ],
        custom_properties={
            "framework": "pytorch",
            "task": "classification",
        },
        model_group=MlModelGroupUrn("mlflow", "test_group"),
        training_jobs=[DataProcessInstanceUrn("training_job")],
        downstream_jobs=[DataProcessInstanceUrn("inference_job")],
    )

    # Verify properties
    assert model.name == "Complex Model"
    assert model.description == "A model with all properties"
    assert model.version == "2.0.0"
    assert model.created == test_date

    # Verify collections
    assert model.training_metrics is not None and len(model.training_metrics) == 2
    assert model.hyper_params is not None and len(model.hyper_params) == 2
    assert model.custom_properties is not None
    assert model.training_jobs is not None and len(model.training_jobs) == 1
    assert model.downstream_jobs is not None and len(model.downstream_jobs) == 1


def test_mlmodel_validation() -> None:
    """Test MLModel validation errors."""
    # Test invalid platform
    with pytest.raises(InvalidUrnError):
        MLModel(id="test", platform="")

    # Test invalid ID
    with pytest.raises(InvalidUrnError):
        MLModel(id="", platform="test_platform")


def test_client_get_mlmodel() -> None:
    """Test retrieving MLModels using client.entities.get()."""
    # Set up mock
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    # Basic retrieval
    model_urn = MlModelUrn("mlflow", "test_model", "PROD")
    expected_model = MLModel(
        id="test_model",
        platform="mlflow",
        name="Test Model",
        description="A test model",
    )
    mock_entities.get.return_value = expected_model

    result = mock_client.entities.get(model_urn)
    assert result == expected_model
    mock_entities.get.assert_called_once_with(model_urn)
    mock_entities.get.reset_mock()

    # String URN
    urn_str = "urn:li:mlModel:(urn:li:dataPlatform:mlflow,string_model,PROD)"
    mock_entities.get.return_value = MLModel(id="string_model", platform="mlflow")
    result = mock_client.entities.get(urn_str)
    mock_entities.get.assert_called_once_with(urn_str)
    mock_entities.get.reset_mock()

    # Complex model with properties
    complex_model = MLModel(
        id="complex_model",
        platform="mlflow",
        name="Complex Model",
        description="Complex test model",
        training_metrics=[MLMetricClass(name="accuracy", value="0.95")],
        hyper_params=[MLHyperParamClass(name="learning_rate", value="0.001")],
    )
    test_date = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    complex_model.set_created(test_date)
    complex_model.set_version("1.0.0")

    # Set relationships
    group_urn = MlModelGroupUrn("mlflow", "test_group")
    complex_model.set_model_group(group_urn)
    complex_model.set_training_jobs([DataProcessInstanceUrn("job1")])

    model_urn = MlModelUrn("mlflow", "complex_model", "PROD")
    mock_entities.get.return_value = complex_model

    result = mock_client.entities.get(model_urn)
    assert result.name == "Complex Model"
    assert result.version == "1.0.0"
    assert result.created == test_date
    assert result.training_metrics is not None
    assert result.model_group is not None
    mock_entities.get.assert_called_once_with(model_urn)
    mock_entities.get.reset_mock()

    # Not found case
    error_message = f"Entity {model_urn} not found"
    mock_entities.get.side_effect = ItemNotFoundError(error_message)
    with pytest.raises(ItemNotFoundError, match=re.escape(error_message)):
        mock_client.entities.get(model_urn)
