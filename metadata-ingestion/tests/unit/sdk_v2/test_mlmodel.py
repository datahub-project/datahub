from __future__ import annotations

from datetime import datetime, timezone

import pytest

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


def test_mlmodel_basic() -> None:
    """Test basic MLModel functionality."""
    model = MLModel(
        id="test_model",
        platform="mlflow",
        name="test_model",
    )

    # Test basic properties
    assert model.urn == MlModelUrn("mlflow", "test_model")
    assert model.name == "test_model"
    assert model.platform == DataPlatformUrn("urn:li:dataPlatform:mlflow")

    # Test version and aliases
    model.set_version("1.0.0")
    assert model.version == "1.0.0"

    model.add_alias("alias1")
    model.add_alias("alias2")
    assert model.aliases is not None
    assert "alias1" in model.aliases
    assert "alias2" in model.aliases

    model.remove_alias("alias1")
    assert model.aliases is not None
    assert "alias1" not in model.aliases
    assert "alias2" in model.aliases

    # Test description
    model.set_description("A test model")
    assert model.description == "A test model"

    # Test training metrics
    model.add_training_metric("accuracy", 0.95)
    model.add_training_metric("loss", 0.1)
    metrics = model.training_metrics
    assert metrics is not None
    assert len(metrics) == 2

    # Check for specific metrics using a more type-safe approach
    found_accuracy = False
    found_loss = False
    for m in metrics:
        if hasattr(m, "name") and hasattr(m, "value"):
            if m.name == "accuracy" and m.value == "0.95":
                found_accuracy = True
            if m.name == "loss" and m.value == "0.1":
                found_loss = True
    assert found_accuracy
    assert found_loss

    # Test hyper parameters
    model.add_hyper_param("learning_rate", 0.001)
    model.add_hyper_param("batch_size", 32)
    params = model.hyper_params
    assert params is not None
    assert len(params) == 2

    # Check for specific parameters using a more type-safe approach
    found_learning_rate = False
    found_batch_size = False
    for p in params:
        if hasattr(p, "name") and hasattr(p, "value"):
            if p.name == "learning_rate" and p.value == "0.001":
                found_learning_rate = True
            if p.name == "batch_size" and p.value == "32":
                found_batch_size = True
    assert found_learning_rate
    assert found_batch_size

    # Test external URL
    model.set_external_url("https://example.com/model")
    assert model.external_url == "https://example.com/model"

    # Test timestamps
    test_date = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)  # Fixed date
    model.set_created(test_date)
    model.set_last_modified(test_date)
    assert model.created == test_date
    assert model.last_modified == test_date

    # Test custom properties
    model.set_custom_properties(
        {"framework": "pytorch", "task": "classification", "dataset": "imagenet"}
    )
    assert model.custom_properties == {
        "framework": "pytorch",
        "task": "classification",
        "dataset": "imagenet",
    }

    # Test groups
    group_urn = MlModelGroupUrn("mlflow", "test_group")
    model.add_group(group_urn)
    assert model.groups is not None
    assert str(group_urn) in model.groups

    model.remove_group(group_urn)
    assert model.groups is not None
    assert len(model.groups) == 0

    # Test training jobs
    job_urn = DataProcessInstanceUrn("job1")
    model.add_training_job(job_urn)
    assert model.training_jobs is not None
    assert str(job_urn) in model.training_jobs

    model.remove_training_job(job_urn)
    assert model.training_jobs is not None
    assert len(model.training_jobs) == 0

    # Test downstream jobs
    model.add_downstream_job(job_urn)
    assert model.downstream_jobs is not None
    assert str(job_urn) in model.downstream_jobs

    model.remove_downstream_job(job_urn)
    assert model.downstream_jobs is not None
    assert len(model.downstream_jobs) == 0


def test_mlmodel_complex() -> None:
    """Test more complex MLModel scenarios."""
    # Test initialization with all properties
    model = MLModel(
        id="complex_model",
        platform="test_platform",
        name="complex_model",
        description="A complex test model",
        external_url="https://example.com/complex_model",
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
    )

    assert model.name == "complex_model"
    assert model.description == "A complex test model"
    assert model.external_url == "https://example.com/complex_model"
    assert model.training_metrics is not None
    assert len(model.training_metrics) == 2
    assert model.hyper_params is not None
    assert len(model.hyper_params) == 2
    assert model.custom_properties == {
        "framework": "pytorch",
        "task": "classification",
    }

    # Test setting multiple training metrics at once
    model.set_training_metrics(
        {
            "precision": 0.92,
            "recall": 0.88,
            "f1": 0.90,
        }
    )
    assert model.training_metrics is not None
    assert len(model.training_metrics) == 3

    # Check for a specific metric using a more type-safe approach
    found_precision = False
    for m in model.training_metrics:
        if hasattr(m, "name") and hasattr(m, "value"):
            if m.name == "precision" and m.value == "0.92":
                found_precision = True
    assert found_precision

    # Test setting multiple hyper parameters at once
    model.set_hyper_params(
        {
            "epochs": 100,
            "optimizer": "adam",
            "dropout": 0.5,
        }
    )
    assert model.hyper_params is not None
    assert len(model.hyper_params) == 3

    # Check for a specific parameter using a more type-safe approach
    found_epochs = False
    for p in model.hyper_params:
        if hasattr(p, "name") and hasattr(p, "value"):
            if p.name == "epochs" and p.value == "100":
                found_epochs = True
    assert found_epochs

    # Test multiple groups
    group1 = MlModelGroupUrn("mlflow", "group1")
    group2 = MlModelGroupUrn("mlflow", "group2")
    model.add_group(group1)
    model.add_group(group2)
    assert model.groups is not None
    assert len(model.groups) == 2
    assert str(group1) in model.groups
    assert str(group2) in model.groups

    # Test multiple training jobs
    job1 = DataProcessInstanceUrn("job1")
    job2 = DataProcessInstanceUrn("job2")
    model.add_training_job(job1)
    model.add_training_job(job2)
    assert model.training_jobs is not None
    assert len(model.training_jobs) == 2
    assert str(job1) in model.training_jobs
    assert str(job2) in model.training_jobs

    # Test multiple downstream jobs
    model.add_downstream_job(job1)
    model.add_downstream_job(job2)
    assert model.downstream_jobs is not None
    assert len(model.downstream_jobs) == 2
    assert str(job1) in model.downstream_jobs
    assert str(job2) in model.downstream_jobs


def test_mlmodel_validation() -> None:
    """Test MLModel validation and error cases."""
    # Test invalid platform
    with pytest.raises(InvalidUrnError):
        MLModel(id="test", platform="")

    # Test invalid ID
    with pytest.raises(InvalidUrnError):
        MLModel(id="", platform="test_platform")
