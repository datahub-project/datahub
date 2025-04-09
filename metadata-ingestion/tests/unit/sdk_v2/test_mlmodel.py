import pathlib
from datetime import datetime, timezone

from datahub.metadata.schema_classes import (
    MLHyperParamClass,
    MLMetricClass,
    TagAssociationClass,
)
from datahub.metadata.urns import MlModelGroupUrn, MlModelUrn
from datahub.sdk.mlmodel import MLModel
from tests.test_helpers.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "mlmodel_golden"


def test_mlmodel_basic() -> None:
    created = datetime(2025, 4, 9, 22, 30, tzinfo=timezone.utc)
    updated = datetime(2025, 4, 9, 22, 30, tzinfo=timezone.utc)

    m = MLModel(
        id="test_model",
        version="1.0.0",
        platform="mlflow",
        name="Test Model",
        description="A basic test model for demonstration",
        training_metrics={"accuracy": "0.85", "loss": "0.12"},
        hyper_params={"learning_rate": "0.001", "batch_size": "64"},
        external_url="https://mlflow.example.com/models/test_model",
        custom_properties={
            "framework": "pytorch",
            "task": "classification",
            "dataset": "mnist",
        },
        aliases=["test_model_v1", "mnist_classifier"],
        training_jobs=[
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_model)"
        ],
        created=created,
        last_modified=updated,
        tags=["urn:li:tag:test_tag"],
    )

    # Check urn setup
    assert MLModel.get_urn_type() == MlModelUrn
    assert isinstance(m.urn, MlModelUrn)
    assert str(m.urn) == "urn:li:mlModel:(urn:li:dataPlatform:mlflow,test_model,PROD)"
    assert str(m.urn) in repr(m)

    # Check most attributes
    assert m.version == "1.0.0"
    assert str(m.platform) == "urn:li:dataPlatform:mlflow"
    assert m.name == "Test Model"
    assert m.description == "A basic test model for demonstration"
    assert m.training_metrics is not None
    assert len(m.training_metrics) == 2
    assert any(m.name == "accuracy" and m.value == "0.85" for m in m.training_metrics)
    assert any(m.name == "loss" and m.value == "0.12" for m in m.training_metrics)
    assert m.hyper_params is not None
    assert len(m.hyper_params) == 2
    assert any(p.name == "learning_rate" and p.value == "0.001" for p in m.hyper_params)
    assert any(p.name == "batch_size" and p.value == "64" for p in m.hyper_params)
    assert m.external_url == "https://mlflow.example.com/models/test_model"
    assert m.created == created
    assert m.last_modified == updated
    assert m.tags == [TagAssociationClass("urn:li:tag:test_tag")]
    assert m.groups is None
    assert m.training_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_model)"
    ]
    assert m.aliases == ["test_model_v1", "mnist_classifier"]
    assert m.custom_properties == {
        "framework": "pytorch",
        "task": "classification",
        "dataset": "mnist",
    }

    assert_entity_golden(m, _GOLDEN_DIR / "test_mlmodel_basic_golden.json")


def test_mlmodel_complex() -> None:
    created = datetime(2025, 4, 9, 22, 30, tzinfo=timezone.utc)
    updated = datetime(2025, 4, 9, 22, 30, tzinfo=timezone.utc)

    training_metrics = {"accuracy": "0.95", "f1": "0.92"}

    hyper_params = {"learning_rate": "0.01", "batch_size": "32"}

    m = MLModel(
        id="test_model",
        version="1.0.0",
        platform="mlflow",
        name="Test Model",
        description="A test model",
        training_metrics=training_metrics,
        hyper_params=hyper_params,
        external_url="https://example.com/model",
        created=created,
        last_modified=updated,
        custom_properties={
            "framework": "tensorflow",
            "task": "object_detection",
            "dataset": "coco",
            "architecture": "yolov5",
            "pretrained": "true",
        },
        aliases=["test_model_v1", "yolov5_coco"],
        training_jobs=[
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_model)",
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),validate_model)",
        ],
        tags=["urn:li:tag:test_tag"],
    )

    # Check properties
    assert m.version == "1.0.0"
    assert str(m.platform) == "urn:li:dataPlatform:mlflow"
    assert m.name == "Test Model"
    assert m.description == "A test model"
    assert m.training_metrics is not None
    assert m.training_metrics == [
        MLMetricClass(name="accuracy", value="0.95"),
        MLMetricClass(name="f1", value="0.92"),
    ]
    assert m.hyper_params is not None
    assert m.hyper_params == [
        MLHyperParamClass(name="learning_rate", value="0.01"),
        MLHyperParamClass(name="batch_size", value="32"),
    ]
    assert m.external_url == "https://example.com/model"
    assert m.created == created
    assert m.last_modified == updated
    assert m.tags == [TagAssociationClass("urn:li:tag:test_tag")]
    assert m.groups is None
    assert m.training_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_model)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),validate_model)",
    ]
    assert m.aliases == ["test_model_v1", "yolov5_coco"]
    assert m.custom_properties == {
        "framework": "tensorflow",
        "task": "object_detection",
        "dataset": "coco",
        "architecture": "yolov5",
        "pretrained": "true",
    }

    # Test property setters
    m.set_name("New Name")
    assert m.name == "New Name"

    m.set_description("New Description")
    assert m.description == "New Description"

    m.set_external_url("https://example.com/new")
    assert m.external_url == "https://example.com/new"

    m.set_custom_properties(
        {"framework": "pytorch", "task": "segmentation", "dataset": "cityscapes"}
    )
    assert m.custom_properties == {
        "framework": "pytorch",
        "task": "segmentation",
        "dataset": "cityscapes",
    }

    # Create new timestamps with microseconds truncated
    new_created = datetime.now(timezone.utc).replace(microsecond=0)
    m.set_created(new_created)
    assert m.created == new_created

    new_modified = datetime.now(timezone.utc).replace(microsecond=0)
    m.set_last_modified(new_modified)
    assert m.last_modified == new_modified

    # Test training metrics
    m.add_training_metric("precision", "0.94")
    assert m.training_metrics is not None
    assert len(m.training_metrics) == 3
    assert any(m.name == "precision" and m.value == "0.94" for m in m.training_metrics)

    # Test hyper parameters
    m.add_hyper_param("epochs", "100")
    assert m.hyper_params is not None
    assert len(m.hyper_params) == 3
    assert any(p.name == "epochs" and p.value == "100" for p in m.hyper_params)

    # Test aliases
    m.add_aliases(["alias3"])
    assert m.aliases == ["test_model_v1", "yolov5_coco", "alias3"]

    # Add to group
    group_urn = str(MlModelGroupUrn(platform="mlflow", name="test_group"))
    m.add_to_group(group_urn)
    assert m.groups == [group_urn]

    # Remove from group
    m.remove_from_group(group_urn)
    assert m.groups == []

    # Add training job
    job_urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_flow,prod),test_job)"
    m.add_training_jobs([job_urn])
    assert m.training_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_model)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),validate_model)",
        job_urn,
    ]

    assert_entity_golden(m, _GOLDEN_DIR / "test_mlmodel_complex_golden.json")
