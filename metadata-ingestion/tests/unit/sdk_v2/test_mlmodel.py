import pathlib

from datahub.metadata.schema_classes import (
    MLHyperParamClass,
    MLMetricClass,
    TimeStampClass,
)
from datahub.metadata.urns import MlModelGroupUrn, MlModelUrn
from datahub.sdk.mlmodel import MLModel
from tests.test_helpers.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "mlmodel_golden"


def test_mlmodel_basic() -> None:
    m = MLModel(
        id="test_model",
        version="1.0.0",
        platform="mlflow",
        name="Test Model",
    )

    # Check urn setup
    assert MLModel.get_urn_type() == MlModelUrn
    assert isinstance(m.urn, MlModelUrn)
    assert str(m.urn) == "urn:li:mlModel:(urn:li:dataPlatform:mlflow,test_model,PROD)"
    assert str(m.urn) in repr(m)

    # Check most attributes
    assert m.id == "test_model"
    assert m.version == "1.0.0"
    assert m.platform == "mlflow"
    assert m.name == "Test Model"
    assert m.description is None
    assert m.trainingMetrics is None
    assert m.hyperParams is None
    assert m.externalUrl is None
    assert m.created is None
    assert m.lastModified is None
    assert m.tags == []
    assert m.groups == []
    assert m.trainingJobs == []

    assert_entity_golden(m, _GOLDEN_DIR / "test_mlmodel_basic_golden.json")


def test_mlmodel_complex() -> None:
    created = TimeStampClass(time=1628580000000, actor="urn:li:corpuser:datahub")
    updated = TimeStampClass(time=1628580000000, actor="urn:li:corpuser:datahub")

    training_metrics = [
        MLMetricClass(
            name="accuracy",
            value="0.95",
        ),
        MLMetricClass(
            name="f1",
            value="0.92",
        ),
    ]

    hyper_params = [
        MLHyperParamClass(
            name="learning_rate",
            value="0.01",
        ),
        MLHyperParamClass(
            name="batch_size",
            value="32",
        ),
    ]

    m = MLModel(
        id="test_model",
        version="1.0.0",
        platform="mlflow",
        name="Test Model",
        description="A test model",
        trainingMetrics=training_metrics,
        hyperParams=hyper_params,
        externalUrl="https://example.com/model",
        created=created,
        lastModified=updated,
    )

    # Check properties
    assert m.id == "test_model"
    assert m.version == "1.0.0"
    assert m.platform == "mlflow"
    assert m.name == "Test Model"
    assert m.description == "A test model"
    assert m.trainingMetrics == training_metrics
    assert m.hyperParams == hyper_params
    assert m.externalUrl == "https://example.com/model"
    assert m.created == created
    assert m.lastModified == updated
    assert m.tags == []
    assert m.groups == []
    assert m.trainingJobs == []

    # Add to group
    group_urn = str(MlModelGroupUrn(platform="mlflow", name="test_group"))
    m.add_to_group(group_urn)
    assert m.groups == [group_urn]

    # Add training job
    job_urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_flow,prod),test_job)"
    m.add_training_job(job_urn)
    assert m.trainingJobs == [job_urn]

    assert_entity_golden(m, _GOLDEN_DIR / "test_mlmodel_complex_golden.json")
