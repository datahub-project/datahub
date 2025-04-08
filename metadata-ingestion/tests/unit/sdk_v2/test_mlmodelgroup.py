import pathlib

from datahub.metadata.schema_classes import TimeStampClass
from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk.mlmodelgroup import MLModelGroup
from tests.test_helpers.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "mlmodelgroup_golden"


def test_mlmodelgroup_basic() -> None:
    mg = MLModelGroup(
        id="test_model_group",
        platform="mlflow",
        name="Test Model Group",
    )

    # Check urn setup
    assert MLModelGroup.get_urn_type() == MlModelGroupUrn
    assert isinstance(mg.urn, MlModelGroupUrn)
    assert (
        str(mg.urn)
        == "urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,test_model_group,PROD)"
    )
    assert str(mg.urn) in repr(mg)

    # Check most attributes
    assert mg.id == "test_model_group"
    assert mg.platform == "mlflow"
    assert mg.name == "Test Model Group"
    assert mg.description is None
    assert mg.created is None
    assert mg.lastModified is None
    assert mg.customProperties is None

    assert_entity_golden(mg, _GOLDEN_DIR / "test_mlmodelgroup_basic_golden.json")


def test_mlmodelgroup_complex() -> None:
    created = TimeStampClass(time=1628580000000, actor="urn:li:corpuser:datahub")
    updated = TimeStampClass(time=1628580000000, actor="urn:li:corpuser:datahub")

    mg = MLModelGroup(
        id="test_model_group",
        platform="mlflow",
        name="Test Model Group",
        description="A test model group",
        created=created,
        lastModified=updated,
        customProperties={
            "key1": "value1",
            "key2": "value2",
        },
    )

    # Check properties
    assert mg.id == "test_model_group"
    assert mg.platform == "mlflow"
    assert mg.name == "Test Model Group"
    assert mg.description == "A test model group"
    assert mg.created == created
    assert mg.lastModified == updated
    assert mg.customProperties == {
        "key1": "value1",
        "key2": "value2",
    }

    assert_entity_golden(mg, _GOLDEN_DIR / "test_mlmodelgroup_complex_golden.json")
