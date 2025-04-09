import pathlib
from datetime import datetime, timezone

from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk.mlmodelgroup import MLModelGroup
from tests.test_helpers.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "mlmodelgroup_golden"


def test_mlmodelgroup_basic() -> None:
    created = datetime(2025, 4, 9, 22, 30, tzinfo=timezone.utc)
    updated = datetime(2025, 4, 9, 22, 30, tzinfo=timezone.utc)

    mg = MLModelGroup(
        id="test_model_group",
        platform="mlflow",
        name="Test Model Group",
        description="A group of test models for demonstration",
        external_url="https://mlflow.example.com/groups/test_group",
        custom_properties={
            "purpose": "testing",
            "environment": "development",
            "owner": "ml_team",
        },
        training_jobs=[
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_models)"
        ],
        downstream_jobs=[
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,serving_pipeline,prod),serve_models)"
        ],
        created=created,
        last_modified=updated,
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
    assert str(mg.platform) == "urn:li:dataPlatform:mlflow"
    assert mg.name == "Test Model Group"
    assert mg.description == "A group of test models for demonstration"
    assert mg.external_url == "https://mlflow.example.com/groups/test_group"
    assert mg.created == created
    assert mg.last_modified == updated
    assert mg.custom_properties == {
        "purpose": "testing",
        "environment": "development",
        "owner": "ml_team",
    }
    assert mg.training_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_models)"
    ]
    assert mg.downstream_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,serving_pipeline,prod),serve_models)"
    ]

    assert_entity_golden(mg, _GOLDEN_DIR / "test_mlmodelgroup_basic_golden.json")


def test_mlmodelgroup_complex() -> None:
    created = datetime(2025, 4, 9, 22, 30, tzinfo=timezone.utc)
    updated = datetime(2025, 4, 9, 22, 30, tzinfo=timezone.utc)

    mg = MLModelGroup(
        id="test_model_group",
        platform="mlflow",
        name="Test Model Group",
        description="A test model group for complex scenarios",
        created=created,
        last_modified=updated,
        external_url="https://mlflow.example.com/groups/complex_group",
        custom_properties={
            "purpose": "production",
            "environment": "staging",
            "owner": "ml_team",
            "framework": "tensorflow",
            "task": "object_detection",
        },
        training_jobs=[
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_models)",
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),validate_models)",
        ],
        downstream_jobs=[
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,serving_pipeline,prod),serve_models)",
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,monitoring_pipeline,prod),monitor_models)",
        ],
    )

    # Check properties
    assert str(mg.platform) == "urn:li:dataPlatform:mlflow"
    assert mg.name == "Test Model Group"
    assert mg.description == "A test model group for complex scenarios"
    assert mg.created == created
    assert mg.last_modified == updated
    assert mg.external_url == "https://mlflow.example.com/groups/complex_group"
    assert mg.custom_properties == {
        "purpose": "production",
        "environment": "staging",
        "owner": "ml_team",
        "framework": "tensorflow",
        "task": "object_detection",
    }
    assert mg.training_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_models)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),validate_models)",
    ]
    assert mg.downstream_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,serving_pipeline,prod),serve_models)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,monitoring_pipeline,prod),monitor_models)",
    ]

    # Test property setters
    mg.set_name("New Name")
    assert mg.name == "New Name"

    mg.set_description("New Description")
    assert mg.description == "New Description"

    mg.set_external_url("https://example.com/new")
    assert mg.external_url == "https://example.com/new"

    mg.set_custom_properties(
        {"purpose": "research", "environment": "development", "owner": "research_team"}
    )
    assert mg.custom_properties == {
        "purpose": "research",
        "environment": "development",
        "owner": "research_team",
    }

    # Create new timestamps with microseconds truncated
    new_created = datetime.now(timezone.utc).replace(microsecond=0)
    mg.set_created(new_created)
    assert mg.created == new_created

    new_modified = datetime.now(timezone.utc).replace(microsecond=0)
    mg.set_last_modified(new_modified)
    assert mg.last_modified == new_modified

    # Test training jobs
    mg.add_training_jobs(
        [
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),retrain_models)"
        ]
    )
    assert mg.training_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_models)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),validate_models)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),retrain_models)",
    ]

    mg.remove_training_jobs(
        [
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),train_models)"
        ]
    )
    assert mg.training_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),validate_models)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,training_pipeline,prod),retrain_models)",
    ]

    # Test downstream jobs
    mg.add_downstream_jobs(
        [
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,serving_pipeline,prod),update_models)"
        ]
    )
    assert mg.downstream_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,serving_pipeline,prod),serve_models)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,monitoring_pipeline,prod),monitor_models)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,serving_pipeline,prod),update_models)",
    ]

    mg.remove_downstream_jobs(
        [
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,serving_pipeline,prod),serve_models)"
        ]
    )
    assert mg.downstream_jobs == [
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,monitoring_pipeline,prod),monitor_models)",
        "urn:li:dataJob:(urn:li:dataFlow:(airflow,serving_pipeline,prod),update_models)",
    ]

    assert_entity_golden(mg, _GOLDEN_DIR / "test_mlmodelgroup_complex_golden.json")
