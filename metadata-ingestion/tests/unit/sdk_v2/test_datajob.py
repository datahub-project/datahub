import pathlib
from datetime import datetime, timezone

import pytest

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.metadata.urns import CorpUserUrn, DataJobUrn, TagUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.testing.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "datajob_golden"
_GOLDEN_DIR.mkdir(exist_ok=True)


def test_datajob_basic(pytestconfig: pytest.Config) -> None:
    # Create a dataflow first
    flow = DataFlow(
        platform="airflow",
        id="example_dag",
    )

    # Create a basic datajob
    job = DataJob(
        platform="airflow",
        id="example_task",
        flow_urn=flow.urn,
    )

    # Check URN setup
    assert DataJob.get_urn_type() == DataJobUrn
    assert isinstance(job.urn, DataJobUrn)
    assert (
        str(job.urn)
        == f"urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,{DEFAULT_ENV}),example_task)"
    )
    assert str(job.urn) in repr(job)

    # Check basic attributes
    assert job.platform is not None
    assert job.platform.platform_name == "airflow"
    assert job.platform_instance is None
    assert job.browse_path is None
    assert job.tags is None
    assert job.terms is None
    assert job.created is None
    assert job.last_modified is None
    assert job.description is None
    assert job.custom_properties == {}
    assert job.domain is None
    assert job.name == "example_task"

    # Validate errors for non-existent attributes
    with pytest.raises(AttributeError):
        assert job.extra_attribute  # type: ignore
    with pytest.raises(AttributeError):
        job.extra_attribute = "slots should reject extra fields"  # type: ignore

    # Validate golden file
    assert_entity_golden(job, _GOLDEN_DIR / "test_datajob_basic_golden.json")


def test_datajob_complex() -> None:
    # Create a dataflow first
    flow = DataFlow(
        platform="airflow",
        platform_instance="my_instance",
        id="example_dag",
    )

    created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    updated = datetime(2025, 1, 9, 3, 4, 6, tzinfo=timezone.utc)

    # Create a complex datajob with all attributes
    job = DataJob(
        platform="airflow",
        platform_instance="my_instance",
        id="complex_task",
        flow_urn=flow.urn,
        name="Complex Task",
        description="A complex data processing task",
        external_url="https://example.com/airflow/task",
        created=created,
        last_modified=updated,
        custom_properties={
            "schedule": "daily",
            "owner_team": "data-engineering",
        },
        tags=[TagUrn("tag1"), TagUrn("tag2")],
        owners=[
            CorpUserUrn("admin@datahubproject.io"),
        ],
    )

    # Check attributes
    assert job.name == "Complex Task"
    assert job.description == "A complex data processing task"
    assert job.external_url == "https://example.com/airflow/task"
    assert job.created == created
    assert job.last_modified == updated
    assert job.custom_properties == {
        "schedule": "daily",
        "owner_team": "data-engineering",
    }
    assert job.platform_instance is not None
    assert (
        str(job.platform_instance)
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:airflow,my_instance)"
    )

    assert (
        str(job.urn.get_data_flow_urn())
        == "urn:li:dataFlow:(airflow,my_instance.example_dag,PROD)"
    )

    # Validate golden file
    assert_entity_golden(job, _GOLDEN_DIR / "test_datajob_complex_golden.json")
