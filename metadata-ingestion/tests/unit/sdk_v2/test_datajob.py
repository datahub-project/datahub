import pathlib
import re
from datetime import datetime, timezone
from unittest import mock

import pytest

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.errors import ItemNotFoundError
from datahub.metadata.urns import (
    CorpUserUrn,
    DataFlowUrn,
    DataJobUrn,
    DomainUrn,
    TagUrn,
)
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.testing.sdk_v2_helpers import assert_entity_golden

GOLDEN_DIR = pathlib.Path(__file__).parent / "datajob_golden"
GOLDEN_DIR.mkdir(exist_ok=True)


def test_datajob_basic(pytestconfig: pytest.Config) -> None:
    # Create a dataflow first
    flow = DataFlow(
        platform="airflow",
        name="example_dag",
    )

    # Create a basic datajob
    job = DataJob(
        flow_urn=flow.urn,
        name="example_task",
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
    assert_entity_golden(job, GOLDEN_DIR / "test_datajob_basic_golden.json")


def test_datajob_complex() -> None:
    # Create a dataflow first
    flow = DataFlow(
        platform="airflow",
        platform_instance="my_instance",
        name="example_dag",
    )

    created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    updated = datetime(2025, 1, 9, 3, 4, 6, tzinfo=timezone.utc)

    # Create a complex datajob with all attributes
    job = DataJob(
        flow_urn=flow.urn,
        name="complex_task",
        display_name="Complex Task",
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
        platform_instance="my_instance",
    )

    # Check attributes
    assert job.name == "complex_task"
    assert job.display_name == "Complex Task"
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

    # Extract the flow_urn and verify it
    assert job.flow_urn is not None
    assert str(flow.urn) == job.flow_urn

    # Validate golden file
    assert_entity_golden(job, GOLDEN_DIR / "test_datajob_complex_golden.json")


def test_client_get_datajob() -> None:
    """Test retrieving DataJobs using client.entities.get()."""
    # Set up mock
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    # Create a test flow URN
    flow_urn = DataFlowUrn("airflow", "test_dag", DEFAULT_ENV)

    # Basic retrieval
    job_urn = DataJobUrn.create_from_ids(
        job_id="test_task",
        data_flow_urn=str(flow_urn),
    )
    expected_job = DataJob(
        flow_urn=flow_urn,
        name="test_task",
        description="A test data job",
    )
    mock_entities.get.return_value = expected_job

    result = mock_client.entities.get(job_urn)
    assert result == expected_job
    mock_entities.get.assert_called_once_with(job_urn)
    mock_entities.get.reset_mock()

    # String URN
    urn_str = f"urn:li:dataJob:(urn:li:dataFlow:(airflow,string_dag,{DEFAULT_ENV}),string_task)"
    mock_entities.get.return_value = DataJob(
        flow_urn=f"urn:li:dataFlow:(airflow,string_dag,{DEFAULT_ENV})",
        name="string_task",
    )
    result = mock_client.entities.get(urn_str)
    mock_entities.get.assert_called_once_with(urn_str)
    mock_entities.get.reset_mock()

    # Complex job with properties
    test_date = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    complex_job = DataJob(
        flow_urn=flow_urn,
        name="complex_task",
        description="Complex test job",
        display_name="My Complex Task",
        external_url="https://example.com/task",
        created=test_date,
        last_modified=test_date,
        custom_properties={"env": "production", "owner_team": "data-eng"},
    )

    # Set relationships and tags
    complex_job.set_tags([TagUrn("important"), TagUrn("data-pipeline")])
    complex_job.set_domain(DomainUrn("Data Engineering"))
    complex_job.set_owners([CorpUserUrn("john@example.com")])

    complex_job_urn = DataJobUrn.create_from_ids(
        job_id="complex_task",
        data_flow_urn=str(flow_urn),
    )
    mock_entities.get.return_value = complex_job

    result = mock_client.entities.get(complex_job_urn)
    assert result.name == "complex_task"
    assert result.display_name == "My Complex Task"
    assert result.created == test_date
    assert result.description == "Complex test job"
    assert result.tags is not None
    assert result.domain is not None
    assert result.owners is not None
    mock_entities.get.assert_called_once_with(complex_job_urn)
    mock_entities.get.reset_mock()

    # Not found case
    error_message = f"Entity {complex_job_urn} not found"
    mock_entities.get.side_effect = ItemNotFoundError(error_message)
    with pytest.raises(ItemNotFoundError, match=re.escape(error_message)):
        mock_client.entities.get(complex_job_urn)


def test_datajob_flow_relationship() -> None:
    """Test the relationship between DataJob and DataFlow."""
    # Create a dataflow
    flow = DataFlow(
        platform="airflow",
        name="test_dag",
    )

    # Create a job associated with this flow
    job = DataJob(
        flow_urn=flow.urn,
        name="test_task",
    )

    # Verify the flow relationship
    assert job.flow_urn is not None
    assert job.flow_urn == str(flow.urn)

    # Test updating the flow_urn
    new_flow = DataFlow(
        platform="airflow",
        name="new_dag",
    )
    job.set_flow_urn(str(new_flow.urn))
    assert job.flow_urn == str(new_flow.urn)

    # Test job URN is updated correctly based on flow
    expected_urn = (
        f"urn:li:dataJob:(urn:li:dataFlow:(airflow,new_dag,{DEFAULT_ENV}),test_task)"
    )
    assert str(job.urn) != expected_urn  # URN does not automatically update

    # Create new job with the new flow to verify URN construction
    new_job = DataJob(
        flow_urn=new_flow.urn,
        name="test_task",
    )
    assert str(new_job.urn) == expected_urn
