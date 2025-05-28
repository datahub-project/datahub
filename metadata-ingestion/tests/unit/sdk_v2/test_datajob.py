import pathlib
import re
from datetime import datetime, timezone
from unittest import mock

import pytest

from datahub.emitter.mcp_builder import ContainerKey
from datahub.errors import ItemNotFoundError
from datahub.metadata.urns import (
    CorpUserUrn,
    DataJobUrn,
    DataPlatformInstanceUrn,
    DataPlatformUrn,
    DomainUrn,
    TagUrn,
)
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.testing.sdk_v2_helpers import assert_entity_golden
from datahub.utilities.urns.error import InvalidUrnError

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
        flow=flow,
        name="example_task",
    )

    # Check URN setup
    assert DataJob.get_urn_type() == DataJobUrn
    assert isinstance(job.urn, DataJobUrn)
    assert (
        str(job.urn)
        == "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),example_task)"
    )
    assert str(job.urn) in repr(job)

    # Check basic attributes
    assert job.platform == flow.platform
    assert job.platform_instance is None
    assert job.browse_path == [flow.urn]
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
        flow=flow,
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
    assert job.platform == flow.platform
    assert job.platform == DataPlatformUrn("airflow")
    assert job.platform_instance == flow.platform_instance
    assert job.platform_instance == DataPlatformInstanceUrn("airflow", "my_instance")
    assert job.browse_path == [flow.urn]

    # Validate golden file
    assert_entity_golden(job, GOLDEN_DIR / "test_datajob_complex_golden.json")


def test_client_get_datajob() -> None:
    """Test retrieving DataJobs using client.entities.get()."""
    # Set up mock
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    # Create a test flow URN
    flow = DataFlow(
        platform="airflow",
        name="test_dag",
    )

    # Basic retrieval
    job_urn = DataJobUrn.create_from_ids(
        job_id="test_task",
        data_flow_urn=str(flow.urn),
    )
    expected_job = DataJob(
        flow=flow,
        name="test_task",
        description="A test data job",
    )
    mock_entities.get.return_value = expected_job

    result = mock_client.entities.get(job_urn)
    assert result == expected_job
    mock_entities.get.assert_called_once_with(job_urn)
    mock_entities.get.reset_mock()

    # String URN
    urn_str = "urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),string_task)"
    mock_entities.get.return_value = DataJob(
        flow=flow,
        name="string_task",
    )
    result = mock_client.entities.get(urn_str)
    mock_entities.get.assert_called_once_with(urn_str)
    mock_entities.get.reset_mock()

    # Complex job with properties
    test_date = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    complex_job = DataJob(
        flow=flow,
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
        data_flow_urn=str(flow.urn),
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


def test_datajob_init_with_flow_urn() -> None:
    # Create a dataflow first
    flow = DataFlow(
        platform="airflow",
        name="example_dag",
        platform_instance="my_instance",
    )

    # Create a datajob with the flow URN
    job = DataJob(
        flow_urn=flow.urn,
        platform_instance="my_instance",
        name="example_task",
    )

    print("\n")
    print("job.flow_urn:", job.flow_urn)
    print("flow.urn:", flow.urn)
    print("job.platform_instance:", job.platform_instance)
    print("flow.platform_instance:", flow.platform_instance)
    print("job.name:", job.name)
    print("flow.name:", flow.name)

    assert job.flow_urn == flow.urn
    assert job.platform_instance == flow.platform_instance
    assert job.name == "example_task"

    assert_entity_golden(
        job, GOLDEN_DIR / "test_datajob_init_with_flow_urn_golden.json"
    )


def test_invalid_init() -> None:
    flow = DataFlow(
        platform="airflow",
        name="example_dag",
    )

    with pytest.raises(
        ValueError,
        match=re.escape(
            "You must provide either: 1. a DataFlow object, or 2. a DataFlowUrn (and a platform_instance config if required)"
        ),
    ):
        DataJob(
            name="example_task",
            flow_urn=flow.urn,
        )
    with pytest.raises(
        ValueError,
        match=re.escape(
            "You must provide either: 1. a DataFlow object, or 2. a DataFlowUrn (and a platform_instance config if required)"
        ),
    ):
        DataJob(
            name="example_task",
            platform_instance="my_instance",
        )


def test_datajob_browse_path_without_container() -> None:
    # Create a dataflow without a container
    flow = DataFlow(
        platform="airflow",
        name="example_dag",
    )

    # Create a datajob with the flow
    job = DataJob(
        flow=flow,
        name="example_task",
    )

    # Check that parent and browse paths are set correctly
    assert job.parent_container is None
    assert job.browse_path == [flow.urn]

    assert_entity_golden(
        job, GOLDEN_DIR / "test_datajob_browse_path_without_container_golden.json"
    )


def test_datajob_browse_path_with_container() -> None:
    # Create a container
    container = Container(
        container_key=ContainerKey(
            platform="airflow", name="my_container", instance="my_instance"
        ),
        display_name="My Container",
    )
    # Create a dataflow with the container
    flow = DataFlow(
        platform="airflow",
        name="example_dag",
        parent_container=container,
    )

    # Create a datajob with the flow
    job = DataJob(
        flow=flow,
        name="example_task",
    )

    # Check that parent and browse paths are set correctly
    assert flow.parent_container == container.urn
    assert flow.browse_path == [container.urn]

    # The job's browse path should extend the flow's browse path with the job name
    expected_job_path = [container.urn, flow.urn]
    assert job.browse_path == expected_job_path

    # Use golden file for verification
    assert_entity_golden(job, GOLDEN_DIR / "test_datajob_browse_path_golden.json")


def test_datajob_browse_path_with_containers() -> None:
    # Create a container
    container1 = Container(
        container_key=ContainerKey(
            platform="airflow", name="my_container1", instance="my_instance"
        ),
        display_name="My Container",
    )

    container2 = Container(
        container_key=ContainerKey(
            platform="airflow", name="my_container2", instance="my_instance"
        ),
        display_name="My Container",
        parent_container=container1,
    )

    # Create a dataflow with the container
    flow = DataFlow(
        platform="airflow",
        name="example_dag",
        parent_container=container2,
    )

    # Create a datajob with the flow
    job = DataJob(
        flow=flow,
        name="example_task",
    )

    # Check that parent and browse paths are set correctly
    assert flow.parent_container == container2.urn
    assert flow.browse_path == [container1.urn, container2.urn]
    assert job.browse_path == [container1.urn, container2.urn, flow.urn]

    assert_entity_golden(
        job, GOLDEN_DIR / "test_datajob_browse_path_with_containers_golden.json"
    )


def test_datajob_inlets_outlets() -> None:
    # Create a dataflow first
    flow = DataFlow(
        platform="airflow",
        name="example_dag",
    )

    # Create a datajob with the flow
    job = DataJob(
        flow=flow,
        name="example_task",
        inlets=[
            "urn:li:dataset:(urn:li:dataPlatform:airflow,example_dataset1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:airflow,example_dataset2,PROD)",
        ],
        outlets=["urn:li:dataset:(urn:li:dataPlatform:airflow,example_dataset3,PROD)"],
    )

    assert job.inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:airflow,example_dataset1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:airflow,example_dataset2,PROD)",
    ]
    assert job.outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:airflow,example_dataset3,PROD)"
    ]

    assert_entity_golden(job, GOLDEN_DIR / "test_datajob_inlets_outlets_golden.json")


def test_datajob_invalid_inlets_outlets() -> None:
    # Create a dataflow first
    flow = DataFlow(
        platform="airflow",
        name="example_dag",
    )

    # Create a datajob with the flow
    job = DataJob(
        flow=flow,
        name="example_task",
    )

    with pytest.raises(InvalidUrnError):
        job.set_inlets(
            ["urn:li:dataJob:(urn:li:dataFlow:(airflow,example_dag,PROD),example_task)"]
        )

    with pytest.raises(InvalidUrnError):
        job.set_outlets(
            ["urn:li:datajob:(urn:li:dataFlow:(airflow,example_dag,PROD),example_task)"]
        )
