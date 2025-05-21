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
    DomainUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.testing.sdk_v2_helpers import assert_entity_golden

GOLDEN_DIR = pathlib.Path(__file__).parent / "dataflow_golden"


def test_dataflow_basic(pytestconfig: pytest.Config) -> None:
    d = DataFlow(
        platform="airflow",
        name="example_dag",
    )

    # Check urn setup.
    assert DataFlow.get_urn_type() == DataFlowUrn
    assert isinstance(d.urn, DataFlowUrn)
    assert str(d.urn) == f"urn:li:dataFlow:(airflow,example_dag,{DEFAULT_ENV})"
    assert str(d.urn) in repr(d)

    # Check most attributes.
    assert d.platform is not None
    assert d.platform.platform_name == "airflow"
    assert d.platform_instance is None
    assert d.tags is None
    assert d.terms is None
    assert d.created is None
    assert d.last_modified is None
    assert d.description is None
    assert d.custom_properties == {}
    assert d.domain is None

    with pytest.raises(AttributeError):
        assert d.extra_attribute  # type: ignore
    with pytest.raises(AttributeError):
        d.extra_attribute = "slots should reject extra fields"  # type: ignore
    with pytest.raises(AttributeError):
        # This should fail. Eventually we should make it suggest calling set_owners instead.
        d.owners = []  # type: ignore

    assert_entity_golden(d, GOLDEN_DIR / "test_dataflow_basic_golden.json")


def build_complex_dataflow() -> DataFlow:
    created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    updated = datetime(2025, 1, 9, 3, 4, 6, tzinfo=timezone.utc)

    d = DataFlow(
        platform="airflow",
        platform_instance="my_instance",
        name="example_dag",
        display_name="Example DAG",
        created=created,
        last_modified=updated,
        custom_properties={
            "key1": "value1",
            "key2": "value2",
        },
        description="Test dataflow",
        external_url="https://example.com",
        owners=[
            CorpUserUrn("admin@datahubproject.io"),
        ],
        links=[
            "https://example.com/doc1",
            ("https://example.com/doc2", "Documentation 2"),
        ],
        tags=[
            TagUrn("tag1"),
            TagUrn("tag2"),
        ],
        terms=[
            GlossaryTermUrn("DataPipeline"),
        ],
        domain=DomainUrn("Data Engineering"),
    )

    assert d.platform is not None
    assert d.platform.platform_name == "airflow"
    assert d.platform_instance is not None
    assert (
        str(d.platform_instance)
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:airflow,my_instance)"
    )

    # Properties.
    assert d.description == "Test dataflow"
    assert d.display_name == "Example DAG"
    assert d.external_url == "https://example.com"
    assert d.created == created
    assert d.last_modified == updated
    assert d.custom_properties == {"key1": "value1", "key2": "value2"}

    # Check standard aspects.
    assert d.owners is not None and len(d.owners) == 1
    assert d.links is not None and len(d.links) == 2
    assert d.tags is not None and len(d.tags) == 2
    assert d.terms is not None and len(d.terms) == 1
    assert d.domain == DomainUrn("Data Engineering")

    # Add assertions for links
    assert d.links is not None
    assert len(d.links) == 2
    assert d.links[0].url == "https://example.com/doc1"
    assert d.links[1].url == "https://example.com/doc2"

    return d


def test_dataflow_complex() -> None:
    d = build_complex_dataflow()
    assert_entity_golden(d, GOLDEN_DIR / "test_dataflow_complex_golden.json")


def test_client_get_dataflow() -> None:
    """Test retrieving DataFlows using client.entities.get()."""
    # Set up mock
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    # Basic retrieval
    flow_urn = DataFlowUrn("airflow", "test_dag", DEFAULT_ENV)
    expected_flow = DataFlow(
        platform="airflow",
        name="test_dag",
        description="A test dataflow",
    )
    mock_entities.get.return_value = expected_flow

    result = mock_client.entities.get(flow_urn)
    assert result == expected_flow
    mock_entities.get.assert_called_once_with(flow_urn)
    mock_entities.get.reset_mock()

    # String URN
    urn_str = f"urn:li:dataFlow:(airflow,string_dag,{DEFAULT_ENV})"
    mock_entities.get.return_value = DataFlow(platform="airflow", name="string_dag")
    result = mock_client.entities.get(urn_str)
    mock_entities.get.assert_called_once_with(urn_str)
    mock_entities.get.reset_mock()

    # Complex dataflow with properties
    test_date = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    complex_flow = DataFlow(
        platform="airflow",
        name="complex_dag",
        description="Complex test dataflow",
        display_name="My Complex DAG",
        external_url="https://example.com/dag",
        created=test_date,
        last_modified=test_date,
        custom_properties={"env": "production", "owner_team": "data-eng"},
    )

    # Set relationships and tags
    complex_flow.set_tags([TagUrn("important"), TagUrn("data-pipeline")])
    complex_flow.set_domain(DomainUrn("Data Engineering"))
    complex_flow.set_owners([CorpUserUrn("john@example.com")])

    flow_urn = DataFlowUrn("airflow", "complex_dag", DEFAULT_ENV)
    mock_entities.get.return_value = complex_flow

    result = mock_client.entities.get(flow_urn)
    assert result.name == "complex_dag"
    assert result.display_name == "My Complex DAG"
    assert result.created == test_date
    assert result.description == "Complex test dataflow"
    assert result.tags is not None
    assert result.domain is not None
    assert result.owners is not None
    mock_entities.get.assert_called_once_with(flow_urn)
    mock_entities.get.reset_mock()

    # Not found case
    error_message = f"Entity {flow_urn} not found"
    mock_entities.get.side_effect = ItemNotFoundError(error_message)
    with pytest.raises(ItemNotFoundError, match=re.escape(error_message)):
        mock_client.entities.get(flow_urn)


def test_dataflow_create_job() -> None:
    """Test creating a job from a dataflow."""
    # Create a basic dataflow
    flow = DataFlow(
        platform="airflow",
        name="test_dag",
    )

    # Create a job with minimal parameters
    job = flow.create_job("task1")

    # Verify job was created correctly
    assert isinstance(job, DataJob)
    assert job.name == "task1"
    assert job.flow_urn == str(flow.urn)

    # Check that platform is inherited
    assert job.platform is not None
    assert job.platform.platform_name == "airflow"

    # Create a job with additional parameters
    job2 = flow.create_job(
        "task2",
        description="A complex task",
        tags=[TagUrn("important")],
        custom_properties={"priority": "high"},
    )

    # Verify job was created with the specified properties
    assert job2.name == "task2"
    assert job2.description == "A complex task"
    assert job2.tags is not None
    assert len(job2.tags) == 1
    assert job2.custom_properties == {"priority": "high"}


def test_dataflow_create_jobs() -> None:
    """Test creating multiple jobs from a dataflow."""
    # Create a basic dataflow
    flow = DataFlow(
        platform="airflow",
        name="test_dag",
    )

    # Create multiple jobs with minimal parameters
    job_names = ["extract", "transform", "load"]
    jobs = flow.create_jobs(job_names)

    # Verify correct number of jobs created
    assert len(jobs) == 3

    # Verify each job was created with the correct name
    assert jobs[0].name == "extract"
    assert jobs[1].name == "transform"
    assert jobs[2].name == "load"

    # Verify all jobs have the correct flow_urn and platform
    for job in jobs:
        assert job.flow_urn == str(flow.urn)
        assert job.platform is not None
        assert job.platform.platform_name == "airflow"

    # Create multiple jobs with shared parameters
    tagged_jobs = flow.create_jobs(
        ["validate", "analyze"],
        tags=[TagUrn("processing")],
        description="Processing task",
    )

    # Verify shared parameters were applied to all jobs
    for job in tagged_jobs:
        assert job.description == "Processing task"
        assert job.tags is not None
        assert len(job.tags) == 1
