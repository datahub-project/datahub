import pathlib
from datetime import datetime, timezone

import pytest

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.metadata.urns import (
    CorpUserUrn,
    DataFlowUrn,
    DomainUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk.dataflow import DataFlow
from datahub.testing.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "dataflow_golden"


def test_dataflow_basic(pytestconfig: pytest.Config) -> None:
    d = DataFlow(
        platform="airflow",
        id="example_dag",
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
    assert d.browse_path is None
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

    assert_entity_golden(d, _GOLDEN_DIR / "test_dataflow_basic_golden.json")


def _build_complex_dataflow() -> DataFlow:
    created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    updated = datetime(2025, 1, 9, 3, 4, 6, tzinfo=timezone.utc)

    d = DataFlow(
        platform="airflow",
        platform_instance="my_instance",
        id="example_dag",
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
    d = _build_complex_dataflow()
    assert_entity_golden(d, _GOLDEN_DIR / "test_dataflow_complex_golden.json")
