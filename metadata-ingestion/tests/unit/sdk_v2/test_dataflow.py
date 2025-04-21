import pathlib
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pytest

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.metadata.urns import (
    CorpGroupUrn,
    CorpUserUrn,
    DataFlowUrn,
    DomainUrn,
    GlossaryTermUrn,
    OwnershipTypeUrn,
    TagUrn,
    Urn,
)
from datahub.sdk._attribution import KnownAttribution, change_default_attribution
from datahub.sdk.dataflow import Dataflow
from tests.test_helpers.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "dataflow_golden"


def test_dataflow_basic(pytestconfig: pytest.Config) -> None:
    d = Dataflow(
        platform="airflow",  # TODO: shoudl be a full platform urn?
        id="example_dag",
    )

    # Check urn setup.
    assert Dataflow.get_urn_type() == DataFlowUrn
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


def _build_complex_dataflow() -> Dataflow:
    created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    updated = datetime(2025, 1, 9, 3, 4, 6, tzinfo=timezone.utc)

    d = Dataflow(
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


def test_dataflow_ingestion() -> None:
    with change_default_attribution(KnownAttribution.INGESTION):
        d = _build_complex_dataflow()
        assert_entity_golden(d, _GOLDEN_DIR / "test_dataflow_ingestion_golden.json")


def _tag_names(tags: Optional[List[models.TagAssociationClass]]) -> List[str]:
    if tags is None:
        return []
    return [TagUrn(t.tag).name for t in tags]


def test_tags_add_remove() -> None:
    d = Dataflow(
        platform="airflow",
        id="example_dag",
        tags=[TagUrn("tag1"), TagUrn("tag2")],
    )

    # For each loop - the second iteration should be a no-op.

    # Test tag add/remove flows.
    assert _tag_names(d.tags) == ["tag1", "tag2"]
    for _ in range(2):
        d.add_tag(TagUrn("tag3"))
        assert _tag_names(d.tags) == ["tag1", "tag2", "tag3"]
    for _ in range(2):
        d.remove_tag(TagUrn("tag1"))
        assert _tag_names(d.tags) == ["tag2", "tag3"]

    assert_entity_golden(d, _GOLDEN_DIR / "test_tags_add_remove_golden.json")


def _term_names(
    terms: Optional[List[models.GlossaryTermAssociationClass]],
) -> List[str]:
    if terms is None:
        return []
    return [GlossaryTermUrn(t.urn).name for t in terms]


def test_terms_add_remove() -> None:
    d = Dataflow(
        platform="airflow",
        id="example_dag",
        terms=[GlossaryTermUrn("DataPipeline")],
    )

    # Test term add/remove flows.
    assert _term_names(d.terms) == ["DataPipeline"]
    for _ in range(2):
        d.add_term(GlossaryTermUrn("ETL"))
        assert _term_names(d.terms) == ["DataPipeline", "ETL"]
    for _ in range(2):
        d.remove_term(GlossaryTermUrn("DataPipeline"))
        assert _term_names(d.terms) == ["ETL"]

    assert_entity_golden(d, _GOLDEN_DIR / "test_terms_add_remove_golden.json")


def _owner_names(owners: Optional[List[models.OwnerClass]]) -> List[Tuple[Urn, str]]:
    if owners is None:
        return []
    return [(Urn.from_string(o.owner), o.typeUrn or str(o.type)) for o in owners]


def test_owners_add_remove() -> None:
    admin = CorpUserUrn("admin@datahubproject.io")
    group = CorpGroupUrn("group@datahubproject.io")

    business = models.OwnershipTypeClass.BUSINESS_OWNER
    technical = models.OwnershipTypeClass.TECHNICAL_OWNER
    custom = OwnershipTypeUrn("urn:li:ownershipType:custom_1")

    d = Dataflow(
        platform="airflow",
        id="example_dag",
        owners=[
            (admin, business),
            (group, technical),
        ],
    )
    assert _owner_names(d.owners) == [
        (admin, business),
        (group, technical),
    ]

    # Add the admin as a technical owner.
    for _ in range(2):
        d.add_owner(admin)
        assert _owner_names(d.owners) == [
            (admin, business),
            (group, technical),
            (admin, technical),
        ]

    # Add the admin as a custom owner.
    for _ in range(2):
        d.add_owner((admin, custom))
        assert _owner_names(d.owners) == [
            (admin, business),
            (group, technical),
            (admin, technical),
            (admin, str(custom)),
        ]

    # Type-specific removal.
    for _ in range(2):
        d.remove_owner((admin, technical))
        assert _owner_names(d.owners) == [
            (admin, business),
            (group, technical),
            (admin, str(custom)),
        ]

    # This should remove both admin owner types.
    for _ in range(2):
        d.remove_owner(admin)
        assert _owner_names(d.owners) == [(group, technical)]

    assert_entity_golden(d, _GOLDEN_DIR / "test_owners_add_remove_golden.json")


def test_links_add_remove() -> None:
    d = Dataflow(
        platform="airflow",
        id="example_dag",
        links=[
            "https://example.com/doc1",
            ("https://example.com/doc2", "Documentation 2"),
        ],
    )

    # Test initial state
    assert d.links is not None
    assert len(d.links) == 2
    assert d.links[0].url == "https://example.com/doc1"
    assert d.links[0].description == "https://example.com/doc1"
    assert d.links[1].url == "https://example.com/doc2"
    assert d.links[1].description == "Documentation 2"

    # Test link add/remove flows
    for _ in range(2):  # Second iteration should be a no-op
        d.add_link(("https://example.com/doc3", "Documentation 3"))
        assert len(d.links) == 3
        assert d.links[2].url == "https://example.com/doc3"
        assert d.links[2].description == "Documentation 3"

    for _ in range(2):  # Second iteration should be a no-op
        d.remove_link("https://example.com/doc1")
        assert len(d.links) == 2
        assert d.links[0].url == "https://example.com/doc2"
        assert d.links[1].url == "https://example.com/doc3"

    assert_entity_golden(d, _GOLDEN_DIR / "test_links_add_remove_golden.json")
