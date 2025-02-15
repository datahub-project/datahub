import pathlib
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pytest

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp_builder import SchemaKey
from datahub.errors import SchemaFieldKeyError
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.urns import (
    CorpGroupUrn,
    CorpUserUrn,
    DatasetUrn,
    DomainUrn,
    GlossaryTermUrn,
    OwnershipTypeUrn,
    TagUrn,
    Urn,
)
from datahub.sdk._attribution import KnownAttribution, change_default_attribution
from datahub.sdk._shared import UrnOrStr
from datahub.sdk.dataset import Dataset
from tests.test_helpers.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "dataset_golden"


def test_dataset_basic(pytestconfig: pytest.Config) -> None:
    d = Dataset(
        platform="bigquery",
        name="proj.dataset.table",
        subtype=DatasetSubTypes.TABLE,
        schema=[
            ("field1", "string", "field1 description"),
            ("field2", "int64", "field2 description"),
        ],
    )

    # Check urn setup.
    assert Dataset.get_urn_type() == DatasetUrn
    assert isinstance(d.urn, DatasetUrn)
    assert (
        str(d.urn)
        == "urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.dataset.table,PROD)"
    )
    assert str(d.urn) in repr(d)

    # Check most attributes.
    assert d.platform is not None
    assert d.platform.platform_name == "bigquery"
    assert d.platform_instance is None
    assert d.browse_path is None
    assert d.tags is None
    assert d.terms is None
    assert d.created is None
    assert d.last_modified is None
    assert d.description is None
    assert d.custom_properties == {}
    assert d.domain is None

    # TODO: The column descriptions should go in the editable fields, since we're not in ingestion mode.
    assert len(d.schema) == 2
    assert d["field1"].description == "field1 description"

    with pytest.raises(SchemaFieldKeyError, match=r"Field .* not found"):
        d["should_be_missing"]

    with pytest.raises(AttributeError):
        assert d.extra_attribute  # type: ignore
    with pytest.raises(AttributeError):
        d.extra_attribute = "slots should reject extra fields"  # type: ignore
    with pytest.raises(AttributeError):
        # This should fail. Eventually we should make it suggest calling set_owners instead.
        d.owners = []  # type: ignore

    assert_entity_golden(d, _GOLDEN_DIR / "test_dataset_basic_golden.json")


def _build_complex_dataset() -> Dataset:
    schema = SchemaKey(
        platform="snowflake",
        instance="my_instance",
        database="MY_DB",
        schema="MY_SCHEMA",
    )
    db = schema.parent_key()
    assert db is not None

    created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    updated = datetime(2025, 1, 9, 3, 4, 6, tzinfo=timezone.utc)

    d = Dataset(
        platform=schema.platform,
        platform_instance=schema.instance,
        name="my_db.my_schema.my_table",
        parent_container=schema,
        subtype=DatasetSubTypes.TABLE,
        schema=[
            ("field1", "string"),
            ("field2", "int64", "field2 description"),
        ],
        display_name="MY_TABLE",
        qualified_name="MY_DB.MY_SCHEMA.MY_TABLE",
        created=created,
        last_modified=updated,
        custom_properties={
            "key1": "value1",
            "key2": "value2",
        },
        description="test",
        external_url="https://example.com",
        owners=[
            CorpUserUrn("admin@datahubproject.io"),
        ],
        tags=[
            TagUrn("tag1"),
            TagUrn("tag2"),
        ],
        terms=[
            GlossaryTermUrn("AccountBalance"),
        ],
        domain=DomainUrn("Marketing"),
    )

    assert d.platform is not None
    assert d.platform.platform_name == "snowflake"
    assert d.platform_instance is not None
    assert (
        str(d.platform_instance)
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,my_instance)"
    )
    assert schema.parent_key() is not None
    assert d.browse_path == [
        d.platform_instance,
        db.as_urn_typed(),
        schema.as_urn_typed(),
    ]

    # Properties.
    assert d.description == "test"
    assert d.display_name == "MY_TABLE"
    assert d.qualified_name == "MY_DB.MY_SCHEMA.MY_TABLE"
    assert d.external_url == "https://example.com"
    assert d.created == created
    assert d.last_modified == updated
    assert d.custom_properties == {"key1": "value1", "key2": "value2"}

    # Check standard aspects.
    assert d.subtype == "Table"
    assert d.domain == DomainUrn("Marketing")
    assert d.tags is not None
    assert len(d.tags) == 2
    assert d.terms is not None
    assert len(d.terms) == 1
    assert d.owners is not None
    assert len(d.owners) == 1

    assert len(d.schema) == 2

    # Schema field description.
    assert d["field1"].description is None
    assert d["field2"].description == "field2 description"
    d["field1"].set_description("field1 description")
    assert d["field1"].description == "field1 description"

    # Schema field tags.
    assert d["field1"].tags is None
    d["field1"].set_tags([TagUrn("field1_tag1"), TagUrn("field1_tag2")])
    assert d["field1"].tags is not None
    assert len(d["field1"].tags) == 2

    # Schema field terms.
    assert d["field2"].terms is None
    d["field2"].set_terms(
        [GlossaryTermUrn("field2_term1"), GlossaryTermUrn("field2_term2")]
    )
    assert d["field2"].terms is not None
    assert len(d["field2"].terms) == 2

    return d


def test_dataset_complex() -> None:
    d = _build_complex_dataset()
    assert_entity_golden(d, _GOLDEN_DIR / "test_dataset_complex_golden.json")


def test_dataset_ingestion() -> None:
    with change_default_attribution(KnownAttribution.INGESTION):
        d = _build_complex_dataset()
        assert_entity_golden(d, _GOLDEN_DIR / "test_dataset_ingestion_golden.json")


def _tag_names(tags: Optional[List[models.TagAssociationClass]]) -> List[str]:
    if tags is None:
        return []
    return [TagUrn(t.tag).name for t in tags]


def test_tags_add_remove() -> None:
    d = Dataset(
        platform="bigquery",
        name="proj.dataset.table",
        schema=[
            ("field1", "string"),
            ("field2", "int64", "field2 description"),
        ],
        tags=[TagUrn("tag1"), TagUrn("tag2")],
    )
    d["field1"].set_tags([TagUrn("field1_tag1"), TagUrn("field1_tag2")])

    # For each loop - the second iteration should be a no-op.

    # Test tag add/remove flows.
    assert _tag_names(d.tags) == ["tag1", "tag2"]
    for _ in range(2):
        d.add_tag(TagUrn("tag3"))
        assert _tag_names(d.tags) == ["tag1", "tag2", "tag3"]
    for _ in range(2):
        d.remove_tag(TagUrn("tag1"))
        assert _tag_names(d.tags) == ["tag2", "tag3"]

    # Test field tag add/remove flows.
    field = d["field1"]
    assert _tag_names(field.tags) == ["field1_tag1", "field1_tag2"]
    for _ in range(2):
        field.add_tag(TagUrn("field1_tag3"))
        assert _tag_names(field.tags) == ["field1_tag1", "field1_tag2", "field1_tag3"]
    for _ in range(2):
        field.remove_tag(TagUrn("field1_tag1"))
        assert _tag_names(field.tags) == ["field1_tag2", "field1_tag3"]

    assert_entity_golden(d, _GOLDEN_DIR / "test_tags_add_remove_golden.json")


# TODO: We should have add/remove/set tests where there's tags/terms
# in both base and editable entries for a schema field.


def _term_names(
    terms: Optional[List[models.GlossaryTermAssociationClass]],
) -> List[str]:
    if terms is None:
        return []
    return [GlossaryTermUrn(t.urn).name for t in terms]


def test_terms_add_remove() -> None:
    d = Dataset(
        platform="bigquery",
        name="proj.dataset.table",
        schema=[
            ("field1", "string"),
            ("field2", "int64", "field2 description"),
        ],
        terms=[GlossaryTermUrn("AccountBalance")],
    )
    d["field2"].set_terms(
        [GlossaryTermUrn("field2_term1"), GlossaryTermUrn("field2_term2")]
    )

    # Test term add/remove flows.
    assert _term_names(d.terms) == ["AccountBalance"]
    for _ in range(2):
        d.add_term(GlossaryTermUrn("Sensitive"))
        assert _term_names(d.terms) == ["AccountBalance", "Sensitive"]
    for _ in range(2):
        d.remove_term(GlossaryTermUrn("AccountBalance"))
        assert _term_names(d.terms) == ["Sensitive"]

    # Test field term add/remove flows.
    field = d["field2"]
    assert _term_names(field.terms) == ["field2_term1", "field2_term2"]
    for _ in range(2):
        field.add_term(GlossaryTermUrn("PII"))
        assert _term_names(field.terms) == ["field2_term1", "field2_term2", "PII"]
    for _ in range(2):
        field.remove_term(GlossaryTermUrn("field2_term1"))
        assert _term_names(field.terms) == ["field2_term2", "PII"]

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

    d = Dataset(
        platform="redshift",
        name="db.schema.table",
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


def test_browse_path() -> None:
    schema = SchemaKey(
        platform="snowflake",
        database="MY_DB",
        schema="MY_SCHEMA",
    )
    db = schema.parent_key()
    assert db is not None

    path: List[UrnOrStr] = [
        "Folders",
        db.as_urn_typed(),
        "Subfolder",
        schema.as_urn_typed(),
    ]
    d = Dataset(
        platform="snowflake",
        name="MY_DB.MY_SCHEMA.MY_TABLE",
        parent_container=path,
    )
    assert d.parent_container == schema.as_urn_typed()
    assert d.browse_path == path

    assert_entity_golden(d, _GOLDEN_DIR / "test_browse_path_golden.json")
