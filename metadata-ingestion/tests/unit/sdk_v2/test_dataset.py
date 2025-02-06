import pathlib
from datetime import datetime, timezone

import pytest

from datahub.emitter.mcp_builder import SchemaKey
from datahub.errors import SchemaFieldKeyError
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.urns import (
    CorpUserUrn,
    DatasetUrn,
    DomainUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk._attribution import KnownAttribution, change_default_attribution
from datahub.sdk._entity import Entity
from datahub.sdk.dataset import Dataset
from tests.test_helpers import mce_helpers

_GOLDEN_DIR = pathlib.Path(__file__).parent / "dataset_golden"


def assert_entity_golden(
    pytestconfig: pytest.Config, entity: Entity, golden_path: pathlib.Path
) -> None:
    mce_helpers.check_goldens_stream(
        pytestconfig=pytestconfig,
        outputs=entity._as_mcps(),
        golden_path=golden_path,
        ignore_order=False,
    )


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
    assert d.platform_instance is None
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

    assert_entity_golden(
        pytestconfig, d, _GOLDEN_DIR / "test_dataset_basic_golden.json"
    )


def _build_complex_dataset() -> Dataset:
    schema = SchemaKey(
        platform="snowflake",
        instance="my_instance",
        database="MY_DB",
        schema="MY_SCHEMA",
    )

    created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    updated = datetime(2025, 1, 9, 3, 4, 6, tzinfo=timezone.utc)

    d = Dataset(
        platform="snowflake",
        platform_instance="my_instance",
        name="my_db.my_schema.my_table",
        container=schema,
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
    assert d.platform_instance is not None
    assert (
        str(d.platform_instance)
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,my_instance)"
    )
    assert d.subtype == "Table"
    assert d.description == "test"
    assert d.display_name == "MY_TABLE"
    assert d.qualified_name == "MY_DB.MY_SCHEMA.MY_TABLE"
    assert d.external_url == "https://example.com"
    assert d.created == created
    assert d.last_modified == updated
    assert d.custom_properties == {"key1": "value1", "key2": "value2"}

    # Check standard aspects.
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


def test_dataset_complex(pytestconfig: pytest.Config) -> None:
    d = _build_complex_dataset()
    assert_entity_golden(
        pytestconfig, d, _GOLDEN_DIR / "test_dataset_complex_golden.json"
    )


def test_dataset_ingestion(pytestconfig: pytest.Config) -> None:
    with change_default_attribution(KnownAttribution.INGESTION):
        d = _build_complex_dataset()

        assert_entity_golden(
            pytestconfig, d, _GOLDEN_DIR / "test_dataset_ingestion_golden.json"
        )
