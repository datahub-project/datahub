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

    # TODO: The column descriptions should go in the editable fields, since we're not in ingestion mode.
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
            ("field1", "string", "field1 description"),
            ("field2", "int64", "field2 description"),
        ],
        display_name="MY_TABLE",
        created=created,
        last_modified=updated,
        custom_properties={
            "key1": "value1",
            "key2": "value2",
        },
        description="test",
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
    assert d.created == created
    assert d.last_modified == updated
    assert d.custom_properties == {"key1": "value1", "key2": "value2"}
    assert d.description == "test"

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
