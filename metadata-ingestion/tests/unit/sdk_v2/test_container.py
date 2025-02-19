import pathlib
from datetime import datetime, timezone

import pytest

from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
)
from datahub.metadata.urns import (
    ContainerUrn,
    CorpUserUrn,
    DomainUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk.container import Container
from tests.test_helpers.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "container_golden"


def test_container_basic() -> None:
    db_key = DatabaseKey(
        platform="bigquery",
        database="my_bq_project",
    )

    c = Container(
        db_key,
        display_name="my_bq_project",
        subtype=DatasetContainerSubTypes.BIGQUERY_PROJECT,
    )

    # Check urn setup.
    assert Container.get_urn_type() == ContainerUrn
    assert isinstance(c.urn, ContainerUrn)
    assert str(c.urn) == "urn:li:container:1e476e4c36434ae8a7ea78e467e5b59d"
    assert str(c.urn) in repr(c)

    # Check most attributes.
    assert c.platform is not None
    assert c.platform.platform_name == "bigquery"
    assert c.platform_instance is None
    assert c.browse_path == []
    assert c.tags is None
    assert c.terms is None
    assert c.created is None
    assert c.last_modified is None
    assert c.description is None
    assert c.custom_properties == {
        "platform": "bigquery",
        "database": "my_bq_project",
    }
    assert c.domain is None

    # Check slots.
    with pytest.raises(AttributeError):
        assert c.extra_attribute  # type: ignore
    with pytest.raises(AttributeError):
        c.extra_attribute = "slots should reject extra fields"  # type: ignore
    with pytest.raises(AttributeError):
        # This should fail. Eventually we should make it suggest calling set_owners instead.
        c.owners = []  # type: ignore

    assert_entity_golden(c, _GOLDEN_DIR / "test_container_basic_golden.json")


def test_container_complex() -> None:
    schema_key = SchemaKey(
        platform="snowflake",
        instance="my_instance",
        database="MY_DB",
        schema="MY_SCHEMA",
    )
    db_key = schema_key.parent_key()
    assert db_key is not None

    created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    updated = datetime(2025, 1, 9, 3, 4, 6, tzinfo=timezone.utc)

    c = Container(
        schema_key,
        display_name="MY_SCHEMA",
        qualified_name="MY_DB.MY_SCHEMA",
        subtype=DatasetContainerSubTypes.SCHEMA,
        created=created,
        last_modified=updated,
        extra_properties={
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
    assert c.platform is not None
    assert c.platform.platform_name == "snowflake"
    assert c.platform_instance is not None
    assert (
        str(c.platform_instance)
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,my_instance)"
    )
    assert c.browse_path == [
        c.platform_instance,
        db_key.as_urn_typed(),
    ]

    # Properties.
    assert c.description == "test"
    assert c.display_name == "MY_SCHEMA"
    assert c.qualified_name == "MY_DB.MY_SCHEMA"
    assert c.external_url == "https://example.com"
    assert c.created == created
    assert c.last_modified == updated
    assert c.custom_properties == {
        "platform": "snowflake",
        "instance": "my_instance",
        "database": "MY_DB",
        "schema": "MY_SCHEMA",
        "key1": "value1",
        "key2": "value2",
    }

    # Check standard aspects.
    assert c.subtype == "Schema"
    assert c.domain == DomainUrn("Marketing")
    assert c.tags is not None
    assert len(c.tags) == 2
    assert c.terms is not None
    assert len(c.terms) == 1
    assert c.owners is not None
    assert len(c.owners) == 1

    assert_entity_golden(c, _GOLDEN_DIR / "test_container_complex_golden.json")
