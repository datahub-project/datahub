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


def test_container_basic(pytestconfig: pytest.Config) -> None:
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
    assert c.platform_instance is None
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

    assert_entity_golden(
        pytestconfig, c, _GOLDEN_DIR / "test_container_basic_golden.json"
    )


def test_container_complex(pytestconfig: pytest.Config) -> None:
    schema_key = SchemaKey(
        platform="snowflake",
        instance="my_instance",
        database="MY_DB",
        schema="MY_SCHEMA",
    )
    created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    updated = datetime(2025, 1, 9, 3, 4, 6, tzinfo=timezone.utc)

    d = Container(
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
    assert d.platform_instance is not None
    assert (
        str(d.platform_instance)
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,my_instance)"
    )
    assert d.subtype == "Schema"
    assert d.description == "test"
    assert d.display_name == "MY_SCHEMA"
    assert d.qualified_name == "MY_DB.MY_SCHEMA"
    assert d.external_url == "https://example.com"
    assert d.created == created
    assert d.last_modified == updated
    assert d.custom_properties == {
        "platform": "snowflake",
        "instance": "my_instance",
        "database": "MY_DB",
        "schema": "MY_SCHEMA",
        "key1": "value1",
        "key2": "value2",
    }

    # Check standard aspects.
    assert d.domain == DomainUrn("Marketing")
    assert d.tags is not None
    assert len(d.tags) == 2
    assert d.terms is not None
    assert len(d.terms) == 1
    assert d.owners is not None
    assert len(d.owners) == 1

    assert_entity_golden(
        pytestconfig, d, _GOLDEN_DIR / "test_container_complex_golden.json"
    )
