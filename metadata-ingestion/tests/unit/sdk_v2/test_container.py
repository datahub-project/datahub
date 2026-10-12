import pathlib
from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    auto_stale_entity_removal,
)
from datahub.ingestion.workunit_processors.auto_status_aspect import (
    AutoStatusAspectProcessor,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    StatusClass,
    SubTypesClass,
)
from datahub.metadata.urns import (
    ContainerUrn,
    CorpUserUrn,
    DomainUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk.container import Container
from datahub.testing.sdk_v2_helpers import assert_entity_golden

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
    assert c.owners is None
    assert c.links is None
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
        links=["https://example.com/doc1"],
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
    assert c.owners is not None and len(c.owners) == 1
    assert c.links is not None and len(c.links) == 1
    assert c.tags is not None and len(c.tags) == 2
    assert c.terms is not None and len(c.terms) == 1
    assert c.domain == DomainUrn("Marketing")

    assert_entity_golden(c, _GOLDEN_DIR / "test_container_complex_golden.json")


def test_container_emits_data_platform_instance_first() -> None:
    c = Container(
        DatabaseKey(platform="snowflake", instance="inst", database="db"),
        display_name="db",
        subtype="Database",
    )

    assert c.as_mcps()[0].aspectName == "dataPlatformInstance"


def test_container_emits_data_platform_instance_before_extra_aspects() -> None:
    c = Container(
        DatabaseKey(platform="snowflake", instance="inst", database="db"),
        display_name="db",
        extra_aspects=[
            SubTypesClass(typeNames=["Database"]),
            DataPlatformInstanceClass(platform="urn:li:dataPlatform:snowflake"),
        ],
    )

    mcps = c.as_mcps()
    assert mcps[0].aspectName == "dataPlatformInstance"
    # The key's instance still wins over an extra dataPlatformInstance.
    assert c.platform_instance is not None
    assert [m.aspectName for m in mcps].count("dataPlatformInstance") == 1


def _aspect_names(mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
    return [mcp.aspectName or "" for mcp in mcps]


def test_container_with_instance_emits_status_after_data_platform_instance() -> None:
    c = Container(
        DatabaseKey(platform="snowflake", instance="inst", database="db"),
        display_name="db",
    )

    mcps = c.as_mcps()
    assert _aspect_names(mcps)[:2] == ["dataPlatformInstance", "status"]
    assert mcps[1].aspect == StatusClass(removed=False)


def test_container_without_instance_emits_no_status() -> None:
    c = Container(DatabaseKey(platform="snowflake", database="db"), display_name="db")

    assert "status" not in _aspect_names(c.as_mcps())


def test_container_keeps_its_own_status() -> None:
    c = Container(
        DatabaseKey(platform="snowflake", instance="inst", database="db"),
        display_name="db",
        extra_aspects=[StatusClass(removed=True)],
    )

    statuses = [mcp.aspect for mcp in c.as_mcps() if mcp.aspectName == "status"]
    assert statuses == [StatusClass(removed=True)]


def test_container_status_suppresses_auto_status_patch() -> None:
    c = Container(
        DatabaseKey(platform="snowflake", instance="inst", database="db"),
        display_name="db",
    )
    wus = c.as_workunits()

    with patch(
        "datahub.ingestion.workunit_processors.auto_status_aspect._gms_supports_status_patch",
        return_value=True,
    ):
        out = list(AutoStatusAspectProcessor.create(MagicMock()).process(wus))

    assert len(out) == len(wus)


def test_container_status_cannot_undo_a_later_soft_delete() -> None:
    # A soft delete from stale-entity removal is generated only after the
    # source stream ends, so it always lands after the entity's own
    # status(removed=False).
    c = Container(
        DatabaseKey(platform="snowflake", instance="inst", database="db"),
        display_name="db",
    )
    soft_delete = MetadataChangeProposalWrapper(
        entityUrn=str(c.urn), aspect=StatusClass(removed=True)
    ).as_workunit()
    handler = MagicMock()
    handler.gen_removed_entity_workunits.return_value = [soft_delete]

    out = list(auto_stale_entity_removal(handler, c.as_workunits()))

    statuses = [
        wu.metadata.aspect
        for wu in out
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "status"
    ]
    assert statuses == [StatusClass(removed=False), StatusClass(removed=True)]
