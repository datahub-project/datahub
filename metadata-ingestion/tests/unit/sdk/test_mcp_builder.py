from typing import Optional
from unittest.mock import MagicMock

import datahub.emitter.mcp_builder as builder
from datahub.emitter.mce_builder import make_tag_urn
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    StatusClass,
    TagAssociationClass,
    TelemetryClientIdClass,
)


def test_guid_generator():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance="TestInstance"
    )

    guid = key.guid()
    assert guid == "f096b3799fc86a3e5d5d0c083eb1f2a4"


def test_guid_generator_with_empty_instance():
    key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance=None,
    )

    guid = key.guid()
    assert guid == "693ed953c7192bcf46f8b9db36d71c2b"


def test_guid_generator_with_instance():
    key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance="TestInstance",
        backcompat_env_as_instance=True,
    )
    guid = key.guid()
    assert guid == "f096b3799fc86a3e5d5d0c083eb1f2a4"


def test_guid_generator_with_instance_and_env():
    key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance="TestInstance",
        env="PROD",
        backcompat_env_as_instance=True,
    )
    guid = key.guid()
    assert guid == "f096b3799fc86a3e5d5d0c083eb1f2a4"

    assert key.property_dict() == {
        "database": "test",
        "schema": "Test",
        "platform": "mysql",
        "instance": "TestInstance",
        "env": "PROD",
    }


def test_guid_generator_with_env():
    key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance=None,
        env="TestInstance",
        backcompat_env_as_instance=True,
    )
    guid = key.guid()
    assert guid == "f096b3799fc86a3e5d5d0c083eb1f2a4"

    assert key.property_dict() == {
        "database": "test",
        "schema": "Test",
        "platform": "mysql",
        "env": "TestInstance",
    }


def test_guid_generators():
    key = builder.SchemaKey(
        database="test", schema="Test", platform="mysql", instance="TestInstance"
    )
    guid_datahub = key.guid()

    guid = key.guid()
    assert guid == guid_datahub


def _assert_eq(a: builder.ContainerKey, b: builder.ContainerKey) -> None:
    assert a == b
    assert a.__class__ is b.__class__
    assert a.guid() == b.guid()
    assert a.property_dict() == b.property_dict()


def test_parent_key() -> None:
    schema_key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance="TestInstance",
        env="DEV",
    )
    db_key = schema_key.parent_key()
    assert db_key is not None
    _assert_eq(
        db_key,
        builder.DatabaseKey(
            database="test",
            platform="mysql",
            instance="TestInstance",
            env="DEV",
        ),
    )

    assert db_key.parent_key() is None


def test_parent_key_with_backcompat_env_as_instance() -> None:
    schema_key = builder.SchemaKey(
        database="test",
        schema="Test",
        platform="mysql",
        instance=None,
        env="PROD",
        backcompat_env_as_instance=True,
    )
    db_key = schema_key.parent_key()
    assert db_key is not None
    _assert_eq(
        db_key,
        builder.DatabaseKey(
            database="test",
            platform="mysql",
            instance=None,
            env="PROD",
            backcompat_env_as_instance=True,
        ),
    )


def test_parent_key_on_container_key() -> None:
    # In general, people shouldn't be calling parent_key() on ContainerKey directly.
    # But just in case, we should make sure it works.
    container_key = builder.ContainerKey(
        platform="bigquery",
        name="test",
        env="DEV",
    )
    assert container_key.parent_key() is None


def test_entity_supports_aspect():
    assert builder.entity_supports_aspect("dataset", StatusClass)
    assert not builder.entity_supports_aspect("telemetry", StatusClass)

    assert not builder.entity_supports_aspect("dataset", TelemetryClientIdClass)
    assert builder.entity_supports_aspect("telemetry", TelemetryClientIdClass)


def _make_tag_association(tag_name: str, source_urn: Optional[str] = None):
    """Helper to create TagAssociationClass with optional attribution."""
    from datahub.metadata.schema_classes import MetadataAttributionClass

    tag = TagAssociationClass(tag=make_tag_urn(tag_name))
    if source_urn:
        tag.attribution = MetadataAttributionClass(
            time=0,
            source=source_urn,
            actor="urn:li:corpuser:datahub",
        )
    return tag


def test_add_source_tags_no_graph():
    """Test adding tags without graph (no merge with existing tags)."""
    new_tags = [_make_tag_association("tag1", "urn:li:dataPlatform:source1")]

    workunits = list(
        builder.add_source_tags_to_entity_wu(
            entity_type="dataset",
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,foo,PROD)",
            new_tags=new_tags,
            source_urn="urn:li:dataPlatform:source1",
            graph=None,
        )
    )

    assert len(workunits) == 1
    aspect = workunits[0].metadata.aspect
    assert isinstance(aspect, GlobalTagsClass)
    assert len(aspect.tags) == 1
    assert aspect.tags[0].tag == make_tag_urn("tag1")


def test_add_source_tags_merges_with_different_sources():
    """Test that tags from different sources are preserved."""
    new_tags = [_make_tag_association("source1_tag", "urn:li:dataPlatform:source1")]

    existing_tags = GlobalTagsClass(
        tags=[
            _make_tag_association("source2_tag", "urn:li:dataPlatform:source2"),
            _make_tag_association("manual_tag", None),
        ]
    )

    mock_graph = MagicMock()
    mock_graph.get_aspect.return_value = existing_tags

    workunits = list(
        builder.add_source_tags_to_entity_wu(
            entity_type="dataset",
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,foo,PROD)",
            new_tags=new_tags,
            source_urn="urn:li:dataPlatform:source1",
            graph=mock_graph,
        )
    )

    assert len(workunits) == 1
    aspect = workunits[0].metadata.aspect
    assert isinstance(aspect, GlobalTagsClass)
    assert len(aspect.tags) == 3

    tag_names = {tag.tag for tag in aspect.tags}
    assert make_tag_urn("source1_tag") in tag_names
    assert make_tag_urn("source2_tag") in tag_names
    assert make_tag_urn("manual_tag") in tag_names


def test_add_source_tags_replaces_same_source():
    """Test that tags from the same source are replaced."""
    new_tags = [
        _make_tag_association("new_tag1", "urn:li:dataPlatform:source1"),
        _make_tag_association("new_tag2", "urn:li:dataPlatform:source1"),
    ]

    existing_tags = GlobalTagsClass(
        tags=[
            _make_tag_association("old_tag", "urn:li:dataPlatform:source1"),
            _make_tag_association("other_source_tag", "urn:li:dataPlatform:source2"),
        ]
    )

    mock_graph = MagicMock()
    mock_graph.get_aspect.return_value = existing_tags

    workunits = list(
        builder.add_source_tags_to_entity_wu(
            entity_type="dataset",
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,foo,PROD)",
            new_tags=new_tags,
            source_urn="urn:li:dataPlatform:source1",
            graph=mock_graph,
        )
    )

    assert len(workunits) == 1
    aspect = workunits[0].metadata.aspect
    assert isinstance(aspect, GlobalTagsClass)
    assert len(aspect.tags) == 3

    tag_names = {tag.tag for tag in aspect.tags}
    assert make_tag_urn("new_tag1") in tag_names
    assert make_tag_urn("new_tag2") in tag_names
    assert make_tag_urn("other_source_tag") in tag_names
    assert make_tag_urn("old_tag") not in tag_names


def test_add_source_tags_handles_graph_errors():
    """Test graceful handling when graph fetch fails."""
    new_tags = [_make_tag_association("tag1", "urn:li:dataPlatform:source1")]

    mock_graph = MagicMock()
    mock_graph.get_aspect.side_effect = Exception("Connection failed")

    workunits = list(
        builder.add_source_tags_to_entity_wu(
            entity_type="dataset",
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,foo,PROD)",
            new_tags=new_tags,
            source_urn="urn:li:dataPlatform:source1",
            graph=mock_graph,
        )
    )

    # Should still emit workunit with new tags only
    assert len(workunits) == 1
    aspect = workunits[0].metadata.aspect
    assert isinstance(aspect, GlobalTagsClass)
    assert len(aspect.tags) == 1
    assert aspect.tags[0].tag == make_tag_urn("tag1")


def test_add_source_tags_empty_new_tags():
    """Test that empty new_tags removes all tags from the source."""
    new_tags = []

    existing_tags = GlobalTagsClass(
        tags=[
            _make_tag_association("source1_tag", "urn:li:dataPlatform:source1"),
            _make_tag_association("other_tag", "urn:li:dataPlatform:source2"),
        ]
    )

    mock_graph = MagicMock()
    mock_graph.get_aspect.return_value = existing_tags

    workunits = list(
        builder.add_source_tags_to_entity_wu(
            entity_type="dataset",
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,foo,PROD)",
            new_tags=new_tags,
            source_urn="urn:li:dataPlatform:source1",
            graph=mock_graph,
        )
    )

    assert len(workunits) == 1
    aspect = workunits[0].metadata.aspect
    assert isinstance(aspect, GlobalTagsClass)
    assert len(aspect.tags) == 1
    assert aspect.tags[0].tag == make_tag_urn("other_tag")
