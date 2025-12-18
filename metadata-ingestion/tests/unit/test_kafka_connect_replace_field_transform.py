"""
Tests for ReplaceField SMT (Single Message Transform) support in Kafka Connect lineage extraction.

This module tests the implementation of ReplaceField transformations that can:
- Filter fields using include/exclude
- Rename fields using from:to format
- Apply multiple transformations in sequence

Reference: https://docs.confluent.io/platform/current/connect/transforms/replacefield.html
"""

import pytest

from datahub.ingestion.source.kafka_connect.common import (
    BaseConnector,
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)


@pytest.fixture
def config():
    """Create test configuration."""
    return KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )


@pytest.fixture
def report():
    """Create test report."""
    return KafkaConnectSourceReport()


def test_no_transforms():
    """Test that columns pass through unchanged when no transforms are configured."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["id", "name", "email", "created_at"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    # All columns should map 1:1 when no transforms present
    assert column_mapping == {
        "id": "id",
        "name": "name",
        "email": "email",
        "created_at": "created_at",
    }


def test_exclude_single_field():
    """Test excluding a single field from the output."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "dropSensitive",
            "transforms.dropSensitive.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.dropSensitive.exclude": "password",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["id", "username", "password", "email"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    assert column_mapping == {
        "id": "id",
        "username": "username",
        "password": None,  # Excluded field mapped to None
        "email": "email",
    }


def test_exclude_multiple_fields():
    """Test excluding multiple fields from the output."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "dropSensitive",
            "transforms.dropSensitive.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.dropSensitive.exclude": "password,ssn,credit_card",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["id", "name", "password", "ssn", "email", "credit_card"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    assert column_mapping == {
        "id": "id",
        "name": "name",
        "password": None,
        "ssn": None,
        "email": "email",
        "credit_card": None,
    }


def test_include_only_specified_fields():
    """Test keeping only specified fields (all others dropped)."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "keepOnly",
            "transforms.keepOnly.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.keepOnly.include": "id,name,email",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["id", "name", "email", "password", "ssn", "internal_notes"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    assert column_mapping == {
        "id": "id",
        "name": "name",
        "email": "email",
        "password": None,  # Not in include list
        "ssn": None,  # Not in include list
        "internal_notes": None,  # Not in include list
    }


def test_rename_single_field():
    """Test renaming a single field."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "renameField",
            "transforms.renameField.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.renameField.renames": "user_id:id",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["user_id", "name", "email"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    assert column_mapping == {
        "user_id": "id",  # Renamed
        "name": "name",
        "email": "email",
    }


def test_rename_multiple_fields():
    """Test renaming multiple fields."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "renameFields",
            "transforms.renameFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.renameFields.renames": "user_id:id,user_name:name,user_email:email",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["user_id", "user_name", "user_email", "created_at"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    assert column_mapping == {
        "user_id": "id",
        "user_name": "name",
        "user_email": "email",
        "created_at": "created_at",
    }


def test_exclude_and_rename_combined():
    """Test combining exclude and rename operations."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "cleanupFields",
            "transforms.cleanupFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.cleanupFields.exclude": "password,ssn",
            "transforms.cleanupFields.renames": "user_id:id,user_name:name",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["user_id", "user_name", "email", "password", "ssn"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    assert column_mapping == {
        "user_id": "id",  # Renamed
        "user_name": "name",  # Renamed
        "email": "email",  # Unchanged
        "password": None,  # Excluded
        "ssn": None,  # Excluded
    }


def test_multiple_transforms_in_sequence():
    """Test applying multiple ReplaceField transforms in sequence."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "first,second",
            "transforms.first.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.first.renames": "old_id:user_id",
            "transforms.second.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.second.renames": "user_id:id",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["old_id", "name"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    # First transform: old_id -> user_id
    # Second transform: user_id -> id
    assert column_mapping == {
        "old_id": "id",  # Chained renames applied
        "name": "name",
    }


def test_non_replacefield_transforms_ignored():
    """Test that non-ReplaceField transforms are ignored."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "mask,rename",
            "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
            "transforms.mask.fields": "password",
            "transforms.rename.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.rename.renames": "user_id:id",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["user_id", "name", "password"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    # Only the ReplaceField transform should be applied
    assert column_mapping == {
        "user_id": "id",
        "name": "name",
        "password": "password",  # MaskField ignored, password unchanged
    }


def test_replacefield_key_transform_ignored():
    """Test that ReplaceField$Key transforms are ignored (we only support $Value)."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "keyTransform,valueTransform",
            "transforms.keyTransform.type": "org.apache.kafka.connect.transforms.ReplaceField$Key",
            "transforms.keyTransform.exclude": "internal_key",
            "transforms.valueTransform.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.valueTransform.exclude": "password",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["id", "password", "internal_key"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    # Only Value transform applied, Key transform ignored
    assert column_mapping == {
        "id": "id",
        "password": None,  # Excluded by Value transform
        "internal_key": "internal_key",  # Key transform ignored
    }


def test_empty_transform_config():
    """Test handling of empty transform configurations."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "empty",
            "transforms.empty.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            # No include, exclude, or renames specified
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["id", "name", "email"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    # Empty config should result in 1:1 mapping
    assert column_mapping == {
        "id": "id",
        "name": "name",
        "email": "email",
    }


def test_whitespace_in_field_names():
    """Test that whitespace in configuration is handled correctly."""
    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "cleanup",
            "transforms.cleanup.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.cleanup.exclude": " password , ssn ",  # Extra whitespace
            "transforms.cleanup.renames": " user_id : id , user_name : name ",  # Extra whitespace
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    source_columns = ["user_id", "user_name", "password", "ssn", "email"]
    column_mapping = connector._apply_replace_field_transform(source_columns)

    # Whitespace should be trimmed
    assert column_mapping == {
        "user_id": "id",
        "user_name": "name",
        "password": None,
        "ssn": None,
        "email": "email",
    }


def test_integration_with_fine_grained_lineage():
    """
    Test that ReplaceField transforms are properly integrated with fine-grained lineage extraction.

    This is an integration test that verifies the transform is applied when extracting column-level lineage.
    """
    from unittest.mock import Mock

    from datahub.emitter.mce_builder import schema_field_urn_to_key

    manifest = ConnectorManifest(
        name="test-connector",
        type="source",
        config={
            "connector.class": "TestConnector",
            "transforms": "cleanup",
            "transforms.cleanup.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.cleanup.exclude": "password",
            "transforms.cleanup.renames": "user_id:id",
        },
        tasks=[],
    )

    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083",
        cluster_name="test",
        use_schema_resolver=True,
        schema_resolver_finegrained_lineage=True,
    )
    report = KafkaConnectSourceReport()
    connector = BaseConnector(manifest, config, report)

    # Mock schema resolver
    mock_resolver = Mock()
    mock_resolver.resolve_table.return_value = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,PROD)",
        {  # SchemaInfo is Dict[str, str] mapping column names to types
            "user_id": "INT",
            "name": "VARCHAR",
            "email": "VARCHAR",
            "password": "VARCHAR",
        },
    )
    connector.schema_resolver = mock_resolver

    # Extract fine-grained lineage
    lineages = connector._extract_fine_grained_lineage(
        source_dataset="public.users",
        source_platform="postgres",
        target_dataset="users_topic",
        target_platform="kafka",
    )

    # Verify lineages were generated
    assert lineages is not None
    assert len(lineages) == 3  # 4 columns - 1 excluded (password) = 3

    # Verify password was excluded and fields were renamed
    downstream_fields = []
    for lineage in lineages:
        for downstream_urn in lineage["downstreams"]:
            # Use proper URN parser to extract field path
            key = schema_field_urn_to_key(downstream_urn)
            if key:
                downstream_fields.append(key.fieldPath)

    assert "password" not in downstream_fields
    assert "id" in downstream_fields  # user_id renamed to id
    assert "name" in downstream_fields
    assert "email" in downstream_fields
