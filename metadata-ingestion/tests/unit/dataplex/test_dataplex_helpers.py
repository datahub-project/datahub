"""Unit tests for Dataplex helper functions and utilities."""

import json
from unittest.mock import Mock

from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntryDataTuple,
    make_audit_stamp,
    make_bigquery_dataset_container_key,
    serialize_field_value,
)


class TestEntryDataTuple:
    """Test EntryDataTuple dataclass."""

    def test_create_entry_data_tuple(self):
        """Test creating EntryDataTuple."""
        entry_data = EntryDataTuple(
            source_platform="bigquery",
            dataset_id="test-project.test-dataset.test-table",
            location="us",
            project_id="test-project",
            fqn="bigquery:test-project.test-dataset.test-table",
        )

        assert entry_data.source_platform == "bigquery"
        assert entry_data.dataset_id == "test-project.test-dataset.test-table"
        assert entry_data.location == "us"
        assert entry_data.project_id == "test-project"
        assert entry_data.fqn == "bigquery:test-project.test-dataset.test-table"

    def test_entry_data_tuple_hashable(self):
        """Test that EntryDataTuple is hashable (frozen)."""
        entry_data1 = EntryDataTuple(
            source_platform="bigquery",
            dataset_id="project.dataset.table",
            location="us",
            project_id="project",
            fqn="bigquery:project.dataset.table",
        )

        entry_data2 = EntryDataTuple(
            source_platform="bigquery",
            dataset_id="project.dataset.table",
            location="us",
            project_id="project",
            fqn="bigquery:project.dataset.table",
        )

        # Should be able to add to set
        entry_set = {entry_data1, entry_data2}
        assert len(entry_set) == 1


class TestMakeBigQueryDatasetContainerKey:
    """Test make_bigquery_dataset_container_key function."""

    def test_make_container_key(self):
        """Test creating BigQuery dataset container key."""
        key = make_bigquery_dataset_container_key(
            project_id="test-project",
            dataset_id="test_dataset",
            platform="bigquery",
            env="PROD",
        )

        assert key.project_id == "test-project"
        assert key.dataset_id == "test_dataset"
        assert key.platform == "bigquery"
        assert key.env == "PROD"
        assert key.backcompat_env_as_instance is True


class TestMakeAuditStamp:
    """Test make_audit_stamp function."""

    def test_make_audit_stamp_with_timestamp(self):
        """Test creating audit stamp with valid timestamp."""
        mock_timestamp = Mock()
        mock_timestamp.timestamp.return_value = 1234567890.0

        result = make_audit_stamp(mock_timestamp)

        assert result is not None
        assert result["time"] == 1234567890000
        assert result["actor"] == "urn:li:corpuser:dataplex"

    def test_make_audit_stamp_none(self):
        """Test creating audit stamp with None timestamp."""
        result = make_audit_stamp(None)
        assert result is None


class TestSerializeFieldValue:
    """Test serialize_field_value function."""

    def test_serialize_none(self):
        """Test serializing None value."""
        result = serialize_field_value(None)
        assert result == ""

    def test_serialize_primitive_string(self):
        """Test serializing primitive string."""
        result = serialize_field_value("test_value")
        assert result == "test_value"

    def test_serialize_primitive_int(self):
        """Test serializing primitive int."""
        result = serialize_field_value(42)
        assert result == "42"

    def test_serialize_primitive_float(self):
        """Test serializing primitive float."""
        result = serialize_field_value(3.14)
        assert result == "3.14"

    def test_serialize_primitive_bool(self):
        """Test serializing primitive bool."""
        result = serialize_field_value(True)
        assert result == "True"

    def test_serialize_list_primitives(self):
        """Test serializing list of primitives."""
        result = serialize_field_value([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_serialize_regular_dict(self):
        """Test serializing regular dict."""
        result = serialize_field_value({"key": "value"})
        parsed = json.loads(result)
        assert parsed == {"key": "value"}

    def test_serialize_repeated_composite(self):
        """Test serializing RepeatedComposite proto object."""
        mock_item = Mock()
        mock_item.__class__.__name__ = "MapComposite"
        mock_item.items = Mock(return_value=[("field1", "value1")])

        mock_repeated = Mock()
        mock_repeated.__class__.__name__ = "RepeatedComposite"
        mock_repeated.__iter__ = Mock(return_value=iter([mock_item]))

        result = serialize_field_value(mock_repeated)
        assert isinstance(result, str)

    def test_serialize_map_composite(self):
        """Test serializing MapComposite proto object."""
        mock_map = Mock()
        mock_map.__class__.__name__ = "MapComposite"
        mock_map.items = Mock(return_value=[("key1", "value1")])

        result = serialize_field_value(mock_map)
        assert isinstance(result, str)
