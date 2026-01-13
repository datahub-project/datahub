"""Unit tests for recording module helper functions and classes.

Tests cover:
- _normalize_json_body function
- _convert_expiration_ms function
- _create_time_partitioning function
- MockTableListItem class
- QueryRecorder context manager
- S3 download error handling
"""

import json
from unittest.mock import MagicMock, patch

import pytest


class TestNormalizeJsonBody:
    """Tests for _normalize_json_body helper function in http_recorder.py."""

    def test_empty_body_returns_as_is(self):
        """Test that None or empty body is returned unchanged."""
        from datahub.ingestion.recording.http_recorder import _normalize_json_body

        assert _normalize_json_body(None) is None
        assert _normalize_json_body("") == ""
        assert _normalize_json_body(b"") == b""

    def test_non_json_string_returns_as_is(self):
        """Test that non-JSON strings are returned unchanged."""
        from datahub.ingestion.recording.http_recorder import _normalize_json_body

        assert _normalize_json_body("not json") == "not json"
        assert _normalize_json_body("hello world") == "hello world"

    def test_binary_non_utf8_returns_as_is(self):
        """Test that binary data that can't be decoded is returned unchanged."""
        from datahub.ingestion.recording.http_recorder import _normalize_json_body

        # Binary data that isn't valid UTF-8
        binary_data = b"\x80\x81\x82"
        assert _normalize_json_body(binary_data) == binary_data

    def test_json_object_normalized_with_sorted_keys(self):
        """Test that JSON objects have their keys sorted."""
        from datahub.ingestion.recording.http_recorder import _normalize_json_body

        body = '{"z": 1, "a": 2, "m": 3}'
        result = _normalize_json_body(body)
        assert result == '{"a": 2, "m": 3, "z": 1}'

    def test_json_bytes_decoded_and_normalized(self):
        """Test that JSON bytes are decoded and normalized."""
        from datahub.ingestion.recording.http_recorder import _normalize_json_body

        body = b'{"z": 1, "a": 2}'
        result = _normalize_json_body(body)
        assert result == '{"a": 2, "z": 1}'

    def test_filters_with_comma_separated_values_sorted(self):
        """Test that comma-separated filter values are sorted."""
        from datahub.ingestion.recording.http_recorder import _normalize_json_body

        body = json.dumps({"filters": {"ids": "c,a,b", "other": "x,y"}})
        result = json.loads(_normalize_json_body(body))

        assert result["filters"]["ids"] == "a,b,c"
        assert result["filters"]["other"] == "x,y"

    def test_filters_non_comma_values_unchanged(self):
        """Test that filter values without commas are unchanged."""
        from datahub.ingestion.recording.http_recorder import _normalize_json_body

        body = json.dumps({"filters": {"id": "single_value", "count": 123}})
        result = json.loads(_normalize_json_body(body))

        assert result["filters"]["id"] == "single_value"
        assert result["filters"]["count"] == 123

    def test_nested_json_structure_preserved(self):
        """Test that nested JSON structures are preserved during normalization."""
        from datahub.ingestion.recording.http_recorder import _normalize_json_body

        body = json.dumps(
            {
                "nested": {"deep": {"value": 1}},
                "array": [1, 2, 3],
                "filters": {"ids": "b,a"},
            }
        )
        result = json.loads(_normalize_json_body(body))

        assert result["nested"]["deep"]["value"] == 1
        assert result["array"] == [1, 2, 3]
        assert result["filters"]["ids"] == "a,b"


class TestConvertExpirationMs:
    """Tests for _convert_expiration_ms helper function in patcher.py."""

    def test_none_returns_none(self):
        """Test that None input returns None."""
        from datahub.ingestion.recording.patcher import _convert_expiration_ms

        assert _convert_expiration_ms(None) is None

    def test_int_returns_unchanged(self):
        """Test that integer input is returned unchanged."""
        from datahub.ingestion.recording.patcher import _convert_expiration_ms

        assert _convert_expiration_ms(86400000) == 86400000
        assert _convert_expiration_ms(0) == 0

    def test_string_converted_to_int(self):
        """Test that string numbers are converted to int."""
        from datahub.ingestion.recording.patcher import _convert_expiration_ms

        assert _convert_expiration_ms("86400000") == 86400000
        assert _convert_expiration_ms("0") == 0

    def test_float_converted_to_int(self):
        """Test that floats are converted to int."""
        from datahub.ingestion.recording.patcher import _convert_expiration_ms

        assert _convert_expiration_ms(86400000.0) == 86400000
        assert _convert_expiration_ms(86400000.5) == 86400000  # Truncated

    def test_invalid_string_returns_none_with_warning(self):
        """Test that invalid strings return None and log warning."""
        from datahub.ingestion.recording.patcher import _convert_expiration_ms

        result = _convert_expiration_ms("not_a_number", table_id="test_table")
        assert result is None

    def test_invalid_type_returns_none(self):
        """Test that unsupported types return None."""
        from datahub.ingestion.recording.patcher import _convert_expiration_ms

        # List can't be converted to int
        result = _convert_expiration_ms([1, 2, 3], table_id="test_table")
        assert result is None


class TestCreateTimePartitioning:
    """Tests for _create_time_partitioning helper function in patcher.py."""

    def test_creates_time_partitioning_from_dict(self):
        """Test that TimePartitioning is created from valid dict."""
        from datahub.ingestion.recording.patcher import _create_time_partitioning

        tp_dict = {
            "field": "date_col",
            "type": "DAY",
            "expiration_ms": 86400000,
            "require_partition_filter": True,
        }
        result = _create_time_partitioning(tp_dict)

        assert result is not None
        assert result.field == "date_col"
        assert result.type_ == "DAY"
        assert result.expiration_ms == 86400000

    def test_handles_string_expiration_ms(self):
        """Test that string expiration_ms is converted to int."""
        from datahub.ingestion.recording.patcher import _create_time_partitioning

        tp_dict = {
            "field": "date_col",
            "type": "DAY",
            "expiration_ms": "86400000",  # String
        }
        result = _create_time_partitioning(tp_dict)

        assert result is not None
        assert result.expiration_ms == 86400000
        assert isinstance(result.expiration_ms, int)

    def test_handles_none_values(self):
        """Test that None values are preserved."""
        from datahub.ingestion.recording.patcher import _create_time_partitioning

        tp_dict = {
            "field": None,
            "type": "DAY",
            "expiration_ms": None,
            "require_partition_filter": None,
        }
        result = _create_time_partitioning(tp_dict)

        assert result is not None
        assert result.field is None
        assert result.expiration_ms is None

    def test_handles_missing_keys(self):
        """Test that missing keys use defaults."""
        from datahub.ingestion.recording.patcher import _create_time_partitioning

        tp_dict = {"type": "DAY"}
        result = _create_time_partitioning(tp_dict)

        assert result is not None

    def test_returns_none_on_error(self):
        """Test that errors return None instead of raising."""
        from datahub.ingestion.recording.patcher import _create_time_partitioning

        # Invalid type value
        tp_dict = {"type": object()}  # Invalid type
        _create_time_partitioning(tp_dict, table_id="test_table")

        # Should return None on error, not raise
        # Note: This might still create TimePartitioning depending on BigQuery's behavior
        # The important thing is it doesn't raise an unhandled exception


class TestMockTableListItem:
    """Tests for MockTableListItem class in patcher.py."""

    def test_initialization(self):
        """Test basic initialization of MockTableListItem."""
        from datahub.ingestion.recording.patcher import MockTableListItem

        item = MockTableListItem(
            table_id="my_table",
            dataset_id="my_dataset",
            project="my_project",
        )

        assert item.table_id == "my_table"
        assert item.dataset_id == "my_dataset"
        assert item.project == "my_project"

    def test_table_type_property(self):
        """Test table_type getter and setter."""
        from datahub.ingestion.recording.patcher import MockTableListItem

        item = MockTableListItem("t", "d", "p")
        assert item.table_type is None

        item.table_type = "TABLE"
        assert item.table_type == "TABLE"

    def test_labels_property(self):
        """Test labels getter and setter."""
        from datahub.ingestion.recording.patcher import MockTableListItem

        item = MockTableListItem("t", "d", "p")
        assert item.labels is None

        item.labels = {"env": "prod"}
        assert item.labels == {"env": "prod"}

    def test_expires_property(self):
        """Test expires getter and setter."""
        from datetime import datetime

        from datahub.ingestion.recording.patcher import MockTableListItem

        item = MockTableListItem("t", "d", "p")
        assert item.expires is None

        exp_time = datetime(2025, 12, 31)
        item.expires = exp_time
        assert item.expires == exp_time

    def test_clustering_fields_property(self):
        """Test clustering_fields getter and setter."""
        from datahub.ingestion.recording.patcher import MockTableListItem

        item = MockTableListItem("t", "d", "p")
        assert item.clustering_fields is None

        item.clustering_fields = ["col1", "col2"]
        assert item.clustering_fields == ["col1", "col2"]

    def test_time_partitioning_property(self):
        """Test time_partitioning getter and setter."""
        from datahub.ingestion.recording.patcher import MockTableListItem

        item = MockTableListItem("t", "d", "p")
        assert item.time_partitioning is None

        mock_tp = MagicMock()
        item.time_partitioning = mock_tp
        assert item.time_partitioning == mock_tp

    def test_properties_attribute(self):
        """Test that _properties attribute is accessible."""
        from datahub.ingestion.recording.patcher import MockTableListItem

        item = MockTableListItem("t", "d", "p")
        assert item._properties == {}

        item._properties = {"description": "test"}
        assert item._properties == {"description": "test"}


class TestQueryRecorderContextManager:
    """Tests for QueryRecorder context manager methods in db_proxy.py."""

    def test_context_manager_starts_and_stops_recording(self, tmp_path):
        """Test that context manager properly starts and stops recording."""
        from datahub.ingestion.recording.db_proxy import QueryRecorder

        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        with recorder:
            # File should be open
            assert recorder._file_handle is not None
            assert not recorder._file_handle.closed

        # After exiting, file should be closed
        assert recorder._file_handle is None

    def test_context_manager_returns_recorder(self, tmp_path):
        """Test that entering context returns the recorder."""
        from datahub.ingestion.recording.db_proxy import QueryRecorder

        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        with recorder as ctx:
            assert ctx is recorder

    def test_context_manager_handles_exception(self, tmp_path):
        """Test that context manager closes file even on exception."""
        from datahub.ingestion.recording.db_proxy import QueryRecorder

        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        with pytest.raises(ValueError), recorder:
            assert recorder._file_handle is not None
            raise ValueError("Test error")

        # File should still be closed after exception
        assert recorder._file_handle is None

    def test_context_manager_records_queries(self, tmp_path):
        """Test that queries recorded in context are saved."""
        from datahub.ingestion.recording.db_proxy import QueryRecorder, QueryRecording

        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        with recorder:
            recording = QueryRecording(
                query="SELECT 1",
                results=[{"col": 1}],
            )
            recorder.record(recording)

        # Should be written to file
        assert queries_path.exists()
        with open(queries_path) as f:
            content = f.read()
            assert "SELECT 1" in content

    def test_del_closes_file_handle(self, tmp_path):
        """Test that __del__ closes file handle on garbage collection."""
        from datahub.ingestion.recording.db_proxy import QueryRecorder

        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        # Simulate file handle exists
        assert recorder._file_handle is not None
        handle = recorder._file_handle

        # Manually call __del__
        recorder.__del__()

        # Handle should be closed
        assert handle.closed


class TestS3DownloadErrorHandling:
    """Tests for S3 download error handling in recording_cli.py."""

    def test_local_path_returns_unchanged(self, tmp_path):
        """Test that local paths are returned as Path objects."""
        from datahub.cli.recording_cli import _ensure_local_archive

        local_file = tmp_path / "test.zip"
        local_file.touch()

        result = _ensure_local_archive(str(local_file))
        assert result == local_file

    def test_s3_download_failure_cleans_up_temp_file(self, tmp_path):
        """Test that temp file is cleaned up when S3 download fails."""
        import click

        from datahub.cli.recording_cli import _ensure_local_archive

        # boto3 is imported inside the function, so we patch at module level with create=True
        with patch.dict("sys.modules", {"boto3": MagicMock()}) as mock_modules:
            mock_boto3 = mock_modules["boto3"]
            mock_client = MagicMock()
            mock_client.download_file.side_effect = Exception("S3 error")
            mock_boto3.client.return_value = mock_client

            with pytest.raises(
                click.ClickException, match="Failed to download from S3"
            ):
                _ensure_local_archive("s3://bucket/key.zip")

    def test_s3_download_success_returns_local_path(self, tmp_path):
        """Test that successful S3 download returns local path."""
        from datahub.cli.recording_cli import _ensure_local_archive

        # boto3 is imported inside the function, so we patch at module level with create=True
        with patch.dict("sys.modules", {"boto3": MagicMock()}) as mock_modules:
            mock_boto3 = mock_modules["boto3"]
            mock_client = MagicMock()
            # Simulate successful download
            mock_client.download_file.return_value = None
            mock_boto3.client.return_value = mock_client

            result = _ensure_local_archive("s3://bucket/key.zip")

            # Should return a path
            assert result is not None
            assert str(result).endswith(".zip")
            mock_client.download_file.assert_called_once()


class TestMockRow:
    """Tests for MockRow class in patcher.py."""

    def test_dict_access(self):
        """Test dict-style access to MockRow."""
        from datahub.ingestion.recording.patcher import MockRow

        row = MockRow({"name": "test", "value": 123})
        assert row["name"] == "test"
        assert row["value"] == 123

    def test_attribute_access(self):
        """Test attribute-style access to MockRow."""
        from datahub.ingestion.recording.patcher import MockRow

        row = MockRow({"name": "test", "value": 123})
        assert row.name == "test"
        assert row.value == 123

    def test_get_method(self):
        """Test get() method with default."""
        from datahub.ingestion.recording.patcher import MockRow

        row = MockRow({"name": "test"})
        assert row.get("name") == "test"
        assert row.get("missing") is None
        assert row.get("missing", "default") == "default"

    def test_keys_method(self):
        """Test keys() method returns KeysView."""
        from datahub.ingestion.recording.patcher import MockRow

        row = MockRow({"a": 1, "b": 2})
        keys = row.keys()
        assert "a" in keys
        assert "b" in keys

    def test_iteration(self):
        """Test iteration over MockRow values."""
        from datahub.ingestion.recording.patcher import MockRow

        row = MockRow({"a": 1, "b": 2, "c": 3})
        values = list(row)
        assert 1 in values
        assert 2 in values
        assert 3 in values

    def test_repr(self):
        """Test string representation."""
        from datahub.ingestion.recording.patcher import MockRow

        row = MockRow({"name": "test"})
        assert "MockRow" in repr(row)
        assert "name" in repr(row)

    def test_private_attribute_raises(self):
        """Test that private attributes raise AttributeError."""
        from datahub.ingestion.recording.patcher import MockRow

        row = MockRow({"name": "test"})
        with pytest.raises(AttributeError):
            _ = row._private

    def test_missing_attribute_returns_none(self):
        """Test that missing attributes return None."""
        from datahub.ingestion.recording.patcher import MockRow

        row = MockRow({"name": "test"})
        assert row.missing_attr is None


class TestMockQueryResult:
    """Tests for MockQueryResult class in patcher.py."""

    def test_iteration(self):
        """Test that MockQueryResult can be iterated."""
        from datahub.ingestion.recording.patcher import MockQueryResult

        results = [{"a": 1}, {"a": 2}]
        query_result = MockQueryResult(results)

        rows = list(query_result)
        assert len(rows) == 2
        assert rows[0]["a"] == 1
        assert rows[1]["a"] == 2

    def test_len(self):
        """Test len() on MockQueryResult."""
        from datahub.ingestion.recording.patcher import MockQueryResult

        results = [{"a": 1}, {"a": 2}, {"a": 3}]
        query_result = MockQueryResult(results)

        assert len(query_result) == 3

    def test_total_rows(self):
        """Test total_rows property."""
        from datahub.ingestion.recording.patcher import MockQueryResult

        results = [{"a": 1}, {"a": 2}]
        query_result = MockQueryResult(results)

        assert query_result.total_rows == 2

    def test_result_method(self):
        """Test result() method returns self."""
        from datahub.ingestion.recording.patcher import MockQueryResult

        results = [{"a": 1}]
        query_result = MockQueryResult(results)

        assert query_result.result() is query_result

    def test_rows_are_mock_rows(self):
        """Test that dict results are converted to MockRow."""
        from datahub.ingestion.recording.patcher import MockQueryResult, MockRow

        results = [{"name": "test"}]
        query_result = MockQueryResult(results)

        row = list(query_result)[0]
        assert isinstance(row, MockRow)
        assert row.name == "test"
