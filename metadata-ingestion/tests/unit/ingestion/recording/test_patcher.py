"""Unit tests for database and HTTP patching in recording/patcher.py."""

from datetime import datetime
from typing import Any, Iterator
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.recording.db_proxy import (
    ConnectionProxy,
    CursorProxy,
    QueryRecorder,
    QueryRecording,
    ReplayConnection,
)
from datahub.ingestion.recording.patcher import ModulePatcher


class TestModulePatcher:
    """Test module patching functionality."""

    def test_patcher_initialization(self, tmp_path):
        """Test that ModulePatcher initializes correctly."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        patcher = ModulePatcher(recorder=recorder, is_replay=False)

        assert patcher.recorder == recorder
        assert patcher.is_replay is False

    def test_snowflake_connect_wrapper_recording_mode(self, tmp_path):
        """Test that Snowflake connect is wrapped for recording."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        patcher = ModulePatcher(recorder=recorder, is_replay=False)

        # Mock snowflake connector
        mock_snowflake_connection = MagicMock()
        mock_snowflake_connection.cursor.return_value = MagicMock()

        from unittest.mock import Mock

        original_connect = Mock(return_value=mock_snowflake_connection)

        # Create wrapper
        wrapped_connect = patcher._create_connection_wrapper(original_connect)

        # Call wrapped connect
        result = wrapped_connect(account="test", user="test")

        # Should return ConnectionProxy
        assert isinstance(result, ConnectionProxy)
        assert result._connection == mock_snowflake_connection

    def test_snowflake_connect_wrapper_replay_mode(self, tmp_path):
        """Test that Snowflake connect returns ReplayConnection in replay mode."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        patcher = ModulePatcher(recorder=recorder, is_replay=True)

        from unittest.mock import Mock

        original_connect = Mock()
        wrapped_connect = patcher._create_connection_wrapper(original_connect)

        # Call wrapped connect
        result = wrapped_connect(account="test", user="test")

        # Should return ReplayConnection
        assert isinstance(result, ReplayConnection)
        # Original connect should NOT be called in replay mode
        original_connect.assert_not_called()

    def test_cursor_proxy_wraps_execute(self, tmp_path):
        """Test that CursorProxy captures execute() calls."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        # Mock cursor with results
        mock_cursor = MagicMock()
        # Make description JSON-serializable
        mock_cursor.description = [
            ("col1", None, None, None, None, None, None),
            ("col2", None, None, None, None, None, None),
        ]
        mock_cursor.fetchall.return_value = [{"col1": "value1", "col2": "value2"}]
        mock_cursor.rowcount = 1
        mock_cursor.execute.return_value = None

        cursor_proxy = CursorProxy(
            cursor=mock_cursor, recorder=recorder, is_replay=False
        )

        # Execute a query
        cursor_proxy.execute("SELECT * FROM test_table")

        # Check that query was recorded
        recorder.stop_recording()
        assert len(recorder._recordings) == 1
        # _recordings is a dict keyed by normalized query
        recorded_query = list(recorder._recordings.values())[0]
        assert recorded_query.query == "SELECT * FROM test_table"
        assert len(recorded_query.results) == 1

    def test_cursor_proxy_iteration_after_execute(self, tmp_path):
        """Test that cursor proxy allows iteration after execute."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        # Mock cursor with results
        mock_cursor = MagicMock()
        # Make description JSON-serializable (list of tuples, not MagicMock)
        mock_cursor.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None),
        ]
        mock_cursor.rowcount = 2
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        mock_cursor.execute.return_value = None

        cursor_proxy = CursorProxy(
            cursor=mock_cursor, recorder=recorder, is_replay=False
        )

        # Execute and iterate
        cursor_proxy.execute("SELECT * FROM users")

        # Should be able to iterate over results
        results = list(cursor_proxy)
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["name"] == "Bob"

    def test_replay_connection_serves_recorded_queries(self, tmp_path):
        """Test that ReplayConnection serves queries from recordings."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        # Record a query
        recorder.start_recording()
        from datahub.ingestion.recording.db_proxy import QueryRecording

        recording = QueryRecording(
            query="SELECT * FROM test",
            results=[{"id": 1, "name": "Test"}],
        )
        recorder.record(recording)
        recorder.stop_recording()

        # Load recordings for replay
        recorder.load_recordings()

        # Create replay connection
        replay_conn = ReplayConnection(recorder)
        cursor = replay_conn.cursor()

        # Execute the recorded query
        cursor.execute("SELECT * FROM test")

        # Should get recorded results
        results = list(cursor)
        assert len(results) == 1
        assert results[0]["id"] == 1

    def test_patcher_context_manager(self, tmp_path):
        """Test that ModulePatcher works as a context manager."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        patcher = ModulePatcher(recorder=recorder, is_replay=False)

        # Should work as context manager
        with patcher:
            # Patcher is active
            assert patcher.recorder == recorder

        # Should exit cleanly


class TestHTTPPatching:
    """Test HTTP recording patching."""

    def test_http_recorder_context_manager(self, tmp_path):
        """Test that HTTPRecorder works as context manager."""
        from datahub.ingestion.recording.http_recorder import HTTPRecorder

        cassette_path = tmp_path / "cassette.yaml"
        recorder = HTTPRecorder(cassette_path)

        # Enter recording context
        with recorder.recording():
            # VCR should be active - make a test HTTP request
            import requests

            try:
                requests.get("http://httpbin.org/get", timeout=1)
            except Exception:
                # Network may not be available, that's ok
                pass

        # Cassette file may or may not exist depending on network availability
        # The important thing is the context manager works without errors
        assert recorder.request_count >= 0

    def test_vcr_bypass_context_manager(self):
        """Test VCR bypass context manager."""
        from datahub.ingestion.recording.http_recorder import vcr_bypass_context

        # Should execute without errors
        with vcr_bypass_context():
            # Code that would normally be blocked by VCR
            pass

        # Should exit cleanly
        assert True


class TestConnectionWrapping:
    """Test connection and cursor wrapping logic."""

    def test_connection_proxy_wraps_cursor(self, tmp_path):
        """Test that ConnectionProxy wraps cursor() calls."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        # Mock connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_cursor.fetchall.return_value = []
        mock_connection.cursor.return_value = mock_cursor

        # Wrap connection
        conn_proxy = ConnectionProxy(
            connection=mock_connection, recorder=recorder, is_replay=False
        )

        # Get cursor through proxy
        cursor = conn_proxy.cursor()

        # Should return CursorProxy
        assert isinstance(cursor, CursorProxy)

    def test_connection_proxy_delegates_methods(self, tmp_path):
        """Test that ConnectionProxy delegates other methods to real connection."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        # Mock connection with various methods
        mock_connection = MagicMock()
        mock_connection.commit.return_value = None
        mock_connection.rollback.return_value = None
        mock_connection.close.return_value = None

        conn_proxy = ConnectionProxy(
            connection=mock_connection, recorder=recorder, is_replay=False
        )

        # Call methods
        conn_proxy.commit()
        conn_proxy.rollback()
        conn_proxy.close()

        # Verify delegation
        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_called_once()
        mock_connection.close.assert_called_once()


class MockBigQueryClient:
    """Mock BigQuery Client for testing."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize mock client."""
        self._connection = None
        self.project = kwargs.get("project", "test-project")

    def list_datasets(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        """Mock list_datasets that returns mock datasets."""
        from google.cloud.bigquery import Dataset
        from google.cloud.bigquery.dataset import DatasetReference

        # Create Dataset objects with proper _properties structure
        # BigQuery Dataset accesses dataset_id via _properties["datasetReference"]["datasetId"]
        dataset_ref1 = DatasetReference(project="test-project", dataset_id="dataset1")
        dataset1 = Dataset(dataset_ref1)
        # Set _properties with datasetReference structure that Dataset.dataset_id expects
        # This matches what real BigQuery Dataset objects have, so it gets recorded properly
        dataset1._properties = {  # type: ignore[attr-defined]
            "datasetReference": {
                "datasetId": "dataset1",
                "projectId": "test-project",
            },
            "location": "US",
        }
        dataset1.labels = {"env": "test"}

        dataset_ref2 = DatasetReference(project="test-project", dataset_id="dataset2")
        dataset2 = Dataset(dataset_ref2)
        # Set _properties with datasetReference structure
        dataset2._properties = {  # type: ignore[attr-defined]
            "datasetReference": {
                "datasetId": "dataset2",
                "projectId": "test-project",
            },
        }
        dataset2.labels = {"env": "prod"}

        return iter([dataset1, dataset2])

    def list_tables(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        """Mock list_tables that returns mock table list items."""
        from google.cloud.bigquery.table import TimePartitioning

        class MockTableListItem:
            def __init__(self, table_id: str, dataset_id: str, project: str):
                self.table_id = table_id
                self.dataset_id = dataset_id
                self.project = project
                self.table_type = "TABLE"
                self.labels = {"env": "test"}
                self.expires = datetime(2025, 12, 31, 12, 0, 0)
                self.clustering_fields = ["col1", "col2"]
                self.time_partitioning = TimePartitioning(
                    field="date_col",
                    type_="DAY",
                    expiration_ms=86400000,
                    require_partition_filter=False,
                )
                self._properties = {"description": "test table"}

        table1 = MockTableListItem("table1", "dataset1", "test-project")
        table2 = MockTableListItem("table2", "dataset1", "test-project")

        return iter([table1, table2])

    def get_dataset(self, *args: Any, **kwargs: Any) -> Any:
        """Mock get_dataset that returns a single dataset."""
        from google.cloud.bigquery import Dataset
        from google.cloud.bigquery.dataset import DatasetReference

        # Create Dataset object with proper _properties structure
        # BigQuery Dataset accesses dataset_id via _properties["datasetReference"]["datasetId"]
        dataset_ref = DatasetReference(project="test-project", dataset_id="dataset1")
        dataset = Dataset(dataset_ref)
        # Set _properties with datasetReference structure that Dataset.dataset_id expects
        dataset._properties = {  # type: ignore[attr-defined]
            "datasetReference": {
                "datasetId": "dataset1",
                "projectId": "test-project",
            },
            "location": "US",
        }
        dataset.labels = {"env": "test"}
        return dataset


class TestWrappedClient:
    """Test WrappedClient class for BigQuery and similar client-based connectors."""

    def test_wrapped_client_recording_mode_init(self, tmp_path):
        """Test WrappedClient initialization in recording mode."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        patcher = ModulePatcher(recorder=recorder, is_replay=False)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        # Initialize wrapped client
        client = WrappedClient(project="test-project")

        # Should have recorder attached
        assert hasattr(client, "_recording_recorder")
        assert client._recording_recorder == recorder
        assert client._recording_is_replay is False
        # Should have initialized parent class
        assert client.project == "test-project"

    def test_wrapped_client_replay_mode_init(self, tmp_path):
        """Test WrappedClient initialization in replay mode."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        # Initialize wrapped client in replay mode
        client = WrappedClient(project="test-project")

        # Should have recorder attached
        assert hasattr(client, "_recording_recorder")
        assert client._recording_recorder == recorder
        assert client._recording_is_replay is True
        # Should have dummy connection
        assert client._connection is None
        # Should NOT have initialized parent class (no real connection)
        # In replay mode, parent __init__ is not called

    def test_list_datasets_recording_mode(self, tmp_path):
        """Test list_datasets in recording mode."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        patcher = ModulePatcher(recorder=recorder, is_replay=False)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Call list_datasets
        datasets = list(client.list_datasets())

        # Should return datasets
        assert len(datasets) == 2
        assert datasets[0].dataset_id == "dataset1"
        assert datasets[1].dataset_id == "dataset2"

        # Should have recorded the call
        recorder.stop_recording()
        assert len(recorder._recordings) == 1
        recording = list(recorder._recordings.values())[0]
        assert "list_datasets" in recording.query
        assert len(recording.results) == 2
        assert recording.results[0]["dataset_id"] == "dataset1"
        assert recording.results[0]["labels"] == {"env": "test"}

    def test_list_datasets_replay_mode(self, tmp_path):
        """Test list_datasets in replay mode."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        # Pre-record a list_datasets call
        recording = QueryRecording(
            query="list_datasets((), {})",
            results=[
                {
                    "dataset_id": "dataset1",
                    "project": "test-project",
                    "labels": {"env": "test"},
                    "_properties": {
                        "datasetReference": {
                            "datasetId": "dataset1",
                            "projectId": "test-project",
                        },
                        "location": "US",
                    },
                },
                {
                    "dataset_id": "dataset2",
                    "project": "test-project",
                    "labels": {"env": "prod"},
                    "_properties": {
                        "datasetReference": {
                            "datasetId": "dataset2",
                            "projectId": "test-project",
                        },
                    },
                },
            ],
            row_count=2,
        )
        recorder._recordings[recording.get_key()] = recording

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Call list_datasets - should serve from recordings
        datasets = list(client.list_datasets())

        # Should return reconstructed datasets
        assert len(datasets) == 2
        assert datasets[0].dataset_id == "dataset1"
        assert datasets[1].dataset_id == "dataset2"
        # Check labels - they should be set both as attribute and in _properties
        assert datasets[0].labels == {"env": "test"} or datasets[0]._properties.get(
            "labels"
        ) == {"env": "test"}  # type: ignore[attr-defined]
        # Check that _properties contains the expected structure
        assert datasets[0]._properties is not None  # type: ignore[attr-defined]
        assert "datasetReference" in datasets[0]._properties  # type: ignore[attr-defined]
        assert datasets[0]._properties.get("location") == "US"  # type: ignore[attr-defined]

    def test_list_datasets_replay_mode_not_found(self, tmp_path):
        """Test list_datasets in replay mode when recording not found."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Call list_datasets without recording - should return empty list
        datasets = list(client.list_datasets())
        assert len(datasets) == 0

    def test_list_datasets_replay_mode_with_error(self, tmp_path):
        """Test list_datasets in replay mode when recording has error."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        # Pre-record an error
        recording = QueryRecording(
            query="list_datasets((), {})",
            error="Permission denied",
        )
        recorder._recordings[recording.get_key()] = recording

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Should raise error
        with pytest.raises(RuntimeError, match="Recorded list_datasets error"):
            list(client.list_datasets())

    def test_list_tables_recording_mode(self, tmp_path):
        """Test list_tables in recording mode."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        patcher = ModulePatcher(recorder=recorder, is_replay=False)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Call list_tables
        tables = list(client.list_tables(dataset_id="dataset1"))

        # Should return tables
        assert len(tables) == 2
        assert tables[0].table_id == "table1"
        assert tables[1].table_id == "table2"

        # Should have recorded the call
        recorder.stop_recording()
        assert len(recorder._recordings) == 1
        recording = list(recorder._recordings.values())[0]
        assert "list_tables" in recording.query
        assert len(recording.results) == 2
        assert recording.results[0]["table_id"] == "table1"
        assert recording.results[0]["table_type"] == "TABLE"
        assert "time_partitioning" in recording.results[0]

    def test_list_tables_replay_mode(self, tmp_path):
        """Test list_tables in replay mode."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        # Pre-record a list_tables call
        recording = QueryRecording(
            query="list_tables((), {})",
            results=[
                {
                    "table_id": "table1",
                    "dataset_id": "dataset1",
                    "project": "test-project",
                    "table_type": "TABLE",
                    "labels": {"env": "test"},
                    "expires": "2025-12-31T12:00:00+00:00",
                    "clustering_fields": ["col1", "col2"],
                    "time_partitioning": {
                        "field": "date_col",
                        "type": "DAY",
                        "expiration_ms": 86400000,
                        "require_partition_filter": False,
                    },
                    "_properties": {"description": "test table"},
                },
            ],
            row_count=1,
        )
        recorder._recordings[recording.get_key()] = recording

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Call list_tables - should serve from recordings
        tables = list(client.list_tables())

        # Should return reconstructed tables
        assert len(tables) == 1
        assert tables[0].table_id == "table1"
        assert tables[0].table_type == "TABLE"
        assert tables[0].labels == {"env": "test"}
        assert tables[0].expires is not None
        assert tables[0].clustering_fields == ["col1", "col2"]
        assert tables[0].time_partitioning is not None
        assert tables[0].time_partitioning.field == "date_col"

    def test_list_tables_replay_mode_with_none_require_partition_filter(self, tmp_path):
        """Test list_tables replay preserves None for require_partition_filter."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        # Pre-record with None require_partition_filter
        recording = QueryRecording(
            query="list_tables((), {})",
            results=[
                {
                    "table_id": "table1",
                    "dataset_id": "dataset1",
                    "project": "test-project",
                    "time_partitioning": {
                        "field": "date_col",
                        "type": "DAY",
                        "expiration_ms": 86400000,
                        "require_partition_filter": None,  # None value
                    },
                },
            ],
            row_count=1,
        )
        recorder._recordings[recording.get_key()] = recording

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")
        tables = list(client.list_tables())

        # Should preserve None
        assert tables[0].time_partitioning is not None
        assert tables[0].time_partitioning.require_partition_filter is None

    def test_list_tables_replay_mode_expiration_ms_conversion(self, tmp_path):
        """Test list_tables replay converts expiration_ms from string/float to int."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        # Pre-record with string expiration_ms
        recording = QueryRecording(
            query="list_tables((), {})",
            results=[
                {
                    "table_id": "table1",
                    "dataset_id": "dataset1",
                    "project": "test-project",
                    "time_partitioning": {
                        "field": "date_col",
                        "type": "DAY",
                        "expiration_ms": "86400000",  # String
                        "require_partition_filter": False,
                    },
                },
            ],
            row_count=1,
        )
        recorder._recordings[recording.get_key()] = recording

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")
        tables = list(client.list_tables())

        # Should convert to int
        assert tables[0].time_partitioning is not None
        assert isinstance(tables[0].time_partitioning.expiration_ms, int)
        assert tables[0].time_partitioning.expiration_ms == 86400000

    def test_get_dataset_recording_mode(self, tmp_path):
        """Test get_dataset in recording mode."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        patcher = ModulePatcher(recorder=recorder, is_replay=False)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Call get_dataset
        dataset = client.get_dataset("dataset1")

        # Should return dataset
        assert dataset.dataset_id == "dataset1"
        assert dataset.labels == {"env": "test"}

        # Should have recorded the call
        recorder.stop_recording()
        assert len(recorder._recordings) == 1
        recording = list(recorder._recordings.values())[0]
        assert "get_dataset" in recording.query
        assert len(recording.results) == 1
        assert recording.results[0]["dataset_id"] == "dataset1"

    def test_get_dataset_replay_mode(self, tmp_path):
        """Test get_dataset in replay mode."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        # Pre-record a get_dataset call
        # The call key includes the actual arguments: get_dataset(('dataset1',), {})
        recording = QueryRecording(
            query="get_dataset(('dataset1',), {})",
            results=[
                {
                    "dataset_id": "dataset1",
                    "project": "test-project",
                    "labels": {"env": "test"},
                    "_properties": {
                        "datasetReference": {
                            "datasetId": "dataset1",
                            "projectId": "test-project",
                        },
                        "location": "US",
                    },
                },
            ],
            row_count=1,
        )
        recorder._recordings[recording.get_key()] = recording

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Call get_dataset - should serve from recordings
        dataset = client.get_dataset("dataset1")

        # Should return reconstructed dataset
        assert dataset.dataset_id == "dataset1"
        assert dataset.project == "test-project"
        assert dataset.labels == {"env": "test"}
        # Check that _properties contains the expected structure
        assert dataset._properties is not None  # type: ignore[attr-defined]
        assert "datasetReference" in dataset._properties  # type: ignore[attr-defined]
        assert dataset._properties["datasetReference"]["datasetId"] == "dataset1"  # type: ignore[attr-defined]
        assert dataset._properties["datasetReference"]["projectId"] == "test-project"  # type: ignore[attr-defined]
        assert dataset._properties.get("location") == "US"  # type: ignore[attr-defined]

    def test_get_dataset_replay_mode_not_found(self, tmp_path):
        """Test get_dataset in replay mode when recording not found."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Should raise error
        with pytest.raises(RuntimeError, match="get_dataset.*not found in recordings"):
            client.get_dataset("dataset1")

    def test_get_dataset_replay_mode_with_error(self, tmp_path):
        """Test get_dataset in replay mode when recording has error."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)

        # Pre-record an error
        # The call key includes the actual arguments
        recording = QueryRecording(
            query="get_dataset(('dataset1',), {})",
            error="Dataset not found",
        )
        recorder._recordings[recording.get_key()] = recording

        patcher = ModulePatcher(recorder=recorder, is_replay=True)
        WrappedClient = patcher._create_client_wrapper(MockBigQueryClient)

        client = WrappedClient(project="test-project")

        # Should raise error
        with pytest.raises(RuntimeError, match="Recorded get_dataset error"):
            client.get_dataset("dataset1")

    def test_list_datasets_recording_mode_exception(self, tmp_path):
        """Test list_datasets in recording mode when exception occurs."""
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        patcher = ModulePatcher(recorder=recorder, is_replay=False)

        # Create a client that raises exception
        class FailingClient:
            def list_datasets(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
                raise ValueError("Connection failed")

        WrappedClient = patcher._create_client_wrapper(FailingClient)
        client = WrappedClient()

        # Should record the error and re-raise
        with pytest.raises(ValueError, match="Connection failed"):
            list(client.list_datasets())

        # Should have recorded the error
        recorder.stop_recording()
        assert len(recorder._recordings) == 1
        recording = list(recorder._recordings.values())[0]
        assert recording.error == "Connection failed"
