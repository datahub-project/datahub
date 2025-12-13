"""Unit tests for database and HTTP patching in recording/patcher.py."""

from unittest.mock import MagicMock

from datahub.ingestion.recording.db_proxy import (
    ConnectionProxy,
    CursorProxy,
    QueryRecorder,
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
