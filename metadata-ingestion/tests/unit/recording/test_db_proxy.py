"""Tests for database proxy functionality."""

import tempfile
from pathlib import Path
from typing import Any, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.recording.db_proxy import (
    ConnectionProxy,
    CursorProxy,
    QueryRecorder,
    QueryRecording,
    ReplayConnection,
)


class TestQueryRecording:
    """Tests for QueryRecording dataclass."""

    def test_create_recording(self) -> None:
        """Test creating a query recording."""
        recording = QueryRecording(
            query="SELECT * FROM users",
            parameters={"id": 1},
            results=[{"id": 1, "name": "Alice"}],
            row_count=1,
        )
        assert recording.query == "SELECT * FROM users"
        assert recording.results == [{"id": 1, "name": "Alice"}]

    def test_to_dict(self) -> None:
        """Test serializing recording to dict."""
        recording = QueryRecording(
            query="SELECT 1",
            results=[{"value": 1}],
        )
        data = recording.to_dict()
        assert data["query"] == "SELECT 1"
        assert data["results"] == [{"value": 1}]

    def test_from_dict(self) -> None:
        """Test deserializing recording from dict."""
        data = {
            "query": "SELECT * FROM table",
            "parameters": {"x": 1},
            "results": [{"a": 1}],
            "row_count": 1,
            "description": [("a", "int")],
            "error": None,
        }
        recording = QueryRecording.from_dict(data)
        assert recording.query == "SELECT * FROM table"
        assert recording.row_count == 1

    def test_get_key_unique(self) -> None:
        """Test that get_key produces unique keys for different queries."""
        r1 = QueryRecording(query="SELECT 1")
        r2 = QueryRecording(query="SELECT 2")
        r3 = QueryRecording(query="SELECT 1", parameters={"x": 1})

        assert r1.get_key() != r2.get_key()
        assert r1.get_key() != r3.get_key()

    def test_get_key_consistent(self) -> None:
        """Test that get_key is consistent for same query."""
        r1 = QueryRecording(query="SELECT 1", parameters={"x": 1})
        r2 = QueryRecording(query="SELECT 1", parameters={"x": 1})
        assert r1.get_key() == r2.get_key()


class TestQueryRecorder:
    """Tests for QueryRecorder class."""

    def test_record_and_retrieve(self) -> None:
        """Test recording and retrieving queries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queries.jsonl"
            recorder = QueryRecorder(path)
            recorder.start_recording()

            recording = QueryRecording(
                query="SELECT * FROM test",
                results=[{"id": 1}],
            )
            recorder.record(recording)
            recorder.stop_recording()

            # Reload and verify
            recorder2 = QueryRecorder(path)
            recorder2.load_recordings()
            retrieved = recorder2.get_recording("SELECT * FROM test")

            assert retrieved is not None
            assert retrieved.results == [{"id": 1}]

    def test_load_multiple_recordings(self) -> None:
        """Test loading multiple recordings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queries.jsonl"
            recorder = QueryRecorder(path)
            recorder.start_recording()

            for i in range(3):
                recorder.record(
                    QueryRecording(
                        query=f"SELECT {i}",
                        results=[{"value": i}],
                    )
                )
            recorder.stop_recording()

            recorder2 = QueryRecorder(path)
            recorder2.load_recordings()

            for i in range(3):
                r = recorder2.get_recording(f"SELECT {i}")
                assert r is not None
                assert r.results == [{"value": i}]


class MockCursor:
    """Mock cursor for testing."""

    def __init__(self, results: List[Tuple]) -> None:
        self._results = results
        self._index = 0
        self.description = [("col1", "string")]
        self.rowcount = len(results)

    def execute(self, query: str, params: Any = None) -> "MockCursor":
        return self

    def fetchall(self) -> List[Tuple]:
        return self._results

    def fetchone(self) -> Optional[Tuple]:
        if self._index < len(self._results):
            row = self._results[self._index]
            self._index += 1
            return row
        return None


class TestCursorProxy:
    """Tests for CursorProxy class."""

    def test_recording_mode(self) -> None:
        """Test cursor proxy in recording mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queries.jsonl"
            recorder = QueryRecorder(path)
            recorder.start_recording()

            mock_cursor = MockCursor([("value1",), ("value2",)])
            proxy = CursorProxy(mock_cursor, recorder, is_replay=False)

            proxy.execute("SELECT col1 FROM test")

            recorder.stop_recording()

            # Verify recording was created
            recorder2 = QueryRecorder(path)
            recorder2.load_recordings()
            recording = recorder2.get_recording("SELECT col1 FROM test")
            assert recording is not None

    def test_replay_mode(self) -> None:
        """Test cursor proxy in replay mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queries.jsonl"

            # First, create a recording
            recorder = QueryRecorder(path)
            recorder.start_recording()
            recorder.record(
                QueryRecording(
                    query="SELECT * FROM users",
                    results=[{"id": 1, "name": "Alice"}],
                    row_count=1,
                )
            )
            recorder.stop_recording()

            # Now replay
            recorder2 = QueryRecorder(path)
            recorder2.load_recordings()

            proxy = CursorProxy(None, recorder2, is_replay=True)
            proxy.execute("SELECT * FROM users")
            results = proxy.fetchall()

            assert results == [{"id": 1, "name": "Alice"}]

    def test_replay_missing_query_raises(self) -> None:
        """Test that replaying a missing query raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queries.jsonl"
            recorder = QueryRecorder(path)
            recorder.start_recording()
            recorder.stop_recording()

            recorder2 = QueryRecorder(path)
            recorder2.load_recordings()

            proxy = CursorProxy(None, recorder2, is_replay=True)

            with pytest.raises(RuntimeError, match="Query not found"):
                proxy.execute("SELECT * FROM nonexistent")


class TestConnectionProxy:
    """Tests for ConnectionProxy class."""

    def test_cursor_returns_proxy(self) -> None:
        """Test that cursor() returns a CursorProxy."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queries.jsonl"
            recorder = QueryRecorder(path)

            mock_conn = MagicMock()
            mock_conn.cursor.return_value = MockCursor([])

            proxy = ConnectionProxy(mock_conn, recorder, is_replay=False)
            cursor = proxy.cursor()

            assert isinstance(cursor, CursorProxy)


class TestReplayConnection:
    """Tests for ReplayConnection class."""

    def test_replay_connection_cursor(self) -> None:
        """Test that ReplayConnection returns replay cursors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queries.jsonl"
            recorder = QueryRecorder(path)

            conn = ReplayConnection(recorder)
            cursor = conn.cursor()

            assert isinstance(cursor, CursorProxy)

    def test_replay_connection_noop_methods(self) -> None:
        """Test that ReplayConnection methods are no-ops."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queries.jsonl"
            recorder = QueryRecorder(path)

            conn = ReplayConnection(recorder)

            # These should not raise
            conn.close()
            conn.commit()
            conn.rollback()
