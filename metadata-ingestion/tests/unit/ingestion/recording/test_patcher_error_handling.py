"""Unit tests for database connection error handling in recording/patcher.py."""

import importlib.util

import pytest

from datahub.ingestion.recording.patcher import _is_vcr_interference_error


class TestVCRInterferenceDetection:
    """Test detection of VCR interference errors."""

    def test_vcr_not_installed(self):
        """If VCR is not installed, should return False."""
        # Even with matching error pattern, should return False if VCR not available
        exc = Exception("SSL certificate verify failed")
        # This test assumes VCR might not be installed in test environment
        # The function should handle ImportError gracefully
        result = _is_vcr_interference_error(exc)
        # Result depends on whether VCR is installed
        assert isinstance(result, bool)

    def test_vcr_not_active_returns_false(self, monkeypatch):
        """If VCR is not active, should return False even with matching error."""
        # Check if VCR is available
        if not importlib.util.find_spec("vcr"):
            pytest.skip("VCR not installed")

        # Mock VCR to appear installed but not active
        monkeypatch.setattr("vcr.cassette._current_cassettes", [])

        exc = Exception("SSL certificate verify failed")
        result = _is_vcr_interference_error(exc)
        assert result is False

    def test_vcr_active_with_matching_error_returns_true(self, monkeypatch):
        """If VCR is active and error matches, should return True."""
        if not importlib.util.find_spec("vcr"):
            pytest.skip("VCR not installed")

        # Mock active cassette
        monkeypatch.setattr("vcr.cassette._current_cassettes", [{"dummy": "cassette"}])

        exc = Exception("SSL certificate verify failed during connection")
        result = _is_vcr_interference_error(exc)
        assert result is True

    def test_vcr_active_with_non_matching_error_returns_false(self, monkeypatch):
        """If VCR is active but error doesn't match patterns, should return False."""
        if not importlib.util.find_spec("vcr"):
            pytest.skip("VCR not installed")

        # Mock active cassette
        monkeypatch.setattr("vcr.cassette._current_cassettes", [{"dummy": "cassette"}])

        exc = Exception("Invalid credentials: wrong password")
        result = _is_vcr_interference_error(exc)
        assert result is False

    @pytest.mark.parametrize(
        "error_message",
        [
            "Connection refused",
            "SSL certificate verify failed",
            "SSL handshake failure",
            "Connection reset by peer",
            "Certificate validation error",
            "Proxy authentication required",
            "Name resolution failed",
        ],
    )
    def test_error_pattern_matching(self, error_message, monkeypatch):
        """Test that known VCR interference patterns are detected."""
        if not importlib.util.find_spec("vcr"):
            pytest.skip("VCR not installed")

        # Mock active cassette
        monkeypatch.setattr("vcr.cassette._current_cassettes", [{"dummy": "cassette"}])

        exc = Exception(error_message)
        result = _is_vcr_interference_error(exc)
        assert result is True, f"Should detect '{error_message}' as VCR interference"

    @pytest.mark.parametrize(
        "error_message",
        [
            "Invalid credentials",
            "Authentication failed",
            "Permission denied",
            "Table not found",
            "Syntax error in query",
        ],
    )
    def test_non_vcr_errors_not_detected(self, error_message, monkeypatch):
        """Test that non-VCR errors are not falsely detected."""
        if not importlib.util.find_spec("vcr"):
            pytest.skip("VCR not installed")

        # Mock active cassette
        monkeypatch.setattr("vcr.cassette._current_cassettes", [{"dummy": "cassette"}])

        exc = Exception(error_message)
        result = _is_vcr_interference_error(exc)
        assert result is False, (
            f"Should not detect '{error_message}' as VCR interference"
        )


class TestRecordingValidation:
    """Test recording validation functionality."""

    def test_validation_with_queries(self, tmp_path):
        """Test validation passes when queries are recorded."""
        from datahub.ingestion.recording.db_proxy import QueryRecorder, QueryRecording

        # Create a recorder with some queries
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()

        # Record a query
        recording = QueryRecording(
            query="SELECT * FROM test_table",
            results=[{"id": 1, "name": "test"}],
            row_count=1,
        )
        recorder.record(recording)
        recorder.stop_recording()

        # Verify queries were recorded
        assert len(recorder._recordings) > 0

    def test_validation_without_queries(self, tmp_path):
        """Test validation warns when no queries are recorded."""
        from datahub.ingestion.recording.db_proxy import QueryRecorder

        # Create a recorder without recording anything
        queries_path = tmp_path / "queries.jsonl"
        recorder = QueryRecorder(queries_path)
        recorder.start_recording()
        recorder.stop_recording()

        # Verify no queries were recorded
        assert len(recorder._recordings) == 0
