"""Tests for HTTP recording functionality.

Note: These tests require the debug-recording plugin to be installed.
They will be skipped if vcrpy is not available.
"""

import tempfile
from pathlib import Path

import pytest

# Skip all tests if vcrpy is not installed
vcr = pytest.importorskip("vcr")


class TestHTTPRecorder:
    """Tests for HTTPRecorder class."""

    def test_recording_creates_cassette(self) -> None:
        """Test that recording creates a cassette file."""
        from datahub.ingestion.recording.http_recorder import HTTPRecorder

        with tempfile.TemporaryDirectory() as tmpdir:
            cassette_path = Path(tmpdir) / "http" / "cassette.json"
            recorder = HTTPRecorder(cassette_path)

            with recorder.recording():
                # Make a request (would be mocked in real test)
                pass

            # In a real scenario with actual requests, cassette would exist
            # This just verifies the context manager works
            assert cassette_path.parent.exists()

    def test_request_count_property(self) -> None:
        """Test request_count property."""
        from datahub.ingestion.recording.http_recorder import HTTPRecorder

        with tempfile.TemporaryDirectory() as tmpdir:
            cassette_path = Path(tmpdir) / "cassette.json"
            recorder = HTTPRecorder(cassette_path)

            # Outside of context, count should be 0
            assert recorder.request_count == 0

    def test_replaying_requires_cassette(self) -> None:
        """Test that replaying requires an existing cassette."""
        from datahub.ingestion.recording.http_recorder import HTTPRecorder

        with tempfile.TemporaryDirectory() as tmpdir:
            cassette_path = Path(tmpdir) / "nonexistent.json"
            recorder = HTTPRecorder(cassette_path)

            with (
                pytest.raises(FileNotFoundError, match="cassette not found"),
                recorder.replaying(),
            ):
                pass


class TestHTTPReplayerForLiveSink:
    """Tests for HTTPReplayerForLiveSink class."""

    def test_live_hosts_configuration(self) -> None:
        """Test that live hosts are configured correctly."""
        from datahub.ingestion.recording.http_recorder import HTTPReplayerForLiveSink

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a dummy cassette file
            cassette_path = Path(tmpdir) / "cassette.json"
            cassette_path.write_text("[]")

            replayer = HTTPReplayerForLiveSink(
                cassette_path,
                live_hosts=["localhost:8080", "gms.example.com"],
            )

            assert "localhost:8080" in replayer.live_hosts
            assert "gms.example.com" in replayer.live_hosts
