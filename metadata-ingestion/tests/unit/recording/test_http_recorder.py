"""Tests for HTTP recording functionality.

Note: These tests require the debug-recording plugin to be installed.
They will be skipped if vcrpy is not available.
"""

import tempfile
from pathlib import Path

import pytest

# Skip all tests if vcrpy is not installed
pytest.importorskip("vcr")

import vcr.cassette
import vcr.errors
import vcr.request


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


class TestOrderedPlayback:
    """Tests for _play_response_in_order."""

    @staticmethod
    def _polling_cassette() -> vcr.cassette.Cassette:
        """A cassette holding one STARTING then one RUNNING reply to the same GET."""
        cassette = vcr.cassette.Cassette(
            path="/tmp/warehouse.yaml", allow_playback_repeats=True
        )
        for state in ("STARTING", "RUNNING"):
            cassette.append(
                vcr.request.Request(
                    method="GET",
                    uri="https://example.cloud.databricks.com/api/2.0/sql/warehouses/1",
                    body=None,
                    headers={},
                ),
                {"body": {"string": state}, "status": {"code": 200, "message": "OK"}},
            )
        return cassette

    @staticmethod
    def _poll() -> vcr.request.Request:
        return vcr.request.Request(
            method="GET",
            uri="https://example.cloud.databricks.com/api/2.0/sql/warehouses/1",
            body=None,
            headers={},
        )

    def test_repeated_request_advances_then_repeats_last(self) -> None:
        """A polled state must reach RUNNING, then keep reporting RUNNING."""
        from datahub.ingestion.recording.http_recorder import _play_response_in_order

        cassette = self._polling_cassette()
        states = [
            _play_response_in_order(cassette, self._poll())["body"]["string"]
            for _ in range(4)
        ]
        assert states == ["STARTING", "RUNNING", "RUNNING", "RUNNING"]

    def test_unmatched_request_raises(self) -> None:
        from datahub.ingestion.recording.http_recorder import _play_response_in_order

        cassette = self._polling_cassette()
        unknown = vcr.request.Request(
            method="GET",
            uri="https://example.cloud.databricks.com/api/2.0/sql/warehouses/999",
            body=None,
            headers={},
        )
        with pytest.raises(vcr.errors.UnhandledHTTPRequestError):
            _play_response_in_order(cassette, unknown)


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
