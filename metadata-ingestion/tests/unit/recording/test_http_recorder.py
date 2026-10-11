"""Tests for HTTP recording functionality.

Note: These tests require the debug-recording plugin to be installed.
They will be skipped if vcrpy is not available.
"""

import gzip
import json
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, Iterator, Tuple

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


REAL_SECRET = "real-client-secret-value"
REAL_TOKEN = "real-issued-access-token"


class _FakeOAuthHandler(BaseHTTPRequestHandler):
    """Fake identity provider + API: gzip-compressed token response, then data."""

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        body = gzip.compress(
            json.dumps({"access_token": REAL_TOKEN, "token_type": "Bearer"}).encode()
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Encoding", "gzip")
        self.send_header("Set-Cookie", "session=real-session-cookie")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        body = json.dumps({"data": [1, 2, 3]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def fake_oauth_server() -> Iterator[Tuple[HTTPServer, int]]:
    server = HTTPServer(("127.0.0.1", 0), _FakeOAuthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield server, server.server_address[1]
    finally:
        server.shutdown()


class TestRecordReplayRoundTrip:
    """Record -> persist -> replay round trip against a local server.

    This is the invariant that protects users: the on-disk cassette must
    contain no secret material, and replay must still match every
    interaction using the scrubbed values.
    """

    @staticmethod
    def _run_connector_flow(port: int, client_secret: str) -> Dict[str, Any]:
        """Simulate a connector: token exchange, then a data call that
        embeds the granted token in the URL."""
        import requests

        token_response = requests.post(
            f"http://127.0.0.1:{port}/oauth/token",
            data={"grant_type": "client_credentials", "client_secret": client_secret},
        )
        token = token_response.json()["access_token"]
        data_response = requests.get(
            f"http://127.0.0.1:{port}/api/data",
            params={"access_token": token, "page": "1"},
        )
        return data_response.json()

    def test_cassette_scrubbed_and_replayable(
        self, fake_oauth_server: Tuple[HTTPServer, int]
    ) -> None:
        from datahub.ingestion.recording.http_recorder import HTTPRecorder

        server, port = fake_oauth_server

        with tempfile.TemporaryDirectory() as tmpdir:
            cassette_path = Path(tmpdir) / "http" / "cassette.yaml"

            recorder = HTTPRecorder(cassette_path)
            with recorder.recording():
                result = self._run_connector_flow(port, REAL_SECRET)
            assert result == {"data": [1, 2, 3]}

            # (a) No secret material anywhere on the tape - the token
            # response is gzip-compressed on the wire, so this also verifies
            # decode_compressed_response feeds the scrubber decoded text
            raw = cassette_path.read_text()
            assert REAL_SECRET not in raw
            assert REAL_TOKEN not in raw
            assert "real-session-cookie" not in raw

            # (b) Air-gapped replay still matches, through both replay paths
            server.shutdown()
            for use_responses in (False, True):
                replayer = HTTPRecorder(cassette_path)
                with replayer.replaying(use_responses_library=use_responses):
                    result = self._run_connector_flow(
                        port, "replay-mode-no-secret-needed"
                    )
                assert result == {"data": [1, 2, 3]}


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

    def test_before_record_request_drops_live_hosts_and_scrubs_rest(self) -> None:
        """Live-host traffic is not recorded; everything else is scrubbed."""
        from datahub.ingestion.recording.http_recorder import HTTPReplayerForLiveSink

        with tempfile.TemporaryDirectory() as tmpdir:
            cassette_path = Path(tmpdir) / "cassette.yaml"
            cassette_path.write_text("interactions: []")

            replayer = HTTPReplayerForLiveSink(
                cassette_path, live_hosts=["gms.example.com"]
            )

            class FakeRequest:
                def __init__(self, uri: str) -> None:
                    self.uri = uri
                    self.body = "client_secret=real-secret"
                    self.headers: dict = {}

            assert (
                replayer._before_record_request(
                    FakeRequest("http://gms.example.com/aspects")
                )
                is None
            )

            recorded = replayer._before_record_request(
                FakeRequest("https://api.other.com/oauth/token")
            )
            assert recorded is not None
            assert "real-secret" not in recorded.body
