"""Tests for cassette secret scrubbing."""

import json

from datahub.ingestion.recording.config import REPLAY_DUMMY_MARKER
from datahub.ingestion.recording.redaction import (
    is_secret_key,
    scrub_body,
    scrub_request_for_recording,
    scrub_response_for_recording,
    scrub_uri,
)


class TestIsSecretKey:
    def test_secret_keys(self) -> None:
        assert is_secret_key("client_secret")
        assert is_secret_key("access_token")
        assert is_secret_key("refresh_token")
        assert is_secret_key("password")
        assert is_secret_key("X-Api-Key")

    def test_non_secret_keys(self) -> None:
        assert not is_secret_key("token_type")
        assert not is_secret_key("token_url")
        assert not is_secret_key("authentication_type")
        assert not is_secret_key("grant_type")
        assert not is_secret_key("expires_in")


class TestScrubBody:
    def test_form_encoded_oauth_request(self) -> None:
        body = "grant_type=client_credentials&client_id=my-cid&client_secret=my-secret"
        scrubbed = scrub_body(body)
        assert isinstance(scrubbed, str)
        assert "my-secret" not in scrubbed
        assert f"client_secret={REPLAY_DUMMY_MARKER}" in scrubbed
        assert "grant_type=client_credentials" in scrubbed
        assert "client_id=my-cid" in scrubbed

    def test_json_token_response(self) -> None:
        body = json.dumps(
            {
                "access_token": "issued-token",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": "issued-refresh",
            }
        )
        scrubbed = scrub_body(body)
        assert isinstance(scrubbed, str)
        data = json.loads(scrubbed)
        assert data["access_token"] == REPLAY_DUMMY_MARKER
        assert data["refresh_token"] == REPLAY_DUMMY_MARKER
        # token_type is an enum sources may validate during replay
        assert data["token_type"] == "Bearer"
        assert data["expires_in"] == 3600

    def test_nested_json(self) -> None:
        body = json.dumps({"connection": {"host": "db.example.com", "password": "pw"}})
        scrubbed = scrub_body(body)
        assert isinstance(scrubbed, str)
        data = json.loads(scrubbed)
        assert data["connection"]["password"] == REPLAY_DUMMY_MARKER
        assert data["connection"]["host"] == "db.example.com"

    def test_secret_list_values_redacted(self) -> None:
        body = json.dumps({"access_tokens": ["tok-1", "tok-2"], "names": ["a", "b"]})
        scrubbed = scrub_body(body)
        assert isinstance(scrubbed, str)
        data = json.loads(scrubbed)
        assert data["access_tokens"] == [REPLAY_DUMMY_MARKER, REPLAY_DUMMY_MARKER]
        assert data["names"] == ["a", "b"]

    def test_freeform_text_with_equals_not_mangled(self) -> None:
        # Not form-encoded: keys with spaces fail the field-name check, so
        # the body must pass through byte-identical
        body = "the client_secret = something & more prose here"
        assert scrub_body(body) is body

    def test_json_scalar_body_unchanged(self) -> None:
        # Valid JSON but not an object/array - nothing to scrub by key
        body = "12345"
        assert scrub_body(body) is body

    def test_form_body_without_secrets_unchanged(self) -> None:
        # Plausible form keys but no secret params: must return the exact
        # original string, not a re-encoded copy
        body = "grant_type=refresh_grant&scope=all&page=1"
        assert scrub_body(body) is body

    def test_body_without_secrets_unchanged(self) -> None:
        # Replay matches on exact body, so bodies without secrets must keep
        # their original serialization byte-for-byte
        body = '{"filters":  {"ids": "3,1,2"}}'
        assert scrub_body(body) is body

    def test_bytes_body(self) -> None:
        body = b"client_secret=my-secret"
        scrubbed = scrub_body(body)
        assert isinstance(scrubbed, bytes)
        assert b"my-secret" not in scrubbed

    def test_binary_body_passthrough(self) -> None:
        body = b"\x80\x81\x82binary"
        assert scrub_body(body) is body

    def test_empty_body(self) -> None:
        assert scrub_body(None) is None
        assert scrub_body("") == ""


class TestScrubUri:
    def test_secret_query_param(self) -> None:
        uri = "https://api.example.com/data?page=1&access_token=issued-token"
        scrubbed = scrub_uri(uri)
        assert "issued-token" not in scrubbed
        assert f"access_token={REPLAY_DUMMY_MARKER}" in scrubbed
        assert "page=1" in scrubbed

    def test_uri_without_secrets_unchanged(self) -> None:
        # Replay matches on exact URI, so URIs without secrets must not be
        # re-encoded
        uri = "https://api.example.com/data?filter=a%2Cb&page=1"
        assert scrub_uri(uri) is uri

    def test_uri_without_query(self) -> None:
        uri = "https://api.example.com/data"
        assert scrub_uri(uri) is uri


class TestScrubRequest:
    def test_scrubs_uri_body_and_headers(self) -> None:
        class FakeRequest:
            uri = "https://idp.example.com/oauth/token?api_key=k"
            body = "grant_type=client_credentials&client_secret=my-secret"
            headers = {"X-Api-Key": "k", "Content-Type": "application/json"}

        request = FakeRequest()
        scrubbed = scrub_request_for_recording(request)
        assert "my-secret" not in scrubbed.body
        assert f"api_key={REPLAY_DUMMY_MARKER}" in scrubbed.uri
        assert scrubbed.headers["X-Api-Key"] == REPLAY_DUMMY_MARKER
        assert scrubbed.headers["Content-Type"] == "application/json"

    def test_cookie_request_header_values_scrubbed(self) -> None:
        class FakeRequest:
            uri = "https://api.example.com/data"
            body = None
            headers = {"Cookie": "session=abc; csrf=xyz"}

        scrubbed = scrub_request_for_recording(FakeRequest())
        assert scrubbed.headers["Cookie"] == (
            f"session={REPLAY_DUMMY_MARKER}; csrf={REPLAY_DUMMY_MARKER}"
        )


class TestScrubResponse:
    def test_scrubs_token_response(self) -> None:
        response = {
            "status": {"code": 200, "message": "OK"},
            "headers": {
                "Set-Cookie": ["session=abc123"],
                "Content-Type": ["application/json"],
            },
            "body": {
                "string": '{"access_token":"issued-token","token_type":"Bearer","expires_in":3600}'
            },
        }
        scrubbed = scrub_response_for_recording(response)
        assert "issued-token" not in scrubbed["body"]["string"]
        data = json.loads(scrubbed["body"]["string"])
        assert data["access_token"] == REPLAY_DUMMY_MARKER
        assert data["token_type"] == "Bearer"
        # Cookie name is preserved so replayed clients can still parse it
        assert scrubbed["headers"]["Set-Cookie"] == [f"session={REPLAY_DUMMY_MARKER}"]
        assert scrubbed["headers"]["Content-Type"] == ["application/json"]

    def test_set_cookie_attributes_preserved(self) -> None:
        response = {
            "status": {"code": 200, "message": "OK"},
            "headers": {
                "Set-Cookie": ["session=abc123; Path=/; HttpOnly; SameSite=Lax"]
            },
            "body": {"string": ""},
        }
        scrubbed = scrub_response_for_recording(response)
        assert scrubbed["headers"]["Set-Cookie"] == [
            f"session={REPLAY_DUMMY_MARKER}; Path=/; HttpOnly; SameSite=Lax"
        ]

    def test_content_length_updated_when_body_scrubbed(self) -> None:
        # Replay serves the recorded body with the recorded headers; a stale
        # Content-Length causes IncompleteRead errors in the HTTP client
        body = '{"access_token":"a-fairly-long-issued-token-value"}'
        response = {
            "status": {"code": 200, "message": "OK"},
            "headers": {"Content-Length": [str(len(body))]},
            "body": {"string": body},
        }
        scrubbed = scrub_response_for_recording(response)
        new_body = scrubbed["body"]["string"]
        assert new_body != body
        assert scrubbed["headers"]["Content-Length"] == [str(len(new_body))]

    def test_content_length_updated_for_scalar_header_value(self) -> None:
        # Header values may be plain strings rather than lists depending on
        # where VCR is in its serialization pipeline
        body = '{"access_token":"a-fairly-long-issued-token-value"}'
        response = {
            "status": {"code": 200, "message": "OK"},
            "headers": {"Content-Length": str(len(body))},
            "body": {"string": body},
        }
        scrubbed = scrub_response_for_recording(response)
        new_body = scrubbed["body"]["string"]
        assert scrubbed["headers"]["Content-Length"] == str(len(new_body))

    def test_non_json_response_unchanged(self) -> None:
        response = {
            "status": {"code": 200, "message": "OK"},
            "headers": {"Content-Type": ["text/html"]},
            "body": {"string": "<html>hello</html>"},
        }
        scrubbed = scrub_response_for_recording(response)
        assert scrubbed["body"]["string"] == "<html>hello</html>"

    def test_marker_is_stable_across_request_and_response(self) -> None:
        # The token in the response and the same token in a follow-up request
        # URI must scrub to the same value, or replay URI matching breaks
        response_body = scrub_body('{"access_token":"tok-123"}')
        assert isinstance(response_body, str)
        uri = scrub_uri("https://api.example.com/data?access_token=tok-123")
        assert json.loads(response_body)["access_token"] == REPLAY_DUMMY_MARKER
        assert f"access_token={REPLAY_DUMMY_MARKER}" in uri
