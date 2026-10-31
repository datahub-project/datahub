import base64
from unittest.mock import patch

import pytest

from datahub.ingestion.source.elastic_search_common import (
    ElasticsearchConnectionConfig,
    _api_key_authorization,
)


def test_api_key_authorization_tuple_is_base64_encoded():
    header = _api_key_authorization(("my-id", "my-secret"))
    assert header == f"ApiKey {base64.b64encode(b'my-id:my-secret').decode()}"


def test_api_key_authorization_string_passed_through():
    assert _api_key_authorization("already-encoded") == "ApiKey already-encoded"


def test_http_auth_none_without_username():
    assert ElasticsearchConnectionConfig().http_auth is None


def test_http_auth_pairs_username_with_password():
    config = ElasticsearchConnectionConfig(username="u", password="p")
    assert config.http_auth == ("u", "p")


def test_http_auth_defaults_blank_password():
    assert ElasticsearchConnectionConfig(username="u").http_auth == ("u", "")


def test_host_validator_accepts_protocol_and_comma_separated():
    config = ElasticsearchConnectionConfig(
        host="https://a.internal:9200,b.internal:9200"
    )
    assert config.host == "https://a.internal:9200,b.internal:9200"


def test_host_validator_rejects_invalid_port():
    with pytest.raises(ValueError):
        ElasticsearchConnectionConfig(host="a.internal:not-a-port")


def test_get_client_forwards_connection_settings():
    config = ElasticsearchConnectionConfig(
        host="es.internal:9200", username="u", password="p", use_ssl=True
    )
    with patch("opensearchpy.OpenSearch") as mock_opensearch:
        config.get_client()

    args, kwargs = mock_opensearch.call_args
    assert args == ("es.internal:9200",)
    assert kwargs["http_auth"] == ("u", "p")
    assert kwargs["use_ssl"] is True
    # No api_key configured, so no Authorization header is injected.
    assert "headers" not in kwargs


def test_get_client_sends_api_key_as_authorization_header():
    config = ElasticsearchConnectionConfig(
        host="es.internal:9200", api_key=("id", "key")
    )
    with patch("opensearchpy.OpenSearch") as mock_opensearch:
        config.get_client()

    headers = mock_opensearch.call_args.kwargs["headers"]
    assert headers["Authorization"] == _api_key_authorization(("id", "key"))
