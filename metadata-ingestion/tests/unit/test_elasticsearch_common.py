import base64
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.elastic_search_common import (
    ElasticsearchConnectionConfig,
    _api_key_authorization,
)


def test_api_key_authorization_tuple_is_base64_encoded():
    header = _api_key_authorization(("my-id", SecretStr("my-secret")))
    assert header == f"ApiKey {base64.b64encode(b'my-id:my-secret').decode()}"


def test_api_key_authorization_string_passed_through():
    assert (
        _api_key_authorization(SecretStr("already-encoded")) == "ApiKey already-encoded"
    )


def test_api_key_is_masked_in_repr():
    config = ElasticsearchConnectionConfig(api_key=["my-id", "my-secret"])
    assert "my-secret" not in repr(config)

    config = ElasticsearchConnectionConfig(api_key="encoded-token")
    assert "encoded-token" not in repr(config)


def test_http_auth_pairs_username_with_password():
    config = ElasticsearchConnectionConfig(username="u", password="p")
    assert config.http_auth == ("u", "p")


def test_http_auth_defaults_blank_password():
    assert ElasticsearchConnectionConfig(username="u").http_auth == ("u", "")


def test_host_validator_accepts_protocol_and_comma_separated():
    config = ElasticsearchConnectionConfig(
        host="https://a.internal:9200/,b.internal:9200"
    )
    assert config.host == "https://a.internal:9200/,b.internal:9200"


def test_host_validator_rejects_invalid_port():
    with pytest.raises(ValueError):
        ElasticsearchConnectionConfig(host="a.internal:not-a-port")


def test_api_key_and_basic_auth_are_mutually_exclusive():
    with pytest.raises(ValueError, match="exactly one authentication method"):
        ElasticsearchConnectionConfig(username="u", password="p", api_key="token")


def test_aws_config_requires_tls_and_region():
    with pytest.raises(ValueError, match="aws_region"):
        ElasticsearchConnectionConfig(
            use_ssl=True, verify_certs=True, aws_config={"aws_profile": "p"}
        )
    with pytest.raises(ValueError, match="use_ssl"):
        ElasticsearchConnectionConfig(aws_config={"aws_region": "us-east-1"})


def test_get_client_forwards_connection_settings_and_extra_kwargs():
    config = ElasticsearchConnectionConfig(
        host="es.internal:9200", username="u", password="p", use_ssl=True
    )
    with patch(
        "datahub.ingestion.source.elastic_search_common.OpenSearch"
    ) as mock_opensearch:
        config.get_client(SourceReport(), timeout=30)

    args, kwargs = mock_opensearch.call_args
    assert args == ("es.internal:9200",)
    assert kwargs["http_auth"] == ("u", "p")
    assert kwargs["use_ssl"] is True
    assert kwargs["timeout"] == 30
    # No api_key configured, so no Authorization header is injected.
    assert "headers" not in kwargs


def test_get_client_sends_api_key_as_authorization_header():
    config = ElasticsearchConnectionConfig(
        host="es.internal:9200", api_key=["id", "key"]
    )
    with patch(
        "datahub.ingestion.source.elastic_search_common.OpenSearch"
    ) as mock_opensearch:
        config.get_client(SourceReport())

    headers = mock_opensearch.call_args.kwargs["headers"]
    assert headers["Authorization"] == "ApiKey aWQ6a2V5"
