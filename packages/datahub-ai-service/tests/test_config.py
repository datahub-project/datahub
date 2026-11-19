import pytest

from datahub_ai_service.config import AiServiceConfig, ConfigError


def test_defaults() -> None:
    config = AiServiceConfig.from_env({"DATAHUB_GMS_URL": "http://gms:8080/"})

    assert config == AiServiceConfig(gms_url="http://gms:8080")
    assert (config.host, config.port) == ("0.0.0.0", 9005)
    assert config.mcp_enabled
    assert config.mcp_response_mode == "sse"


def test_overrides() -> None:
    config = AiServiceConfig.from_env(
        {
            "DATAHUB_GMS_URL": "https://example.com/datahub/gms",
            "DATAHUB_AI_PORT": "9105",
            "DATAHUB_AI_MCP_ENABLED": "false",
            "DATAHUB_AI_MCP_RESPONSE_MODE": "JSON",
            "DATAHUB_MCP_TOKEN_CACHE_TTL_SECONDS": "30",
            "DATAHUB_AI_TOOL_TIME_LIMIT_SECONDS": "60",
            "DATAHUB_AI_ALLOWED_HOSTS": "datahub.example.com, localhost:9002",
            "DATAHUB_AI_TRUSTED_PROXIES": "datahub-frontend-react",
            "DATAHUB_AI_AUTH_MODE": "required",
        }
    )

    assert config.gms_url == "https://example.com/datahub/gms"
    assert config.port == 9105
    assert not config.mcp_enabled
    assert config.mcp_response_mode == "json"
    assert config.token_cache_ttl_seconds == 30
    assert config.tool_time_limit_seconds == 60
    assert config.allowed_hosts == ("datahub.example.com", "localhost:9002")
    assert config.trusted_proxies == ("datahub-frontend-react",)
    assert config.auth_mode == "required"


def test_refuses_a_shared_token() -> None:
    with pytest.raises(ConfigError, match="DATAHUB_GMS_TOKEN"):
        AiServiceConfig.from_env(
            {"DATAHUB_GMS_URL": "http://gms:8080", "DATAHUB_GMS_TOKEN": "secret"}
        )


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"DATAHUB_GMS_URL": "http://gms:8080", "DATAHUB_AI_MCP_RESPONSE_MODE": "xml"},
        {"DATAHUB_GMS_URL": "http://gms:8080", "DATAHUB_AI_PORT": "nine"},
        {"DATAHUB_GMS_URL": "http://gms:8080", "DATAHUB_AI_MCP_ENABLED": "maybe"},
        {"DATAHUB_GMS_URL": "http://gms:8080", "DATAHUB_AI_AUTH_MODE": "off"},
    ],
)
def test_invalid(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError):
        AiServiceConfig.from_env(env)
