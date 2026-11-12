"""Service configuration, read from the environment."""

import dataclasses
from typing import Callable, Literal, Mapping, TypeVar

_N = TypeVar("_N", int, float)

ResponseMode = Literal["sse", "json"]


class ConfigError(ValueError):
    """The environment doesn't describe a service that can start."""


@dataclasses.dataclass(frozen=True)
class AiServiceConfig:
    gms_url: str
    """GMS base URL; may include a path prefix."""

    host: str = "0.0.0.0"
    port: int = 9005

    mcp_enabled: bool = True
    """Serve MCP at /mcp."""

    mcp_response_mode: ResponseMode = "sse"
    """sse streams each response, so progress can reach the client during slow
    calls. json returns one plain body, for proxies that can't stream."""

    token_cache_ttl_seconds: float = 300.0
    """How long a verified caller token is trusted before GMS checks it again."""

    gms_timeout_seconds: float = 30.0
    """Connect and read timeout for each request to GMS."""

    @classmethod
    def from_env(cls, environ: Mapping[str, str]) -> "AiServiceConfig":
        # Each HTTP caller sends their own token. A shared one would let every
        # caller act as its owner.
        if environ.get("DATAHUB_GMS_TOKEN"):
            raise ConfigError(
                "DATAHUB_GMS_TOKEN is not allowed: datahub-ai-service calls DataHub with "
                "each caller's own token. Remove it and have each MCP client send "
                "a personal access token."
            )

        gms_url = environ.get("DATAHUB_GMS_URL", "").strip().rstrip("/")
        if not gms_url:
            raise ConfigError("DATAHUB_GMS_URL is required")

        response_mode = environ.get("DATAHUB_AI_MCP_RESPONSE_MODE", "sse").lower()
        if response_mode not in ("sse", "json"):
            raise ConfigError(
                f"DATAHUB_AI_MCP_RESPONSE_MODE must be sse or json, not {response_mode!r}"
            )

        return cls(
            gms_url=gms_url,
            host=environ.get("DATAHUB_AI_HOST", cls.host),
            port=_number(environ, "DATAHUB_AI_PORT", cls.port, int),
            mcp_enabled=_flag(environ, "DATAHUB_AI_MCP_ENABLED", default=True),
            mcp_response_mode=response_mode,  # type: ignore[arg-type]
            token_cache_ttl_seconds=_number(
                environ,
                "DATAHUB_MCP_TOKEN_CACHE_TTL_SECONDS",
                cls.token_cache_ttl_seconds,
                float,
            ),
            gms_timeout_seconds=_number(
                environ,
                "DATAHUB_AI_GMS_TIMEOUT_SECONDS",
                cls.gms_timeout_seconds,
                float,
            ),
        )


def _flag(environ: Mapping[str, str], name: str, *, default: bool) -> bool:
    raw = environ.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in ("true", "1", "yes"):
        return True
    if raw in ("false", "0", "no"):
        return False
    raise ConfigError(f"{name} must be true or false, not {raw!r}")


def _number(
    environ: Mapping[str, str], name: str, default: _N, kind: Callable[[str], _N]
) -> _N:
    raw = environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = kind(raw)
    except ValueError:
        raise ConfigError(f"{name} must be a number, not {raw!r}") from None
    if value <= 0:
        raise ConfigError(f"{name} must be positive, not {raw!r}")
    return value
