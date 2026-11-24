"""Service configuration, read from the environment."""

import dataclasses
from typing import Callable, Literal, Mapping, TypeVar

_N = TypeVar("_N", int, float)

ResponseMode = Literal["sse", "json"]
AuthMode = Literal["auto", "required"]


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

    auth_mode: AuthMode = "auto"
    """auto follows GMS: a token when GMS auth is on, none when it's off.
    required serves /mcp only while GMS auth is on."""

    allowed_hosts: tuple[str, ...] = ("localhost", "127.0.0.1", "[::1]")
    """While GMS auth is off, /mcp answers only requests addressed to these
    hosts (``host`` for any port, ``host:port`` for one)."""

    allowed_origins: tuple[str, ...] = (
        "http://localhost",
        "https://localhost",
        "http://127.0.0.1",
        "https://127.0.0.1",
        "http://[::1]",
        "https://[::1]",
    )
    """While GMS auth is off, a request with an Origin must come from one of
    these (``scheme://host`` for any port)."""

    trusted_proxies: tuple[str, ...] = ()
    """Peers (IPs, networks or hostnames) whose X-Forwarded-Host is believed:
    the frontend that proxies /mcp."""

    token_cache_ttl_seconds: float = 300.0
    """How long a verified caller token is trusted before GMS checks it again."""

    gms_timeout_seconds: float = 30.0
    """Connect and read timeout for each request to GMS."""

    tool_time_limit_seconds: float = 300.0
    """How long one tool call may keep sending requests to GMS. Separate from
    proxy timeouts, which bound how long a response may take or stay idle."""

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

        auth_mode = environ.get("DATAHUB_AI_AUTH_MODE", "auto").strip().lower()
        if auth_mode not in ("auto", "required"):
            raise ConfigError(
                f"DATAHUB_AI_AUTH_MODE must be auto or required, not {auth_mode!r}"
            )

        return cls(
            gms_url=gms_url,
            auth_mode=auth_mode,  # type: ignore[arg-type]
            allowed_hosts=_list(environ, "DATAHUB_AI_ALLOWED_HOSTS", cls.allowed_hosts),
            allowed_origins=_list(
                environ, "DATAHUB_AI_ALLOWED_ORIGINS", cls.allowed_origins
            ),
            trusted_proxies=_list(
                environ, "DATAHUB_AI_TRUSTED_PROXIES", cls.trusted_proxies
            ),
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
            tool_time_limit_seconds=_number(
                environ,
                "DATAHUB_AI_TOOL_TIME_LIMIT_SECONDS",
                cls.tool_time_limit_seconds,
                float,
            ),
            gms_timeout_seconds=_number(
                environ,
                "DATAHUB_AI_GMS_TIMEOUT_SECONDS",
                cls.gms_timeout_seconds,
                float,
            ),
        )


def _list(
    environ: Mapping[str, str], name: str, default: tuple[str, ...]
) -> tuple[str, ...]:
    if name not in environ:
        return default
    return tuple(item.strip() for item in environ[name].split(",") if item.strip())


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
