from enum import Enum, auto
from typing import Dict, List, Optional

from pydantic import ConfigDict, model_validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.env_vars import (
    get_datahub_component,
    get_rest_sink_default_tcp_keepalive,
)
from datahub.emitter.emit_mode import EmitMode
from datahub.ingestion.auth.registry import AuthConfig


class ClientMode(Enum):
    INGESTION = auto()
    CLI = auto()
    SDK = auto()


DATAHUB_COMPONENT_ENV: str = get_datahub_component().lower()
_DEFAULT_TCP_KEEPALIVE: bool = get_rest_sink_default_tcp_keepalive()


class DatahubClientConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str
    token: Optional[str] = None
    auth: Optional[AuthConfig] = None
    timeout_sec: Optional[float] = None
    retry_status_codes: Optional[List[int]] = None
    retry_max_times: Optional[int] = None
    pool_connections: Optional[int] = None
    pool_maxsize: Optional[int] = None
    extra_headers: Optional[Dict[str, str]] = None
    ca_certificate_path: Optional[str] = None
    client_certificate_path: Optional[str] = None
    client_key_path: Optional[str] = None
    disable_ssl_verification: bool = False
    openapi_ingestion: Optional[bool] = None
    # Opt this client into marker-aware sync routing: a batch is upgraded to
    # explicit async=false (synchronous) if any of its MCPs carries the
    # syncIngest marker in its system metadata; otherwise the configured
    # emit_mode is honored unchanged. It only ever forces more synchronicity,
    # never less. Never implicit. The marker is read, not produced, here: a
    # producer must populate the syncIngest system-metadata property on writes
    # that must remain synchronous (e.g. via a custom aspect mutator/validator
    # or an upstream processing step).
    special_respect_mcp_sync_marker: Optional[bool] = None
    client_mode: Optional[ClientMode] = None
    datahub_component: Optional[str] = None
    server_config_refresh_interval: Optional[int] = None
    tcp_keepalive: bool = _DEFAULT_TCP_KEEPALIVE
    # Default emit mode for emit calls that don't pass one. None falls back to the
    # emitter's global default. Not used by the `datahub-rest` sink (it has `mode`).
    default_emit_mode: Optional[EmitMode] = None

    @model_validator(mode="after")
    def _validate_auth_exclusive(self) -> "DatahubClientConfig":
        if self.token is not None and self.auth is not None:
            raise ValueError("Provide either 'token' or 'auth', not both.")
        return self

    model_config = ConfigDict(extra="ignore")
