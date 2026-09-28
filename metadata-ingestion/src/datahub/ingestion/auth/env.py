"""Environment-based auth configuration for DataHub clients.

Builds a declarative :class:`AuthConfig` from ``DATAHUB_AUTH_TYPE`` plus
provider-specific environment variables, so that any process using the default
client config resolution (the ``datahub`` CLI, the default ingestion sink, the
Remote Executor and its recipe subprocesses) can authenticate with short-lived
OAuth tokens instead of a static ``DATAHUB_GMS_TOKEN``.

Supported values of ``DATAHUB_AUTH_TYPE`` and their variables:

- ``k8s_oidc``: ``DATAHUB_AUTH_TOKEN_FILE`` (optional),
  ``DATAHUB_AUTH_AUDIENCE`` (optional)
- ``azure_entra``: ``DATAHUB_AUTH_AZURE_TENANT_ID``,
  ``DATAHUB_AUTH_AZURE_CLIENT_ID``, ``DATAHUB_AUTH_AZURE_SCOPE`` (required);
  ``DATAHUB_AUTH_AZURE_CLIENT_SECRET`` (optional — omit for workload identity)
- ``oidc_client_credentials``: ``DATAHUB_AUTH_TOKEN_ENDPOINT``,
  ``DATAHUB_AUTH_CLIENT_ID``, ``DATAHUB_AUTH_CLIENT_SECRET`` (required);
  ``DATAHUB_AUTH_SCOPE``, ``DATAHUB_AUTH_AUDIENCE`` (optional)
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import SecretStr

from datahub.configuration import env_vars
from datahub.configuration.common import ConfigurationError
from datahub.ingestion.auth.registry import AuthConfig

ENV_AUTH_TYPE = "DATAHUB_AUTH_TYPE"


def _require_env(auth_type: str, values: Dict[str, Optional[str]]) -> Dict[str, str]:
    """Values keyed by env-var name (for the error message), read via env_vars."""
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ConfigurationError(
            f"{ENV_AUTH_TYPE}={auth_type} requires the following environment "
            f"variables to be set: {', '.join(missing)}"
        )
    return {name: value for name, value in values.items() if value}


def build_auth_config_from_env() -> Optional[AuthConfig]:
    """Parse ``DATAHUB_AUTH_TYPE`` + provider env vars into an AuthConfig.

    Returns None when ``DATAHUB_AUTH_TYPE`` is unset, so callers fall back to
    the static ``DATAHUB_GMS_TOKEN`` / system-client behavior. Secrets are
    wrapped in :class:`pydantic.SecretStr` so config dumps/reprs mask them.
    """
    auth_type = env_vars.get_auth_type()
    if not auth_type:
        return None

    config: Dict[str, Any] = {}
    if auth_type == "k8s_oidc":
        if token_file := env_vars.get_auth_token_file():
            config["token_file"] = token_file
        if audience := env_vars.get_auth_audience():
            config["audience"] = audience
    elif auth_type == "azure_entra":
        required = _require_env(
            auth_type,
            {
                "DATAHUB_AUTH_AZURE_TENANT_ID": env_vars.get_auth_azure_tenant_id(),
                "DATAHUB_AUTH_AZURE_CLIENT_ID": env_vars.get_auth_azure_client_id(),
                "DATAHUB_AUTH_AZURE_SCOPE": env_vars.get_auth_azure_scope(),
            },
        )
        config = {
            "tenant_id": required["DATAHUB_AUTH_AZURE_TENANT_ID"],
            "client_id": required["DATAHUB_AUTH_AZURE_CLIENT_ID"],
            "scope": required["DATAHUB_AUTH_AZURE_SCOPE"],
        }
        if azure_secret := env_vars.get_auth_azure_client_secret():
            config["client_secret"] = SecretStr(azure_secret)
    elif auth_type == "oidc_client_credentials":
        required = _require_env(
            auth_type,
            {
                "DATAHUB_AUTH_TOKEN_ENDPOINT": env_vars.get_auth_token_endpoint(),
                "DATAHUB_AUTH_CLIENT_ID": env_vars.get_auth_client_id(),
                "DATAHUB_AUTH_CLIENT_SECRET": env_vars.get_auth_client_secret(),
            },
        )
        config = {
            "token_endpoint": required["DATAHUB_AUTH_TOKEN_ENDPOINT"],
            "client_id": required["DATAHUB_AUTH_CLIENT_ID"],
            "client_secret": SecretStr(required["DATAHUB_AUTH_CLIENT_SECRET"]),
        }
        if scope := env_vars.get_auth_scope():
            config["scope"] = scope
        if audience := env_vars.get_auth_audience():
            config["audience"] = audience
    else:
        raise ConfigurationError(
            f"Unsupported {ENV_AUTH_TYPE}: '{auth_type}'. Supported types: "
            "k8s_oidc, azure_entra, oidc_client_credentials. Custom providers "
            "can be configured via the 'auth' field of the client config "
            "(e.g. datahub_api.auth in a recipe) instead."
        )

    return AuthConfig(type=auth_type, config=config)
