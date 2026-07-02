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

import os
from typing import Any, Dict, List, Optional

from pydantic import SecretStr

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.auth.registry import AuthConfig

ENV_AUTH_TYPE = "DATAHUB_AUTH_TYPE"


def _require_env(auth_type: str, names: List[str]) -> Dict[str, str]:
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise ConfigurationError(
            f"{ENV_AUTH_TYPE}={auth_type} requires the following environment "
            f"variables to be set: {', '.join(missing)}"
        )
    return {name: os.environ[name] for name in names}


def build_auth_config_from_env() -> Optional[AuthConfig]:
    """Parse ``DATAHUB_AUTH_TYPE`` + provider env vars into an AuthConfig.

    Returns None when ``DATAHUB_AUTH_TYPE`` is unset, so callers fall back to
    the static ``DATAHUB_GMS_TOKEN`` / system-client behavior. Secrets are
    wrapped in :class:`pydantic.SecretStr` so config dumps/reprs mask them.
    """
    auth_type = os.environ.get(ENV_AUTH_TYPE)
    if not auth_type:
        return None

    config: Dict[str, Any] = {}
    if auth_type == "k8s_oidc":
        if os.environ.get("DATAHUB_AUTH_TOKEN_FILE"):
            config["token_file"] = os.environ["DATAHUB_AUTH_TOKEN_FILE"]
        if os.environ.get("DATAHUB_AUTH_AUDIENCE"):
            config["audience"] = os.environ["DATAHUB_AUTH_AUDIENCE"]
    elif auth_type == "azure_entra":
        required = _require_env(
            auth_type,
            [
                "DATAHUB_AUTH_AZURE_TENANT_ID",
                "DATAHUB_AUTH_AZURE_CLIENT_ID",
                "DATAHUB_AUTH_AZURE_SCOPE",
            ],
        )
        config = {
            "tenant_id": required["DATAHUB_AUTH_AZURE_TENANT_ID"],
            "client_id": required["DATAHUB_AUTH_AZURE_CLIENT_ID"],
            "scope": required["DATAHUB_AUTH_AZURE_SCOPE"],
        }
        if os.environ.get("DATAHUB_AUTH_AZURE_CLIENT_SECRET"):
            config["client_secret"] = SecretStr(
                os.environ["DATAHUB_AUTH_AZURE_CLIENT_SECRET"]
            )
    elif auth_type == "oidc_client_credentials":
        required = _require_env(
            auth_type,
            [
                "DATAHUB_AUTH_TOKEN_ENDPOINT",
                "DATAHUB_AUTH_CLIENT_ID",
                "DATAHUB_AUTH_CLIENT_SECRET",
            ],
        )
        config = {
            "token_endpoint": required["DATAHUB_AUTH_TOKEN_ENDPOINT"],
            "client_id": required["DATAHUB_AUTH_CLIENT_ID"],
            "client_secret": SecretStr(required["DATAHUB_AUTH_CLIENT_SECRET"]),
        }
        if os.environ.get("DATAHUB_AUTH_SCOPE"):
            config["scope"] = os.environ["DATAHUB_AUTH_SCOPE"]
        if os.environ.get("DATAHUB_AUTH_AUDIENCE"):
            config["audience"] = os.environ["DATAHUB_AUTH_AUDIENCE"]
    else:
        raise ConfigurationError(
            f"Unsupported {ENV_AUTH_TYPE}: '{auth_type}'. Supported types: "
            "k8s_oidc, azure_entra, oidc_client_credentials. Custom providers "
            "can be configured via the 'auth' field of the client config "
            "(e.g. datahub_api.auth in a recipe) instead."
        )

    return AuthConfig(type=auth_type, config=config)
