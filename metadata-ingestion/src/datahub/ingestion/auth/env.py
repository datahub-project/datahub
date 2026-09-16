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

import logging
from typing import Any, Dict, Optional

from pydantic import SecretStr

from datahub.configuration import env_vars
from datahub.configuration.common import ConfigurationError
from datahub.ingestion.auth.registry import AuthConfig

ENV_AUTH_TYPE = "DATAHUB_AUTH_TYPE"

logger = logging.getLogger(__name__)


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


def resolve_env_auth_config(server: str, *, origin_guard: bool) -> Optional[AuthConfig]:
    """Resolve env-based OAuth (``DATAHUB_AUTH_TYPE``) for a client whose caller
    supplied no explicit credentials. Returns None when ``DATAHUB_AUTH_TYPE`` is
    unset. This is the single place emitter- and sink-side clients read OAuth
    credentials from the environment, so the rule is not reimplemented per layer.

    ``origin_guard`` decides whether the credential is restricted to the
    ``DATAHUB_GMS_URL`` origin:

    - ``False`` — trust ``server``. Its callers (the REST emitter, and through it
      the Airflow hook and lineage listener, GX, Prefect, ``DataHubGraph``) pass a
      server from code, and the minted token is audience-scoped to DataHub. They
      routinely run where ``DATAHUB_GMS_URL`` is unset or spelled differently,
      where a guard would wrongly decline.
    - ``True`` — attach the credential only when ``server`` matches
      ``DATAHUB_GMS_URL`` (via requests' ``should_strip_auth``, which permits the
      benign http->https upgrade). A recipe-configured ``datahub-rest`` sink can
      point at an arbitrary host, so env OAuth must not mint tokens for a server
      the operator never set in the environment.
    """
    env_auth = build_auth_config_from_env()
    if env_auth is None or not origin_guard:
        return env_auth

    # Lazy imports: config_utils imports this module at load time, so importing
    # it (and its neighbours) at module scope would create a cycle.
    from requests.sessions import SessionRedirectMixin

    from datahub.cli.cli_utils import fixup_gms_url
    from datahub.cli.config_utils import get_url_from_env

    env_url = get_url_from_env()
    if env_url is None:
        logger.warning(
            "DATAHUB_AUTH_TYPE is set but DATAHUB_GMS_URL is not; not applying "
            "env OAuth to %s. Set DATAHUB_GMS_URL to inherit env auth.",
            server,
        )
        return None
    if SessionRedirectMixin().should_strip_auth(
        fixup_gms_url(env_url), fixup_gms_url(server)
    ):
        logger.warning(
            "Not applying env OAuth (DATAHUB_AUTH_TYPE) to %s — it does not match "
            "the env-configured server %s.",
            server,
            env_url,
        )
        return None
    return env_auth
