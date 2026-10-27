from __future__ import annotations

import logging
import time
from typing import Optional

import requests
from pydantic import Field, SecretStr

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.emitter.token_provider import TokenProvider, TokenResult

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SEC = 30


class OidcClientCredentialsTokenProviderConfig(ConfigModel):
    token_endpoint: str = Field(
        description="OIDC token endpoint, e.g. "
        "'https://<idp>/realms/<realm>/protocol/openid-connect/token'."
    )
    client_id: str = Field(description="OAuth client ID.")
    client_secret: SecretStr = Field(description="OAuth client secret.")
    scope: Optional[str] = Field(
        default=None, description="Optional space-delimited scopes."
    )
    audience: Optional[str] = Field(
        default=None,
        description="Optional 'audience' parameter (used by some IdPs, e.g. Auth0).",
    )


class OidcClientCredentialsTokenProvider(TokenProvider):
    """Generic OIDC client-credentials grant. Works with any standards-compliant
    IdP (Keycloak, Okta, generic OIDC) — also the local smoke-test enabler."""

    def __init__(self, config: OidcClientCredentialsTokenProviderConfig) -> None:
        self._config = config
        self._session = requests.Session()

    def get_token(self) -> TokenResult:
        data = {
            "grant_type": "client_credentials",
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret.get_secret_value(),
        }
        if self._config.scope:
            data["scope"] = self._config.scope
        if self._config.audience:
            data["audience"] = self._config.audience
        # Treat the token as issued at request-send time, so refresh fires
        # slightly early rather than late if the round-trip is slow.
        requested_at = time.time()
        try:
            resp = self._session.post(
                self._config.token_endpoint, data=data, timeout=_DEFAULT_TIMEOUT_SEC
            )
            resp.raise_for_status()
            payload = resp.json()
        except requests.RequestException as e:
            raise ConfigurationError(
                f"client-credentials token request to "
                f"'{self._config.token_endpoint}' failed: {e}"
            ) from e
        token = payload.get("access_token")
        if not token:
            raise ConfigurationError(
                "token endpoint response did not contain 'access_token'"
            )
        # Use the standard `expires_in` (RFC 6749 §5.1) rather than decoding the
        # opaque token. If the IdP omits it, fall back to per-call re-fetch.
        expires_in = payload.get("expires_in")
        if expires_in is None:
            logger.warning(
                "client-credentials response from '%s' omitted 'expires_in'; "
                "a fresh token will be requested on every API call.",
                self._config.token_endpoint,
            )
            return TokenResult(token)
        return TokenResult(token, requested_at + float(expires_in))

    @classmethod
    def create(cls, config: Optional[dict]) -> "OidcClientCredentialsTokenProvider":
        return cls(
            OidcClientCredentialsTokenProviderConfig.model_validate(config or {})
        )
