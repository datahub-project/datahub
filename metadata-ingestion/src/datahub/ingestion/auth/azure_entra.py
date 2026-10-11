from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from pydantic import Field, SecretStr

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.emitter.token_provider import TokenProvider, TokenResult

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


class AzureEntraTokenProviderConfig(ConfigModel):
    tenant_id: str = Field(description="Entra (Azure AD) tenant ID.")
    client_id: str = Field(description="App registration (client) ID.")
    scope: str = Field(
        description="Token scope, e.g. 'api://<datahub-app-id>/.default'."
    )
    client_secret: Optional[SecretStr] = Field(
        default=None,
        description="Client secret. INTERIM ONLY — omit to use federated "
        "workload identity (no secret at rest).",
    )


class AzureEntraTokenProvider(TokenProvider):
    """Acquires an Entra access token via workload identity (preferred) or a
    client secret (interim). Requires the 'azure-auth' extra."""

    def __init__(self, config: AzureEntraTokenProviderConfig) -> None:
        try:
            from azure.identity import (  # type: ignore[import]
                ClientSecretCredential,
                WorkloadIdentityCredential,
            )
        except (ImportError, TypeError) as e:
            raise ConfigurationError(
                "The azure_entra token provider requires the 'azure-auth' extra. "
                "Install with: pip install 'acryl-datahub[azure-auth]'"
            ) from e

        self._scope = config.scope
        self._credential: TokenCredential
        if config.client_secret:
            self._credential = ClientSecretCredential(
                tenant_id=config.tenant_id,
                client_id=config.client_id,
                client_secret=config.client_secret.get_secret_value(),
            )
        else:
            self._credential = WorkloadIdentityCredential(
                tenant_id=config.tenant_id,
                client_id=config.client_id,
            )

    def get_token(self) -> TokenResult:
        # azure-identity's AccessToken carries expires_on (absolute epoch
        # seconds), so we never decode the token to learn its expiry.
        access_token = self._credential.get_token(self._scope)
        return TokenResult(access_token.token, float(access_token.expires_on))

    @classmethod
    def create(cls, config: Optional[dict]) -> "AzureEntraTokenProvider":
        return cls(AzureEntraTokenProviderConfig.model_validate(config or {}))
