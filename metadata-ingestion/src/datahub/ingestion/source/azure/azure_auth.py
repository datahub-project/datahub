"""Unified Azure authentication module for DataHub connectors.

This module provides a reusable authentication configuration that can be used
across all Azure connectors (ADF, Synapse, Fabric, etc.).

Supports multiple authentication methods:
- Service Principal (client_id + client_secret + tenant_id)
- Managed Identity (system-assigned or user-assigned)
- Azure CLI credentials (for local development)
- DefaultAzureCredential (auto-detects environment)
"""

from typing import Optional

from azure.core.credentials import TokenCredential
from azure.identity import (
    AzureCliCredential,
    ClientSecretCredential,
    DefaultAzureCredential,
    ManagedIdentityCredential,
)
from pydantic import Field, SecretStr, model_validator

from datahub.configuration import ConfigModel
from datahub.utilities.str_enum import StrEnum


class AzureAuthenticationMethod(StrEnum):
    """Supported Azure authentication methods.

    - DEFAULT: Uses DefaultAzureCredential which auto-detects credentials from
      environment variables, managed identity, Azure CLI, etc.
    - SERVICE_PRINCIPAL: Uses client ID, client secret, and tenant ID
    - MANAGED_IDENTITY: Uses Azure Managed Identity (system or user-assigned)
    - CLI: Uses Azure CLI credential (requires `az login`)
    """

    DEFAULT = "default"
    SERVICE_PRINCIPAL = "service_principal"
    MANAGED_IDENTITY = "managed_identity"
    CLI = "cli"


class AzureCredentialConfig(ConfigModel):
    """Unified Azure authentication configuration.

    This class provides a reusable authentication configuration that can be
    composed into any Azure connector's configuration. It supports multiple
    authentication methods and returns a TokenCredential that works with
    any Azure SDK client.

    Example usage in a connector config:
        class MyAzureConnectorConfig(ConfigModel):
            credential: AzureCredentialConfig = Field(
                default_factory=AzureCredentialConfig,
                description="Azure authentication configuration"
            )
            subscription_id: str = Field(...)
    """

    authentication_method: AzureAuthenticationMethod = Field(
        default=AzureAuthenticationMethod.DEFAULT,
        description=(
            "Authentication method to use. Options: "
            "'default' (auto-detects from environment), "
            "'service_principal' (client ID + secret + tenant), "
            "'managed_identity' (Azure Managed Identity), "
            "'cli' (Azure CLI credential). "
            "Recommended: Use 'default' which tries multiple methods automatically."
        ),
    )

    # Service Principal credentials (required when authentication_method = "service_principal")
    client_id: Optional[str] = Field(
        default=None,
        description=(
            "Azure Application (client) ID. Required for service_principal authentication. "
            "Find this in Azure Portal > App registrations > Your app > Overview."
        ),
    )
    client_secret: Optional[SecretStr] = Field(
        default=None,
        description=(
            "Azure client secret. Required for service_principal authentication. "
            "Create in Azure Portal > App registrations > Your app > Certificates & secrets."
        ),
    )
    tenant_id: Optional[str] = Field(
        default=None,
        description=(
            "Azure tenant (directory) ID. Required for service_principal authentication. "
            "Find this in Azure Portal > Microsoft Entra ID > Overview."
        ),
    )

    # Managed Identity options (optional, for user-assigned managed identity)
    managed_identity_client_id: Optional[str] = Field(
        default=None,
        description=(
            "Client ID for user-assigned managed identity. "
            "Leave empty to use system-assigned managed identity. "
            "Only used when authentication_method is 'managed_identity'."
        ),
    )

    # Additional options for DefaultAzureCredential
    exclude_cli_credential: bool = Field(
        default=False,
        description=(
            "When using 'default' authentication, exclude Azure CLI credential. "
            "Useful in production to avoid accidentally using developer credentials."
        ),
    )
    exclude_environment_credential: bool = Field(
        default=False,
        description=(
            "When using 'default' authentication, exclude environment variables. "
            "Environment variables checked: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID."
        ),
    )
    exclude_managed_identity_credential: bool = Field(
        default=False,
        description=(
            "When using 'default' authentication, exclude managed identity. "
            "Useful during local development when managed identity is not available."
        ),
    )

    def get_credential(self) -> TokenCredential:
        """Get Azure credential based on the configured authentication method.

        Returns:
            TokenCredential: An Azure credential object that can be used with
                any Azure SDK client (e.g., DataFactoryManagementClient).

        Raises:
            ValueError: If required credentials are missing for the chosen method.
        """
        if self.authentication_method == AzureAuthenticationMethod.SERVICE_PRINCIPAL:
            # Validate all required fields (also validated in validate_credentials())
            if not self.client_secret:
                raise ValueError(
                    "client_secret is required for service_principal authentication"
                )
            if not self.tenant_id:
                raise ValueError(
                    "tenant_id is required for service_principal authentication"
                )
            if not self.client_id:
                raise ValueError(
                    "client_id is required for service_principal authentication"
                )
            return ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret.get_secret_value(),
            )

        elif self.authentication_method == AzureAuthenticationMethod.MANAGED_IDENTITY:
            return ManagedIdentityCredential(client_id=self.managed_identity_client_id)

        elif self.authentication_method == AzureAuthenticationMethod.CLI:
            return AzureCliCredential()

        else:  # DEFAULT
            return DefaultAzureCredential(
                exclude_cli_credential=self.exclude_cli_credential,
                exclude_environment_credential=self.exclude_environment_credential,
                exclude_managed_identity_credential=self.exclude_managed_identity_credential,
            )

    @model_validator(mode="after")
    def validate_credentials(self) -> "AzureCredentialConfig":
        """Validate that required credentials are provided for the chosen method."""
        if self.authentication_method == AzureAuthenticationMethod.SERVICE_PRINCIPAL:
            missing = []
            if not self.client_id:
                missing.append("client_id")
            if not self.client_secret:
                missing.append("client_secret")
            if not self.tenant_id:
                missing.append("tenant_id")

            if missing:
                raise ValueError(
                    f"Service principal authentication requires: {', '.join(missing)}. "
                    f"These can be found in Azure Portal > App registrations."
                )

        return self
