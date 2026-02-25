from pydantic import Field, SecretStr

from datahub.configuration import ConfigModel


class AzureAuthConfig(ConfigModel):
    client_secret: SecretStr = Field(
        description="Azure application client secret used for authentication. This is a confidential credential that should be kept secure."
    )
    client_id: str = Field(
        description="Azure application (client) ID. This is the unique identifier for the registered Azure AD application.",
    )
    tenant_id: str = Field(
        description="Azure tenant (directory) ID. This identifies the Azure AD tenant where the application is registered.",
    )
