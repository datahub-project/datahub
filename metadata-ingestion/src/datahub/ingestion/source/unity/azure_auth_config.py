from pydantic import Field

from datahub.configuration import ConfigModel


class AzureAuthConfig(ConfigModel):
    client_secret: str = Field(description="Azure client secret")
    client_id: str = Field(
        description="Azure client (Application) ID",
    )
    tenant_id: str = Field(
        description="Azure tenant (Directory) ID required when a `client_secret` is used as a credential.",
    )
