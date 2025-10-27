from enum import Enum
from typing import List, Optional

from pydantic import Field, SecretStr

from datahub.configuration import ConfigModel


class AzureAuthConfig(ConfigModel):
    client_secret: str = Field(
        description="Azure client secret"
    )
    client_id: str = Field(
        description="Azure client (Application) ID",
        default=None,
    )
    tenant_id: str = Field(
        description="Azure tenant (Directory) ID required when a `client_secret` is used as a credential.",
        default=None,
    )