from enum import Enum
from typing import List, Optional

from pydantic import Field, SecretStr

from datahub.configuration import ConfigModel


class OAuthIdentityProvider(Enum):
    MICROSOFT = "microsoft"
    OKTA = "okta"


class OAuthConfiguration(ConfigModel):
    provider: OAuthIdentityProvider = Field(
        description="Identity provider for oauth."
        "Supported providers are microsoft and okta."
    )
    authority_url: str = Field(description="Authority url of your identity provider")
    client_id: str = Field(description="client id of your registered application")
    scopes: List[str] = Field(description="scopes required to connect to snowflake")
    use_certificate: bool = Field(
        description="Do you want to use certificate and private key to authenticate using oauth",
        default=False,
    )
    client_secret: Optional[SecretStr] = Field(
        None, description="client secret of the application if use_certificate = false"
    )
    encoded_oauth_public_key: Optional[str] = Field(
        None, description="base64 encoded certificate content if use_certificate = true"
    )
    encoded_oauth_private_key: Optional[str] = Field(
        None, description="base64 encoded private key content if use_certificate = true"
    )
