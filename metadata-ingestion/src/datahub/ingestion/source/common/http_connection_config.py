from typing import Optional

from pydantic import Field, model_validator

from datahub.configuration.common import ConfigModel, TransparentSecretStr


class HTTPConnectionConfig(ConfigModel):
    """Authentication and TLS options for reading files over http(s)://."""

    token: Optional[TransparentSecretStr] = Field(
        default=None,
        description="Bearer token sent as an `Authorization: Bearer <token>` header. "
        "Mutually exclusive with username/password.",
    )
    username: Optional[str] = Field(
        default=None,
        description="Username for HTTP Basic authentication (requires password).",
    )
    password: Optional[TransparentSecretStr] = Field(
        default=None,
        description="Password for HTTP Basic authentication (requires username).",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Verify the server's TLS certificate. Disable only for trusted "
        "hosts with self-signed certificates.",
    )

    @model_validator(mode="after")
    def _validate_auth(self) -> "HTTPConnectionConfig":
        if self.token is not None and (
            self.username is not None or self.password is not None
        ):
            raise ValueError(
                "Set either token (bearer) or username/password (basic), not both."
            )
        if (self.username is None) != (self.password is None):
            raise ValueError(
                "Both username and password are required for HTTP basic authentication."
            )
        return self
