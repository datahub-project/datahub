from typing import Optional

from pydantic import Field, field_validator, model_validator

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

    @field_validator("token", "username", "password", mode="before")
    @classmethod
    def _blank_to_none(cls, value: object) -> object:
        # Empty/whitespace-only strings are treated as "unset" so a blank field
        # never produces a "Bearer " header or an empty ("", "") basic-auth tuple.
        if isinstance(value, str) and not value.strip():
            return None
        return value

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

    def to_request_kwargs(self) -> dict:
        """Build the ``verify``/``headers``/``auth`` kwargs for a ``requests`` call.

        Only bearer and basic auth are exposed (not arbitrary headers) because
        ``requests`` strips the ``Authorization`` header on cross-host
        redirects, so a bearer token / basic credentials are not leaked to a
        redirected origin — a custom-header token would not get that protection.
        """
        kwargs: dict = {"verify": self.verify_ssl}
        if self.token is not None:
            kwargs["headers"] = {
                "Authorization": f"Bearer {self.token.get_secret_value()}"
            }
        elif self.username is not None and self.password is not None:
            kwargs["auth"] = (self.username, self.password.get_secret_value())
        return kwargs
