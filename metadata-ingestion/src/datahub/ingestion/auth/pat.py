from __future__ import annotations

from typing import Optional

from pydantic import Field, SecretStr

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.emitter.token_provider import TokenProvider, TokenResult


class PatTokenProviderConfig(ConfigModel):
    token: Optional[SecretStr] = Field(
        default=None,
        description="The personal access token value.",
    )
    token_file: Optional[str] = Field(
        default=None,
        description="Path to a file containing the personal access token, "
        "e.g. a Kubernetes Secret mounted as a volume.",
    )


class PatTokenProvider(TokenProvider):
    """Presents a DataHub personal access token from a value or a file.

    This is the explicit spelling of the default static-token mechanism. The
    file variant re-reads the file on each refresh, so a rotated mounted
    Secret is picked up without a process restart.
    """

    def __init__(self, config: PatTokenProviderConfig) -> None:
        if (config.token is None) == (config.token_file is None):
            raise ConfigurationError(
                "The 'pat' token provider requires exactly one of 'token' or "
                "'token_file'."
            )
        self._token = config.token
        self._token_file = config.token_file

    def get_token(self) -> TokenResult:
        if self._token_file is not None:
            try:
                with open(self._token_file) as f:
                    return TokenResult(f.read().strip())
            except OSError as e:
                raise ConfigurationError(
                    f"Could not read the personal access token file "
                    f"'{self._token_file}': {e}"
                ) from e
        assert self._token is not None
        return TokenResult(self._token.get_secret_value())

    @classmethod
    def create(cls, config: Optional[dict]) -> "PatTokenProvider":
        return cls(PatTokenProviderConfig.model_validate(config or {}))
