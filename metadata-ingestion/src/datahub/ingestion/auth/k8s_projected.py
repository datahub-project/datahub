from __future__ import annotations

from typing import Optional

from pydantic import Field

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.emitter.token_provider import TokenProvider, TokenResult

DEFAULT_K8S_TOKEN_FILE = "/var/run/secrets/eks.amazonaws.com/serviceaccount/token"


class K8sProjectedTokenProviderConfig(ConfigModel):
    token_file: str = Field(
        default=DEFAULT_K8S_TOKEN_FILE,
        description="Path to the projected Kubernetes service-account token file.",
    )
    audience: Optional[str] = Field(
        default=None,
        description="Informational: the audience the projected token was minted for "
        "(set on the pod's serviceAccountToken volume; must match GMS allowed audiences).",
    )


class K8sProjectedTokenProvider(TokenProvider):
    """Presents a projected Kubernetes service-account token (an OIDC JWT) directly.

    Covers AWS/EKS (GMS trusts the cluster OIDC issuer) and AKS. The kubelet
    rotates the token file in place, so the file is re-read on each refresh.
    """

    def __init__(self, config: K8sProjectedTokenProviderConfig) -> None:
        self._token_file = config.token_file

    def get_token(self) -> TokenResult:
        # No token-endpoint response to read an expiry from, and we don't decode
        # the JWT. Report expires_at=None: the caching layer re-reads the file
        # each call (a cheap tmpfs read), so kubelet rotation is picked up.
        try:
            with open(self._token_file) as f:
                return TokenResult(f.read().strip())
        except OSError as e:
            raise ConfigurationError(
                f"Could not read projected service-account token at "
                f"'{self._token_file}': {e}. Ensure the pod mounts a "
                f"serviceAccountToken projected volume."
            ) from e

    @classmethod
    def create(cls, config: Optional[dict]) -> "K8sProjectedTokenProvider":
        return cls(K8sProjectedTokenProviderConfig.model_validate(config or {}))
