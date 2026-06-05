from __future__ import annotations

from datahub.configuration.common import ConfigurationError, DynamicTypedConfig
from datahub.emitter.token_provider import (
    CachingTokenProvider,
    StaticTokenProvider,
    TokenProvider,
)
from datahub.ingestion.api.registry import PluginRegistry


class AuthConfig(DynamicTypedConfig):
    """Declarative token-provider config: a provider `type` plus its `config`."""


token_provider_registry = PluginRegistry[TokenProvider]()
# `static` is built-in; cloud providers are discovered via entry points (added later).
token_provider_registry.register("static", StaticTokenProvider)
token_provider_registry.register_from_entrypoint("datahub.token_provider.plugins")


def build_token_provider(auth: AuthConfig) -> TokenProvider:
    """Resolve a declarative AuthConfig into a refresh-wrapped TokenProvider."""
    try:
        provider_cls = token_provider_registry.get(auth.type)
    except Exception as e:
        # Unknown type (KeyError) or an unimportable lazy entry-point provider.
        raise ConfigurationError(
            f"Unknown or unavailable auth provider type '{auth.type}': {e}"
        ) from e
    raw = provider_cls.create(auth.config)
    return CachingTokenProvider(raw.get_token)
