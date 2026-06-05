import pytest

from datahub.configuration.common import ConfigurationError
from datahub.emitter.token_provider import CachingTokenProvider
from datahub.ingestion.auth.registry import (
    AuthConfig,
    build_token_provider,
)


def test_static_provider_resolves_and_wraps_in_caching():
    auth = AuthConfig(type="static", config={"token": "abc"})
    provider = build_token_provider(auth)
    assert isinstance(provider, CachingTokenProvider)
    assert provider.get_token() == "abc"


def test_unknown_type_raises_configuration_error():
    with pytest.raises(ConfigurationError):
        build_token_provider(AuthConfig(type="nope", config={}))


def test_entrypoint_providers_are_discoverable():
    # k8s_oidc has no heavy deps, so it must resolve via the entry point.
    auth = AuthConfig(type="k8s_oidc", config={"token_file": "/tmp/does-not-matter"})
    provider = build_token_provider(auth)
    assert provider is not None
