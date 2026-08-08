from __future__ import annotations

from typing import Optional

from gql.transport.requests import RequestsHTTPTransport

from datahub.emitter.token_provider import TokenProviderAuth
from datahub.ingestion.auth.env import build_auth_config_from_env
from datahub.ingestion.auth.registry import AuthConfig, build_token_provider

_GRAPHQL_PATH = "/api/graphql"


def build_gql_transport(
    url: str,
    token: Optional[str] = None,
    auth: Optional[AuthConfig] = None,
    timeout: Optional[int] = None,
) -> RequestsHTTPTransport:
    """Build a gql transport that authenticates to DataHub.

    Precedence: an explicit declarative ``auth`` wins; then a static ``token``
    (baked as a Bearer header, backward-compatible); then env-based OAuth
    (``DATAHUB_AUTH_TYPE``) when the caller supplied no credentials. A resolved
    OAuth provider is installed as the transport's per-request auth so the token
    refreshes, never as a static header.
    """
    resolved_auth = auth
    if resolved_auth is None and token is None:
        resolved_auth = build_auth_config_from_env()

    if resolved_auth is not None:
        return RequestsHTTPTransport(
            url=url + _GRAPHQL_PATH,
            auth=TokenProviderAuth(build_token_provider(resolved_auth)),
            method="POST",
            timeout=timeout,
        )
    return RequestsHTTPTransport(
        url=url + _GRAPHQL_PATH,
        headers=({"Authorization": "Bearer " + token} if token is not None else None),
        method="POST",
        timeout=timeout,
    )
