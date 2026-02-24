"""
OAuth module for AI Plugins and Provider OAuth integration.

This module provides OAuth state management and flow handling for user
authentication with external providers (e.g., Glean, Confluence, Jira).

Phase 2 implementation uses in-memory state storage with TTL for singleton
deployments. The interfaces are designed to allow easy migration to Redis
in the future.
"""

from datahub_integrations.oauth.credential_store import (
    AI_PLUGIN_PLATFORM_URN,
    ApiKeyCredential,
    CredentialStore,
    DataHubConnectionCredentialStore,
    OAuthTokens,
    UserCredentials,
    build_connection_urn,
)
from datahub_integrations.oauth.router import (
    authenticated_router as oauth_authenticated_router,
    build_oauth_callback_url,
    callback_router as oauth_callback_router,
    get_auth_token,
    get_authenticated_user,
    get_state_store,
    get_user_urn_from_token,
    validate_token_and_get_user,
)
from datahub_integrations.oauth.state_store import (
    CreateStateResult,
    InMemoryOAuthStateStore,
    OAuthState,
    OAuthStateStore,
    generate_code_challenge,
    generate_code_verifier,
)

__all__ = [
    # State storage
    "CreateStateResult",
    "InMemoryOAuthStateStore",
    "OAuthState",
    "OAuthStateStore",
    "generate_code_challenge",
    "generate_code_verifier",
    # Credential storage
    "AI_PLUGIN_PLATFORM_URN",
    "ApiKeyCredential",
    "CredentialStore",
    "DataHubConnectionCredentialStore",
    "OAuthTokens",
    "UserCredentials",
    "build_connection_urn",
    # Authentication utilities
    "build_oauth_callback_url",
    "get_auth_token",
    "get_authenticated_user",
    "get_state_store",
    "get_user_urn_from_token",
    "validate_token_and_get_user",
    # Routers
    "oauth_authenticated_router",
    "oauth_callback_router",
]
