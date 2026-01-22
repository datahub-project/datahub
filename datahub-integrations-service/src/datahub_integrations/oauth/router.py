"""
OAuth router for AI Plugin user authentication.

This module provides FastAPI endpoints for:
- Initiating OAuth flows for AI plugins
- Handling OAuth callbacks
- Saving user API keys
- Disconnecting user credentials

Endpoints:
  AUTHENTICATED (requires DataHub JWT token):
    Internal paths: /private/oauth/plugins/... OR /public/oauth/plugins/...
    External path: /integrations/oauth/plugins/... (via frontend proxy → /public/...)
    - POST /{pluginId}/connect - Initiate OAuth flow
    - POST /{pluginId}/api-key - Save user API key
    - DELETE /{pluginId}/disconnect - Remove credentials

    Note: These endpoints are mounted on BOTH /private and /public routers.
    Security is enforced via JWT token validation in the endpoint handlers,
    not by the path prefix. The /public path is used by the frontend.

  UNAUTHENTICATED (external callback from OAuth providers):
    Internal path: /public/oauth/...
    External path: /integrations/oauth/... (via frontend proxy)
    - GET /callback - Handle OAuth callback (RECOMMENDED - single fixed URL)
    - GET /plugins/{pluginId}/callback - Legacy per-plugin callback (deprecated)

CALLBACK URL DESIGN:
    We use a SINGLE, FIXED callback URL for all OAuth providers:

        https://datahub.example.com/integrations/oauth/callback

    This solves the chicken-and-egg problem where you need to register the
    callback URL with an OAuth provider BEFORE creating the plugin configuration
    in DataHub (since the plugin URN isn't known until after creation).

    The plugin is identified from the OAuth `state` parameter, which already
    contains the plugin_id. This is a standard OAuth pattern.

URL PATH MAPPING:
    The DataHub frontend proxies requests from /integrations/* to the
    integrations service at /public/*. OAuth providers should be registered
    with the EXTERNAL path:

    External (OAuth provider registration):
        https://datahub.example.com/integrations/oauth/callback

    Internal (what integrations service receives):
        http://integrations-service:9003/public/oauth/callback

    This mapping is defined in:
        datahub-frontend/app/controllers/IntegrationsController.java

SECURITY MODEL:
    Authenticated endpoints use Bearer token authentication (JWT). Token validity
    is verified by making a GraphQL call to GMS, which validates the signature.

    1. AUTHENTICATED ENDPOINTS (connect, api-key, disconnect):
       - Require Authorization: Bearer <token> header
       - Token is VALIDATED by calling GMS GraphQL API with the token
       - If GMS accepts the token, it's valid; if rejected, authentication fails
       - User URN is retrieved from GMS (authoritative source), not decoded locally
       - This prevents spoofed/forged tokens from being accepted

       WHY WE VALIDATE VIA GMS (not local JWT verification):
       - This service doesn't have access to the JWT signing secret
       - The frontend proxy (/integrations/* -> /public/*) passes through
         Authorization headers without validation
       - An attacker could craft a fake JWT and send it directly
       - Without validation, we would accept fake tokens and write data
       - By calling GMS first, we ensure only valid tokens are accepted

       DEFENSE IN DEPTH:
       - Token validated BEFORE any writes occur
       - User URN comes from GMS response, not from potentially-forged token
       - Even if validation is bypassed, credential writes use user-scoped URNs
         that won't be accessible to other users

    2. CALLBACK ENDPOINTS (OAuth provider callbacks):
       - No authentication required (OAuth provider redirects browser here)
       - User identity comes from OAuth state parameter (stored during connect)
       - State is validated against the in-memory state store
       - One-time use: state is consumed after successful validation
       - User's auth token is stored in state during connect, used for
         subsequent GraphQL calls to update user settings
"""

from typing import Annotated, Any, Dict, Optional
from urllib.parse import urlencode

import fastapi
import jwt
from fastapi import Depends, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from loguru import logger
from pydantic import BaseModel

from datahub_integrations.app import DATAHUB_FRONTEND_URL, graph
from datahub_integrations.oauth.credential_store import (
    DataHubConnectionCredentialStore,
    OAuthTokens,
)
from datahub_integrations.oauth.state_store import (
    InMemoryOAuthStateStore,
    generate_code_challenge,
    generate_code_verifier,
)

# Create routers for OAuth endpoints:
# - authenticated_router: endpoints that require JWT authentication (connect, api-key, disconnect)
# - callback_router: endpoints for OAuth provider callbacks (no auth required)
# Both routers are mounted on external_router (/public) and accessible via /integrations/ proxy.
# Security for authenticated_router is enforced via Depends(get_authenticated_user) in handlers.
authenticated_router = fastapi.APIRouter(prefix="/oauth/plugins", tags=["OAuth"])
callback_router = fastapi.APIRouter(prefix="/oauth", tags=["OAuth"])

# Global state store (singleton for the service)
# In Phase 3, this would be replaced with a Redis-backed implementation
_state_store: Optional[InMemoryOAuthStateStore] = None


def get_state_store() -> InMemoryOAuthStateStore:
    """
    Get the global OAuth state store.

    Creates a singleton instance on first access.

    Returns:
        The OAuth state store instance.
    """
    global _state_store
    if _state_store is None:
        _state_store = InMemoryOAuthStateStore(
            ttl_seconds=600,  # 10 minutes
            max_states=10000,
        )
    return _state_store


def get_credential_store() -> DataHubConnectionCredentialStore:
    """
    Get a credential store instance.

    Returns:
        A credential store backed by DataHubConnection.
    """
    return DataHubConnectionCredentialStore(graph=graph)


# ═══════════════════════════════════════════════════════════════════════════════
# Request/Response Models
# ═══════════════════════════════════════════════════════════════════════════════


class ConnectRequest(BaseModel):
    """Request to initiate an OAuth connection.

    Currently empty - the callback URL is constructed server-side using
    DATAHUB_FRONTEND_URL. Future fields could include scopes override, etc.
    """

    pass


class ConnectResponse(BaseModel):
    """Response with the authorization URL to redirect to."""

    authorization_url: str


class ApiKeyRequest(BaseModel):
    """Request to save a user's API key."""

    api_key: str


class ApiKeyResponse(BaseModel):
    """Response after saving an API key."""

    success: bool
    connection_urn: str


class DisconnectResponse(BaseModel):
    """Response after disconnecting credentials."""

    success: bool


class CallbackSuccessResponse(BaseModel):
    """Response for successful OAuth callback (for popup flow)."""

    success: bool
    plugin_id: str
    connection_urn: str


# ═══════════════════════════════════════════════════════════════════════════════
# Authentication Helper Functions
# ═══════════════════════════════════════════════════════════════════════════════


def get_auth_token(authorization: Optional[str] = Header(None)) -> str:
    """
    Extract and validate the Bearer token from the Authorization header.

    Args:
        authorization: The Authorization header value (injected by FastAPI).

    Returns:
        The extracted token string.

    Raises:
        HTTPException: 401 if authorization header is missing or invalid.
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
        )

    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header format; expected 'Bearer <token>'",
        )

    token = authorization.split(" ", 1)[1]
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Empty token",
        )

    return token


def get_user_urn_from_token(token: str) -> str:
    """
    Extract the user URN from a JWT token WITHOUT signature verification.

    WARNING: This function does NOT validate the token. It only extracts claims.
    DO NOT use this for authentication of incoming requests. For that, use
    validate_token_and_get_user() which validates via GMS.

    USE CASES FOR THIS FUNCTION:
    - Extracting user URN from tokens stored in OAuth state (which were validated
      at connect time via validate_token_and_get_user)
    - Logging/debugging purposes where validation already happened

    WHY NO SIGNATURE VERIFICATION:
    This service doesn't have access to the JWT signing secret. For incoming
    requests, we validate tokens by calling GMS (see validate_token_and_get_user).
    For tokens retrieved from OAuth state, they were already validated when stored.

    Args:
        token: The JWT token string.

    Returns:
        The user URN from the 'sub' claim (e.g., "urn:li:corpuser:johndoe").

    Raises:
        HTTPException: 401 if the token cannot be decoded or lacks 'sub' claim.
    """
    try:
        # Decode without signature verification - we only need the 'sub' claim
        # Signature verification would require access to the secret/public key
        payload = jwt.decode(token, options={"verify_signature": False})

        # JWT sub claim is typically just the user ID (e.g., "admin"), not the full URN
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="JWT token missing 'sub' claim",
            )

        # Convert to full URN format if not already
        if user_id.startswith("urn:li:corpuser:"):
            user_urn = user_id
        else:
            user_urn = f"urn:li:corpuser:{user_id}"

        if not user_urn:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token missing 'sub' claim",
            )

        return str(user_urn)

    except jwt.InvalidTokenError as e:
        logger.warning(f"Failed to decode JWT token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token format",
        ) from None


def validate_token_and_get_user(token: str) -> str:
    """
    Validate a JWT token by making a GraphQL call to GMS.

    This function serves as the authoritative token validation for OAuth endpoints.
    Instead of verifying the JWT signature locally (which would require access to
    the signing secret), we validate by making a GraphQL call to GMS with the token.
    If GMS accepts the token, it's valid. If GMS rejects it, authentication fails.

    WHY THIS APPROACH:
    The integrations service is accessible via the frontend proxy at /integrations/*,
    which maps to /public/* on this service. The frontend proxy passes through
    Authorization headers from incoming requests without validation. This creates
    a potential attack vector:

    1. Attacker crafts a fake JWT with arbitrary claims (e.g., sub: "admin")
    2. Attacker sends request to /integrations/oauth/plugins/xxx/connect
    3. Frontend proxy forwards the fake Authorization header
    4. Without validation, this service would accept the fake token

    By calling GMS first, we ensure:
    - Only tokens with valid signatures are accepted
    - The user URN comes from GMS (authoritative), not from the potentially-forged token
    - Validation happens BEFORE any writes (credentials, state) occur

    Args:
        token: The JWT token to validate.

    Returns:
        The authenticated user's URN from GMS.

    Raises:
        HTTPException: 401 if the token is invalid or GMS rejects it.
    """
    import httpx

    # Get the GMS URL from the existing graph client
    gms_url = f"{graph._gms_server}/api/graphql"

    # Use the 'me' query to validate token and get user identity
    # This is a lightweight query that returns the authenticated user
    query = """
    query GetMe {
        me {
            corpUser {
                urn
            }
        }
    }
    """

    try:
        response = httpx.post(
            gms_url,
            json={"query": query},
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=10.0,
        )

        # Check for HTTP-level auth failures
        if response.status_code == 401:
            logger.warning("Token validation failed: GMS returned 401")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
            )

        if response.status_code == 403:
            logger.warning("Token validation failed: GMS returned 403")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied",
            )

        response.raise_for_status()
        result = response.json()

        # Check for GraphQL-level errors
        if result.get("errors"):
            error_msg = result["errors"][0].get("message", "Unknown error")
            logger.warning(f"Token validation failed: GraphQL error - {error_msg}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token validation failed",
            )

        # Extract user URN from response
        user_urn = result.get("data", {}).get("me", {}).get("corpUser", {}).get("urn")

        if not user_urn:
            logger.warning("Token validation failed: No user URN in response")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not determine user identity",
            )

        logger.debug(f"Token validated successfully for user: {user_urn}")
        return user_urn

    except httpx.RequestError as e:
        logger.error(f"Token validation failed: Network error - {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service unavailable",
        ) from e


def get_authenticated_user(authorization: Optional[str] = Header(None)) -> str:
    """
    FastAPI dependency to get the authenticated user's URN from Bearer token.

    This is the primary authentication dependency for OAuth endpoints.
    It extracts the JWT token from the Authorization header and validates it
    by making a GraphQL call to GMS. The user URN is returned from GMS's
    response, not decoded locally from the token.

    SECURITY NOTE:
    This function validates the token via GMS rather than decoding the JWT
    locally. This is necessary because:
    - We don't have access to the JWT signing secret
    - The frontend proxy passes Authorization headers without validation
    - Attackers could send forged JWTs that would be accepted without validation
    See validate_token_and_get_user() for detailed explanation.

    Usage:
        @router.post("/endpoint")
        async def my_endpoint(user_urn: str = Depends(get_authenticated_user)):
            ...

    Args:
        authorization: The Authorization header (injected by FastAPI).

    Returns:
        The authenticated user's URN (from GMS, authoritative source).

    Raises:
        HTTPException: 401 if not authenticated, token is invalid, or GMS rejects it.
    """
    token = get_auth_token(authorization)
    return validate_token_and_get_user(token)


def build_oauth_callback_url(plugin_id: Optional[str] = None) -> str:
    """
    Build the OAuth callback URL.

    Returns a SINGLE, FIXED callback URL that works for ALL OAuth providers.
    This solves the chicken-and-egg problem where you need to register the
    callback URL BEFORE creating the plugin configuration (since the plugin
    URN isn't known until after creation).

    The plugin is identified from the OAuth `state` parameter during the
    callback, not from the URL path. This is a standard OAuth pattern.

    URL Path Mapping:
        - External URL (for OAuth providers): /integrations/oauth/callback
        - Frontend proxy remaps: /integrations/* → /public/*
        - Integrations service receives: /public/oauth/callback

    This mapping is defined in datahub-frontend/app/controllers/IntegrationsController.java

    Args:
        plugin_id: DEPRECATED - This parameter is ignored. The callback URL
                   is fixed and doesn't include the plugin ID.

    Returns:
        The fixed callback URL (e.g., "https://datahub.example.com/integrations/oauth/callback").
    """
    # Note: External URLs use /integrations/ which the frontend proxy remaps to /public/
    # The integrations service listens on /public/oauth/callback
    # but OAuth providers call the external /integrations/ path
    #
    # The plugin_id parameter is kept for backwards compatibility but is ignored.
    # Plugin identification happens via the state parameter in the callback.
    return f"{DATAHUB_FRONTEND_URL}/integrations/oauth/callback"


def get_plugin_config(plugin_id: str) -> dict:
    """
    Get the AI plugin configuration from GlobalSettings.

    Args:
        plugin_id: The ID of the AI plugin.

    Returns:
        The plugin configuration dictionary.

    Raises:
        HTTPException: If the plugin is not found or not configured for OAuth.
    """
    try:
        result = graph.execute_graphql(
            query="""
query GetGlobalSettings {
  globalSettings {
    aiPlugins {
      id
      authType
      oauthConfig {
        serverUrn
        requiredScopes
      }
    }
  }
}
""".strip()
        )

        ai_plugins = result.get("globalSettings", {}).get("aiPlugins") or []

        for plugin in ai_plugins:
            if plugin.get("id") == plugin_id:
                return plugin

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Plugin not found: {plugin_id}",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get plugin config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve plugin configuration",
        ) from None


def get_oauth_server_config(server_urn: str) -> dict:
    """
    Get the OAuth authorization server configuration.

    Args:
        server_urn: The URN of the OAuthAuthorizationServer entity.

    Returns:
        The server configuration dictionary with resolved client secret.

    Raises:
        HTTPException: If the server is not found.
    """
    try:
        result = graph.execute_graphql(
            query="""
query GetOAuthServer($urn: String!) {
  oauthAuthorizationServer(urn: $urn) {
    urn
    properties {
      displayName
      authorizationUrl
      tokenUrl
      clientId
      clientSecretUrn
      scopes
      tokenAuthMethod
      authLocation
      authHeaderName
      authScheme
      authQueryParam
    }
  }
}
""".strip(),
            variables={"urn": server_urn},
        )

        server = result.get("oauthAuthorizationServer")
        if not server:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"OAuth server not found: {server_urn}",
            )

        properties = server["properties"]

        # Resolve the client secret if a secret URN is provided
        client_secret_urn = properties.get("clientSecretUrn")
        if client_secret_urn:
            client_secret = _resolve_secret_value(client_secret_urn)
            properties["clientSecret"] = client_secret

        return properties

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get OAuth server config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve OAuth server configuration",
        ) from None


def _resolve_secret_value(secret_urn: str) -> Optional[str]:
    """
    Resolve a secret value from its URN.

    Args:
        secret_urn: The URN of the DataHubSecret entity.
                    Format: urn:li:dataHubSecret:<secret-name>

    Returns:
        The secret value, or None if not found.
    """
    # Extract secret name from URN
    # URN format: urn:li:dataHubSecret:<secret-name>
    prefix = "urn:li:dataHubSecret:"
    if not secret_urn.startswith(prefix):
        logger.warning(f"Invalid secret URN format: {secret_urn}")
        return None

    secret_name = secret_urn[len(prefix) :]

    try:
        result = graph.execute_graphql(
            query="""
query GetSecretValues($input: GetSecretValuesInput!) {
    getSecretValues(input: $input) {
        name
        value
    }
}
""".strip(),
            variables={"input": {"secrets": [secret_name]}},
        )

        secret_values = result.get("getSecretValues", [])
        # getSecretValues returns the decrypted value for the requested secret(s)
        # The returned 'name' is the display name, which may differ from the URN ID
        # Since we're querying for a single specific secret, return the first result
        if secret_values:
            return secret_values[0].get("value")

        logger.warning(f"Secret not found: {secret_name}")
        return None

    except Exception as e:
        logger.error(f"Failed to resolve secret {secret_name}: {e}")
        return None


def build_authorization_url(
    server_config: dict,
    redirect_uri: str,
    code_challenge: str,
    additional_scopes: Optional[list[str]] = None,
) -> str:
    """
    Build the OAuth authorization URL.

    Args:
        server_config: The OAuth server configuration.
        redirect_uri: The callback URI.
        code_challenge: The PKCE code challenge.
        additional_scopes: Additional scopes to request.

    Returns:
        The authorization URL (without state parameter).
    """
    base_scopes = server_config.get("scopes") or []
    all_scopes = list(set(base_scopes + (additional_scopes or [])))

    params = {
        "response_type": "code",
        "client_id": server_config["clientId"],
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }

    if all_scopes:
        params["scope"] = " ".join(all_scopes)

    return f"{server_config['authorizationUrl']}?{urlencode(params)}"


async def exchange_code_for_tokens(
    server_config: dict,
    code: str,
    redirect_uri: str,
    code_verifier: str,
) -> OAuthTokens:
    """
    Exchange an authorization code for OAuth tokens.

    Args:
        server_config: The OAuth server configuration.
        code: The authorization code from the callback.
        redirect_uri: The redirect URI used in the authorization request.
        code_verifier: The PKCE code verifier.

    Returns:
        The OAuth tokens.

    Raises:
        HTTPException: If the token exchange fails.
    """
    import base64

    import httpx

    token_url = server_config["tokenUrl"]
    client_id = server_config["clientId"]
    client_secret = server_config.get("clientSecret")

    # Get authentication method settings
    # tokenAuthMethod: "BASIC" | "POST_BODY" | "NONE" (defaults to POST_BODY)
    token_auth_method = server_config.get("tokenAuthMethod", "POST_BODY")
    auth_scheme = server_config.get("authScheme")  # e.g., "Bearer", "Basic", "Token"
    auth_header_name = server_config.get("authHeaderName", "Authorization")

    # Base request data (always included)
    data: dict[str, str] = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
    }

    # Build headers
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        # GitHub (and some other providers) require Accept header to return JSON
        # Without it, they return application/x-www-form-urlencoded
        "Accept": "application/json",
    }

    # Handle authentication based on tokenAuthMethod
    if token_auth_method == "BASIC":
        # HTTP Basic Auth: credentials in Authorization header
        # Format: Authorization: Basic base64(client_id:client_secret)
        if client_secret:
            credentials = base64.b64encode(
                f"{client_id}:{client_secret}".encode()
            ).decode()
            headers[auth_header_name] = f"Basic {credentials}"
        # client_id still goes in body for some providers
        data["client_id"] = client_id
    elif token_auth_method == "CUSTOM" and auth_scheme and client_secret:
        # Custom auth scheme (e.g., "Token" for dbt Cloud)
        # Format: Authorization: {authScheme} {client_secret}
        # NOTE: authScheme is normally used for formatting access tokens when calling APIs,
        # but when tokenAuthMethod is CUSTOM, it's also used for token endpoint auth.
        headers[auth_header_name] = f"{auth_scheme} {client_secret}"
        data["client_id"] = client_id
    elif token_auth_method == "NONE":
        # No client authentication (public clients)
        # Only client_id goes in body, no secret
        data["client_id"] = client_id
    else:
        # POST_BODY (default): credentials in request body
        data["client_id"] = client_id
        if client_secret:
            data["client_secret"] = client_secret

    logger.debug(
        f"Token exchange using auth method: {token_auth_method}, "
        f"auth_scheme: {auth_scheme}, header: {auth_header_name}"
    )
    logger.debug(f"Token exchange URL: {token_url}")
    logger.debug(f"Token exchange data keys: {list(data.keys())}")
    logger.debug(
        f"Has client_secret: {client_secret is not None and len(client_secret) > 0}"
    )
    logger.debug(f"Authorization header set: {'Authorization' in headers}")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                token_url,
                data=data,
                headers=headers,
            )

            if response.status_code != 200:
                logger.error(
                    f"Token exchange failed: {response.status_code} - {response.text}"
                )
                logger.error(f"Response headers: {dict(response.headers)}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Failed to exchange authorization code for tokens",
                )

            token_data = response.json()
            logger.debug(f"Token response keys: {list(token_data.keys())}")

            # Check for OAuth error response
            if "error" in token_data:
                error = token_data.get("error")
                error_description = token_data.get(
                    "error_description", "No description"
                )
                logger.error(
                    f"OAuth provider returned error: {error} - {error_description}"
                )
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"OAuth error: {error} - {error_description}",
                )

            # Validate required field
            if "access_token" not in token_data:
                logger.error(
                    f"Token response missing access_token. Keys: {list(token_data.keys())}"
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Token response missing access_token",
                )

            # Calculate expires_at from expires_in if provided
            import time

            expires_at = None
            if "expires_in" in token_data:
                expires_at = time.time() + token_data["expires_in"]

            return OAuthTokens(
                access_token=token_data["access_token"],
                refresh_token=token_data.get("refresh_token"),
                expires_at=expires_at,
                token_type=token_data.get("token_type", "Bearer"),
                scope=token_data.get("scope"),
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Token exchange error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Token exchange failed",
        ) from None


# ═══════════════════════════════════════════════════════════════════════════════
# OAuth Endpoints
# ═══════════════════════════════════════════════════════════════════════════════


@authenticated_router.post("/{plugin_id}/connect", response_model=ConnectResponse)
async def initiate_oauth_connect(
    plugin_id: str,
    user_urn: Annotated[str, Depends(get_authenticated_user)],
    auth_token: Annotated[str, Depends(get_auth_token)],
    state_store: Annotated[InMemoryOAuthStateStore, Depends(get_state_store)],
) -> ConnectResponse:
    """
    Initiate an OAuth connection flow for a plugin.

    This endpoint:
    1. Validates the plugin exists and uses OAuth
    2. Retrieves the OAuth server configuration
    3. Generates PKCE code verifier/challenge
    4. Creates OAuth state in the store (including user's auth token for callback)
    5. Returns the authorization URL for the frontend to redirect to

    The callback URL is constructed server-side using DATAHUB_FRONTEND_URL.
    This URL must be pre-registered with the OAuth provider.

    Authentication:
        Requires Authorization: Bearer <token> header.
        User identity is extracted from the JWT 'sub' claim.
        The token is stored in state for use during the callback.

    Args:
        plugin_id: The ID of the AI plugin to connect.
        user_urn: The authenticated user's URN (injected from token).
        auth_token: The user's auth token (injected from header).
        state_store: The OAuth state store (injected).

    Returns:
        ConnectResponse with the authorization URL.
    """

    # Get plugin configuration
    plugin_config = get_plugin_config(plugin_id)

    if plugin_config.get("authType") != "USER_OAUTH":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Plugin {plugin_id} does not use OAuth authentication",
        )

    oauth_config = plugin_config.get("oauthConfig")
    if not oauth_config or not oauth_config.get("serverUrn"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Plugin {plugin_id} is not properly configured for OAuth",
        )

    # Get OAuth server configuration
    server_config = get_oauth_server_config(oauth_config["serverUrn"])

    # Generate PKCE code verifier and challenge
    code_verifier = generate_code_verifier()
    code_challenge = generate_code_challenge(code_verifier)

    # Build the callback URL server-side for security and consistency
    # This URL must be pre-registered with the OAuth provider
    redirect_uri = build_oauth_callback_url(plugin_id)

    # Build the base authorization URL
    base_auth_url = build_authorization_url(
        server_config=server_config,
        redirect_uri=redirect_uri,
        code_challenge=code_challenge,
        additional_scopes=oauth_config.get("requiredScopes"),
    )

    # Create and store state (including auth token for callback API calls)
    result = state_store.create_state(
        user_urn=user_urn,
        plugin_id=plugin_id,
        redirect_uri=redirect_uri,
        authorization_url=base_auth_url,
        code_verifier=code_verifier,
        auth_token=auth_token,
    )

    logger.info(f"Initiated OAuth flow for user {user_urn} and plugin {plugin_id}")

    return ConnectResponse(authorization_url=result.authorization_url)


@callback_router.get("/callback")
async def handle_oauth_callback_unified(
    state_store: Annotated[InMemoryOAuthStateStore, Depends(get_state_store)],
    credential_store: Annotated[
        DataHubConnectionCredentialStore, Depends(get_credential_store)
    ],
    code: str = Query(..., description="Authorization code from OAuth provider"),
    state: str = Query(..., description="OAuth state parameter (nonce)"),
    error: Optional[str] = Query(
        None, description="Error code if authorization failed"
    ),
    error_description: Optional[str] = Query(
        None, description="Error description if authorization failed"
    ),
) -> HTMLResponse:
    """
    Handle OAuth callback from authorization server (RECOMMENDED).

    This is the unified callback endpoint for ALL OAuth providers. The plugin
    is identified from the `state` parameter rather than the URL path.

    Benefits of this approach:
    - Single, fixed callback URL to register with OAuth providers
    - Can register callback URL BEFORE creating the plugin in DataHub
    - No chicken-and-egg problem with plugin URNs

    Register this URL with your OAuth provider:
        https://your-datahub.com/integrations/oauth/callback

    This endpoint:
    1. Validates the state parameter and extracts the plugin_id
    2. Exchanges the authorization code for tokens
    3. Stores the tokens in DataHubConnection
    4. Updates the user's CorpUserSettings
    5. Returns a success page for the popup window

    Args:
        code: The authorization code from the OAuth provider.
        state: The state nonce from the OAuth callback (contains plugin_id).
        error: Error code if authorization failed.
        error_description: Error description if authorization failed.
        state_store: The OAuth state store (injected).
        credential_store: The credential store (injected).

    Returns:
        HTML page for popup window communication.
    """
    # Handle OAuth errors (plugin_id unknown at this point)
    if error:
        logger.warning(f"OAuth error: {error} - {error_description}")
        return _create_popup_response(
            success=False,
            plugin_id="unknown",
            error=error_description or error,
        )

    # Validate and consume state - this gives us the plugin_id
    oauth_state = state_store.get_and_consume_state(state)

    if not oauth_state:
        logger.warning(f"Invalid or expired OAuth state: {state}")
        return _create_popup_response(
            success=False,
            plugin_id="unknown",
            error="Invalid or expired OAuth state. Please try connecting again.",
        )

    plugin_id = oauth_state.plugin_id

    try:
        # Get OAuth server config for token exchange
        plugin_config = get_plugin_config(plugin_id)
        server_config = get_oauth_server_config(
            plugin_config["oauthConfig"]["serverUrn"]
        )

        # Exchange code for tokens
        tokens = await exchange_code_for_tokens(
            server_config=server_config,
            code=code,
            redirect_uri=oauth_state.redirect_uri,
            code_verifier=oauth_state.code_verifier,
        )

        # Save tokens to DataHubConnection
        connection_urn = credential_store.save_oauth_tokens(
            user_urn=oauth_state.user_urn,
            plugin_id=plugin_id,
            tokens=tokens,
        )

        # Update user's CorpUserSettings (via GraphQL mutation)
        # Use the user's auth token from state for proper authorization
        await _update_user_plugin_settings(
            user_urn=oauth_state.user_urn,
            plugin_id=plugin_id,
            connection_urn=connection_urn,
            is_oauth=True,
            auth_token=oauth_state.auth_token,
        )

        logger.info(
            f"OAuth flow completed for user {oauth_state.user_urn} and plugin {plugin_id}"
        )

        return _create_popup_response(
            success=True,
            plugin_id=plugin_id,
            connection_urn=connection_urn,
        )

    except HTTPException as e:
        logger.error(f"OAuth callback failed for plugin {plugin_id}: {e.detail}")
        return _create_popup_response(
            success=False,
            plugin_id=plugin_id,
            error=e.detail,
        )
    except Exception as e:
        logger.exception(
            f"Unexpected error in OAuth callback for plugin {plugin_id}: {e}"
        )
        return _create_popup_response(
            success=False,
            plugin_id=plugin_id,
            error="An unexpected error occurred. Please try again.",
        )


@callback_router.get("/plugins/{plugin_id}/callback", deprecated=True)
async def handle_oauth_callback_legacy(
    plugin_id: str,
    state_store: Annotated[InMemoryOAuthStateStore, Depends(get_state_store)],
    credential_store: Annotated[
        DataHubConnectionCredentialStore, Depends(get_credential_store)
    ],
    code: str = Query(..., description="Authorization code from OAuth provider"),
    state: str = Query(..., description="OAuth state parameter (nonce)"),
    error: Optional[str] = Query(
        None, description="Error code if authorization failed"
    ),
    error_description: Optional[str] = Query(
        None, description="Error description if authorization failed"
    ),
) -> HTMLResponse:
    """
    Handle OAuth callback from authorization server (DEPRECATED).

    DEPRECATED: Use /oauth/callback instead. This endpoint is kept for
    backwards compatibility but the unified callback is recommended.

    The unified callback (/oauth/callback) has these advantages:
    - Single, fixed URL to register with OAuth providers
    - Can register callback URL BEFORE creating the plugin
    - No chicken-and-egg problem with plugin URNs

    This endpoint:
    1. Validates the state parameter
    2. Exchanges the authorization code for tokens
    3. Stores the tokens in DataHubConnection
    4. Updates the user's CorpUserSettings
    5. Returns a success page for the popup window

    Args:
        plugin_id: The ID of the AI plugin (from URL path).
        code: The authorization code from the OAuth provider.
        state: The state nonce from the OAuth callback.
        error: Error code if authorization failed.
        error_description: Error description if authorization failed.
        state_store: The OAuth state store (injected).
        credential_store: The credential store (injected).

    Returns:
        HTML page for popup window communication.
    """
    # Handle OAuth errors
    if error:
        logger.warning(
            f"OAuth error for plugin {plugin_id}: {error} - {error_description}"
        )
        return _create_popup_response(
            success=False,
            plugin_id=plugin_id,
            error=error_description or error,
        )

    # Validate and consume state
    oauth_state = state_store.get_and_consume_state(state)

    if not oauth_state:
        logger.warning(f"Invalid or expired OAuth state: {state}")
        return _create_popup_response(
            success=False,
            plugin_id=plugin_id,
            error="Invalid or expired OAuth state. Please try connecting again.",
        )

    # Verify plugin_id matches
    if oauth_state.plugin_id != plugin_id:
        logger.warning(
            f"Plugin ID mismatch: expected {oauth_state.plugin_id}, got {plugin_id}"
        )
        return _create_popup_response(
            success=False,
            plugin_id=plugin_id,
            error="Plugin ID mismatch. Please try connecting again.",
        )

    try:
        # Get OAuth server config for token exchange
        plugin_config = get_plugin_config(plugin_id)
        server_config = get_oauth_server_config(
            plugin_config["oauthConfig"]["serverUrn"]
        )

        # Exchange code for tokens
        tokens = await exchange_code_for_tokens(
            server_config=server_config,
            code=code,
            redirect_uri=oauth_state.redirect_uri,
            code_verifier=oauth_state.code_verifier,
        )

        # Save tokens to DataHubConnection
        connection_urn = credential_store.save_oauth_tokens(
            user_urn=oauth_state.user_urn,
            plugin_id=plugin_id,
            tokens=tokens,
        )

        # Update user's CorpUserSettings (via GraphQL mutation)
        # Use the user's auth token from state for proper authorization
        await _update_user_plugin_settings(
            user_urn=oauth_state.user_urn,
            plugin_id=plugin_id,
            connection_urn=connection_urn,
            is_oauth=True,
            auth_token=oauth_state.auth_token,
        )

        logger.info(
            f"OAuth flow completed for user {oauth_state.user_urn} and plugin {plugin_id}"
        )

        return _create_popup_response(
            success=True,
            plugin_id=plugin_id,
            connection_urn=connection_urn,
        )

    except HTTPException as e:
        logger.error(f"OAuth callback failed: {e.detail}")
        return _create_popup_response(
            success=False,
            plugin_id=plugin_id,
            error=e.detail,
        )
    except Exception as e:
        logger.exception(f"Unexpected error in OAuth callback: {e}")
        return _create_popup_response(
            success=False,
            plugin_id=plugin_id,
            error="An unexpected error occurred. Please try again.",
        )


@authenticated_router.post("/{plugin_id}/api-key", response_model=ApiKeyResponse)
async def save_api_key(
    plugin_id: str,
    body: ApiKeyRequest,
    user_urn: Annotated[str, Depends(get_authenticated_user)],
    auth_token: Annotated[str, Depends(get_auth_token)],
    credential_store: Annotated[
        DataHubConnectionCredentialStore, Depends(get_credential_store)
    ],
) -> ApiKeyResponse:
    """
    Save a user's API key for a plugin.

    This endpoint:
    1. Validates the plugin exists and uses API key auth
    2. Stores the API key in DataHubConnection
    3. Updates the user's CorpUserSettings

    Authentication:
        Requires Authorization: Bearer <token> header.
        User identity is extracted from the JWT 'sub' claim.

    Args:
        plugin_id: The ID of the AI plugin.
        body: The API key request.
        user_urn: The authenticated user's URN (injected from token).
        auth_token: The user's auth token (injected from header).
        credential_store: The credential store (injected).

    Returns:
        ApiKeyResponse with success status and connection URN.
    """

    # Validate plugin configuration
    plugin_config = get_plugin_config(plugin_id)

    if plugin_config.get("authType") != "USER_API_KEY":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Plugin {plugin_id} does not use API key authentication",
        )

    # Save API key
    connection_urn = credential_store.save_api_key(
        user_urn=user_urn,
        plugin_id=plugin_id,
        api_key=body.api_key,
    )

    # Update user's CorpUserSettings
    # Use the user's auth token for proper authorization
    await _update_user_plugin_settings(
        user_urn=user_urn,
        plugin_id=plugin_id,
        connection_urn=connection_urn,
        is_oauth=False,
        auth_token=auth_token,
    )

    logger.info(f"Saved API key for user {user_urn} and plugin {plugin_id}")

    return ApiKeyResponse(success=True, connection_urn=connection_urn)


@authenticated_router.delete(
    "/{plugin_id}/disconnect", response_model=DisconnectResponse
)
async def disconnect_plugin(
    plugin_id: str,
    user_urn: Annotated[str, Depends(get_authenticated_user)],
    auth_token: Annotated[str, Depends(get_auth_token)],
    credential_store: Annotated[
        DataHubConnectionCredentialStore, Depends(get_credential_store)
    ],
) -> DisconnectResponse:
    """
    Disconnect a user's credentials for a plugin.

    This endpoint:
    1. Deletes the DataHubConnection with the user's credentials
    2. Updates the user's CorpUserSettings to remove the connection reference

    Authentication:
        Requires Authorization: Bearer <token> header.
        User identity is extracted from the JWT 'sub' claim.

    Args:
        plugin_id: The ID of the AI plugin.
        user_urn: The authenticated user's URN (injected from token).
        auth_token: The user's auth token (injected from header).
        credential_store: The credential store (injected).

    Returns:
        DisconnectResponse with success status.
    """

    # Delete credentials from DataHubConnection
    deleted = credential_store.delete_credentials(
        user_urn=user_urn,
        plugin_id=plugin_id,
    )

    # Always try to update user's CorpUserSettings to remove connection reference
    # Even if credentials weren't found, the reference might still exist in user settings
    await _remove_user_plugin_connection(
        user_urn=user_urn,
        plugin_id=plugin_id,
        auth_token=auth_token,
    )

    logger.info(
        f"Disconnected plugin {plugin_id} for user {user_urn} (credentials deleted: {deleted})"
    )

    return DisconnectResponse(success=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Helper Functions for User Settings Updates
# ═══════════════════════════════════════════════════════════════════════════════


def _execute_graphql_as_user(
    auth_token: str,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Execute a GraphQL query using the user's auth token.

    This allows making API calls on behalf of the user (using their permissions)
    rather than as the system user. Used for updating user settings after OAuth.

    Args:
        auth_token: The user's JWT auth token.
        query: The GraphQL query/mutation.
        variables: Optional query variables.

    Returns:
        The 'data' portion of the GraphQL response.

    Raises:
        Exception: If the query fails or returns errors.
    """
    import httpx

    # Get the GMS URL from the existing graph client
    gms_url = f"{graph._gms_server}/api/graphql"

    body: Dict[str, Any] = {"query": query}
    if variables:
        body["variables"] = variables

    response = httpx.post(
        gms_url,
        json=body,
        headers={
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        },
        timeout=30.0,
    )
    response.raise_for_status()
    result = response.json()

    if result.get("errors"):
        raise Exception(f"GraphQL errors: {result['errors']}")

    return result.get("data", {})


async def _update_user_plugin_settings(
    user_urn: str,
    plugin_id: str,
    connection_urn: str,
    is_oauth: bool,
    auth_token: Optional[str] = None,
) -> None:
    """
    Update the user's CorpUserSettings to reference the new connection.

    This uses the updateUserAiPluginSettings GraphQL mutation.
    If auth_token is provided, the mutation is executed as the user (more secure).
    Otherwise, falls back to the system user (less secure, not recommended).

    Args:
        user_urn: The user's URN.
        plugin_id: The plugin ID.
        connection_urn: The connection URN storing the credentials.
        is_oauth: Whether this is an OAuth connection (vs API key).
        auth_token: The user's auth token (from OAuth state). If provided,
                    the mutation runs as this user for proper authorization.
    """
    # Build the mutation input based on connection type
    if is_oauth:
        # For OAuth, we pass the connection URN directly
        # The Java resolver will set oauthConfig.connectionUrn
        mutation_input = {
            "pluginId": plugin_id,
            "oauthConnectionUrn": connection_urn,
        }
    else:
        # For API keys, pass a non-empty apiKey to trigger the Java resolver
        # to set apiKeyConfig.connectionUrn. The actual key value doesn't matter
        # here - it's already stored in DataHubConnection. This just signals
        # that the connection exists.
        mutation_input = {
            "pluginId": plugin_id,
            "apiKey": "connected",  # Non-empty value triggers connection setup
        }

    try:
        query = """
mutation UpdateUserAiPluginSettings($input: UpdateUserAiPluginSettingsInput!) {
  updateUserAiPluginSettings(input: $input)
}
""".strip()

        if auth_token:
            # Execute as the user (recommended for security)
            result = _execute_graphql_as_user(
                auth_token=auth_token,
                query=query,
                variables={"input": mutation_input},
            )
        else:
            # Fallback to system user (not recommended)
            logger.warning(
                f"No auth token provided for user settings update. "
                f"Falling back to system user for {user_urn}/{plugin_id}."
            )
            result = graph.execute_graphql(
                query=query,
                variables={"input": mutation_input},
            )

        success = result.get("updateUserAiPluginSettings", False)
        if success:
            logger.info(
                f"Updated user plugin settings: {user_urn}, {plugin_id}, "
                f"connection={connection_urn}, oauth={is_oauth}"
            )
        else:
            logger.warning(
                f"updateUserAiPluginSettings returned False for {user_urn}/{plugin_id}"
            )

    except Exception as e:
        # Log error but don't fail the OAuth flow
        # The credentials are already saved, user can retry the settings update
        logger.error(
            f"Failed to update user plugin settings for {user_urn}/{plugin_id}: {e}"
        )


async def _remove_user_plugin_connection(
    user_urn: str,
    plugin_id: str,
    is_oauth: bool = True,
    auth_token: Optional[str] = None,
) -> None:
    """
    Remove the connection reference from the user's CorpUserSettings.

    This uses the updateUserAiPluginSettings GraphQL mutation with
    disconnectOAuth=true or apiKey="" to remove the connection.

    Args:
        user_urn: The user's URN.
        plugin_id: The plugin ID.
        is_oauth: Whether this is an OAuth connection (vs API key).
        auth_token: The user's auth token. If provided, mutation runs as the user.
    """
    # Build the mutation input based on connection type
    if is_oauth:
        mutation_input = {
            "pluginId": plugin_id,
            "disconnectOAuth": True,
        }
    else:
        # Empty string signals disconnect for API key
        mutation_input = {
            "pluginId": plugin_id,
            "apiKey": "",
        }

    query = """
mutation UpdateUserAiPluginSettings($input: UpdateUserAiPluginSettingsInput!) {
  updateUserAiPluginSettings(input: $input)
}
""".strip()

    try:
        if auth_token:
            result = _execute_graphql_as_user(
                auth_token=auth_token,
                query=query,
                variables={"input": mutation_input},
            )
        else:
            result = graph.execute_graphql(
                query=query,
                variables={"input": mutation_input},
            )

        success = result.get("updateUserAiPluginSettings", False)
        if success:
            logger.info(
                f"Removed user plugin connection: {user_urn}, {plugin_id}, oauth={is_oauth}"
            )
        else:
            logger.warning(
                f"updateUserAiPluginSettings returned False when removing {user_urn}/{plugin_id}"
            )

    except Exception as e:
        logger.error(
            f"Failed to remove user plugin connection for {user_urn}/{plugin_id}: {e}"
        )


def _create_popup_response(
    success: bool,
    plugin_id: str,
    connection_urn: Optional[str] = None,
    error: Optional[str] = None,
) -> HTMLResponse:
    """
    Create an HTML response for the OAuth popup window.

    This page uses postMessage to communicate the result to the parent
    window and then closes itself.

    Args:
        success: Whether the OAuth flow succeeded.
        plugin_id: The plugin ID.
        connection_urn: The connection URN if successful.
        error: The error message if failed.

    Returns:
        HTMLResponse with JavaScript for popup communication.
    """
    import json

    result = {
        "type": "oauth_callback",
        "success": success,
        "pluginId": plugin_id,
    }

    if connection_urn:
        result["connectionUrn"] = connection_urn

    if error:
        result["error"] = error

    result_json = json.dumps(result)

    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>{"Connected!" if success else "Connection Failed"}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
            background: #f5f5f5;
        }}
        .container {{
            text-align: center;
            padding: 2rem;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .icon {{
            font-size: 48px;
            margin-bottom: 16px;
        }}
        .message {{
            color: #333;
            margin-bottom: 16px;
        }}
        .status {{
            color: {"#22c55e" if success else "#ef4444"};
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="icon">{"✓" if success else "✕"}</div>
        <h2 class="status">{"Connected Successfully!" if success else "Connection Failed"}</h2>
        <p class="message">
            {"You can now close this window." if success else (error or "Please try again.")}
        </p>
    </div>
    <script>
        // Send result to parent window
        if (window.opener) {{
            window.opener.postMessage({result_json}, '*');
        }}
        // Close the popup after a short delay
        setTimeout(function() {{
            window.close();
        }}, 2000);
    </script>
</body>
</html>
"""

    return HTMLResponse(content=html_content)
