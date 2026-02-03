# Integrations Service Authentication Guide

This document explains how authentication works for the DataHub Integrations Service.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              External Clients                                │
│                    (Browser, OAuth Providers, API Clients)                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Production: API Gateway
                                      │ Local Dev: Frontend Proxy
                                      ▼
                     ┌────────────────────────────────┐
                     │   /public/* (external routes)   │
                     └────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Integrations Service (Port 9003)                          │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        /public/* (external)                             │ │
│  │  • Accessible via API Gateway (production) or Frontend (local)          │ │
│  │  • Receives cookies (PLAY_SESSION) and headers (Authorization)          │ │
│  │  • OAuth callbacks, browser requests, API endpoints                     │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        /private/* (internal)                            │ │
│  │  • Called directly by GMS and other internal services                   │ │
│  │  • NOT exposed through API Gateway or Frontend proxy                    │ │
│  │  • Service-to-service communication                                     │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                      ▲
                                      │
                                      │ Direct calls (no proxy)
                                      │
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GMS & Other Internal Services                             │
│  • Call /private/* endpoints directly                                        │
│  • No API Gateway or Frontend proxy involved                                 │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                    DataHub Frontend (Port 9002) - Local Only                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    IntegrationsController.java                       │    │
│  │  • Used for LOCAL DEBUGGING ONLY                                     │    │
│  │  • Remaps paths: /integrations/* → /public/*                         │    │
│  │  • STRIPS x-datahub-actor header (security measure)                  │    │
│  │  • In production, API Gateway handles /public/* routing              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## URL Path Mapping

### Production (API Gateway)

| External URL (Browser) | Internal URL (Integrations Service) | Notes                       |
| ---------------------- | ----------------------------------- | --------------------------- |
| `/integrations/foo`    | `/public/foo`                       | API Gateway routes directly |
| N/A (internal only)    | `/private/foo`                      | Not accessible externally   |

### Local Development (Frontend Proxy)

| External URL (Browser) | Internal URL (Integrations Service) | Notes                      |
| ---------------------- | ----------------------------------- | -------------------------- |
| `/integrations/foo`    | `/public/foo`                       | Frontend proxy remaps path |
| N/A (internal only)    | `/private/foo`                      | Not accessible externally  |

**Example:**

```
# External OAuth callback URL (registered with OAuth provider)
# NOTE: Single, fixed URL for ALL OAuth providers (plugin identified via state parameter)
https://datahub.example.com/integrations/oauth/callback

# In production, API Gateway routes to:
http://integrations-service:9003/public/oauth/callback

# In local development, frontend proxy remaps to:
http://integrations-service:9003/public/oauth/callback
```

## Router Types

### 1. Internal Router (`/private/*`)

**Purpose:** Endpoints called directly by GMS and other internal DataHub services, not by external clients.

**Characteristics:**

- Called directly by internal services (no proxy involved)
- Not exposed through the API Gateway or frontend proxy
- May use service-to-service authentication (x-datahub-actor header)
- Typically handles background operations initiated by GMS

**Example routes:**

- `/private/share/*` - Share functionality
- `/private/sql/*` - SQL execution

**Access pattern:**

```
GMS → http://integrations-service:9003/private/sql/execute
(Direct call, no API Gateway or frontend proxy)
```

### 2. External Router (`/public/*`)

**Purpose:** Endpoints accessible via the API Gateway (production) or frontend proxy (local) from external clients.

**Characteristics:**

- Accessible from browsers and external services
- Can receive browser requests with cookies
- Includes OAuth callbacks, API endpoints

**Example routes:**

- `/public/oauth/callback` - OAuth callbacks (single URL for all providers)
- `/public/dist/*` - Distribution endpoints

## Authentication Methods

### Method 1: JWT Bearer Token (Preferred)

**Used by:** MCP server, Chat API, OAuth private endpoints, API clients

**How it works:**

1. Client sends `Authorization: Bearer <jwt_token>` header
2. Service validates token with GMS via GraphQL API
3. GMS returns the authenticated user URN
4. Service uses the URN for data isolation and per-user operations

**Example:**

```python
from fastapi import Header, HTTPException, Depends
from datahub.ingestion.graph.client import DataHubGraph

def validate_token_and_get_user(token: str) -> str:
    """Validate token with GMS and get user URN."""
    graph = DataHubGraph(...)

    # GMS validates the token and returns the user
    result = graph.execute_graphql(
        query="""
        query Me {
            me {
                corpUser {
                    urn
                }
            }
        }
        """,
        token=token
    )

    if "errors" in result or not result.get("data", {}).get("me"):
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    return result["data"]["me"]["corpUser"]["urn"]

def get_auth_token(
    authorization: Optional[str] = Header(None),
    play_session: Optional[str] = Cookie(None, alias="PLAY_SESSION")
) -> str:
    """Extract token from Authorization header or PLAY_SESSION cookie."""
    # Prefer Authorization header
    if authorization:
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid Authorization header format")
        return authorization.split(" ", 1)[1]

    # Fallback to PLAY_SESSION cookie
    if play_session:
        # Parse JWT cookie from Play Framework
        parts = play_session.split(".")
        if len(parts) != 3:
            raise HTTPException(status_code=401, detail="Invalid PLAY_SESSION format")

        # Decode payload (add padding if needed)
        payload = parts[1]
        padding = 4 - (len(payload) % 4)
        if padding != 4:
            payload += "=" * padding

        decoded = json.loads(base64.urlsafe_b64decode(payload))
        token = decoded.get("data", {}).get("token")
        if not token:
            raise HTTPException(status_code=401, detail="No token in PLAY_SESSION")
        return token

    raise HTTPException(status_code=401, detail="Missing Authorization header or PLAY_SESSION cookie")

def get_authenticated_user(
    authorization: Optional[str] = Header(None),
    play_session: Optional[str] = Cookie(None, alias="PLAY_SESSION")
) -> str:
    """Get authenticated user URN from Bearer token or PLAY_SESSION cookie."""
    token = get_auth_token(authorization, play_session)
    return validate_token_and_get_user(token)
```

**When to use:**

- User-initiated actions where you need to know WHO is making the request
- Storing per-user data (credentials, settings)
- Following the same pattern as MCP and Chat

**Security:**

- GMS is the authoritative source for token validation
- Integrations service never verifies JWT signatures locally
- All authorization decisions are made by GMS

### Method 2: PLAY_SESSION Cookie (Fallback)

**Used by:** Browser requests when Authorization header is not available

**How it works:**

1. Browser sends `PLAY_SESSION` cookie (set by Play Framework frontend)
2. Service extracts JWT-encoded token from cookie payload
3. Token is validated with GMS (same as Method 1)
4. GMS returns the authenticated user URN

**Cookie Structure:**

The PLAY_SESSION cookie is a JWT with this structure:

```json
{
  "data": {
    "actor": "urn:li:corpuser:admin",
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
  },
  "exp": 1769539919,
  "nbf": 1769453519,
  "iat": 1769453519
}
```

**When to use:**

- Browser requests routed through API Gateway that don't include Authorization header
- Frontend pages that need to call integrations service endpoints
- Provides seamless authentication for browser-based flows

**API Gateway Behavior:**

- API Gateway passes cookies from browser to integrations service
- PLAY_SESSION cookie is automatically included in requests
- No additional frontend code needed to extract/forward the cookie

### Method 3: State Parameter (OAuth Callbacks)

**Used by:** OAuth callback endpoints

**How it works:**

1. User initiates OAuth flow (authenticated via JWT or cookie)
2. Service stores `user_urn` in state parameter and in-memory store
3. OAuth provider redirects to callback with `state` parameter
4. Service looks up stored state to get `user_urn`
5. State is one-time use (consumed after lookup)

**Example:**

```python
# During /connect (user is authenticated)
state_result = state_store.create_state(
    user_urn="urn:li:corpuser:johndoe",  # From JWT or cookie
    plugin_id="urn:li:service:glean-search",  # Plugin URN stored in state
    redirect_uri="https://datahub.example.com/integrations/oauth/callback",  # Single fixed URL
    code_verifier="...",
)
# Returns: nonce="abc123", authorization_url="https://glean.com/oauth/authorize?state=abc123&..."

# During /callback (no authentication - browser redirect from OAuth provider)
# Plugin is identified from state, NOT from URL path
oauth_state = state_store.get_and_consume_state("abc123")
# Returns: OAuthState(user_urn="urn:li:corpuser:johndoe", plugin_id="urn:li:service:glean-search", ...)
```

**When to use:**

- OAuth callback endpoints
- Any redirect-based flow where the callback URL is called by an external service

### Method 4: x-datahub-actor Header (Internal Only)

**Used by:** Direct service-to-service calls from GMS to `/private/*` endpoints

**How it works:**

1. GMS calls integrations service `/private/*` endpoint directly (no proxy)
2. GMS includes `x-datahub-actor` header with user URN
3. Integrations service trusts this header since the call is internal

**⚠️ Important Limitations:**

- Only valid for `/private/*` endpoints that are NOT exposed externally
- The frontend proxy **STRIPS** this header before forwarding (security measure)
- API Gateway does NOT pass this header to `/public/*` endpoints
- Cannot be used for endpoints called through `/integrations/*`

**Why it's stripped from external requests:**
Security measure - prevents external clients from spoofing the header to impersonate other users.

```java
// From IntegrationsController.java line 117-121
// Frontend proxy strips this header to prevent spoofing
request = request.removeHeader("X-DataHub-Actor");
```

**When to use:**

- `/private/*` endpoints called directly by GMS or other trusted internal services
- **Do NOT use** for `/public/*` endpoints or any endpoint exposed via `/integrations/*`

**Example:**

```
# GMS calls integrations service directly
GMS → http://integrations-service:9003/private/sql/execute
Headers: x-datahub-actor: urn:li:corpuser:admin
```

## Authentication by Endpoint Type

| Endpoint Type                        | Auth Method                | User Identity Source               |
| ------------------------------------ | -------------------------- | ---------------------------------- |
| User API calls via `/integrations/*` | JWT Bearer or PLAY_SESSION | Header or cookie, validated by GMS |
| OAuth callbacks                      | State parameter            | Stored during `/connect` call      |
| Service-to-service (direct)          | x-datahub-actor            | Header from trusted service        |
| Public/anonymous                     | None                       | N/A                                |

## Complete Example: OAuth Flow

```
┌──────────┐      ┌──────────┐      ┌─────────────┐      ┌──────────────┐
│  Browser │      │API Gateway│      │ Integrations│      │OAuth Provider│
└────┬─────┘      └────┬─────┘      └──────┬──────┘      └──────┬───────┘
     │                  │                   │                    │
     │ POST /integrations/oauth/plugins/{pluginId}/connect      │
     │ Cookie: PLAY_SESSION=...            │                    │
     │─────────────────►│                   │                    │
     │                  │                   │                    │
     │                  │ POST /public/oauth/plugins/{pluginId}/connect
     │                  │ Cookie: PLAY_SESSION=...              │
     │                  │──────────────────►│                    │
     │                  │                   │                    │
     │                  │                   │ Extract token from cookie
     │                  │                   │ Validate with GMS → user_urn
     │                  │                   │ Store state: {nonce: "abc", user_urn, plugin_id}
     │                  │                   │ Build auth URL with state=abc
     │                  │                   │                    │
     │                  │◄──────────────────│                    │
     │◄─────────────────│ {authorization_url: "https://glean.com/oauth?state=abc..."}
     │                  │                   │                    │
     │ Redirect to authorization_url        │                    │
     │─────────────────────────────────────────────────────────►│
     │                  │                   │                    │
     │                  │                   │     User logs in   │
     │                  │                   │◄───────────────────│
     │                  │                   │                    │
     │ GET /integrations/oauth/callback?code=xyz&state=abc      │
     │─────────────────►│  (single fixed URL, plugin in state)  │
     │                  │                   │                    │
     │                  │ GET /public/oauth/callback?code=xyz&state=abc
     │                  │──────────────────►│                    │
     │                  │                   │                    │
     │                  │                   │ Lookup state "abc" → user_urn
     │                  │                   │ Exchange code for tokens
     │                  │                   │ Store credentials for user_urn
     │                  │                   │                    │
     │◄─────────────────│◄──────────────────│                    │
     │ HTML page with postMessage to parent │                    │
     │                  │                   │                    │
```

## Security Considerations

### 1. Token Validation with GMS

**Why GMS validates:**

- GMS is the authoritative source for authentication
- GMS has access to JWT signing secrets
- Integrations service never sees or verifies JWT signatures
- All authorization decisions are delegated to GMS

**Flow:**

1. Integrations service extracts token (from header or cookie)
2. Integrations service sends token to GMS via GraphQL `me` query
3. GMS validates signature, checks expiration, returns user URN
4. Integrations service uses URN for data isolation

**Benefits:**

- Centralized authentication logic
- No need to distribute JWT secrets
- Consistent validation across all services

### 2. PLAY_SESSION Cookie Security

**Protections:**

- Cookie is JWT-signed by Play Framework (validated by Play, not by integrations service)
- Cookie contains a nested DataHub token that IS validated by GMS
- API Gateway passes cookies without modification
- Integrations service treats cookie content as untrusted until GMS validates the nested token

**What if cookie is tampered with?**

- Tampering breaks JWT signature → Play Framework rejects on next request
- Even if signature passes, nested token must be valid → GMS will reject invalid tokens
- Worst case: attacker stores data under a fake URN, but can't access real user data

### 3. State Parameter Security

**Protections:**

- State is a cryptographically random nonce (not guessable)
- One-time use (consumed after validation)
- Short TTL (10 minutes default)
- Stored server-side (attacker can't forge state)

### 4. Credential Storage

**Model:**

- Credentials stored in `DataHubConnection` entities
- Scoped to user URN (extracted from validated token)
- Users can only access their own credentials
- URN is never taken from user input

## Quick Reference

```python
# Pattern 1: JWT Bearer Token or PLAY_SESSION Cookie (user-initiated actions)
@router.post("/endpoint")
async def my_endpoint(user_urn: str = Depends(get_authenticated_user)):
    # user_urn is extracted from Authorization header or PLAY_SESSION cookie
    # and validated with GMS
    ...

# Pattern 2: State Parameter (OAuth callbacks)
@router.get("/callback")
async def oauth_callback(state: str = Query(...)):
    oauth_state = state_store.get_and_consume_state(state)
    user_urn = oauth_state.user_urn  # Stored during /connect
    ...

# Pattern 3: System credentials (no user context needed)
@router.get("/public/info")
async def public_info():
    # Use system DataHubGraph client
    result = graph.execute_graphql(...)
    ...
```

## Related Files

- `datahub-frontend/app/controllers/IntegrationsController.java` - Frontend proxy (local debugging)
- `datahub-integrations-service/src/datahub_integrations/server.py` - Router registration
- `datahub-integrations-service/src/datahub_integrations/oauth/router.py` - OAuth endpoints and authentication functions
- `datahub-integrations-service/src/datahub_integrations/mcp/router.py` - MCP authentication
- `datahub-integrations-service/src/datahub_integrations/chat/chat_api.py` - Chat authentication
