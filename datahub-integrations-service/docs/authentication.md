# Integrations Service Authentication Guide

This document explains how authentication works for the DataHub Integrations Service.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              External Clients                                │
│                    (Browser, OAuth Providers, API Clients)                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DataHub Frontend (Port 9002)                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    IntegrationsController.java                       │    │
│  │  • Authenticates requests (session/JWT)                              │    │
│  │  • Remaps paths: /integrations/* → /public/*                         │    │
│  │  • STRIPS x-datahub-actor header (security measure)                  │    │
│  │  • Adds X-Forwarded-Host, X-Forwarded-Proto                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Integrations Service (Port 9003)                          │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐   │
│  │   /private/* (internal)     │  │        /public/* (external)          │   │
│  │   • Not exposed externally  │  │   • Exposed via frontend proxy       │   │
│  │   • Service-to-service      │  │   • External callbacks (OAuth)       │   │
│  └─────────────────────────────┘  └─────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## URL Path Mapping

| External URL (Browser) | Internal URL (Integrations Service) | Notes                      |
| ---------------------- | ----------------------------------- | -------------------------- |
| `/integrations/foo`    | `/public/foo`                       | Frontend proxy remaps path |
| N/A (internal only)    | `/private/foo`                      | Not accessible externally  |

**Example:**

```
# External OAuth callback URL (registered with OAuth provider)
# NOTE: Single, fixed URL for ALL OAuth providers (plugin identified via state parameter)
https://datahub.example.com/integrations/oauth/callback

# After frontend proxy remapping, integrations service receives:
http://integrations-service:9003/public/oauth/callback
```

## Router Types

### 1. Internal Router (`/private/*`)

**Purpose:** Endpoints called by other DataHub services (GMS, frontend), not directly by browsers.

**Characteristics:**

- Not exposed through the frontend proxy
- May use service-to-service authentication
- Typically handles background operations

**Example routes:**

- `/private/share/*` - Share functionality
- `/private/sql/*` - SQL execution

### 2. External Router (`/public/*`)

**Purpose:** Endpoints accessible via the frontend proxy from external clients.

**Characteristics:**

- Mapped from `/integrations/*` to `/public/*` by frontend
- Can receive browser requests
- Includes OAuth callbacks, API endpoints

**Example routes:**

- `/public/oauth/callback` - OAuth callbacks (single URL for all providers)
- `/public/dist/*` - Distribution endpoints

## Authentication Methods

### Method 1: JWT Bearer Token

**Used by:** MCP server, Chat API, OAuth private endpoints

**How it works:**

1. Client sends `Authorization: Bearer <jwt_token>` header
2. Service extracts user URN from JWT `sub` claim
3. **No signature verification** (service doesn't have the secret)
4. Security relies on using token for downstream GMS calls (GMS validates)

**Example:**

```python
from fastapi import Header, HTTPException
import jwt

def get_authenticated_user(authorization: str = Header(None)) -> str:
    """Extract user URN from JWT token."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    token = authorization.split(" ", 1)[1]

    # Decode WITHOUT signature verification
    # This is safe because:
    # 1. We only use the URN for data isolation (per-user storage)
    # 2. Actual data access goes through GMS which validates the token
    payload = jwt.decode(token, options={"verify_signature": False})
    return payload["sub"]  # e.g., "urn:li:corpuser:johndoe"
```

**When to use:**

- User-initiated actions where you need to know WHO is making the request
- Storing per-user data (credentials, settings)
- Following the same pattern as MCP and Chat

### Method 2: State Parameter (OAuth Callbacks)

**Used by:** OAuth callback endpoints

**How it works:**

1. User initiates OAuth flow (authenticated via JWT)
2. Service stores `user_urn` in state parameter and in-memory store
3. OAuth provider redirects to callback with `state` parameter
4. Service looks up stored state to get `user_urn`
5. State is one-time use (consumed after lookup)

**Example:**

```python
# During /connect (user is authenticated)
state_result = state_store.create_state(
    user_urn="urn:li:corpuser:johndoe",  # From JWT
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

### Method 3: x-datahub-actor Header (Limited Use)

**Used by:** Some internal service-to-service calls

**⚠️ Important Limitations:**

- The frontend proxy **STRIPS** this header before forwarding
- Cannot be used for endpoints called through `/integrations/*`
- Only useful for direct service-to-service calls (e.g., GMS → Integrations)

**Why it's stripped:**
Security measure - prevents clients from spoofing the header to impersonate other users.

```java
// From IntegrationsController.java line 117-121
request = request.removeHeader("X-DataHub-Actor");
```

**When to use:**

- Internal service-to-service calls NOT going through the frontend proxy
- **Do NOT use** for user-facing endpoints exposed via `/integrations/*`

## Authentication by Endpoint Type

| Endpoint Type                        | Auth Method      | User Identity Source                |
| ------------------------------------ | ---------------- | ----------------------------------- |
| User API calls via `/integrations/*` | JWT Bearer token | Decoded from `Authorization` header |
| OAuth callbacks                      | State parameter  | Stored during `/connect` call       |
| Service-to-service (direct)          | x-datahub-actor  | Header from trusted service         |
| Public/anonymous                     | None             | N/A                                 |

## Complete Example: OAuth Flow

```
┌──────────┐      ┌──────────┐      ┌─────────────┐      ┌──────────────┐
│  Browser │      │ Frontend │      │ Integrations│      │OAuth Provider│
└────┬─────┘      └────┬─────┘      └──────┬──────┘      └──────┬───────┘
     │                  │                   │                    │
     │ POST /integrations/oauth/plugins/{pluginId}/connect      │
     │ Authorization: Bearer <jwt>         │                    │
     │─────────────────►│                   │                    │
     │                  │                   │                    │
     │                  │ POST /public/oauth/plugins/{pluginId}/connect
     │                  │ (strips x-datahub-actor, keeps Auth header)
     │                  │──────────────────►│                    │
     │                  │                   │                    │
     │                  │                   │ Decode JWT → user_urn
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

### 1. JWT Without Signature Verification

**Why this is acceptable:**

- The integrations service doesn't make authorization decisions
- It only uses the URN for **data isolation** (per-user storage)
- Any sensitive operations go through GMS, which DOES validate the JWT
- Worst case: attacker stores data under a fake URN that no real user can access

### 2. State Parameter Security

**Protections:**

- State is a cryptographically random nonce (not guessable)
- One-time use (consumed after validation)
- Short TTL (10 minutes default)
- Stored server-side (attacker can't forge state)

### 3. Credential Storage

**Model:**

- Credentials stored in `DataHubConnection` entities
- Scoped to user URN (extracted from JWT or state)
- Users can only access their own credentials
- URN is never taken from user input

## Quick Reference

```python
# Pattern 1: JWT Bearer Token (user-initiated actions)
@router.post("/endpoint")
async def my_endpoint(user_urn: str = Depends(get_authenticated_user)):
    # user_urn is extracted from JWT token
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

- `datahub-frontend/app/controllers/IntegrationsController.java` - Frontend proxy
- `datahub-integrations-service/src/datahub_integrations/server.py` - Router registration
- `datahub-integrations-service/src/datahub_integrations/oauth/router.py` - OAuth endpoints
- `datahub-integrations-service/src/datahub_integrations/mcp/router.py` - MCP authentication
- `datahub-integrations-service/src/datahub_integrations/chat/chat_api.py` - Chat authentication
