# Local OAuth Development Guide

This guide explains how to develop and test OAuth integrations for AI Plugins when running DataHub locally.

## The Challenge

OAuth providers require HTTPS callback URLs that are publicly accessible. When developing locally:

- Your machine is typically behind NAT/firewall
- OAuth providers reject `localhost` or `127.0.0.1` callback URLs
- Most providers require `https://` (not `http://`)

## Solution: ngrok Tunnel

We use [ngrok](https://ngrok.com/) to create a secure tunnel from a public HTTPS URL to your local machine.

```
OAuth Provider → ngrok (public HTTPS) → localhost:9002 → DataHub
```

## Prerequisites

1. **Install ngrok**:

   ```bash
   # macOS
   brew install ngrok

   # Linux
   snap install ngrok

   # Or download from https://ngrok.com/download
   ```

2. **Authenticate ngrok** (free account required):

   ```bash
   ngrok config add-authtoken <your-authtoken>
   ```

   Get your authtoken from [ngrok dashboard](https://dashboard.ngrok.com/get-started/your-authtoken).

3. **DataHub running locally**:
   ```bash
   ./gradlew quickstartDebug
   ```

## Quick Start

### 1. Start the ngrok tunnel

```bash
cd datahub-integrations-service/scripts/oauth
./setup_oauth_tunnel.sh
```

The script will output:

```
✅ OAuth tunnel established!

🔗 Public URL:
   https://abc123.ngrok-free.app

📋 Set this environment variable before starting integrations service:
   export DATAHUB_FRONTEND_URL="https://abc123.ngrok-free.app"

🔐 OAuth Callback URL (register this with ALL OAuth providers):
   https://abc123.ngrok-free.app/integrations/oauth/callback
```

### 2. Configure your OAuth provider

In your OAuth provider's developer portal (e.g., Glean Admin, Atlassian Developer Console):

1. Create or edit your OAuth application
2. Add the **single callback URL** (same for ALL providers):
   ```
   https://abc123.ngrok-free.app/integrations/oauth/callback
   ```
3. Note the Client ID and Client Secret

> **Key Design:** We use a single, fixed callback URL for all OAuth providers. The plugin is identified from the OAuth `state` parameter. This means you can register the callback URL **before** creating the plugin configuration in DataHub - no chicken-and-egg problem!

### 3. Configure DataHub

Create or update the `OAuthAuthorizationServer` entity in DataHub with:

- Client ID from your OAuth provider
- Client Secret (stored as a DataHubSecret)
- Authorization URL
- Token URL

### 4. Configure the integrations service with tunnel URL

**Option A: Using `.env` file (recommended for quickstartDebug)**

Add to `datahub-integrations-service/.env`:

```bash
# OAuth Development - ngrok tunnel for callback URLs
# Comment out or remove when not testing OAuth locally
DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL=https://abc123.ngrok-free.app
```

Then restart the integrations service:

```bash
docker compose -p datahub restart datahub-integrations-service
```

You should see this in the logs confirming it's working:

```
[DEV MODE] Using frontend URL override: https://abc123.ngrok-free.app (set via DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL)
```

**Option B: Running standalone (outside Docker)**

```bash
export DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL="https://abc123.ngrok-free.app"
cd datahub-integrations-service
source venv/bin/activate
python -m datahub_integrations.server
```

### 5. Test the OAuth flow

1. Open DataHub UI at `https://abc123.ngrok-free.app`
2. Navigate to Settings → AI Plugins
3. Click "Connect" on an OAuth-enabled plugin
4. Complete the OAuth flow with the provider
5. Verify the connection is established

## URL Path Mapping

The DataHub frontend proxies requests to the integrations service with a path rewrite:

| Layer                               | URL Path                        |
| ----------------------------------- | ------------------------------- |
| **External** (OAuth provider calls) | `/integrations/oauth/callback`  |
| Frontend receives                   | `/integrations/...`             |
| Frontend remaps & forwards          | `/integrations/*` → `/public/*` |
| **Internal** (integrations service) | `/public/oauth/callback`        |

This is why you register `/integrations/...` URLs with OAuth providers, even though the integrations service listens on `/public/...`.

**Note:** The plugin is identified from the OAuth `state` parameter, not from the URL path. This is why we can use a single callback URL for all plugins.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      OAuth Flow with ngrok                          │
└─────────────────────────────────────────────────────────────────────┘

1. User clicks "Connect" in DataHub UI
   │
   ▼
2. Frontend calls POST /private/oauth/plugins/{pluginId}/connect
   │
   ▼
3. Integrations service builds authorization URL with:
   - redirect_uri: https://abc123.ngrok-free.app/integrations/oauth/callback (FIXED URL)
   - state: contains plugin_id, user_urn, code_verifier (stored in memory)
   - client_id, code_challenge, etc.
   │
   ▼
4. Frontend opens popup to OAuth provider's authorization URL
   │
   ▼
5. User authenticates with OAuth provider
   │
   ▼
6. OAuth provider redirects to callback URL:
   https://abc123.ngrok-free.app/integrations/oauth/callback?code=...&state=...
   │
   ▼
7. ngrok tunnels request to localhost:9002 (DataHub frontend)
   │
   ▼
8. Frontend proxy remaps /integrations/* to /public/* and forwards to integrations service
   │
   ▼
9. Integrations service extracts plugin_id from state, exchanges code for tokens
   │
   ▼
10. Tokens stored in DataHubConnection entity
    │
    ▼
11. Callback page closes popup with postMessage
```

## Important Notes

### Free ngrok limitations

- **URL changes on restart**: Free ngrok gives you a random subdomain each time. You'll need to:

  - Re-register the callback URL with your OAuth provider
  - Restart the integrations service with the new URL

- **Browser interstitial warning**: Free ngrok shows a "Visit Site" warning page for new visitors. This typically doesn't affect OAuth callbacks because:

  - OAuth redirects are browser-based (the user's browser follows the redirect)
  - The warning appears once per session, then ngrok remembers the browser

  **If you encounter issues:**

  - Click "Visit Site" manually once before testing OAuth
  - Or upgrade to a paid ngrok plan (removes the interstitial)
  - Or use `localhost` if your OAuth provider supports it (see below)

- **ngrok paid plans**: For stable subdomains and no interstitial, consider ngrok's paid plans.

### Alternative: localhost OAuth providers

Some OAuth providers allow `http://localhost` for development:

- Google OAuth (must be configured in app settings)
- GitHub OAuth
- Many OAuth providers in "development mode"

If your provider allows localhost, you can skip ngrok and use:

```bash
export DATAHUB_FRONTEND_URL="http://localhost:9002"
```

### Multiple OAuth providers

You can test multiple OAuth providers with the same ngrok tunnel. All providers use the **same callback URL**:

```
https://abc123.ngrok-free.app/integrations/oauth/callback
```

This is a major advantage of our design:

- **Register once** - Same callback URL works for Glean, Confluence, Jira, GitHub, etc.
- **No URN issues** - You don't need to know the plugin URN before registering the callback
- **Simpler configuration** - One URL to manage, not multiple per-plugin URLs

The plugin is identified from the OAuth `state` parameter, which is generated when the user initiates the connection and validated when the callback is received.

## Troubleshooting

### "Invalid redirect_uri" error

1. Verify the callback URL is **exactly** registered in the OAuth provider
2. Check for trailing slashes - URLs must match exactly
3. Ensure the ngrok URL hasn't changed (restart `setup_oauth_tunnel.sh` and re-register)

### "State mismatch" error

1. The OAuth state is stored in-memory with a 10-minute TTL
2. If the integrations service restarts, all states are lost
3. Start a new OAuth flow after service restart

### Cannot reach callback URL

1. Ensure ngrok is running: `curl http://localhost:4040/api/tunnels`
2. Check ngrok logs: `tail -f /tmp/ngrok_oauth.log`
3. Verify the integrations service is running
4. Check the port is correct (default: 9002)

### ngrok "Visit Site" interstitial blocking OAuth

If the OAuth callback gets stuck on ngrok's "You are about to visit..." warning page:

1. **Visit the ngrok URL manually first** in your browser and click "Visit Site"
2. ngrok remembers this for your browser session
3. Then retry the OAuth flow

For programmatic/automated testing, consider:

- Using `localhost` if your OAuth provider supports it
- Upgrading to a paid ngrok plan (removes interstitial)

### OAuth tokens not saved

1. Check the integrations service logs for errors
2. Verify DataHub GMS is running and accessible
3. Check that the user has permission to create DataHubConnection entities

## Environment Variables

| Variable                                 | Default | Description                                                    |
| ---------------------------------------- | ------- | -------------------------------------------------------------- |
| `DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL` | (none)  | Override frontend URL for OAuth callbacks (add to `.env` file) |
| `OAUTH_TUNNEL_PORT`                      | `9002`  | Port to tunnel to (used by setup script)                       |

**Note:** The integrations service uses `DATAHUB_FRONTEND_URL` internally, but in development you should set `DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL` which takes precedence. This keeps the override explicit and easy to remove after testing.

## Related Documentation

- [AI Plugins Implementation Plan](../../../docs/managed-datahub/genai/ai-plugins-implementation-plan.md)
- [AI Plugins Object Model](../../../docs/managed-datahub/genai/ai-plugins-object-model.md)
- [OAuth Module Source](../../src/datahub_integrations/oauth/)
