# OAuth Provider Authentication

DataHub supports OAuth2 / OIDC JWT token authentication through a unified **OAuth Provider** system. This authentication mechanism is designed primarily for service-to-service authentication, allowing external applications and services to authenticate with DataHub using JWT tokens issued by trusted OAuth2/OIDC providers.

## Overview

The OAuth Provider authentication system uses a **unified provider chain** approach that consolidates both static (configuration-based) and dynamic (GlobalSettings-based) OAuth providers into a single, manageable list. When a JWT token is received, DataHub:

1. **Extracts claims** from the JWT token (issuer and audience)
2. **Finds matching provider** from the unified provider chain
3. **Validates token signature** using the provider's JWKS endpoint
4. **Creates service account** automatically if needed
5. **Grants access** with appropriate permissions

This design provides a clean, extensible, and maintainable approach to OAuth authentication that scales from simple single-provider setups to complex multi-provider environments.

## How It Works

### Unified Provider Chain

Instead of managing separate static and dynamic configurations, DataHub maintains a single list of `OAuthProvider` objects. Each provider contains:

- **Name**: Unique identifier for the provider
- **Issuer**: The JWT `iss` claim that must match
- **Audience**: The JWT `aud` claim that must match
- **JWKS URI**: Endpoint to fetch public keys for signature verification
- **Enabled**: Whether this provider is active

### Authentication Flow

```mermaid
sequenceDiagram
    participant Client
    participant DataHub
    participant JWKS
    participant Provider

    Client->>DataHub: API Request with JWT Bearer Token
    DataHub->>DataHub: Extract issuer & audience from JWT
    DataHub->>DataHub: Find matching OAuth provider
    DataHub->>JWKS: Fetch public keys
    JWKS->>DataHub: Return signing keys
    DataHub->>DataHub: Validate JWT signature & claims
    DataHub->>DataHub: Create/update service account
    DataHub->>Client: Authorized response
```

### Service Account Management

OAuth-authenticated users are automatically created as **service accounts** in DataHub with:

- **Username**: `__oauth_{issuer_domain}_{subject}` format
- **Type**: SERVICE (distinguished from regular users)
- **Origin**: Tracking information about OAuth source
- **Auto-creation**: No manual user management required

## Static Configuration

Static OAuth providers are configured via environment variables in your DataHub deployment. This is the recommended approach for production environments with well-known, stable OAuth providers.

### Basic Configuration

Set these environment variables for the `datahub-gms` service:

```bash
# Enable OAuth authentication
AUTH_OAUTH_ENABLED=true

# Required: Trusted JWT issuers (comma-separated)
AUTH_OAUTH_TRUSTED_ISSUERS=https://auth.example.com,https://okta.company.com

# Required: Allowed JWT audiences (comma-separated)
AUTH_OAUTH_ALLOWED_AUDIENCES=datahub-api,my-service-id

# Required: JWKS endpoint for signature verification
AUTH_OAUTH_JWKS_URI=https://auth.example.com/.well-known/jwks.json

# Optional: JWT claim containing user ID (default: "sub")
AUTH_OAUTH_USER_ID_CLAIM=sub

# Optional: Signing algorithm (default: "RS256")
AUTH_OAUTH_ALGORITHM=RS256
```

### Docker Compose Example

```yaml
version: "3.8"
services:
  datahub-gms:
    image: acryldata/datahub-gms:latest
    environment:
      # OAuth Configuration
      - AUTH_OAUTH_ENABLED=true
      - AUTH_OAUTH_TRUSTED_ISSUERS=https://my-okta-domain.okta.com/oauth2/default
      - AUTH_OAUTH_ALLOWED_AUDIENCES=0oa1234567890abcdef
      - AUTH_OAUTH_JWKS_URI=https://my-okta-domain.okta.com/oauth2/default/v1/keys
      - AUTH_OAUTH_USER_ID_CLAIM=sub
      - AUTH_OAUTH_ALGORITHM=RS256

      # Standard DataHub settings
      - DATAHUB_GMS_HOST=0.0.0.0
      - DATAHUB_GMS_PORT=8080
      # ... other configurations
```

### Kubernetes Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datahub-gms
spec:
  template:
    spec:
      containers:
        - name: datahub-gms
          image: acryldata/datahub-gms:latest
          env:
            - name: AUTH_OAUTH_ENABLED
              value: "true"
            - name: AUTH_OAUTH_TRUSTED_ISSUERS
              value: "https://login.microsoftonline.com/tenant-id/v2.0"
            - name: AUTH_OAUTH_ALLOWED_AUDIENCES
              value: "api://datahub-prod"
            - name: AUTH_OAUTH_JWKS_URI
              value: "https://login.microsoftonline.com/tenant-id/discovery/v2.0/keys"
          # ... other environment variables
```

### Multiple Providers

To support multiple OAuth providers, use comma-separated values:

```bash
# Multiple issuers and audiences
AUTH_OAUTH_TRUSTED_ISSUERS=https://okta.company.com,https://auth0.company.com
AUTH_OAUTH_ALLOWED_AUDIENCES=datahub-prod,datahub-staging,service-account-id

# Single JWKS URI (if providers share keys) or discovery URI
AUTH_OAUTH_JWKS_URI=https://okta.company.com/.well-known/jwks.json

# Or use discovery URI to auto-derive JWKS
AUTH_OAUTH_DISCOVERY_URI=https://okta.company.com/.well-known/openid-configuration
```

### Discovery URI vs JWKS URI

You can specify either:

- **JWKS URI**: Direct endpoint to signing keys (recommended for production)
- **Discovery URI**: OIDC discovery document URL (DataHub will auto-derive JWKS URI)

```bash
# Option 1: Direct JWKS URI (faster, more reliable)
AUTH_OAUTH_JWKS_URI=https://auth.example.com/.well-known/jwks.json

# Option 2: Discovery URI (convenient, auto-derives JWKS)
AUTH_OAUTH_DISCOVERY_URI=https://auth.example.com/.well-known/openid-configuration
```

## Dynamic Configuration

OAuth providers can also be configured dynamically through DataHub's **GlobalSettings** system. This allows runtime configuration changes without service restarts.

> **Note**: Dynamic configuration requires the static configuration framework to be enabled (`AUTH_OAUTH_ENABLED=true`). Dynamic providers are loaded every minute automatically.

### Adding Dynamic Providers

Dynamic providers are managed through the `GlobalSettings` entity with the `oauth` aspect:

```json
{
  "providers": [
    {
      "name": "okta-production",
      "issuer": "https://company.okta.com/oauth2/default",
      "audience": "datahub-prod",
      "jwksUri": "https://company.okta.com/oauth2/default/v1/keys",
      "enabled": true
    },
    {
      "name": "auth0-development",
      "issuer": "https://company.auth0.com/",
      "audience": "datahub-dev",
      "jwksUri": "https://company.auth0.com/.well-known/jwks.json",
      "enabled": false
    }
  ]
}
```

## Provider Configuration Examples

### Okta

```bash
AUTH_OAUTH_ENABLED=true
AUTH_OAUTH_TRUSTED_ISSUERS=https://your-domain.okta.com/oauth2/default
AUTH_OAUTH_ALLOWED_AUDIENCES=0oa1234567890abcdef
AUTH_OAUTH_JWKS_URI=https://your-domain.okta.com/oauth2/default/v1/keys
```

### Auth0

```bash
AUTH_OAUTH_ENABLED=true
AUTH_OAUTH_TRUSTED_ISSUERS=https://your-domain.auth0.com/
AUTH_OAUTH_ALLOWED_AUDIENCES=https://your-api-identifier/
AUTH_OAUTH_JWKS_URI=https://your-domain.auth0.com/.well-known/jwks.json
```

### Azure AD / Microsoft Entra

```bash
AUTH_OAUTH_ENABLED=true
AUTH_OAUTH_TRUSTED_ISSUERS=https://login.microsoftonline.com/your-tenant-id/v2.0
AUTH_OAUTH_ALLOWED_AUDIENCES=api://your-app-id
AUTH_OAUTH_JWKS_URI=https://login.microsoftonline.com/your-tenant-id/discovery/v2.0/keys
```

### Google Cloud Identity

```bash
AUTH_OAUTH_ENABLED=true
AUTH_OAUTH_TRUSTED_ISSUERS=https://accounts.google.com
AUTH_OAUTH_ALLOWED_AUDIENCES=your-client-id.apps.googleusercontent.com
AUTH_OAUTH_JWKS_URI=https://www.googleapis.com/oauth2/v3/certs
```

### Keycloak

```bash
AUTH_OAUTH_ENABLED=true
AUTH_OAUTH_TRUSTED_ISSUERS=https://keycloak.company.com/realms/datahub
AUTH_OAUTH_ALLOWED_AUDIENCES=datahub-client
AUTH_OAUTH_JWKS_URI=https://keycloak.company.com/realms/datahub/protocol/openid-connect/certs
```

## Using OAuth Tokens

Once configured, clients can authenticate with DataHub using JWT tokens:

### API Request Example

```bash
curl -H "Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..." \
     -H "Content-Type: application/json" \
     https://your-datahub.com/api/graphql \
     -d '{"query": "{ corpUsers { total } }"}'
```

### Python Client Example

```python
import requests

headers = {
    'Authorization': f'Bearer {jwt_token}',
    'Content-Type': 'application/json'
}

response = requests.post(
    'https://your-datahub.com/api/graphql',
    headers=headers,
    json={'query': '{ corpUsers { total } }'}
)
```

### Java Client Example

```java
import java.net.http.*;

HttpClient client = HttpClient.newHttpClient();
HttpRequest request = HttpRequest.newBuilder()
    .uri(URI.create("https://your-datahub.com/api/graphql"))
    .header("Authorization", "Bearer " + jwtToken)
    .header("Content-Type", "application/json")
    .POST(HttpRequest.BodyPublishers.ofString("{\"query\": \"{ corpUsers { total } }\"}"))
    .build();

HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
```

## Best Practices

### Security

- **Use HTTPS** for all JWKS URIs and discovery endpoints
- **Validate audiences** carefully - use specific audience values, not wildcards
- **Rotate signing keys** regularly at your OAuth provider
- **Monitor token usage** for unusual patterns
- **Implement token expiration** - use short-lived tokens (< 1 hour recommended)

### Configuration Management

- **Static for production** - Use static configuration for stable production providers
- **Dynamic for flexibility** - Use dynamic configuration for development/testing
- **Environment separation** - Use different audiences for prod/staging/dev
- **Provider naming** - Use descriptive names like `okta-production`, `auth0-development`

### Monitoring & Observability

- **Enable debug logging** during setup:
  ```bash
  DATAHUB_GMS_LOG_LEVEL=DEBUG
  ```
- **Monitor authentication failures** in DataHub logs
- **Track service account creation** for audit purposes
- **Set up alerts** for authentication errors

### Performance

- **Cache JWKS responses** - OAuth providers typically cache signing keys
- **Use direct JWKS URIs** instead of discovery URIs when possible
- **Monitor provider latency** - slow JWKS responses affect authentication
- **Consider provider geographic proximity** for better performance

## Troubleshooting

### Common Issues

#### "OAuth authenticator is not configured"

- **Cause**: `AUTH_OAUTH_ENABLED=true` not set or invalid provider configuration
- **Solution**: Verify all required environment variables are set correctly

#### "No configured OAuth provider matches token issuer"

- **Cause**: JWT issuer doesn't match any configured trusted issuer
- **Solution**: Check JWT issuer claim matches `AUTH_OAUTH_TRUSTED_ISSUERS` exactly

#### "Invalid or missing audience claim"

- **Cause**: JWT audience doesn't match configured allowed audiences
- **Solution**: Verify JWT audience claim is in `AUTH_OAUTH_ALLOWED_AUDIENCES` list

#### "Failed to load signing keys"

- **Cause**: JWKS URI unreachable or returning invalid keys
- **Solution**: Test JWKS URI directly, check network connectivity, verify endpoint

#### "JWT signature validation failed"

- **Cause**: Token signed with different key than expected
- **Solution**: Verify JWKS URI points to correct key endpoint, check token was issued by expected provider

### Debug Information

Enable detailed logging to troubleshoot issues:

```bash
# Enable debug logging for OAuth authentication
DATAHUB_GMS_LOG_LEVEL=DEBUG

# Check logs for OAuth-specific messages
docker logs datahub-gms | grep -i oauth
```

### Validating Configuration

Test your configuration by examining JWT tokens:

```bash
# Decode JWT token (without verification) to inspect claims
echo "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguZXhhbXBsZS5jb20iLCJhdWQiOiJkYXRhaHViLWFwaSJ9.signature" | \
  cut -d. -f2 | base64 -d | jq
```

Expected output:

```json
{
  "iss": "https://auth.example.com",
  "aud": "datahub-api",
  "sub": "service-account-123",
  "exp": 1640995200,
  "iat": 1640991600
}
```

### Testing JWKS Endpoints

Verify your JWKS endpoint is accessible:

```bash
# Test JWKS endpoint directly
curl https://auth.example.com/.well-known/jwks.json | jq

# Expected response contains keys array
{
  "keys": [
    {
      "kty": "RSA",
      "kid": "key-id-1",
      "use": "sig",
      "n": "...",
      "e": "AQAB"
    }
  ]
}
```

## Advanced Configuration

### Custom Claims

Configure which JWT claim contains the user identifier:

```bash
# Use email claim instead of default 'sub'
AUTH_OAUTH_USER_ID_CLAIM=email

# Use custom claim
AUTH_OAUTH_USER_ID_CLAIM=preferred_username
```

### Algorithm Selection

Specify the JWT signing algorithm:

```bash
# RSA with SHA-256 (default, recommended)
AUTH_OAUTH_ALGORITHM=RS256

# RSA with SHA-512
AUTH_OAUTH_ALGORITHM=RS512

# ECDSA with SHA-256
AUTH_OAUTH_ALGORITHM=ES256
```

### Service Account Mapping

OAuth-authenticated users are created with predictable usernames:

- **Format**: `__oauth_{issuer_domain}_{subject}`
- **Example**: For issuer `https://auth.company.com` and subject `svc-123`
- **Result**: Username `__oauth_auth_company_com_svc-123`

This ensures unique service account names across different OAuth providers and subjects.

## Migration from Legacy Authentication

If migrating from other authentication methods:

1. **Parallel deployment** - Enable OAuth alongside existing authentication
2. **Gradual rollout** - Move services to OAuth incrementally
3. **Monitor both systems** - Watch for authentication failures during transition
4. **Backup access** - Keep admin credentials available for emergency access
5. **Documentation** - Update service documentation with new OAuth setup

The OAuth Provider system is designed to coexist with other DataHub authentication methods, allowing smooth migrations and hybrid environments.
