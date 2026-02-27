# External OAuth Authentication

:::note DataHub Cloud Customers
Self-service configuration of external OAuth providers is not yet available on DataHub Cloud.

If you'd like to configure external OAuth for DataHub Cloud, please reach out to your customer support representative with the configuration values outlined below!
:::

DataHub supports authenticating API requests using JWT tokens from external identity providers like Okta, Azure AD, Google Identity, and others. This is perfect for service-to-service authentication where your applications need to call DataHub APIs.

## Overview

When you configure OAuth authentication, DataHub will:

1. Accept JWT tokens from your trusted identity provider
2. Validate the token signature and claims
3. Automatically create service accounts for authenticated users
4. Grant API access based on DataHub's permission system

## Configuration

Configure OAuth authentication by setting these environment variables in your DataHub deployment:

Set these environment variables for the `datahub-gms` service:

```bash
# Enable OAuth authentication
EXTERNAL_OAUTH_ENABLED=true

# Required: Trusted JWT issuers (comma-separated)
EXTERNAL_OAUTH_TRUSTED_ISSUERS=https://auth.example.com,https://okta.company.com

# Required: Allowed JWT audiences (comma-separated)
EXTERNAL_OAUTH_ALLOWED_AUDIENCES=datahub-api,my-service-id

# Required: JWKS endpoint for signature verification
EXTERNAL_OAUTH_JWKS_URI=https://auth.example.com/.well-known/jwks.json

# Optional: JWT claim containing user ID (default: "sub")
EXTERNAL_OAUTH_USER_ID_CLAIM=sub

# Optional: Signing algorithm (default: "RS256")
EXTERNAL_OAUTH_ALGORITHM=RS256
```

### Docker Compose Example

```yaml
version: "3.8"
services:
  datahub-gms:
    image: acryldata/datahub-gms:latest
    environment:
      # External OAuth Configuration
      - EXTERNAL_OAUTH_ENABLED=true
      - EXTERNAL_OAUTH_TRUSTED_ISSUERS=https://my-okta-domain.okta.com/oauth2/default
      - EXTERNAL_OAUTH_ALLOWED_AUDIENCES=0oa1234567890abcdef
      - EXTERNAL_OAUTH_JWKS_URI=https://my-okta-domain.okta.com/oauth2/default/v1/keys
      - EXTERNAL_OAUTH_USER_ID_CLAIM=sub
      - EXTERNAL_OAUTH_ALGORITHM=RS256

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
            - name: EXTERNAL_OAUTH_ENABLED
              value: "true"
            - name: EXTERNAL_OAUTH_TRUSTED_ISSUERS
              value: "https://login.microsoftonline.com/tenant-id/v2.0"
            - name: EXTERNAL_OAUTH_ALLOWED_AUDIENCES
              value: "api://datahub-prod"
            - name: EXTERNAL_OAUTH_JWKS_URI
              value: "https://login.microsoftonline.com/tenant-id/discovery/v2.0/keys"
          # ... other environment variables
```

### Multiple Providers

To support multiple OAuth providers, use comma-separated values:

```bash
# Multiple issuers and audiences
EXTERNAL_OAUTH_TRUSTED_ISSUERS=https://okta.company.com,https://auth0.company.com
EXTERNAL_OAUTH_ALLOWED_AUDIENCES=datahub-prod,datahub-staging,service-account-id

# Single JWKS URI (if providers share keys) or discovery URI
EXTERNAL_OAUTH_JWKS_URI=https://okta.company.com/.well-known/jwks.json

# Or use discovery URI to auto-derive JWKS
EXTERNAL_OAUTH_DISCOVERY_URI=https://okta.company.com/.well-known/openid-configuration
```

### Discovery URI vs JWKS URI

You can specify either:

- **JWKS URI**: Direct endpoint to signing keys (recommended for production)
- **Discovery URI**: OIDC discovery document URL (DataHub will auto-derive JWKS URI)

```bash
# Option 1: Direct JWKS URI (faster, more reliable)
EXTERNAL_OAUTH_JWKS_URI=https://auth.example.com/.well-known/jwks.json

# Option 2: Discovery URI (convenient, auto-derives JWKS)
EXTERNAL_OAUTH_DISCOVERY_URI=https://auth.example.com/.well-known/openid-configuration
```

## Provider Examples

### Okta

```bash
EXTERNAL_OAUTH_ENABLED=true
EXTERNAL_OAUTH_TRUSTED_ISSUERS=https://your-domain.okta.com/oauth2/default
EXTERNAL_OAUTH_ALLOWED_AUDIENCES=0oa1234567890abcdef
EXTERNAL_OAUTH_JWKS_URI=https://your-domain.okta.com/oauth2/default/v1/keys
```

### Auth0

```bash
EXTERNAL_OAUTH_ENABLED=true
EXTERNAL_OAUTH_TRUSTED_ISSUERS=https://your-domain.auth0.com/
EXTERNAL_OAUTH_ALLOWED_AUDIENCES=https://your-api-identifier/
EXTERNAL_OAUTH_JWKS_URI=https://your-domain.auth0.com/.well-known/jwks.json
```

### Azure AD / Microsoft Entra

```bash
EXTERNAL_OAUTH_ENABLED=true
EXTERNAL_OAUTH_TRUSTED_ISSUERS=https://login.microsoftonline.com/your-tenant-id/v2.0
EXTERNAL_OAUTH_ALLOWED_AUDIENCES=api://your-app-id
EXTERNAL_OAUTH_JWKS_URI=https://login.microsoftonline.com/your-tenant-id/discovery/v2.0/keys
```

### Google Cloud Identity

```bash
EXTERNAL_OAUTH_ENABLED=true
EXTERNAL_OAUTH_TRUSTED_ISSUERS=https://accounts.google.com
EXTERNAL_OAUTH_ALLOWED_AUDIENCES=your-client-id.apps.googleusercontent.com
EXTERNAL_OAUTH_JWKS_URI=https://www.googleapis.com/oauth2/v3/certs
```

### Keycloak

```bash
EXTERNAL_OAUTH_ENABLED=true
EXTERNAL_OAUTH_TRUSTED_ISSUERS=https://keycloak.company.com/realms/datahub
EXTERNAL_OAUTH_ALLOWED_AUDIENCES=datahub-client
EXTERNAL_OAUTH_JWKS_URI=https://keycloak.company.com/realms/datahub/protocol/openid-connect/certs
```

## Using OAuth Tokens

Once configured, include your JWT token in the Authorization header when making API requests:

```bash
curl -H "Authorization: Bearer YOUR_JWT_TOKEN" \
     -H "Content-Type: application/json" \
     https://your-datahub.com/api/graphql \
     -d '{"query": "{ me { corpUser { urn username }}}"}'
```

For Python applications:

```python
import requests

headers = {
    'Authorization': f'Bearer {your_jwt_token}',
    'Content-Type': 'application/json'
}

response = requests.post(
    'https://your-datahub.com/api/graphql',
    headers=headers,
    json={'query': '{ me { corpUser { urn username }}}'}
)
```

## Best Practices

- Use HTTPS for all JWKS URIs and discovery endpoints
- Use specific audience values (not wildcards) for better security
- Use short-lived tokens (< 1 hour recommended)
- Separate environments with different audiences (prod/staging/dev)
- Enable debug logging during setup: `DATAHUB_GMS_LOG_LEVEL=DEBUG`

## Troubleshooting

### Common Issues

**"OAuth authenticator is not configured"**

- Make sure `EXTERNAL_OAUTH_ENABLED=true` is set
- Verify all required environment variables are configured

**"No configured OAuth provider matches token issuer"**

- Check that your JWT issuer exactly matches `EXTERNAL_OAUTH_TRUSTED_ISSUERS`

**"Invalid or missing audience claim"**

- Verify your JWT audience is listed in `EXTERNAL_OAUTH_ALLOWED_AUDIENCES`

**"Failed to load signing keys"**

- Test your JWKS URI directly: `curl https://your-provider/.well-known/jwks.json`
- Check network connectivity from DataHub to your OAuth provider

### Debugging

Enable debug logging to see detailed OAuth messages:

```bash
# Set environment variable
DATAHUB_GMS_LOG_LEVEL=DEBUG

# Check logs
docker logs datahub-gms | grep -i oauth
```

### Testing Your Setup

Decode your JWT token to verify the claims:

```bash
# Replace with your actual token
echo "YOUR_JWT_TOKEN" | cut -d. -f2 | base64 -d | jq
```

Make sure the `iss` (issuer) and `aud` (audience) claims match your configuration.

## Advanced Options

You can customize which JWT claim contains the user ID:

```bash
# Use email claim instead of default 'sub'
EXTERNAL_OAUTH_USER_ID_CLAIM=email
```

OAuth users are automatically created as service accounts with usernames like `__oauth_{issuer_domain}_{subject}`.
