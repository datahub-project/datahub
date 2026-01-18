# Managing DataHub CLI Profiles

Do you work with multiple DataHub instances? Constantly switching between local development, staging, and production environments? Profile management makes this easy.

## Why Use Profiles?

**The Problem**: Before profiles, you had to:

- Manually edit `~/.datahubenv` every time you switched environments
- Rely on shell aliases or wrapper scripts
- Risk accidentally running commands against the wrong environment

**The Solution**: Profiles let you:

- ✅ **Manage multiple environments** in one configuration file
- ✅ **Switch instantly** with a single command or flag
- ✅ **Stay safe** with production confirmation prompts
- ✅ **Keep secrets secure** using environment variable references

## Quick Start

### 1. Add Your First Profile

```bash
datahub profile add dev \
  --server http://localhost:8080 \
  --token-env DATAHUB_DEV_TOKEN \
  --description "Local development"
```

This creates `~/.datahub/config.yaml` with your first profile.

### 2. Set It as Active

```bash
datahub profile use dev
```

Now all commands use the `dev` profile automatically.

### 3. Run Commands

```bash
# Uses the current profile (dev)
datahub get --urn "urn:li:dataset:..."

# Or override with --profile flag
datahub get --profile staging --urn "urn:li:dataset:..."
```

## Common Use Cases

### Managing Dev, Staging, and Prod

```bash
# Local development
datahub profile add dev \
  --server http://localhost:8080 \
  --token-env DATAHUB_DEV_TOKEN

# Staging environment
datahub profile add staging \
  --server https://datahub-staging.company.com \
  --token-env DATAHUB_STAGING_TOKEN \
  --timeout 60

# Production (with safety confirmation)
datahub profile add prod \
  --server https://datahub.company.com \
  --token-env DATAHUB_PROD_TOKEN \
  --require-confirmation \
  --timeout 120
```

### Secure Token Storage

**Recommended**: Store tokens in environment variables, not the config file.

```bash
# Set environment variables (add these to your ~/.bashrc or ~/.zshrc)
export DATAHUB_DEV_TOKEN="your-dev-token"
export DATAHUB_STAGING_TOKEN="your-staging-token"
export DATAHUB_PROD_TOKEN="your-prod-token"

# Reference them in profiles
datahub profile add prod \
  --server https://datahub.company.com \
  --token-env DATAHUB_PROD_TOKEN
```

Your `~/.datahub/config.yaml` will contain `${DATAHUB_PROD_TOKEN}` which gets interpolated at runtime:

```yaml
profiles:
  prod:
    server: https://datahub.company.com
    token: ${DATAHUB_PROD_TOKEN}
```

### Working with Customer Instances

Support engineers can maintain profiles for multiple customers:

```bash
datahub profile add customer-acme \
  --server https://datahub.acme.com \
  --token-env ACME_TOKEN

datahub profile add customer-globex \
  --server https://datahub.globex.com \
  --token-env GLOBEX_TOKEN

# Switch between them
datahub profile use customer-acme
datahub get --urn "..."

datahub profile use customer-globex
datahub get --urn "..."
```

## Profile Commands

### List All Profiles

```bash
datahub profile list
```

**Output:**

```
Available profiles:

  dev (current)
    Server: http://localhost:8080
    Description: Local development

  staging
    Server: https://datahub-staging.company.com
    Timeout: 60s

  prod
    Server: https://datahub.company.com
    Timeout: 120s
    ⚠️  Requires confirmation for destructive operations
    Description: Production - USE WITH CAUTION
```

### Show Current Profile

```bash
datahub profile current
```

Shows which profile is active and its configuration.

### Show Specific Profile

```bash
datahub profile show prod
```

**Output:**

```
Profile: prod
Server: https://datahub.company.com
Token: ****oken (from env: DATAHUB_PROD_TOKEN)
Timeout: 120s
Require Confirmation: true
Description: Production - USE WITH CAUTION
```

Tokens are automatically redacted in output.

### Switch Profiles

```bash
datahub profile use staging
```

Sets `staging` as the current profile for all subsequent commands.

### Test Connection

```bash
datahub profile test prod
```

Verifies you can connect to the profile's DataHub instance.

### Remove Profile

```bash
datahub profile remove old-dev
```

Prompts for confirmation before removing. Use `--force` to skip the prompt.

### Validate Configuration

```bash
datahub profile validate
```

Checks your `~/.datahub/config.yaml` for syntax errors and warns about missing tokens.

### Export Profile

```bash
datahub profile export prod
```

Exports profile configuration with tokens redacted - useful for sharing team configurations.

## Profile Selection Priority

When running a command, DataHub selects a profile using this priority order (highest to lowest):

1. **`--profile` flag**: `datahub get --profile prod --urn "..."`
2. **`DATAHUB_PROFILE` environment variable**: `export DATAHUB_PROFILE=staging`
3. **Current profile**: Set via `datahub profile use dev`
4. **Profile named "default"**: If you have a profile called `default`
5. **Legacy config**: Falls back to `~/.datahubenv` (backward compatible)

### Example: Temporary Override

```bash
# Current profile is 'dev'
datahub profile current
# Output: dev

# But you can override for a single command
datahub get --profile prod --urn "..."

# Current profile is still 'dev'
datahub profile current
# Output: dev
```

### Example: Per-Shell Environment

```bash
# Terminal 1: Work on staging
export DATAHUB_PROFILE=staging
datahub get --urn "..."  # Uses staging

# Terminal 2: Work on prod
export DATAHUB_PROFILE=prod
datahub get --urn "..."  # Uses prod
```

## Production Safety

### Confirmation Prompts

Profiles with `require_confirmation: true` trigger prompts before destructive operations:

```bash
datahub delete --platform postgres --entity-type dataset
```

**Prompt:**

```
⚠️  DESTRUCTIVE OPERATION WARNING

Profile: prod
Server: https://datahub.company.com
Operation: Delete by filter
Details: platform=postgres, entity_type=dataset

This operation cannot be undone. Type 'PROD' to confirm: _
```

You must type the profile name in **UPPERCASE** to proceed.

### Bypassing for Automation

In CI/CD pipelines, use `--force` to skip confirmation:

```bash
datahub delete --platform postgres --entity-type dataset --force
```

**Security Note**: Only use `--force` in trusted automation contexts.

## Configuration File Format

Your profile configuration lives in `~/.datahub/config.yaml`:

```yaml
version: "1.0"
current_profile: dev

profiles:
  dev:
    server: http://localhost:8080
    token: ${DATAHUB_DEV_TOKEN}
    timeout_sec: 30
    description: "Local development"

  staging:
    server: https://datahub-staging.company.com
    token: ${DATAHUB_STAGING_TOKEN}
    timeout_sec: 60
    extra_headers:
      X-Environment: staging
    description: "Staging environment"

  prod:
    server: https://datahub.company.com
    token: ${DATAHUB_PROD_TOKEN}
    timeout_sec: 120
    require_confirmation: true
    extra_headers:
      X-Environment: production
    description: "Production - USE WITH CAUTION"
```

### Configuration Options

**Per-profile settings:**

- `server` (required): DataHub GMS URL
- `token`: Access token (recommend using environment variable reference)
- `timeout_sec`: Connection timeout in seconds (default: 30)
- `require_confirmation`: Prompt before destructive operations (default: false)
- `description`: Human-readable description
- `extra_headers`: Custom HTTP headers

### Environment Variable Interpolation

Three interpolation syntaxes are supported:

**1. Simple substitution:**

```yaml
token: ${DATAHUB_TOKEN}
```

Uses the environment variable value, or empty string if not set.

**2. Default value:**

```yaml
token: ${DATAHUB_TOKEN:-default-token}
```

Uses the environment variable value, or `default-token` if not set.

**3. Required variable:**

```yaml
token: ${DATAHUB_TOKEN:?Token is required}
```

Uses the environment variable value, or throws an error with the message if not set.

## Migrating from Legacy Config

### Automatic Migration

If you have an existing `~/.datahubenv` file, DataHub automatically detects and uses it. No migration required!

### Opting Into Profiles

When ready, migrate to profiles for better multi-environment management:

```bash
# Your existing ~/.datahubenv is automatically detected
datahub get --urn "..."  # Still works!

# Add new profiles alongside it
datahub profile add staging --server https://staging.datahub.com
datahub profile add prod --server https://prod.datahub.com

# Switch between them
datahub profile use staging
```

### Manual Migration

To explicitly migrate your legacy config to a profile:

```bash
# 1. Check your current config
cat ~/.datahubenv

# 2. Add a 'default' profile with those settings
datahub profile add default \
  --server <your-current-server> \
  --token-env DATAHUB_TOKEN

# 3. Test it works
datahub profile test default

# 4. (Optional) Keep ~/.datahubenv as backup or delete it
```

## Environment Variable Overrides

Environment variables **always** override profile settings:

```yaml
# Profile config
profiles:
  dev:
    server: http://localhost:8080
    token: ${DEV_TOKEN}
```

```bash
# Override server for this command
DATAHUB_GMS_URL=http://localhost:9090 datahub get --urn "..."

# Override token for this command
DATAHUB_GMS_TOKEN=different-token datahub get --urn "..."
```

**Supported override variables:**

- `DATAHUB_GMS_URL`: Override server URL
- `DATAHUB_GMS_TOKEN`: Override access token
- `DATAHUB_GMS_HOST`, `DATAHUB_GMS_PORT`, `DATAHUB_GMS_PROTOCOL`: Construct URL override

This maintains backward compatibility with existing workflows.

## Troubleshooting

### Profile Not Found

**Error:**

```
Error: Profile 'staging' not found. Available profiles: dev, prod

Run 'datahub profile list' to see all profiles.
Run 'datahub profile add staging' to create a new profile.
```

**Solution**: The profile doesn't exist. Create it with `datahub profile add` or check for typos.

### Missing Environment Variable

**Error:**

```
Error: Required environment variable not set: DATAHUB_PROD_TOKEN
Profile 'prod' requires this variable to be set.

Set it with:
  export DATAHUB_PROD_TOKEN=<your-token>
```

**Solution**: Set the required environment variable in your shell or `~/.bashrc`/`~/.zshrc`.

### Configuration Syntax Error

**Error:**

```
Error: Failed to parse configuration file: ~/.datahub/config.yaml
Line 5: mapping values are not allowed here

Run 'datahub profile validate' to check syntax.
```

**Solution**: Your YAML has a syntax error. Run `datahub profile validate` for details, or check [YAML syntax](https://yaml.org/spec/).

### Token Not Working

**Check:**

1. Is the environment variable set? `echo $DATAHUB_PROD_TOKEN`
2. Is the token valid? Test with `datahub profile test prod`
3. Is the interpolation syntax correct? Should be `${VAR_NAME}` in config file

**Debug:**

```bash
# Show effective configuration (tokens redacted)
datahub profile show prod

# Test the connection
datahub profile test prod
```

### Wrong Profile Active

**Problem**: Commands are running against the wrong environment.

**Solution**: Check which profile is active:

```bash
datahub profile current
```

If it's wrong:

```bash
# Switch to correct profile
datahub profile use dev

# Or use --profile flag for single command
datahub get --profile dev --urn "..."
```

## Best Practices

### 1. Use Environment Variables for Tokens

**✅ Good:**

```yaml
token: ${DATAHUB_PROD_TOKEN}
```

**❌ Bad:**

```yaml
token: "actual-token-here"
```

Environment variables keep secrets out of config files and version control.

### 2. Enable Confirmation for Production

**Always** set `require_confirmation: true` for production profiles:

```bash
datahub profile add prod \
  --server https://datahub.company.com \
  --require-confirmation
```

This prevents accidental destructive operations.

### 3. Use Descriptive Profile Names

**✅ Good:**

```bash
dev, staging, prod
customer-acme, customer-globex
local-8080, local-9090
```

**❌ Bad:**

```bash
profile1, profile2
test, test2, test-final
```

Clear names prevent confusion and errors.

### 4. Document Profiles for Your Team

Share a team configuration template:

```bash
# Export your setup (tokens redacted)
datahub profile export dev > dev-profile-template.yaml
```

Team members can import and add their own tokens.

### 5. Test Before Production

```bash
# Always test staging first
datahub profile use staging
datahub ingest -c my-recipe.yml

# Then production
datahub profile use prod
datahub ingest -c my-recipe.yml
```

### 6. Set Appropriate Timeouts

```yaml
profiles:
  dev:
    timeout_sec: 30 # Fast local responses
  prod:
    timeout_sec: 120 # Allow for network latency
```

### 7. Use Profiles in CI/CD

```yaml
# .github/workflows/ingest.yml
- name: Ingest to staging
  run: |
    export DATAHUB_PROFILE=staging
    export DATAHUB_STAGING_TOKEN=${{ secrets.STAGING_TOKEN }}
    datahub ingest -c recipe.yml

- name: Ingest to production
  if: github.ref == 'refs/heads/main'
  run: |
    export DATAHUB_PROFILE=prod
    export DATAHUB_PROD_TOKEN=${{ secrets.PROD_TOKEN }}
    datahub ingest -c recipe.yml --force
```

## Advanced Usage

### Profile-Specific Headers

Add custom headers per profile:

```yaml
profiles:
  prod:
    server: https://datahub.company.com
    token: ${PROD_TOKEN}
    extra_headers:
      X-Environment: production
      X-Team: data-platform
      X-Cost-Center: "1234"
```

### Dynamic Profile Selection

```bash
#!/bin/bash
# deploy.sh - Select profile based on branch

if [[ "$BRANCH" == "main" ]]; then
  PROFILE="prod"
elif [[ "$BRANCH" == "staging" ]]; then
  PROFILE="staging"
else
  PROFILE="dev"
fi

export DATAHUB_PROFILE=$PROFILE
datahub ingest -c recipe.yml
```

### Multiple Profiles for Same Instance

Useful for different access levels:

```yaml
profiles:
  prod-read:
    server: https://datahub.company.com
    token: ${PROD_READ_TOKEN}
    description: "Production read-only"

  prod-write:
    server: https://datahub.company.com
    token: ${PROD_WRITE_TOKEN}
    require_confirmation: true
    description: "Production write access"
```

## Getting Help

- View all profile commands: `datahub profile --help`
- View specific command help: `datahub profile add --help`
- Check profile status: `datahub profile validate`
- Test connection: `datahub profile test <name>`

## Related Documentation

- [DataHub CLI Overview](../cli.md)
- [Deleting Metadata](./delete-metadata.md)
- [Ingestion Configuration](../metadata-ingestion/README.md)
- [Authentication](../authentication/README.md)

## Feedback

Have suggestions for improving profile management? [Open an issue on GitHub](https://github.com/datahub-project/datahub/issues)!
