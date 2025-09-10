# DataHub Teams Bot Development Setup

This directory contains scripts for setting up DataHub Teams bots for development, including Azure Bot Service resource creation and Teams app manifest generation.

## Overview

The setup process creates:

1. **Azure Bot Service resource** - The backend bot infrastructure
2. **Azure App Registration** - Authentication and permissions
3. **Teams App Manifest** - Installable Teams app package
4. **Environment Configuration** - Development environment setup

## Quick Start

### Prerequisites

1. **Azure CLI installed and logged in**:

   ```bash
   az login
   az account set --subscription "6b171c8c-84c4-4293-9865-3d16ff142515"
   ```

2. **Python dependencies**:
   ```bash
   pip install pyyaml loguru
   ```

### Create Complete Bot Setup

```bash
# Create bot + Teams app for username "john"
python3 setup_dev_bot.py john

# With custom webhook URL
python3 setup_dev_bot.py john --webhook-url https://my-custom.ngrok.io/integrations/teams/webhook

# With custom ngrok subdomain
python3 setup_dev_bot.py john --ngrok-subdomain john-datahub
```

This creates:

- Azure bot resource: `datahub-teams-bot-john`
- Resource group: `datahub-teams-john`
- Teams app package in `dev_bots/john/`
- Environment file with credentials

### Create App-Only (for existing bot)

```bash
# Generate new Teams app package for existing bot
python3 setup_dev_bot.py john --app-only

# With specific app ID
python3 setup_dev_bot.py john --app-only --app-id "12345678-1234-1234-1234-123456789abc"
```

### Cleanup Resources

```bash
# Delete Azure resources
python3 setup_dev_bot.py john --cleanup
```

## Files

### Main Scripts

- **`setup_dev_bot.py`** - Main orchestration script (recommended)
- **`create_azure_bot.py`** - Azure Bot Service resource creation
- **`generate_teams_manifest.py`** - Teams app manifest generation (legacy)

### Configuration

- **`bot_config_template.yaml`** - YAML template for bot configuration
- **`dev_bots/{username}/`** - Per-user output directory with:
  - `bot_config.yaml` - Generated bot configuration
  - `bot_creation_results.yaml` - Azure resource details
  - `.env` - Environment variables for development
  - `setup_summary.yaml` - Complete setup information
  - `datahub-teams-{scenario}-{timestamp}.zip` - Teams app package

## Configuration

### YAML Configuration

The bot configuration supports these key settings:

```yaml
global:
  subscription_id: "6b171c8c-84c4-4293-9865-3d16ff142515"
  location: "eastus"

bots:
  - name: "datahub-teams-bot-{username}"
    subscription_id: "{subscription_id}"
    resource_group: "datahub-teams-{username}"
    webhook_url: "https://{username}.ngrok-free.app/integrations/teams/webhook"

    # Behavior controls
    create_if_not_exists: true # Create bot if it doesn't exist
    reuse_existing: true # Reuse existing bot resources

    teams_app:
      enabled: true
      name: "DataHub Dev - {username}"
      scopes: ["personal", "team", "groupchat"]
```

### Template Variables

Available in configuration templates:

- `{username}` - Developer username
- `{timestamp}` - Current timestamp (YYYYMMDD-HHMMSS)
- `{uuid}` - Random UUID
- `{subscription_id}` - Azure subscription ID

## Development Workflow

### 1. Initial Setup

```bash
# First time - creates everything
python3 setup_dev_bot.py myusername
```

### 2. Start Development Environment

```bash
# Source environment variables
source dev_bots/myusername/.env

# Start DataHub integrations service
cd datahub-integrations-service
uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003 --reload
```

### 3. Install Teams App

- Go to Microsoft Teams → Apps → Manage your apps → Upload an app
- Upload the generated `.zip` file from `dev_bots/myusername/`
- Install in a team or personal chat

### 4. Generate New App Versions

```bash
# Create new Teams app package (reuses existing bot)
python3 setup_dev_bot.py myusername --app-only
```

### 5. Cleanup When Done

```bash
# Remove Azure resources
python3 setup_dev_bot.py myusername --cleanup
```

## Advanced Usage

### Custom Configuration

```bash
# Use custom YAML config
python3 setup_dev_bot.py myusername --config my_custom_config.yaml

# Specify different subscription
python3 setup_dev_bot.py myusername --subscription-id "different-sub-id"
```

### Direct Azure Bot Creation

```bash
# Create only Azure resources (no Teams app)
python3 create_azure_bot.py --config bot_config.yaml --output bot_results.yaml
```

### Legacy Manifest Generation

```bash
# Generate Teams app manifest only
python3 generate_teams_manifest.py --bot-id "app-id" --scenario my-test
```

## Resource Management

### What Gets Created

**Azure Resources:**

- Resource Group: `datahub-teams-{username}`
- Bot Service: `datahub-teams-bot-{username}`
- App Registration: `datahub-teams-bot-{username}-app`
- Service Principal (automatic)

**Local Files:**

- Teams app package (`.zip`)
- Environment configuration (`.env`)
- Setup metadata (`.yaml` files)

### Reuse Behavior

- **Existing bots**: Automatically detected and reused
- **New app packages**: Always generate fresh Teams app packages
- **Client secrets**: Cannot retrieve existing secrets (create new if needed)

### Cleanup

The cleanup process removes:

- Azure Bot Service resource
- App Registration and Service Principal
- Local configuration files (optional)

## Troubleshooting

### Common Issues

**Azure CLI not logged in:**

```bash
az login
az account show  # Verify login
```

**Wrong subscription:**

```bash
az account set --subscription "6b171c8c-84c4-4293-9865-3d16ff142515"
```

**Bot already exists:**

- Script automatically reuses existing bots
- Use `--app-only` to generate new Teams packages

**Client secret issues:**

- Existing secrets cannot be retrieved
- Create new secret manually in Azure Portal
- Or delete/recreate the entire bot setup

**Teams app installation fails:**

- Verify webhook URL is accessible
- Check DataHub integrations service is running
- Ensure bot resource has Microsoft Teams channel enabled

### Debug Mode

```bash
# Dry run to see what would be created
python3 setup_dev_bot.py myusername --dry-run

# Check existing resources
az bot list --resource-group "datahub-teams-myusername"
```

## Security Notes

- **Client secrets** are created and stored in `.env` files
- **Never commit** `.env` files to version control
- **Rotate secrets** regularly in production
- **Delete unused** bot resources to avoid costs
- **Webhook URLs** should use HTTPS and authentication

## Migration from Old Scripts

If you were using `generate_teams_manifest.py` directly:

**Old way:**

```bash
python3 generate_teams_manifest.py --bot-id "app-id"
```

**New way:**

```bash
python3 setup_dev_bot.py myusername --app-only --app-id "app-id"
```

The new approach provides better organization, automatic resource management, and integrated environment setup.
