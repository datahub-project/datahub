# DataHub Teams Integration - Developer Setup Guide

This guide walks you through setting up a complete DataHub Teams integration development environment, including Azure Bot resources, multi-tenant routing, and local testing infrastructure.

## Overview

The DataHub Teams integration consists of several components:

- **Azure Bot Resource**: Microsoft-hosted bot service that handles Teams authentication
- **Teams App**: Microsoft Teams application package that users install
- **Multi-Tenant Router**: Routes Teams messages from multiple tenants to appropriate DataHub instances
- **DataHub Integration Service**: Core service that processes Teams messages and provides AI responses
- **Development Tunnel**: Exposes local services to Microsoft Teams via ngrok

## Prerequisites

- **Azure CLI** installed and configured (`az login` completed)
- **Python 3.8+** with `pip` and `virtualenv`
- **Docker** and **Docker Compose** (for DataHub quickstart)
- **Node.js** and **yarn** (for DataHub frontend)
- **ngrok** account and CLI tool installed

## Step 1: Create Azure Bot Resource

### 1.1 Configure Bot Creation

Create a configuration file for your bot:

```bash
cd datahub-integrations-service/scripts/teams/install/
cp bot_config_template.yaml &lt;your-username&gt;_bot_config.yaml
```

Edit `&lt;your-username&gt;_bot_config.yaml`:

```yaml
global:
  subscription_id: "your-azure-subscription-id" # Get from: az account show

bots:
  - name: "datahub-teams-bot-&lt;your-username&gt;"
    resource_group: "datahub-teams-&lt;your-username&gt;"
    location: "global" # Required for bot services
    owner: "&lt;your-username&gt;"
    webhook_url: "https://&lt;your-username&gt;.ngrok-free.app/public/teams/webhook"
    reuse_existing: true
    create_if_not_exists: true
    subscription_id: "your-azure-subscription-id"
```

> **Why this step?** The YAML configuration defines your Azure Bot Service resource, App Registration, and initial webhook URL. This creates the foundational Microsoft identity that Teams will use to communicate with your DataHub instance.

### 1.2 Create the Bot Resource

Run the bot creation script:

```bash
python3 create_azure_bot.py --config &lt;your-username&gt;_bot_config.yaml --output &lt;your-username&gt;_bot_results.yaml
```

**Expected Output:**

```
✅ Created App Registration - App ID: 12345678-1234-1234-1234-123456789abc
✅ Created Bot Service resource: datahub-teams-bot-&lt;your-username&gt;
✅ Enabled Microsoft Teams channel for bot
✅ Successfully created new bot: datahub-teams-bot-&lt;your-username&gt;
```

> **What happens?** This script creates an Azure App Registration (with client secret), Azure Bot Service resource, enables the Microsoft Teams channel, and configures Microsoft Graph permissions for Teams functionality.

### 1.3 Save Bot Credentials

The script generates an environment file:

```bash
# This file is automatically created: &lt;your-username&gt;_updated_env.env
source &lt;your-username&gt;_updated_env.env
```

**Environment Variables Created:**

```bash
DATAHUB_TEAMS_APP_ID=12345678-1234-1234-1234-123456789abc
DATAHUB_TEAMS_APP_PASSWORD=fake~secret~value~for~documentation~purposes
DATAHUB_TEAMS_WEBHOOK_URL=https://&lt;your-username&gt;.ngrok-free.app/public/teams/webhook
AZURE_BOT_NAME=datahub-teams-bot-&lt;your-username&gt;
AZURE_RESOURCE_GROUP=datahub-teams-&lt;your-username&gt;
```

> **Why save these?** These credentials authenticate your bot with Microsoft Teams and Azure. The App ID identifies your bot, the Client Secret proves your bot's identity, and the webhook URL tells Teams where to send messages.

## Step 2: Generate Teams Application Package

### 2.1 Create Teams App

Generate the Microsoft Teams app package that users will install:

```bash
python3 setup_dev_bot.py &lt;your-username&gt; --app-only
```

**Expected Output:**

```
🚀 Register command from <Your Name>: DataHub Dev &lt;your-username&gt;
✅ Generated Teams app package: datahub-teams-app-&lt;your-username&gt;-&lt;timestamp&gt;.zip
📦 Teams app ready for installation
```

> **What is this?** The Teams app package contains the app manifest, icons, and configuration that defines how your bot appears in Microsoft Teams. Users install this package to start chatting with your DataHub bot.

### 2.2 Install the Teams App in Your Organization

**⚠️ Important:** This step requires Teams admin privileges and must be done before users can find your bot.

#### Option A: Teams Admin Portal (Recommended)

1. **Access Admin Portal**: Go to [Microsoft Teams Admin Center](https://admin.teams.microsoft.com/policies/manage-apps)
2. **Upload Custom App**:
   - Click "Upload new app" or "+ Upload"
   - Select the generated `.zip` file: `datahub-teams-app-&lt;your-username&gt;-&lt;timestamp&gt;.zip`
   - Click "Upload" and wait for processing
3. **Approve App**:
   - Find your app in the list (search for "DataHub Dev &lt;your-username&gt;")
   - Set the status to "Allowed" or "Available"
   - Configure any permission policies as needed
4. **Publish**: The app is now available to your organization

> **Admin Note:** You may need to wait 10-15 minutes for the app to appear in Teams search after uploading.

#### Option B: Direct Installation (Personal Testing)

For quick personal testing during development:

1. **Open Microsoft Teams** (web or desktop client)
2. **Navigate to Apps**: Click "Apps" in the left sidebar
3. **Upload Custom App**:
   - Look for "Upload a custom app" or "Manage your apps" → "Upload an app"
   - Select "Upload a custom app" and choose the generated `.zip` file
   - Click "Install" when prompted
4. **Start Conversation**: Find your bot and start a personal chat

> **Note:** Option B only installs the app for you personally, not for your entire organization.

### 2.3 Find and Install the Bot (End User)

Once the Teams admin has uploaded the app, users can install it:

1. **Open Teams Apps Directory**: In Microsoft Teams, click "Apps" in the left sidebar
2. **Search for Your Bot**:
   - In the search bar, type "DataHub Dev &lt;your-username&gt;"
   - Or look under "Built for your org" or "Added by your org"
3. **Install**: Click on your bot and select "Install" or "Add"
4. **Start Chatting**: Begin a personal conversation with your DataHub bot

> **Note:** The bot won't respond until you complete the remaining setup steps (DataHub services and router configuration).

## Step 3: Set Up Local DataHub Environment

### 3.1 Start DataHub Services

Start the complete DataHub stack locally:

```bash
cd datahub-integrations-service/
../gradlew quickstartDebug
```

**Expected Services:**

- **GMS (Backend)**: `http://localhost:8080`
- **Frontend**: `http://localhost:3000`
- **Integrations Service**: `http://localhost:9003`
- **Database, Search, etc.**: Various other ports

> **Why quickstartDebug?** This starts DataHub with all services including the integrations service that handles Teams webhooks. The `Debug` variant includes additional logging and development tools.

### 3.2 Configure Integration Service Environment

Create an environment file for the integration service:

```bash
# In the datahub-integrations-service directory
cat > .env << EOF
DATAHUB_TEAMS_APP_ID=${DATAHUB_TEAMS_APP_ID}
DATAHUB_TEAMS_APP_PASSWORD=${DATAHUB_TEAMS_APP_PASSWORD}
EOF
```

> **Why this matters?** The DataHub integration service needs your bot credentials to authenticate with Microsoft Teams when sending replies back to users.

### 3.3 Restart Integration Service

Restart the integration service to pick up the new environment variables:

```bash
docker restart acryl-datahub-integrations-quickstart-dev-1
```

## Step 4: Set Up Multi-Tenant Router

### 4.1 Install the DataHub Multi-Tenant Router

The router is available as a Python package from the datahub-cloud-router directory:

```bash
cd ../../../datahub-cloud-router/
pip install -e .
```

> **What's the router for?** The multi-tenant router allows multiple developers to share the same Teams app while routing messages to their individual DataHub instances. It handles OAuth flows, tenant management, and message forwarding with production-ready features.

### 4.2 Start the Multi-Tenant Router

The router requires Teams credentials for webhook validation. Export the environment variables and start the router:

```bash
source &lt;your-username&gt;_updated_env.env
export DATAHUB_TEAMS_APP_ID=${DATAHUB_TEAMS_APP_ID}
export DATAHUB_TEAMS_APP_PASSWORD=${DATAHUB_TEAMS_APP_PASSWORD}
datahub-router
```

**⚠️ Important:** The `DATAHUB_TEAMS_APP_ID` and `DATAHUB_TEAMS_APP_PASSWORD` environment variables are required for Teams webhook validation. Without these, Teams messages will fail with "No validator registered for integration: teams" errors.

**Expected Output:**

```
🚀 DataHub Multi-Tenant Router
📊 Router Database: .dev/router.db
🔧 Default Target: http://localhost:9003
💼 Admin API: http://localhost:9005/admin/
🌐 Teams Webhook: http://localhost:9005/public/teams/webhook
🔗 Bot Framework URL: http://localhost:9005/api/messages
✅ Health Check: http://localhost:9005/health
INFO: Uvicorn running on http://0.0.0.0:9005
```

> **Router Architecture:** The production-ready router provides:
>
> - **`/public/teams/webhook`**: Receives messages from Teams
> - **`/api/messages`**: Bot Framework message endpoint
> - **`/public/teams/oauth/callback`**: Handles OAuth authentication with auto-registration
> - **`/admin/`**: RESTful API for managing instances and tenant mappings
> - **`/health`**: Health monitoring and status checks
> - **`/events`**: Event logging and telemetry

### 4.3 Router Configuration Options

The router supports multiple configuration methods:

**Environment Variables:**

```bash
export DATAHUB_ROUTER_TARGET_URL=http://localhost:9003
export DATAHUB_ROUTER_DB_TYPE=sqlite  # or mysql, inmemory
export DATAHUB_ROUTER_DB_PATH=.dev/router.db
```

**Command Line Options:**

```bash
# Custom port and target
datahub-router --port 8090 --target-url http://datahub.company.com

# MySQL database (for production)
datahub-router --db-type mysql --db-host localhost --db-name datahub_router

# Help and options
datahub-router --help
```

**Database Options:**

- **SQLite** (default): File-based, good for development
- **MySQL**: Production-ready, supports high concurrency
- **In-Memory**: For testing only

## Step 5: Set Up Development Tunnels

### 5.1 Configure ngrok

Set up persistent tunnels for development:

```bash
cd datahub-integrations-service/scripts/teams/
./setup_teams_tunnel.sh
```

**Interactive Configuration:**

```
🔧 DataHub Teams Tunnel Setup
========================================

Enter configuration details:
1. ngrok Domain (leave blank for auto): &lt;your-subdomain&gt;.ngrok-free.app
2. DataHub URL (default: http://localhost:3000): [Enter]
3. Teams Bot Name: datahub-teams-bot-&lt;your-username&gt;

🌐 Starting ngrok tunnel...
✅ Tunnel active: https://&lt;your-subdomain&gt;.ngrok-free.app -> localhost:9005
💾 Webhook URL saved to /tmp/teams_webhook_url.txt
```

> **Why ngrok?** Microsoft Teams can only send webhooks to public HTTPS endpoints. ngrok creates a secure tunnel from the internet to your local development environment, allowing Teams to reach your router.

### 5.2 Verify Tunnel Configuration

Check that the tunnel is working:

```bash
curl https://&lt;your-subdomain&gt;.ngrok-free.app/health
# Should return: {"status": "healthy", "router": "active"}
```

## Step 6: Register Your Development Instance

### 6.1 Use the Register Command

From Microsoft Teams, send a message to your bot:

```
register http://localhost:3000
```

**Expected Response:**

```
🚀 DataHub Teams Registration

Hi <Your Name>! Let's connect your Teams to DataHub.

Step 1: Open DataHub Settings
🔗 Go to Teams Integration Settings (http://localhost:3000/settings/integrations/microsoft-teams)

Step 2: Paste Teams URL
Copy and paste this Teams URL into the "Enter Teams URL" field:
https://teams.microsoft.com/l/channel/19%3A...

Step 3: Connect to Teams
Click "Connect to Teams" to complete the setup. DataHub will automatically extract your tenant ID and configure the integration.

After setup is complete, return here to test the integration!
```

> **Auto-Registration Magic:** The register command provides you with the exact Teams URL to paste in DataHub settings. When you complete the OAuth flow, the router automatically creates a tenant mapping, eliminating manual configuration.

### 6.2 Complete OAuth Flow

**Prerequisites:** Ensure your Teams app has been installed via Teams Admin Portal (see Step 2.2).

1. **Open DataHub Settings**: Go to `http://localhost:3000/settings/integrations/microsoft-teams`
2. **Paste Teams URL**: Copy the URL from the register command response
3. **Connect to Teams**: Click the blue "Connect to Teams" button
4. **OAuth Authorization**: Microsoft will redirect you through OAuth approval
5. **Success**: You'll be redirected back to DataHub with "✅ Teams integration configured successfully!"

**Router Logs (Auto-Registration):**

```
🔄 Auto-registering tenant abcd1234-5678-90ab-cdef-123456789012 after OAuth success for http://localhost:3000
✅ Created DataHub instance: oauth-instance-12345678
✅ Created tenant mapping: abcd1234-5678-90ab-cdef-123456789012 -> http://localhost:3000
```

> **If OAuth fails:** Verify that your Teams app is properly installed and approved in the Teams Admin Portal.

## Step 7: Test the Integration

### 7.1 Send Test Messages

From Microsoft Teams, send various test messages:

**Basic Chat:**

```
hello
```

**DataHub Questions:**

```
what datasets do we have?
```

**URL Unfurling:**

```
http://localhost:3000/dataset/urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)
```

### 7.2 Monitor the Flow

Watch the logs to verify the complete flow:

**1. Router Logs (`datahub-router` terminal):**

```
🔄 ROUTING EVENT TO TARGET INSTANCE...
✅ Routed to http://localhost:3000 via tenant mapping (tenant: abcd1234-5678-90ab-cdef-123456789012)
📡 Forwarded to http://localhost:9003/public/teams/webhook -> Status: 200
✅ Event successfully routed and forwarded
```

**2. Integration Service Logs (`docker logs`):**

```bash
docker logs acryl-datahub-integrations-quickstart-dev-1 --tail 10
```

```
DEBUG | Received Teams webhook: {'text': 'hello', 'type': 'message', ...}
INFO  | Processing personal conversation message as question
INFO  | Generated AI response for user query: hello
INFO  | Sent reply back to Teams conversation
```

**3. Teams Response:**
You should receive an AI-generated response in Teams based on your DataHub data.

## Troubleshooting

### Common Issues

**1. Bot Not Responding**

- ✅ Check router is running: `curl http://localhost:9005/health`
- ✅ Verify environment variables are set: `echo $DATAHUB_TEAMS_APP_ID` and `echo $DATAHUB_TEAMS_APP_PASSWORD`
- ✅ Check integration service logs: `docker logs acryl-datahub-integrations-quickstart-dev-1`
- ✅ Look for "No validator registered for integration: teams" errors in router logs

**2. OAuth Fails**

- ✅ Verify ngrok tunnel is active: `curl https://&lt;subdomain&gt;.ngrok-free.app/health`
- ✅ Check Azure bot webhook URL matches ngrok URL
- ✅ Confirm Teams URL contains tenant ID

**3. Messages Not Routing**

- ✅ Check tenant mapping exists: `curl http://localhost:9005/admin/tenants`
- ✅ Verify DataHub is running: `curl http://localhost:3000`
- ✅ Test integration service: `curl http://localhost:9003/ping`

**4. Tenant Not Found Errors**

- ✅ Complete OAuth flow to create tenant mapping
- ✅ Use register command to get correct Teams URL
- ✅ Check router database: `sqlite3 .dev/router.db "SELECT * FROM tenant_instance_mappings;"`

**5. Can't Find Bot in Teams**

- ✅ Verify app was uploaded to Teams Admin Portal
- ✅ Check app status is "Allowed" in admin portal
- ✅ Wait 10-15 minutes after upload for Teams to index the app
- ✅ Search for exact app name: "DataHub Dev &lt;your-username&gt;"

**6. Bot Not Responding After Installation**

- ✅ Ensure register command was used before OAuth
- ✅ Verify OAuth flow completed successfully
- ✅ Check that tenant mapping was auto-created
- ✅ Test with simple message like "hello"

**7. Restarting ngrok Tunnel (Common Scenario)**

When your ngrok tunnel becomes inactive (machine restart, network changes, etc.), follow these steps to restore the connection:

**⚠️ CRITICAL: Start Router First**

The `setup_teams_tunnel.sh` script automatically detects if the router is running:

- **Router running (port 9005)** → ngrok tunnels to router for multi-tenant routing + OAuth
- **No router detected** → ngrok tunnels directly to integration service (port 9003) - bypasses routing

**You MUST start the router first if you want routing and OAuth functionality:**

```bash
cd datahub-cloud-router/
source ~/.datahub/local.env
export DATAHUB_TEAMS_APP_ID=${DATAHUB_TEAMS_APP_ID}
export DATAHUB_TEAMS_APP_PASSWORD=${DATAHUB_TEAMS_APP_PASSWORD}
datahub-router  # Should show "Uvicorn running on http://0.0.0.0:9005"
```

1. **Restart the ngrok tunnel** (after router is running):

   ```bash
   cd datahub-integrations-service/scripts/teams/
   ./setup_teams_tunnel.sh
   ```

2. **Update Azure Bot webhook URL**:

   ```bash
   # Find your bot configuration
   ls install/*_bot_config.yaml  # Look for your &lt;username&gt;_bot_config.yaml

   # Update the correct bot (replace &lt;username&gt; with your actual username)
   az bot update \
     --name "datahub-teams-bot-&lt;username&gt;" \
     --resource-group "datahub-teams-&lt;username&gt;" \
     --subscription "$(grep subscription_id install/&lt;username&gt;_bot_config.yaml | cut -d'"' -f2)" \
     --endpoint "$(cat /tmp/teams_webhook_url.txt)" \
     --output table

   # Verify the update
   az bot show \
     --name "datahub-teams-bot-&lt;username&gt;" \
     --resource-group "datahub-teams-&lt;username&gt;" \
     --subscription "$(grep subscription_id install/&lt;username&gt;_bot_config.yaml | cut -d'"' -f2)" \
     --query "properties.endpoint" --output tsv
   ```

3. **Update Azure App Registration redirect URI** (Critical for OAuth):

   ```bash
   # Get the App ID from your bot config
   APP_ID=$(grep app_id install/&lt;username&gt;_bot_config.yaml | cut -d'"' -f2)

   # Update the redirect URI for OAuth callback
   NGROK_URL=$(cat /tmp/teams_webhook_url.txt | sed 's|/public/teams/webhook||')
   az ad app update \
     --id "$APP_ID" \
     --web-redirect-uris "${NGROK_URL}/public/teams/oauth/callback"

   # Verify the redirect URI was updated
   az ad app show --id "$APP_ID" --query "web.redirectUris" --output table
   ```

4. **Update local environment variables**:

   ```bash
   # Update ~/.datahub/local.env with new ngrok URLs
   NGROK_URL=$(cat /tmp/teams_webhook_url.txt | sed 's|/public/teams/webhook||')
   sed -i "s|DATAHUB_TEAMS_OAUTH_REDIRECT_URI=.*|DATAHUB_TEAMS_OAUTH_REDIRECT_URI=${NGROK_URL}/public/teams/oauth/callback|" ~/.datahub/local.env
   sed -i "s|DATAHUB_TEAMS_WEBHOOK_URL=.*|DATAHUB_TEAMS_WEBHOOK_URL=${NGROK_URL}/public/teams/webhook|" ~/.datahub/local.env
   ```

5. **Restart DataHub integration service** (if needed):

   ```bash
   # Restart integration service to pick up new environment variables
   docker restart acryl-datahub-integrations-quickstart-dev-1
   ```

6. **Test the connection**:

   ```bash
   # Test ngrok tunnel
   curl https://$(cat /tmp/teams_webhook_url.txt | cut -d'/' -f3)/health

   # Should return: {"status": "healthy", "router": "active"}
   ```

> **Important**: The router does NOT need to be restarted when the ngrok URL changes. The router runs on localhost:9005 and ngrok simply forwards traffic to it. Only the Azure Bot Service and local environment variables need to be updated with the new ngrok URL.

> **Why this happens**: ngrok tunnels are temporary and get new URLs when restarted. Since Microsoft Teams needs a fixed webhook URL, you must update the Azure Bot Service with the new ngrok URL each time the tunnel restarts.

> **Tip**: For production deployments, use a permanent domain instead of ngrok to avoid this issue.

### Logs and Debugging

**View Router Admin Interface:**

```bash
open http://localhost:9005/admin/
```

**Check Tenant Mappings:**

```bash
curl http://localhost:9005/admin/tenants | jq
```

**View Router Events:**

```bash
curl http://localhost:9005/events | jq
```

## Architecture Summary

Your development setup now includes:

```
Microsoft Teams
       ↓ (HTTPS webhook)
   ngrok tunnel
       ↓ (localhost:9005)
 DataHub Multi-Tenant Router ←→ .dev/router.db (SQLite)
       ↓ (localhost:9003)        ↓
DataHub Integration Service ←→ DataHub GMS ←→ DataHub Frontend
                                  ↓              ↓
                            (localhost:8080) (localhost:3000)
```

**Message Flow:**

1. User sends message in Teams
2. Teams → ngrok tunnel → Multi-tenant router
3. Router looks up tenant mapping
4. Router forwards to DataHub Integration Service
5. Integration Service processes message (AI, search, unfurling)
6. Integration Service sends reply back to Teams
7. Teams displays response to user

**OAuth Flow:**

1. User runs `register http://localhost:3000` in Teams
2. Router provides Teams URL for DataHub settings
3. User pastes Teams URL in DataHub settings
4. DataHub starts OAuth flow with tenant ID in state
5. Teams → ngrok → Router → DataHub OAuth callback
6. Router auto-creates tenant mapping
7. Integration complete!

## Deployment Workflow Summary

For a complete development setup:

1. **Developer**: Creates Azure bot resources and app package
2. **Teams Admin**: Uploads and approves app in Teams Admin Portal
3. **End Users**: Install bot from Teams Apps directory
4. **Developer**: Starts local DataHub and router services
5. **End Users**: Use register command to link their tenant to developer's DataHub
6. **Integration Complete**: Users can chat with DataHub bot!

## Next Steps

- **Multiple Developers:** Each developer creates their own bot with unique `&lt;username&gt;`
- **Production Deployment:** Replace ngrok with permanent domain and deploy router
- **Custom Features:** Extend Teams handlers in `datahub-integrations-service/src/datahub_integrations/teams/`
- **Team Collaboration:** Share tenant mappings for team-wide DataHub instances
- **Enterprise Setup:** Configure app policies and permissions in Teams Admin Portal

## Important Notes for Teams Admins

- **App Approval Process**: Development bots need admin approval before users can install them
- **Permission Policies**: Configure appropriate permissions for DataHub bot functionality
- **User Communication**: Inform users about the new DataHub bot availability and how to install it
- **Security Review**: Review bot permissions and data access before approving

Happy developing! 🚀
