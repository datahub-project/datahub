#!/bin/bash

# update_azure_bot_webhook.sh - Updates Azure Bot Service webhook URL
# Usage: ./update_azure_bot_webhook.sh [webhook_url]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Load Azure configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.azure_config" ]; then
    source "$SCRIPT_DIR/.azure_config"
else
    echo -e "${RED}❌ Azure configuration file not found: $SCRIPT_DIR/.azure_config${NC}"
    echo "Please ensure .azure_config exists with Azure resource details"
    exit 1
fi

# Use config values
SUBSCRIPTION_ID="$AZURE_SUBSCRIPTION_ID"
RESOURCE_GROUP="$AZURE_RESOURCE_GROUP"
BOT_NAME="$AZURE_BOT_NAME"

# Get webhook URL from argument or file
WEBHOOK_URL="$1"
if [ -z "$WEBHOOK_URL" ]; then
    if [ -f "/tmp/teams_webhook_url.txt" ]; then
        WEBHOOK_URL=$(cat /tmp/teams_webhook_url.txt)
        echo -e "${BLUE}📋 Using webhook URL from /tmp/teams_webhook_url.txt${NC}"
    else
        echo -e "${RED}❌ No webhook URL provided${NC}"
        echo "Usage: $0 [webhook_url]"
        echo "Or run setup_teams_tunnel.sh first to generate /tmp/teams_webhook_url.txt"
        exit 1
    fi
fi

echo -e "${GREEN}🤖 Updating Azure Bot Service webhook${NC}"
echo -e "${BLUE}Bot: $BOT_NAME${NC}"
echo -e "${BLUE}Webhook URL: $WEBHOOK_URL${NC}"

# Check if Azure CLI is installed
if ! command -v az >/dev/null 2>&1; then
    echo -e "${RED}❌ Azure CLI is not installed${NC}"
    echo "Please install Azure CLI: brew install azure-cli"
    exit 1
fi

# Check if logged in to Azure and set subscription
if ! az account show --subscription "$SUBSCRIPTION_ID" >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  Not logged in to Azure CLI or wrong subscription${NC}"
    echo -e "${BLUE}🔐 Please log in to Azure...${NC}"
    az login --tenant "$AZURE_TENANT_ID" --allow-no-subscriptions
    
    echo -e "${BLUE}🔧 Setting Azure subscription...${NC}"
    az account set --subscription "$SUBSCRIPTION_ID"
else
    echo -e "${GREEN}✅ Already logged in with correct subscription${NC}"
fi

# Get current bot configuration
echo -e "${BLUE}📋 Getting current bot configuration...${NC}"
CURRENT_CONFIG=$(az bot show \
    --name "$BOT_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --subscription "$SUBSCRIPTION_ID" \
    --output json)

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to get bot configuration${NC}"
    exit 1
fi

CURRENT_ENDPOINT=$(echo "$CURRENT_CONFIG" | jq -r '.properties.endpoint // "null"')
echo -e "${YELLOW}📍 Current endpoint: $CURRENT_ENDPOINT${NC}"

# Update the bot endpoint
echo -e "${BLUE}🔄 Updating bot messaging endpoint...${NC}"
az bot update \
    --name "$BOT_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --subscription "$SUBSCRIPTION_ID" \
    --endpoint "$WEBHOOK_URL" \
    --output table

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Successfully updated Azure Bot Service webhook!${NC}"
    echo -e "${GREEN}🔗 New endpoint: $WEBHOOK_URL${NC}"
    
    # Verify the update
    echo -e "${BLUE}🔍 Verifying update...${NC}"
    UPDATED_CONFIG=$(az bot show \
        --name "$BOT_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --subscription "$SUBSCRIPTION_ID" \
        --query "properties.endpoint" \
        --output tsv)
    
    if [ "$UPDATED_CONFIG" = "$WEBHOOK_URL" ]; then
        echo -e "${GREEN}✅ Verification successful - endpoint updated correctly${NC}"
    else
        echo -e "${YELLOW}⚠️  Warning: Endpoint may not have updated correctly${NC}"
        echo -e "${YELLOW}Expected: $WEBHOOK_URL${NC}"
        echo -e "${YELLOW}Actual: $UPDATED_CONFIG${NC}"
    fi
else
    echo -e "${RED}❌ Failed to update Azure Bot Service webhook${NC}"
    exit 1
fi

echo -e "${GREEN}🎉 Azure Bot Service is now configured for local development!${NC}"