#!/bin/bash
# Quick script to update OAuth redirect URI for existing bots

# Usage: ./update_bot_oauth.sh <config-file>
# Example: ./update_bot_oauth.sh sdas_bot_config.yaml

CONFIG_FILE=${1:-sdas_bot_config.yaml}

if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Config file not found: $CONFIG_FILE"
    echo "Usage: $0 <config-file>"
    exit 1
fi

echo "🔧 Updating OAuth redirect URI for bot configuration: $CONFIG_FILE"

# Extract app_id from config file if available, otherwise use environment variable
APP_ID=$(grep "app_id:" "$CONFIG_FILE" | sed 's/.*app_id: *[\"'\'']*//' | sed 's/[\"'\''].*//' || echo "$DATAHUB_TEAMS_APP_ID")

if [ -z "$APP_ID" ]; then
    echo "❌ No app_id found in config or DATAHUB_TEAMS_APP_ID environment variable"
    echo "Please set DATAHUB_TEAMS_APP_ID or add app_id to your config file"
    exit 1
fi

export DATAHUB_TEAMS_APP_ID="$APP_ID"
python3 create_azure_bot.py --config "$CONFIG_FILE" --update-oauth

echo "✅ Done! Your bot should now support OAuth flows for personal notifications."