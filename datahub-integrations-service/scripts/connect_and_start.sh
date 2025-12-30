#!/usr/bin/env bash
# Script to connect integrations service to a remote DataHub GMS instance
#
# Usage:
#   ./scripts/connect_to.sh --customer fieldeng
#   ./scripts/connect_to.sh --url https://fieldeng.acryl.io
#   ./scripts/connect_to.sh --customer fieldeng --start
#
# The script will:
# 1. Derive GMS URL from customer name or use provided URL
# 2. Prompt for Personal Access Token (PAT) if not cached
# 3. Validate the connection
# 4. Set up environment variables
# 5. Optionally start the integrations service

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CACHE_DIR="$HOME/.cache/datahub-integrations"
TOKEN_CACHE_FILE="$CACHE_DIR/tokens.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
CUSTOMER_NAME=""
CUSTOM_URL=""
START_SERVICE=false
SKIP_VALIDATION=false
PORT=9003
USE_HELPERS_CLI=false
HELPERS_CLI_PATH="$HOME/workspace/datahub-apps/helpers"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --customer|-c)
      CUSTOMER_NAME="$2"
      shift 2
      ;;
    --url|-u)
      CUSTOM_URL="$2"
      shift 2
      ;;
    --start|-s)
      START_SERVICE=true
      shift
      ;;
    --skip-validation)
      SKIP_VALIDATION=true
      shift
      ;;
    --port|-p)
      PORT="$2"
      shift 2
      ;;
    --use-helpers)
      USE_HELPERS_CLI=true
      shift
      ;;
    --helpers-path)
      HELPERS_CLI_PATH="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --customer, -c NAME        Customer name (e.g., 'fieldeng')"
      echo "  --url, -u URL             Full URL (e.g., 'https://fieldeng.acryl.io')"
      echo "  --start, -s               Start the integrations service after connecting"
      echo "  --skip-validation         Skip connection validation"
      echo "  --port, -p PORT           Port to run service on (default: 9003)"
      echo "  --use-helpers             Auto-generate token using datahub-apps helpers CLI"
      echo "  --helpers-path PATH       Path to datahub-apps helpers (default: ~/workspace/datahub-apps/helpers)"
      echo "  --help, -h                Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0 --customer fieldeng"
      echo "  $0 --url https://customer.acryl.io"
      echo "  $0 --customer dev01 --start"
      exit 0
      ;;
    *)
      echo -e "${RED}Unknown option: $1${NC}"
      exit 1
      ;;
  esac
done

# Validate inputs
if [[ -z "$CUSTOMER_NAME" && -z "$CUSTOM_URL" ]]; then
  echo -e "${RED}Error: Must provide either --customer or --url${NC}"
  echo "Run with --help for usage information"
  exit 1
fi

# Derive URLs
if [[ -n "$CUSTOM_URL" ]]; then
  FRONTEND_URL="$CUSTOM_URL"
  # Strip protocol and extract hostname
  HOSTNAME=$(echo "$FRONTEND_URL" | sed -E 's|^https?://||' | sed 's|/.*$||')
  CUSTOMER_NAME=$(echo "$HOSTNAME" | cut -d. -f1)
else
  # Common customer shortcuts
  case "$CUSTOMER_NAME" in
    dev01|dev02|dev03|dev04|dev05)
      FRONTEND_URL="https://${CUSTOMER_NAME}.acryl.io"
      ;;
    fieldeng)
      FRONTEND_URL="https://fieldeng.acryl.io"
      ;;
    *)
      # Assume it's a customer subdomain
      FRONTEND_URL="https://${CUSTOMER_NAME}.acryl.io"
      ;;
  esac
fi

GMS_URL="${FRONTEND_URL}/gms"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}DataHub Integrations Service Connection${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}Customer:${NC}     $CUSTOMER_NAME"
echo -e "${GREEN}Frontend URL:${NC} $FRONTEND_URL"
echo -e "${GREEN}GMS URL:${NC}      $GMS_URL"
echo ""

# Create cache directory
mkdir -p "$CACHE_DIR"
chmod 700 "$CACHE_DIR"

# Initialize token cache if it doesn't exist
if [[ ! -f "$TOKEN_CACHE_FILE" ]]; then
  echo "{}" > "$TOKEN_CACHE_FILE"
  chmod 600 "$TOKEN_CACHE_FILE"
fi

# Function to get cached token
get_cached_token() {
  local customer=$1
  if command -v jq &> /dev/null; then
    jq -r --arg customer "$customer" '.[$customer].token // empty' "$TOKEN_CACHE_FILE" 2>/dev/null || echo ""
  else
    # Fallback without jq
    grep "\"$customer\"" "$TOKEN_CACHE_FILE" 2>/dev/null | sed -n 's/.*"token": *"\([^"]*\)".*/\1/p' || echo ""
  fi
}

# Function to save token
save_token() {
  local customer=$1
  local token=$2
  local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  if command -v jq &> /dev/null; then
    local temp_file=$(mktemp)
    jq --arg customer "$customer" \
       --arg token "$token" \
       --arg timestamp "$timestamp" \
       '.[$customer] = {token: $token, created_at: $timestamp}' \
       "$TOKEN_CACHE_FILE" > "$temp_file"
    mv "$temp_file" "$TOKEN_CACHE_FILE"
    chmod 600 "$TOKEN_CACHE_FILE"
  else
    echo -e "${YELLOW}Warning: jq not installed. Token caching disabled.${NC}"
  fi
}

# Function to get token from helpers CLI
get_token_from_helpers() {
  local customer=$1

  # Helpers CLI should be at ~/workspace/datahub-apps/helpers by default
  local datahub_apps_root=$(dirname "$HELPERS_CLI_PATH")

  if [[ ! -d "$HELPERS_CLI_PATH" ]]; then
    echo -e "${RED}Error: Helpers CLI not found at $HELPERS_CLI_PATH${NC}"
    echo -e "${YELLOW}Use --helpers-path to specify the correct path${NC}"
    exit 1
  fi

  echo -e "${BLUE}Generating token using helpers CLI...${NC}" >&2
  echo -e "${BLUE}Customer: $customer${NC}" >&2

  # IMPORTANT: Must run from datahub-apps root, not helpers subdirectory
  # The CLI uses DATAHUB_APPS_ROOT_PATH which defaults to "."
  cd "$datahub_apps_root"

  # Generate token with 3 month validity
  # Output goes to ~/.datahubenv file
  python3 helpers/cli.py customer get-dh-token \
    --file-filter "$customer" \
    --validity THREE_MONTHS > /dev/null 2>&1

  # Extract token from ~/.datahubenv file
  local token=$(grep "token:" ~/.datahubenv | awk '{print $2}')

  if [[ -z "$token" ]]; then
    echo -e "${RED}Error: Failed to generate token from helpers CLI${NC}" >&2
    echo -e "${YELLOW}Check that the customer '$customer' exists in datahub-apps/deployments${NC}" >&2
    exit 1
  fi

  echo "$token"
}

# Function to check if token is expired
is_token_expired() {
  local token=$1
  local gms_url=$2

  # Try to make a simple API call to validate the token
  local http_code=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    "${gms_url}/config" 2>/dev/null)

  if [[ "$http_code" == "200" ]]; then
    return 1  # Not expired (false)
  else
    return 0  # Expired or invalid (true)
  fi
}

# Get or generate token
# Step 1: Check if we have a cached token
CACHED_TOKEN=$(get_cached_token "$CUSTOMER_NAME")
NEED_NEW_TOKEN=false

if [[ -n "$CACHED_TOKEN" ]]; then
  echo -e "${GREEN}Found cached token for $CUSTOMER_NAME${NC}"

  # Step 2: Verify token is not expired
  echo -e "${BLUE}Verifying token...${NC}"
  if is_token_expired "$CACHED_TOKEN" "$GMS_URL"; then
    echo -e "${YELLOW}⚠ Cached token is expired or invalid${NC}"
    NEED_NEW_TOKEN=true
  else
    echo -e "${GREEN}✓ Token is valid${NC}"
    TOKEN="$CACHED_TOKEN"
  fi
else
  echo -e "${YELLOW}No cached token found for $CUSTOMER_NAME${NC}"
  NEED_NEW_TOKEN=true
fi

# Step 3: Generate new token if needed
if [[ "$NEED_NEW_TOKEN" == "true" ]]; then
  if [[ "$USE_HELPERS_CLI" == "true" ]]; then
    # Use helpers CLI to auto-generate token
    TOKEN=$(get_token_from_helpers "$CUSTOMER_NAME")
    echo -e "${GREEN}✓ Token generated successfully via helpers CLI${NC}"
    save_token "$CUSTOMER_NAME" "$TOKEN"
  else
    # Prompt for manual token entry
    echo ""
    echo -e "${YELLOW}To get a Personal Access Token (PAT):${NC}"
    echo "  1. Go to $FRONTEND_URL"
    echo "  2. Click your profile icon → Settings → Access Tokens"
    echo "  3. Click 'Generate Personal Access Token'"
    echo "  4. Give it a name (e.g., 'integrations-service-dev')"
    echo "  5. Copy the token and paste it below"
    echo ""
    echo -e "${BLUE}Tip: Use --use-helpers flag to auto-generate token${NC}"
    echo ""
    echo -n "Enter Personal Access Token: "
    read -rs TOKEN
    echo ""

    if [[ -z "$TOKEN" ]]; then
      echo -e "${RED}Error: Token cannot be empty${NC}"
      exit 1
    fi

    save_token "$CUSTOMER_NAME" "$TOKEN"
  fi
fi

# Validate connection
if [[ "$SKIP_VALIDATION" == "false" ]]; then
  echo -e "${BLUE}Validating connection...${NC}"

  VALIDATION_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    "${GMS_URL}/config" 2>/dev/null || echo "000")

  HTTP_CODE=$(echo "$VALIDATION_RESPONSE" | tail -n1)

  if [[ "$HTTP_CODE" == "200" ]]; then
    echo -e "${GREEN}✓ Connection validated successfully${NC}"
  elif [[ "$HTTP_CODE" == "401" ]]; then
    echo -e "${RED}✗ Authentication failed (401 Unauthorized)${NC}"
    echo -e "${YELLOW}Your token may be expired or invalid.${NC}"
    # Clear cached token
    if command -v jq &> /dev/null; then
      temp_file=$(mktemp)
      jq --arg customer "$CUSTOMER_NAME" 'del(.[$customer])' "$TOKEN_CACHE_FILE" > "$temp_file"
      mv "$temp_file" "$TOKEN_CACHE_FILE"
    fi
    exit 1
  else
    echo -e "${YELLOW}⚠ Could not validate connection (HTTP $HTTP_CODE)${NC}"
    echo -e "${YELLOW}Proceeding anyway... Service may fail to start.${NC}"
  fi
fi

# Create .env file
ENV_FILE="$PROJECT_DIR/.env"
echo ""
echo -e "${BLUE}Creating .env file...${NC}"

cat > "$ENV_FILE" << EOF
# Generated by scripts/connect_to.sh on $(date)
# Connected to: $CUSTOMER_NAME

# GMS Configuration
DATAHUB_GMS_URL=$GMS_URL
DATAHUB_GMS_API_TOKEN=$TOKEN

# Frontend Configuration (optional)
# DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL=$FRONTEND_URL

# Kafka Configuration (for Actions system)
KAFKA_AUTOMATIONS_CONSUMER_GROUP_PREFIX=datahub-integrations-dev

# AWS Configuration (for Bedrock - uncomment if needed)
# AWS_PROFILE=acryl-read-write
# AWS_REGION=us-west-2
EOF

chmod 600 "$ENV_FILE"
echo -e "${GREEN}✓ Created $ENV_FILE${NC}"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Connection configured successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Environment variables set:"
echo "  DATAHUB_GMS_URL=$GMS_URL"
echo "  DATAHUB_GMS_API_TOKEN=<hidden>"
echo ""

# Start service if requested
if [[ "$START_SERVICE" == "true" ]]; then
  # Check if port is already in use and kill existing process
  if lsof -ti :"$PORT" > /dev/null 2>&1; then
    echo -e "${YELLOW}Port $PORT is in use. Killing existing process...${NC}"
    lsof -ti :"$PORT" | xargs kill -9 2>/dev/null || true
    sleep 2
    echo -e "${GREEN}✓ Existing process killed${NC}"
  fi

  echo -e "${BLUE}Starting integrations service on port $PORT...${NC}"
  echo ""

  cd "$PROJECT_DIR"

  if [[ ! -d "venv" ]]; then
    echo -e "${YELLOW}Virtual environment not found. Running installDev...${NC}"
    ../gradlew installDev
  fi

  source venv/bin/activate
  uvicorn datahub_integrations.server:app --host 0.0.0.0 --port "$PORT" --reload
else
  echo "To start the service manually:"
  echo ""
  echo "  cd $PROJECT_DIR"
  echo "  source venv/bin/activate"
  echo "  uvicorn datahub_integrations.server:app --host 0.0.0.0 --port $PORT --reload"
  echo ""
  echo "Or run this script with --start flag to start automatically"
fi
