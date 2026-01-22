#!/bin/bash

# setup_oauth_tunnel.sh - Sets up an ngrok tunnel for local OAuth development
# This allows OAuth providers to redirect to your local machine during development.
#
# Usage:
#   ./setup_oauth_tunnel.sh                    # Use default port 9002 (frontend proxy)
#   ./setup_oauth_tunnel.sh --port 9003        # Direct to integrations service
#   OAUTH_TUNNEL_PORT=9002 ./setup_oauth_tunnel.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

echo -e "${GREEN}🔐 Setting up ngrok tunnel for OAuth development${NC}"

# Show help if requested
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo -e "${BLUE}📋 OAuth Tunnel Setup Script${NC}"
    echo
    echo -e "${YELLOW}Usage:${NC}"
    echo "  $0                       # Default: tunnel to port 9002 (frontend)"
    echo "  $0 --port 9003           # Direct tunnel to integrations service"
    echo "  OAUTH_TUNNEL_PORT=9002 $0  # Use environment variable"
    echo
    echo -e "${YELLOW}Ports:${NC}"
    echo -e "  ${PURPLE}9002${NC} - DataHub Frontend (default, includes proxy to integrations)"
    echo -e "  ${YELLOW}9003${NC} - DataHub Integrations Service (direct, requires separate frontend)"
    echo
    echo -e "${YELLOW}Environment Variables:${NC}"
    echo "  OAUTH_TUNNEL_PORT     Override target port"
    echo
    echo -e "${YELLOW}After Setup:${NC}"
    echo "  1. Export the displayed DATAHUB_FRONTEND_URL in your shell"
    echo "  2. Register the callback URL with your OAuth provider"
    echo "  3. Start the integrations service with the exported URL"
    echo
    echo -e "${YELLOW}Examples:${NC}"
    echo "  # Default setup (recommended)"
    echo "  ./setup_oauth_tunnel.sh"
    echo
    echo "  # Direct integrations service (for testing)"
    echo "  ./setup_oauth_tunnel.sh --port 9003"
    exit 0
fi

# Check if ngrok is installed
if ! command -v ngrok >/dev/null 2>&1; then
    echo -e "${RED}❌ ngrok is not installed${NC}"
    echo "Please install ngrok:"
    echo "  macOS: brew install ngrok"
    echo "  Linux: snap install ngrok"
    echo "  Or download from: https://ngrok.com/download"
    echo
    echo "After installing, authenticate ngrok:"
    echo "  ngrok config add-authtoken <your-authtoken>"
    exit 1
fi

# Function to check if a port is listening
check_port() {
    local port=$1
    if command -v nc >/dev/null 2>&1; then
        nc -z localhost $port 2>/dev/null
    elif command -v telnet >/dev/null 2>&1; then
        timeout 1 telnet localhost $port 2>/dev/null | grep -q "Connected"
    else
        # Fallback: try to connect with /dev/tcp
        (echo >/dev/tcp/localhost/$port) 2>/dev/null
    fi
}

# Parse command line arguments
TARGET_PORT=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --port|-p)
            TARGET_PORT="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Determine target port (priority: CLI arg > env var > auto-detect)
if [ -n "$TARGET_PORT" ]; then
    echo -e "${BLUE}📋 Using port $TARGET_PORT from command line${NC}"
elif [ -n "$OAUTH_TUNNEL_PORT" ]; then
    TARGET_PORT="$OAUTH_TUNNEL_PORT"
    echo -e "${BLUE}📋 Using port $TARGET_PORT from OAUTH_TUNNEL_PORT env var${NC}"
else
    # Default to 9002 (frontend proxy)
    TARGET_PORT=9002
    echo -e "${BLUE}📋 Using default port $TARGET_PORT (DataHub frontend)${NC}"
fi

# Check if the target port is listening
if check_port $TARGET_PORT; then
    echo -e "${GREEN}✅ Port $TARGET_PORT is listening${NC}"
else
    echo -e "${YELLOW}⚠️  Port $TARGET_PORT is not listening${NC}"
    echo -e "${YELLOW}   Make sure DataHub is running before OAuth testing${NC}"
fi

# Function to safely kill existing OAuth ngrok tunnels
kill_oauth_tunnel() {
    local target_port=$1
    local ngrok_pids=$(pgrep -f "ngrok http $target_port" 2>/dev/null || true)
    
    if [[ -n "$ngrok_pids" ]]; then
        echo -e "${YELLOW}📍 Found existing tunnel on port $target_port, stopping...${NC}"
        for pid in $ngrok_pids; do
            kill $pid 2>/dev/null || true
        done
        sleep 1
    fi
}

echo -e "${YELLOW}🧹 Checking for existing tunnels on port $TARGET_PORT...${NC}"
kill_oauth_tunnel $TARGET_PORT

# Start ngrok in background
echo -e "${BLUE}🚀 Starting ngrok tunnel on port $TARGET_PORT...${NC}"
ngrok http $TARGET_PORT --log=stdout > /tmp/ngrok_oauth.log 2>&1 &
NGROK_PID=$!

# Wait for ngrok to start and get the URL
echo -e "${YELLOW}⏳ Waiting for ngrok to start...${NC}"
sleep 3

# Get the public URL
NGROK_URL=""
for i in {1..10}; do
    NGROK_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for tunnel in data.get('tunnels', []):
        config = tunnel.get('config', {})
        addr = str(config.get('addr', ''))
        if ':$TARGET_PORT' in addr or addr.endswith('$TARGET_PORT'):
            if tunnel.get('proto') == 'https':
                print(tunnel['public_url'])
                break
except Exception as e:
    pass
" 2>/dev/null)
    
    if [ -n "$NGROK_URL" ]; then
        break
    fi
    echo -e "${YELLOW}⏳ Waiting for tunnel URL (attempt $i/10)...${NC}"
    sleep 1
done

if [ -z "$NGROK_URL" ]; then
    echo -e "${RED}❌ Could not get ngrok URL${NC}"
    echo "Check ngrok logs: tail -f /tmp/ngrok_oauth.log"
    kill $NGROK_PID 2>/dev/null || true
    exit 1
fi

# Single, fixed callback URL for all OAuth providers
# External URLs use /integrations/ which the frontend proxy remaps to /public/
CALLBACK_URL="${NGROK_URL}/integrations/oauth/callback"

echo
echo -e "${GREEN}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ OAuth tunnel established!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════════${NC}"
echo
echo -e "${PURPLE}🔗 Public URL:${NC}"
echo -e "   ${NGROK_URL}"
echo
echo -e "${PURPLE}📋 Set this environment variable before starting integrations service:${NC}"
echo -e "   ${YELLOW}export DATAHUB_FRONTEND_URL=\"${NGROK_URL}\"${NC}"
echo
echo -e "${PURPLE}🔐 OAuth Callback URL (register this with ALL OAuth providers):${NC}"
echo -e "   ${YELLOW}${CALLBACK_URL}${NC}"
echo
echo -e "${BLUE}💡 This single URL works for all plugins! The plugin is identified${NC}"
echo -e "${BLUE}   from the OAuth state parameter, not from the URL path.${NC}"
echo
echo -e "${YELLOW}📝 Remember to:${NC}"
echo -e "   1. Register the callback URL with your OAuth provider's app settings"
echo -e "   2. Restart the integrations service with the new DATAHUB_FRONTEND_URL"
echo -e "   3. The ngrok URL changes each time you restart the tunnel (use ngrok paid for stable URLs)"
echo
echo -e "${GREEN}═══════════════════════════════════════════════════════════════════${NC}"

# Save URL to file for other scripts
echo "$NGROK_URL" > /tmp/oauth_tunnel_url.txt
echo -e "${BLUE}📄 URL saved to /tmp/oauth_tunnel_url.txt${NC}"

# Trap cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up ngrok tunnel...${NC}"
    kill $NGROK_PID 2>/dev/null || true
    rm -f /tmp/oauth_tunnel_url.txt
    echo -e "${GREEN}👋 OAuth tunnel stopped${NC}"
    exit 0
}
trap cleanup SIGINT SIGTERM

echo
echo -e "${GREEN}🎯 Tunnel is running. Press Ctrl+C to stop.${NC}"
echo -e "${BLUE}📊 View traffic: http://localhost:4040${NC}"
echo

# Keep script running
while true; do
    if ! kill -0 $NGROK_PID 2>/dev/null; then
        echo -e "${RED}❌ ngrok process died, exiting${NC}"
        exit 1
    fi
    sleep 5
done
