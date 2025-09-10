#!/bin/bash

# setup_teams_tunnel.sh - Sets up a stable ngrok tunnel for Teams webhook
# Supports both direct integration service connection and multi-tenant router

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

echo -e "${GREEN}🌐 Setting up stable ngrok tunnel for Teams webhook${NC}"

# Show help if requested
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo -e "${BLUE}📋 Teams Tunnel Setup Script${NC}"
    echo
    echo -e "${YELLOW}Usage:${NC}"
    echo "  $0                    # Auto-detect service (Router on 9005, or Integrations on 9003)"
    echo "  $0 --port 9005        # Connect to specific port"
    echo "  TEAMS_TUNNEL_PORT=9005 $0  # Use environment variable"
    echo
    echo -e "${YELLOW}Auto-Detection Priority:${NC}"
    echo -e "  ${PURPLE}1. Teams Multi-Tenant Router (port 9005)${NC} - Provides multi-tenant routing"
    echo -e "  ${YELLOW}2. DataHub Integrations Service (port 9003)${NC} - Direct integration"
    echo
    echo -e "${YELLOW}Environment Variables:${NC}"
    echo "  TEAMS_TUNNEL_PORT     Override target port"
    echo
    echo -e "${YELLOW}Examples:${NC}"
    echo "  # Auto-detect and connect to best available service"
    echo "  ./setup_teams_tunnel.sh"
    echo
    echo "  # Force connection to specific port"
    echo "  ./setup_teams_tunnel.sh --port 9003"
    echo "  ./setup_teams_tunnel.sh --port 9005"
    echo
    echo "  # Use environment variable"
    echo "  export TEAMS_TUNNEL_PORT=9005"
    echo "  ./setup_teams_tunnel.sh"
    exit 0
fi

# Check if ngrok is installed
if ! command -v ngrok >/dev/null 2>&1; then
    echo -e "${RED}❌ ngrok is not installed${NC}"
    echo "Please install ngrok: brew install ngrok"
    exit 1
fi

# Function to check if a port is listening
check_port() {
    local port=$1
    if command -v nc >/dev/null 2>&1; then
        nc -z localhost $port 2>/dev/null
    elif command -v telnet >/dev/null 2>&1; then
        timeout 1 telnet localhost $port 2>/dev/null | grep -q "Connected" 2>/dev/null
    else
        # Fallback using /dev/tcp (works in bash)
        timeout 1 bash -c "echo >/dev/tcp/localhost/$port" 2>/dev/null
    fi
}

# Determine which service to connect to
TARGET_PORT=""
SERVICE_NAME=""
TUNNEL_DESCRIPTION=""

echo -e "${BLUE}🔍 Auto-detecting target service...${NC}"

# Check for multi-tenant router first (port 9005)
if check_port 9005; then
    # Verify it's actually the router by checking the health endpoint
    if command -v curl >/dev/null 2>&1; then
        HEALTH_RESPONSE=$(curl -s http://localhost:9005/health 2>/dev/null || echo "")
        if echo "$HEALTH_RESPONSE" | grep -q "teams-multi-tenant-router" 2>/dev/null; then
            TARGET_PORT=9005
            SERVICE_NAME="Teams Multi-Tenant Router"
            TUNNEL_DESCRIPTION="Multi-tenant routing enabled - events will be routed to appropriate DataHub instances"
            echo -e "${PURPLE}🔄 Found Teams Multi-Tenant Router on port 9005${NC}"
        fi
    fi
    
    # If health check failed but port is open, assume it's the router
    if [ -z "$TARGET_PORT" ]; then
        TARGET_PORT=9005
        SERVICE_NAME="Teams Multi-Tenant Router (assumed)"
        TUNNEL_DESCRIPTION="Routing to port 9005 (health check unavailable)"
        echo -e "${PURPLE}🔄 Found service on port 9005 (assumed to be Multi-Tenant Router)${NC}"
    fi
elif check_port 9003; then
    # Check if it's the integrations service
    if command -v curl >/dev/null 2>&1; then
        HEALTH_RESPONSE=$(curl -s http://localhost:9003/health 2>/dev/null || echo "")
        if echo "$HEALTH_RESPONSE" | grep -q -E "(integrations|datahub)" 2>/dev/null; then
            TARGET_PORT=9003
            SERVICE_NAME="DataHub Integrations Service"
            TUNNEL_DESCRIPTION="Direct connection to integrations service - no multi-tenant routing"
            echo -e "${YELLOW}🔗 Found DataHub Integrations Service on port 9003${NC}"
        fi
    fi
    
    # If health check failed but port is open, assume it's the integrations service
    if [ -z "$TARGET_PORT" ]; then
        TARGET_PORT=9003
        SERVICE_NAME="DataHub Integrations Service (assumed)"
        TUNNEL_DESCRIPTION="Routing to port 9003 (health check unavailable)"
        echo -e "${YELLOW}🔗 Found service on port 9003 (assumed to be Integrations Service)${NC}"
    fi
else
    echo -e "${RED}❌ Neither Multi-Tenant Router (9005) nor Integrations Service (9003) is running${NC}"
    echo -e "${YELLOW}💡 Please start one of the following:${NC}"
    echo -e "   ${PURPLE}• Multi-Tenant Router:${NC} python scripts/teams/install/installation_test_server.py"
    echo -e "   ${YELLOW}• Integrations Service:${NC} uvicorn datahub_integrations.server:app --port 9003"
    exit 1
fi

# Allow manual override via environment variable or command line
if [ -n "$TEAMS_TUNNEL_PORT" ]; then
    TARGET_PORT="$TEAMS_TUNNEL_PORT"
    SERVICE_NAME="Manual Override (port $TARGET_PORT)"
    TUNNEL_DESCRIPTION="Manually specified port via TEAMS_TUNNEL_PORT environment variable"
    echo -e "${BLUE}🎯 Using manual port override: $TARGET_PORT${NC}"
fi

if [ "$1" = "--port" ] && [ -n "$2" ]; then
    TARGET_PORT="$2"
    SERVICE_NAME="Manual Override (port $TARGET_PORT)"
    TUNNEL_DESCRIPTION="Manually specified port via command line argument"
    echo -e "${BLUE}🎯 Using manual port override: $TARGET_PORT${NC}"
fi

echo -e "${GREEN}✅ Target: $SERVICE_NAME${NC}"
echo -e "${BLUE}📋 $TUNNEL_DESCRIPTION${NC}"

# Check for existing Teams tunnel on target port and remove it
echo -e "${YELLOW}🧹 Checking for existing Teams tunnel on port $TARGET_PORT...${NC}"

# Function to safely kill only the Teams ngrok tunnel for the target port
kill_teams_tunnel() {
    local target_port=$1
    # Get list of running ngrok processes for this port
    local ngrok_pids=$(pgrep -f "ngrok http $target_port" 2>/dev/null || true)
    
    if [[ -n "$ngrok_pids" ]]; then
        echo -e "${YELLOW}📍 Found existing Teams tunnel(s) on port $target_port, stopping them...${NC}"
        for pid in $ngrok_pids; do
            echo "  Stopping PID: $pid"
            kill "$pid" 2>/dev/null || true
        done
        sleep 2
    else
        echo -e "${GREEN}✅ No existing Teams tunnel found on port $target_port${NC}"
    fi
    
    # Also check if there's a tunnel using the target port via ngrok API
    if command -v curl >/dev/null 2>&1; then
        local port_tunnels=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    target_port = '$target_port'
    for tunnel in data.get('tunnels', []):
        config = tunnel.get('config', {})
        addr = config.get('addr', '')
        if f':{target_port}' in addr or addr == f'localhost:{target_port}':
            print(f\"Found tunnel: {tunnel.get('name', 'unknown')} -> {tunnel['public_url']}\")
except:
    pass
" 2>/dev/null)
        
        if [[ -n "$port_tunnels" ]]; then
            echo -e "${YELLOW}📍 Found ngrok tunnel(s) using port $target_port:${NC}"
            echo "$port_tunnels"
            echo -e "${YELLOW}⚠️  These will conflict with the Teams tunnel. Consider stopping them manually if needed.${NC}"
        fi
    fi
}

kill_teams_tunnel $TARGET_PORT

# Start ngrok in background for the target port
echo -e "${BLUE}🚀 Starting ngrok tunnel on port $TARGET_PORT ($SERVICE_NAME)...${NC}"
ngrok http $TARGET_PORT --log=stdout > /tmp/ngrok.log 2>&1 &
NGROK_PID=$!

# Wait for ngrok to start and get the URL
echo -e "${YELLOW}⏳ Waiting for ngrok to start...${NC}"
sleep 5

# Show all active tunnels for transparency
echo -e "${BLUE}📋 Current ngrok tunnels:${NC}"
if command -v curl >/dev/null 2>&1; then
    curl -s http://localhost:4040/api/tunnels 2>/dev/null | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    tunnels = data.get('tunnels', [])
    if not tunnels:
        print('  No active tunnels')
    else:
        for tunnel in tunnels:
            name = tunnel.get('name', 'unknown')
            public_url = tunnel.get('public_url', 'unknown')
            addr = tunnel.get('config', {}).get('addr', 'unknown')
            print(f'  {name}: {public_url} -> {addr}')
except Exception as e:
    print(f'  Could not retrieve tunnel info: {e}')
" || echo "  Could not retrieve tunnel info"
fi

# Extract the HTTPS URL specifically for the target port
NGROK_URL=""
for i in {1..10}; do
    if command -v curl >/dev/null 2>&1; then
        NGROK_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    target_port = '$TARGET_PORT'
    for tunnel in data.get('tunnels', []):
        config = tunnel.get('config', {})
        addr = config.get('addr', '')
        # Look for tunnel specifically pointing to our target port
        if f':{target_port}' in addr or addr == f'localhost:{target_port}':
            if tunnel.get('proto') == 'https':
                print(tunnel['public_url'])
                break
except:
    pass
" 2>/dev/null)
    fi
    
    if [ -n "$NGROK_URL" ]; then
        break
    fi
    
    echo -e "${YELLOW}⏳ Waiting for Teams tunnel URL (attempt $i/10)...${NC}"
    sleep 2
done

if [ -z "$NGROK_URL" ]; then
    echo -e "${RED}❌ Could not get ngrok URL${NC}"
    echo "Check ngrok logs: tail -f /tmp/ngrok.log"
    kill $NGROK_PID 2>/dev/null || true
    exit 1
fi

# Set webhook URL
WEBHOOK_URL="${NGROK_URL}/public/teams/webhook"
echo -e "${GREEN}✅ Ngrok tunnel established!${NC}"
echo -e "${GREEN}🎯 Connected to: $SERVICE_NAME (port $TARGET_PORT)${NC}"
echo -e "${BLUE}🔗 Webhook URL: $WEBHOOK_URL${NC}"
echo -e "${YELLOW}📋 Configure this URL in Azure Bot Service:${NC} $WEBHOOK_URL"

# Save the webhook URL to a file for the server script to use
echo "$WEBHOOK_URL" > /tmp/teams_webhook_url.txt
echo -e "${GREEN}💾 Webhook URL saved to /tmp/teams_webhook_url.txt${NC}"

# Function to cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up ngrok tunnel...${NC}"
    kill $NGROK_PID 2>/dev/null || true
    rm -f /tmp/teams_webhook_url.txt
    echo -e "${GREEN}👋 Ngrok tunnel stopped${NC}"
}

# Set trap to cleanup on script exit
trap cleanup EXIT INT TERM

echo -e "${GREEN}🎯 Ngrok tunnel is running and stable${NC}"
echo -e "${GREEN}📡 Routing: Azure Bot Service → ngrok → $SERVICE_NAME${NC}"
echo -e "${YELLOW}💡 Press Ctrl+C to stop the tunnel${NC}"
echo -e "${BLUE}🔗 Webhook URL: $WEBHOOK_URL${NC}"

if [ "$TARGET_PORT" = "9005" ]; then
    echo -e "${PURPLE}🔄 Multi-tenant routing is active - events will be routed to appropriate DataHub instances${NC}"
elif [ "$TARGET_PORT" = "9003" ]; then
    echo -e "${YELLOW}🔗 Direct integration - all events go to the same DataHub instance${NC}"
fi

# Keep the script running to maintain the tunnel
while true; do
    sleep 60
    # Check if ngrok is still running
    if ! kill -0 $NGROK_PID 2>/dev/null; then
        echo -e "${RED}❌ Ngrok process died, exiting${NC}"
        exit 1
    fi
done