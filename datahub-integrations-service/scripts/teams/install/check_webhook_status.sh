#!/bin/bash
# Check Teams Webhook Status
# Properly checks if the Teams webhook is accessible and working.

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Get webhook URL from config
get_webhook_url() {
    WEBHOOK_URL="${DATAHUB_TEAMS_WEBHOOK_URL}"
    
    if [[ -z "$WEBHOOK_URL" ]]; then
        log_error "DATAHUB_TEAMS_WEBHOOK_URL environment variable not set"
        echo "Please set: export DATAHUB_TEAMS_WEBHOOK_URL=\"https://your-webhook-url.com/public/teams/webhook\""
        return 1
    fi
}

# Check webhook accessibility with proper error handling
check_webhook_accessibility() {
    local url="$1"
    
    log_info "Checking webhook: $url"
    
    # Use curl with verbose error handling
    local response
    local http_code
    local curl_exit_code
    
    response=$(curl -s -w "HTTP_CODE:%{http_code}\nRESPONSE_TIME:%{time_total}" \
                   --connect-timeout 5 \
                   --max-time 10 \
                   -H "Accept: application/json" \
                   "$url" 2>&1) || curl_exit_code=$?
    
    if [[ -n "$curl_exit_code" && "$curl_exit_code" -ne 0 ]]; then
        log_error "Connection failed (curl exit code: $curl_exit_code)"
        echo "  Error: $response"
        return 1
    fi
    
    # Extract HTTP code
    http_code=$(echo "$response" | grep "HTTP_CODE:" | cut -d':' -f2)
    response_time=$(echo "$response" | grep "RESPONSE_TIME:" | cut -d':' -f2)
    
    # Get response body (everything before HTTP_CODE line)
    local response_body=$(echo "$response" | grep -v "HTTP_CODE:" | grep -v "RESPONSE_TIME:")
    
    echo "  HTTP Code: $http_code"
    echo "  Response Time: ${response_time}s"
    
    # Check for ngrok errors
    if echo "$response_body" | grep -q "ngrok-error-code\|ERR_NGROK"; then
        local ngrok_error=$(echo "$response_body" | grep -o "ERR_NGROK_[0-9]*" || echo "unknown")
        log_error "ngrok tunnel is not active"
        echo "  Error Code: $ngrok_error"
        echo "  This means the ngrok session has expired or stopped"
        return 1
    fi
    
    # Check HTTP status codes
    case "$http_code" in
        200|201|202)
            log_success "Webhook is accessible and responding correctly"
            return 0
            ;;
        404)
            if [[ "$url" == *"ngrok"* ]]; then
                log_error "ngrok tunnel is running but endpoint doesn't exist"
                echo "  Make sure DataHub is running on the ngrok'd port"
            else
                log_error "Webhook endpoint not found (404)"
            fi
            return 1
            ;;
        500|502|503)
            log_error "Server error ($http_code)"
            echo "  DataHub server might be down or misconfigured"
            return 1
            ;;
        *)
            log_warning "Unexpected HTTP response: $http_code"
            if [[ -n "$response_body" ]]; then
                echo "  Response: $response_body"
            fi
            return 1
            ;;
    esac
}

# Check if ngrok is running
check_ngrok_status() {
    log_info "Checking ngrok status..."
    
    if command -v ngrok &> /dev/null; then
        # Try to get ngrok API info
        if ngrok_info=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null); then
            if command -v jq &> /dev/null; then
                local tunnel_count=$(echo "$ngrok_info" | jq '.tunnels | length')
                if [[ "$tunnel_count" -gt 0 ]]; then
                    log_success "ngrok is running with $tunnel_count tunnel(s)"
                    echo "$ngrok_info" | jq -r '.tunnels[] | "  \(.name): \(.public_url) -> \(.config.addr)"'
                    return 0
                fi
            fi
        fi
        
        log_warning "ngrok is installed but no tunnels are running"
        echo "  Start ngrok with: cd ../.. && ./setup_teams_tunnel.sh"
        return 1
    else
        log_warning "ngrok is not installed"
        echo "  Install with: brew install ngrok"
        return 1
    fi
}

# Show fix recommendations
show_fix_recommendations() {
    local webhook_url="$1"
    local has_webhook_error="$2"
    
    echo
    echo "=================================================================="
    echo "RECOMMENDATIONS TO FIX WEBHOOK"
    echo "=================================================================="
    
    if [[ "$webhook_url" == *"ngrok"* ]]; then
        if [[ "$has_webhook_error" == "true" ]]; then
            log_error "Your ngrok tunnel has expired or stopped"
            echo
            echo "🔧 TO FIX:"
            echo "1. Restart ngrok tunnel:"
            echo "   cd $(dirname "$(dirname "${BASH_SOURCE[0]}")")"
            echo "   ./setup_teams_tunnel.sh"
            echo
            echo "2. Update Azure Bot Service with new webhook URL:"
            echo "   ./update_azure_bot_webhook.sh"
            echo
            echo "3. Update DATAHUB_TEAMS_WEBHOOK_URL environment variable with new URL"
        else
            log_warning "ngrok tunnel exists but DataHub endpoint not found"
            echo
            echo "🔧 TO FIX:"
            echo "1. Make sure DataHub is running:"
            echo "   cd $(dirname "$(dirname "$(dirname "${BASH_SOURCE[0]}")")")"
            echo "   ./gradlew quickstartDebug"
            echo
            echo "2. Ensure integrations service is running on port 9003:"
            echo "   cd datahub-integrations-service"
            echo "   uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003"
        fi
    else
        log_info "Non-ngrok webhook - check if your server is running"
    fi
}

# Main execution
main() {
    echo "Teams Webhook Status Check"
    echo "========================="
    echo
    
    # Get webhook URL
    if ! get_webhook_url; then
        exit 1
    fi
    
    log_info "Found webhook URL: $WEBHOOK_URL"
    echo
    
    # Check webhook accessibility
    local webhook_accessible=false
    local has_webhook_error=false
    
    if check_webhook_accessibility "$WEBHOOK_URL"; then
        webhook_accessible=true
    else
        has_webhook_error=true
    fi
    
    echo
    
    # Check ngrok if applicable
    if [[ "$WEBHOOK_URL" == *"ngrok"* ]]; then
        check_ngrok_status
        echo
    fi
    
    # Show recommendations
    show_fix_recommendations "$WEBHOOK_URL" "$has_webhook_error"
    
    # Exit with appropriate code
    if [[ "$webhook_accessible" == "true" ]]; then
        echo
        log_success "✅ Webhook is ready for real Teams testing!"
        exit 0
    else
        echo
        log_error "❌ Webhook needs to be fixed before real testing"
        exit 1
    fi
}

main "$@"