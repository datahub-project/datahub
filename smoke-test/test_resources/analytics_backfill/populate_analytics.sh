#!/bin/bash
#
# Master script to populate DataHub analytics with synthetic activity data.
#
# This script orchestrates the entire process of:
# 1. Generating synthetic users
# 2. Generating activity events
# 3. Loading events into Elasticsearch
#

set -euo pipefail

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Default configuration
NUM_USERS="${NUM_USERS:-20}"
NUM_DAYS="${NUM_DAYS:-30}"
EVENTS_PER_DAY="${EVENTS_PER_DAY:-200}"
GMS_URL="${DATAHUB_GMS_URL:-http://localhost:8080}"
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
EMAIL_DOMAIN="${EMAIL_DOMAIN:-example.com}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

usage() {
    cat <<EOF
${BLUE}DataHub Analytics Population Tool${NC}

Generate synthetic users and activity events to populate DataHub analytics dashboards.

${GREEN}Usage:${NC}
    $0 [OPTIONS]

${GREEN}OPTIONS:${NC}
    --num-users N           Number of users to generate (default: 20)
    --num-days N            Days of history to generate (default: 30)
    --events-per-day N      Target events per day (default: 200)
    --gms-url URL          DataHub GMS URL (default: http://localhost:8080)
    --token TOKEN          DataHub auth token (required)
    --elasticsearch-url URL Elasticsearch URL (default: http://localhost:9200)
    --email-domain DOMAIN   Email domain for users (default: example.com)
    --skip-users           Skip user generation
    --skip-events          Skip event generation
    --skip-load            Skip loading events to Elasticsearch
    -h, --help             Show this help message

${GREEN}ENVIRONMENT VARIABLES:${NC}
    DATAHUB_GMS_URL        DataHub GMS URL
    ELASTICSEARCH_URL      Elasticsearch URL
    NUM_USERS             Number of users
    NUM_DAYS              Days of history
    EVENTS_PER_DAY        Events per day
    EMAIL_DOMAIN          Email domain

${GREEN}EXAMPLES:${NC}
    # Full pipeline with defaults (requires token)
    $0 --token \$DATAHUB_TOKEN

    # Generate 50 users with 60 days of activity
    $0 --token \$DATAHUB_TOKEN --num-users 50 --num-days 60

    # Only generate events (users already exist)
    $0 --token \$DATAHUB_TOKEN --skip-users

    # Generate users and events but don't load yet
    $0 --token \$DATAHUB_TOKEN --skip-load

${GREEN}OUTPUT:${NC}
    users.json              Generated user profiles
    activity_events.json    Generated activity events
    entity_urns.json        Extracted entity URNs

EOF
    exit 0
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_section() {
    echo
    echo -e "${BLUE}===================================================${NC}"
    echo -e "${BLUE} $1${NC}"
    echo -e "${BLUE}===================================================${NC}"
    echo
}

# Parse arguments
SKIP_USERS=false
SKIP_EVENTS=false
SKIP_LOAD=false
TOKEN=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --num-users)
            NUM_USERS="$2"
            shift 2
            ;;
        --num-days)
            NUM_DAYS="$2"
            shift 2
            ;;
        --events-per-day)
            EVENTS_PER_DAY="$2"
            shift 2
            ;;
        --gms-url)
            GMS_URL="$2"
            shift 2
            ;;
        --token)
            TOKEN="$2"
            shift 2
            ;;
        --elasticsearch-url)
            ELASTICSEARCH_URL="$2"
            shift 2
            ;;
        --email-domain)
            EMAIL_DOMAIN="$2"
            shift 2
            ;;
        --skip-users)
            SKIP_USERS=true
            shift
            ;;
        --skip-events)
            SKIP_EVENTS=true
            shift
            ;;
        --skip-load)
            SKIP_LOAD=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate token
if [ -z "$TOKEN" ]; then
    log_error "DataHub token is required. Use --token or set DATAHUB_TOKEN environment variable."
    echo
    echo "To get a token:"
    echo "  1. Log into DataHub"
    echo "  2. Go to Settings -> Access Tokens"
    echo "  3. Generate a new token"
    exit 1
fi

# Display configuration
log_section "Configuration"
log_info "GMS URL: $GMS_URL"
log_info "Elasticsearch URL: $ELASTICSEARCH_URL"
log_info "Number of users: $NUM_USERS"
log_info "Days of history: $NUM_DAYS"
log_info "Events per day: $EVENTS_PER_DAY"
log_info "Email domain: $EMAIL_DOMAIN"

# Check dependencies
log_section "Checking Dependencies"

if ! command -v python3 &> /dev/null; then
    log_error "python3 is required but not installed"
    exit 1
fi
log_info "✓ python3 found"

if ! command -v jq &> /dev/null; then
    log_error "jq is required but not installed"
    exit 1
fi
log_info "✓ jq found"

if ! command -v curl &> /dev/null; then
    log_error "curl is required but not installed"
    exit 1
fi
log_info "✓ curl found"

# Check Python packages
if ! python3 -c "import datahub" 2>/dev/null; then
    log_error "datahub Python package is required"
    log_error "Install with: pip install 'acryl-datahub[datahub-rest]'"
    exit 1
fi
log_info "✓ datahub Python package found"

# Check DataHub connectivity
log_info "Checking DataHub connectivity..."
if curl -s -f "${GMS_URL}/health" > /dev/null 2>&1; then
    log_info "✓ DataHub GMS is accessible"
else
    log_error "Cannot connect to DataHub GMS at $GMS_URL"
    exit 1
fi

# Check Elasticsearch connectivity
log_info "Checking Elasticsearch connectivity..."
if curl -s -f "${ELASTICSEARCH_URL}/_cluster/health" > /dev/null 2>&1; then
    log_info "✓ Elasticsearch is accessible"
else
    log_error "Cannot connect to Elasticsearch at $ELASTICSEARCH_URL"
    exit 1
fi

# Step 1: Generate users
if [ "$SKIP_USERS" = false ]; then
    log_section "Step 1: Generating Users"
    python3 "$SCRIPT_DIR/generate_users.py" \
        --num-users "$NUM_USERS" \
        --gms-url "$GMS_URL" \
        --token "$TOKEN" \
        --email-domain "$EMAIL_DOMAIN" \
        --output-file "$SCRIPT_DIR/users.json"

    log_info "✓ Users generated and saved to users.json"
else
    log_section "Step 1: Skipping User Generation"
    if [ ! -f "$SCRIPT_DIR/users.json" ]; then
        log_error "users.json not found. Cannot skip user generation."
        exit 1
    fi
    log_info "Using existing users.json"
fi

# Step 1.5: Extract entity URNs from DataHub
log_section "Step 1.5: Extracting Entity URNs"
log_info "Fetching entities from DataHub for realistic activity..."

python3 <<EOF
import json
import requests

gms_url = "$GMS_URL"
token = "$TOKEN"

# GraphQL queries to get different entity types
entity_types = ["DATASET", "DASHBOARD", "CHART"]
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

entity_urns = []
for entity_type in entity_types:
    query = f"""
    {{
      search(input: {{type: {entity_type}, query: "*", start: 0, count: 100}}) {{
        searchResults {{
          entity {{
            urn
          }}
        }}
      }}
    }}
    """

    response = requests.post(f"{gms_url}/api/graphql", json={"query": query}, headers=headers)

    if response.status_code == 200:
        data = response.json()
        urns = [r["entity"]["urn"] for r in data.get("data", {}).get("search", {}).get("searchResults", [])]
        entity_urns.extend(urns)
        print(f"Extracted {len(urns)} {entity_type.lower()}s")

with open("$SCRIPT_DIR/entity_urns.json", "w") as f:
    json.dump(entity_urns, f, indent=2)

print(f"Extracted {len(entity_urns)} entity URNs")
EOF

log_info "✓ Entity URNs extracted"

# Step 1.6: Generate glossary terms and attach to datasets
log_section "Step 1.6: Generating Glossary Terms"
log_info "Creating business glossary terms and attaching to datasets..."

python3 "$SCRIPT_DIR/generate_glossary_terms.py" \
    --gms-url "$GMS_URL" \
    --token "$TOKEN" \
    --entity-urns-file "$SCRIPT_DIR/entity_urns.json" \
    --output-file "$SCRIPT_DIR/glossary_terms.json"

log_info "✓ Glossary terms created and attached to datasets"

# Step 2: Generate activity events
if [ "$SKIP_EVENTS" = false ]; then
    log_section "Step 2: Generating Activity Events"
    python3 "$SCRIPT_DIR/backfill_activity_events.py" \
        --users-file "$SCRIPT_DIR/users.json" \
        --days "$NUM_DAYS" \
        --events-per-day "$EVENTS_PER_DAY" \
        --gms-url "$GMS_URL" \
        --token "$TOKEN" \
        --entity-urns-file "$SCRIPT_DIR/entity_urns.json" \
        --output-file "$SCRIPT_DIR/activity_events.json"

    log_info "✓ Activity events generated and saved to activity_events.json"
else
    log_section "Step 2: Skipping Event Generation"
    if [ ! -f "$SCRIPT_DIR/activity_events.json" ]; then
        log_error "activity_events.json not found. Cannot skip event generation."
        exit 1
    fi
    log_info "Using existing activity_events.json"
fi

# Step 3: Load events into Elasticsearch
if [ "$SKIP_LOAD" = false ]; then
    log_section "Step 3: Loading Events to Elasticsearch"
    bash "$SCRIPT_DIR/load_events_to_elasticsearch.sh" \
        -e "$ELASTICSEARCH_URL" \
        "$SCRIPT_DIR/activity_events.json"

    log_info "✓ Events loaded into Elasticsearch"
else
    log_section "Step 3: Skipping Event Loading"
    log_info "Events saved to activity_events.json"
    log_info "Load manually with: ./load_events_to_elasticsearch.sh activity_events.json"
fi

# Summary
log_section "Summary"
log_info "✅ Analytics population complete!"
echo
log_info "📊 View your analytics dashboards:"
log_info "   http://localhost:9002/analytics"
echo
log_info "📈 Expected charts to be populated:"
log_info "   • Weekly Active Users"
log_info "   • Monthly Active Users"
log_info "   • New Users (Last 30 Days)"
log_info "   • Top Users (Last 30 Days)"
log_info "   • Number of Searches"
log_info "   • Top Searches (Past Week)"
log_info "   • Top Viewed Datasets (Past Week)"
log_info "   • Top Viewed Dashboards (Past Week)"
log_info "   • Tab Views By Entity Type (Past Week)"
log_info "   • Actions By Entity Type (Past Week)"
log_info "   • Data Assets by Term"
echo
log_info "Generated files:"
log_info "   • users.json - User profiles"
log_info "   • entity_urns.json - Entity URNs used"
log_info "   • glossary_terms.json - Glossary terms and attachments"
log_info "   • activity_events.json - Activity events"
echo
log_info "🔄 To regenerate with different parameters:"
log_info "   $0 --token \$TOKEN --num-users 50 --num-days 60"
