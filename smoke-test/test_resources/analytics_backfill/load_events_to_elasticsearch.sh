#!/bin/bash
#
# Load activity events into Elasticsearch for DataHub analytics.
#
# This script takes a JSON file of events and loads them into the
# DataHub usage event index in Elasticsearch.
#

set -euo pipefail

# Configuration
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
INDEX_NAME="${DATAHUB_USAGE_INDEX:-datahub_usage_event}"
BATCH_SIZE=500

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] EVENTS_FILE

Load activity events from JSON file into Elasticsearch.

OPTIONS:
    -e, --elasticsearch-url URL    Elasticsearch URL (default: http://localhost:9200)
    -i, --index INDEX             Index name (default: datahub_usage_event)
    -b, --batch-size SIZE         Batch size for bulk insert (default: 500)
    -h, --help                    Show this help message

EXAMPLES:
    # Load events with defaults
    $0 events.json

    # Load events to custom Elasticsearch
    $0 -e http://es:9200 -i my_usage_index events.json

EOF
    exit 1
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

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--elasticsearch-url)
            ELASTICSEARCH_URL="$2"
            shift 2
            ;;
        -i|--index)
            INDEX_NAME="$2"
            shift 2
            ;;
        -b|--batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            EVENTS_FILE="$1"
            shift
            ;;
    esac
done

if [ -z "${EVENTS_FILE:-}" ]; then
    log_error "Events file is required"
    usage
fi

if [ ! -f "$EVENTS_FILE" ]; then
    log_error "Events file not found: $EVENTS_FILE"
    exit 1
fi

log_info "Loading events from: $EVENTS_FILE"
log_info "Elasticsearch URL: $ELASTICSEARCH_URL"
log_info "Index name: $INDEX_NAME"
log_info "Batch size: $BATCH_SIZE"

# Check if Elasticsearch is accessible
if ! curl -s -f "${ELASTICSEARCH_URL}/_cluster/health" > /dev/null 2>&1; then
    log_error "Cannot connect to Elasticsearch at $ELASTICSEARCH_URL"
    log_error "Make sure Elasticsearch is running and accessible"
    exit 1
fi

log_info "Elasticsearch is accessible"

# Check if index exists
if curl -s -f "${ELASTICSEARCH_URL}/${INDEX_NAME}" > /dev/null 2>&1; then
    log_info "Index $INDEX_NAME exists"
else
    log_warn "Index $INDEX_NAME does not exist, it will be created automatically"
fi

# Convert JSON array to NDJSON format for bulk API
log_info "Converting events to NDJSON format..."

TEMP_NDJSON=$(mktemp)
trap "rm -f $TEMP_NDJSON" EXIT

# Use jq to convert JSON array to NDJSON with proper bulk index format
jq -c '.[] | {"index": {"_index": "'$INDEX_NAME'"}}, .' "$EVENTS_FILE" > "$TEMP_NDJSON"

TOTAL_LINES=$(wc -l < "$TEMP_NDJSON")
TOTAL_EVENTS=$((TOTAL_LINES / 2))  # Each event takes 2 lines in bulk format

log_info "Total events to load: $TOTAL_EVENTS"

# Load events in batches
CURRENT_LINE=1
BATCH_NUM=1
SUCCESS_COUNT=0
ERROR_COUNT=0

while [ $CURRENT_LINE -le $TOTAL_LINES ]; do
    BATCH_LINES=$((BATCH_SIZE * 2))  # Each event is 2 lines
    END_LINE=$((CURRENT_LINE + BATCH_LINES - 1))

    if [ $END_LINE -gt $TOTAL_LINES ]; then
        END_LINE=$TOTAL_LINES
    fi

    log_info "Loading batch $BATCH_NUM (lines $CURRENT_LINE-$END_LINE)..."

    # Extract batch and send to Elasticsearch
    RESPONSE=$(sed -n "${CURRENT_LINE},${END_LINE}p" "$TEMP_NDJSON" | \
        curl -s -X POST "${ELASTICSEARCH_URL}/_bulk" \
        -H "Content-Type: application/x-ndjson" \
        --data-binary @-)

    # Check for errors in response
    ERRORS=$(echo "$RESPONSE" | jq -r '.errors')
    if [ "$ERRORS" = "true" ]; then
        ERROR_COUNT=$((ERROR_COUNT + 1))
        log_warn "Batch $BATCH_NUM had some errors"
        # Log first error for debugging
        FIRST_ERROR=$(echo "$RESPONSE" | jq -r '.items[0].index.error.reason // "Unknown error"')
        log_warn "First error: $FIRST_ERROR"
    else
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    fi

    CURRENT_LINE=$((END_LINE + 1))
    BATCH_NUM=$((BATCH_NUM + 1))

    # Small delay to avoid overwhelming Elasticsearch
    sleep 0.1
done

log_info "Loading complete!"
log_info "  Successful batches: $SUCCESS_COUNT"
if [ $ERROR_COUNT -gt 0 ]; then
    log_warn "  Batches with errors: $ERROR_COUNT"
fi

# Refresh index to make documents searchable
log_info "Refreshing index..."
curl -s -X POST "${ELASTICSEARCH_URL}/${INDEX_NAME}/_refresh" > /dev/null

# Get document count
DOC_COUNT=$(curl -s "${ELASTICSEARCH_URL}/${INDEX_NAME}/_count" | jq -r '.count')
log_info "Index now contains $DOC_COUNT documents"

log_info "✅ Events loaded successfully!"
log_info "You can now view analytics at http://localhost:9002/analytics"
