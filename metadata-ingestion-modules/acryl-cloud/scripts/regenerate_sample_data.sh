#!/bin/bash
set -e

# Configuration
SERVER="https://fieldeng.acryl.io"
SEED_URN="urn:li:dataset:(urn:li:dataPlatform:snowflake,order_entry_db.analytics.order_details,PROD)"
OUTPUT_FILE="datahub-upgrade/src/main/resources/boot/sample_data_mcp.json"

# Get token from ~/.datahubenv or environment variable
if [ -z "$TOKEN" ]; then
    if [ -f ~/.datahubenv ]; then
        echo "Reading token from ~/.datahubenv..."
        source ~/.datahubenv
        TOKEN="$DATAHUB_GMS_TOKEN"
    fi
fi

# Check for token
if [ -z "$TOKEN" ]; then
    echo "ERROR: TOKEN not found"
    echo ""
    echo "Please set TOKEN in one of these ways:"
    echo "  1. Run: ~/bin/token.sh (to generate ~/.datahubenv)"
    echo "  2. Set environment variable: TOKEN='your-token' $0"
    echo "  3. Manually create ~/.datahubenv with DATAHUB_GMS_TOKEN='your-token'"
    exit 1
fi

echo "Regenerating sample data..."
echo "  Server: $SERVER"
echo "  Seed: $SEED_URN"
echo "  Output: $OUTPUT_FILE"
echo ""

# Run export script
python3 metadata-ingestion/scripts/export_sample_data.py \
    --server "$SERVER" \
    --token "$TOKEN" \
    --seed-urn "$SEED_URN" \
    --output "$OUTPUT_FILE" \
    --verbose

echo ""
echo "Sample data regenerated successfully!"
echo "File: $OUTPUT_FILE"

# Show statistics
python3 << 'EOF'
import json
from collections import Counter

with open('datahub-upgrade/src/main/resources/boot/sample_data_mcp.json', 'r') as f:
    mcps = json.load(f)

print("\nStatistics:")
print(f"  Total MCPs: {len(mcps)}")
print("\nEntity types:")
entity_types = Counter(mcp.get('entityType') for mcp in mcps)
for entity_type, count in sorted(entity_types.items(), key=lambda x: -x[1]):
    print(f"  {entity_type}: {count}")

# Verify no sensitive data
import re
content = json.dumps(mcps)
sensitive_patterns = ['@acryl.io', '@datahub.com']
has_sensitive = False
for pattern in sensitive_patterns:
    count = content.count(pattern)
    if count > 0:
        print(f"\nWARNING: Found {count} occurrences of {pattern}")
        has_sensitive = True

if not has_sensitive:
    print("\n✓ No sensitive email domains found")

# Verify no structured properties
sp_count = sum(1 for mcp in mcps if mcp.get('entityType') == 'structuredProperty')
sp_aspect_count = sum(1 for mcp in mcps if mcp.get('aspectName') == 'structuredProperties')
if sp_count == 0 and sp_aspect_count == 0:
    print("✓ No structured properties found")
else:
    print(f"\nWARNING: Found {sp_count} structured properties and {sp_aspect_count} structuredProperties aspects")
EOF
