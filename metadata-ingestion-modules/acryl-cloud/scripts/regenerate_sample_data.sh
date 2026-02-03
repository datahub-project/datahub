#!/bin/bash
set -e

# Configuration
SERVER="https://fieldeng.acryl.io"
SEED_URN="urn:li:dataset:(urn:li:dataPlatform:snowflake,order_entry_db.analytics.order_details,PROD)"
# Note: This is the ORIGINAL URN from fieldeng (without sample_data_ prefix).
# The export script will automatically add the sample_data_ prefix during transformation.
OUTPUT_FILE="datahub-upgrade/src/main/resources/boot/sample_data_mcp.json"

# Get token from ~/.datahubenv (YAML format) or environment variable
if [ -z "$TOKEN" ]; then
    if [ -f ~/.datahubenv ]; then
        echo "Reading token from ~/.datahubenv..."
        TOKEN=$(grep "^  token:" ~/.datahubenv | head -1 | sed 's/.*token: *//')
    fi
fi

# Check for token
if [ -z "$TOKEN" ]; then
    echo "ERROR: TOKEN not found"
    echo ""
    echo "Please set TOKEN in one of these ways:"
    echo "  1. Run: ~/bin/token.sh (to generate ~/.datahubenv)"
    echo "  2. Set environment variable: TOKEN='your-token' $0"
    exit 1
fi

echo "Regenerating sample data..."
echo "  Server: $SERVER"
echo "  Seed: $SEED_URN"
echo "  Output: $OUTPUT_FILE"
echo ""

# Run export script
python3 metadata-ingestion-modules/acryl-cloud/scripts/export_sample_data.py \
    --server "$SERVER" \
    --token "$TOKEN" \
    --seed-urn "$SEED_URN" \
    --output "$OUTPUT_FILE" \
    --verbose

echo ""
echo "Sample data regenerated successfully!"
echo "File: $OUTPUT_FILE"

# Generate manifest for diff-based migration
echo ""
echo "Generating manifest for diff-based migration..."
MANIFEST_DIR="datahub-upgrade/src/main/resources/boot/manifests"
mkdir -p "$MANIFEST_DIR"

# Calculate version hash from MCP file content
MANIFEST_VERSION=$(python3 << 'MANIFEST_EOF'
import json
import hashlib

with open('datahub-upgrade/src/main/resources/boot/sample_data_mcp.json', 'r') as f:
    content = f.read()
    version_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()[:12]
    print(version_hash)
MANIFEST_EOF
)

MANIFEST_OUTPUT="$MANIFEST_DIR/${MANIFEST_VERSION}_manifest.json"

python3 - "$MANIFEST_OUTPUT" "$MANIFEST_VERSION" << 'EOF'
import json
import sys

manifest_file = sys.argv[1]
version = sys.argv[2]

with open('datahub-upgrade/src/main/resources/boot/sample_data_mcp.json', 'r') as f:
    mcps = json.load(f)

# Build entity -> aspects mapping
entities = {}
for mcp in mcps:
    urn = mcp.get('entityUrn')
    aspect_name = mcp.get('aspectName')
    if urn and aspect_name:
        if urn not in entities:
            entities[urn] = set()
        entities[urn].add(aspect_name)

# Create manifest structure
manifest = {
    'version': version,
    'entities': {
        urn: sorted(list(aspects))
        for urn, aspects in sorted(entities.items())
    }
}

# Write manifest
with open(manifest_file, 'w') as f:
    json.dump(manifest, f, indent=2, sort_keys=True)

print(f"\n✓ Manifest generated: {manifest_file}")
print(f"  Version: {manifest['version']}")
print(f"  Total entities: {len(manifest['entities'])}")
print(f"  Total aspects: {sum(len(aspects) for aspects in manifest['entities'].values())}")
EOF

# Run integrity tests to validate the regenerated data
echo ""
echo "Running integrity tests to validate regenerated data..."
if ./gradlew :datahub-upgrade:test --tests com.linkedin.datahub.upgrade.system.bootstrapmcps.SampleDataIntegrityTest; then
    echo "✓ All integrity tests passed!"
else
    echo "✗ Integrity tests failed! Please review the failures above."
    exit 1
fi

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

# Check if sample data actually changed by comparing with git
echo ""
echo "Checking if sample data changed..."

# Check if the generated file differs from what's committed in git
if git diff --quiet "$OUTPUT_FILE" 2>/dev/null; then
    echo "✓ Sample data unchanged (matches committed version)"
    echo "  No need to update CURRENT_VERSION"
    SAMPLE_DATA_CHANGED=false
else
    echo "✓ Sample data changed (differs from committed version or not in git)"
    SAMPLE_DATA_CHANGED=true
fi

# Update CURRENT_VERSION in IngestSampleDataStep.java only if data changed
if [ "$SAMPLE_DATA_CHANGED" = true ]; then
    echo ""
    echo "Updating CURRENT_VERSION in IngestSampleDataStep.java..."
    JAVA_FILE="datahub-upgrade/src/main/java/com/linkedin/datahub/upgrade/system/sampledata/IngestSampleDataStep.java"

    if [ -f "$JAVA_FILE" ]; then
        # Extract current version for comparison
        CURRENT_VERSION_OLD=$(grep -A 1 "private static final String CURRENT_VERSION =" "$JAVA_FILE" | tail -1 | sed 's/.*"\([^"]*\)".*/\1/')

        # Check if version actually needs updating
        if [ "$CURRENT_VERSION_OLD" = "$MANIFEST_VERSION" ]; then
            echo "✓ CURRENT_VERSION already matches: $MANIFEST_VERSION"
        else
            # Use sed to update the CURRENT_VERSION constant value (on the line after the declaration)
            # The pattern matches: "old_version"; // comment
            # And replaces with: "new_version"; // comment (preserving comment)
            if [[ "$OSTYPE" == "darwin"* ]]; then
                # macOS sed requires -i with empty string for in-place editing
                sed -i '' "/private static final String CURRENT_VERSION =/,/;/s/\"$CURRENT_VERSION_OLD\"/\"$MANIFEST_VERSION\"/" "$JAVA_FILE"
            else
                # Linux sed
                sed -i "/private static final String CURRENT_VERSION =/,/;/s/\"$CURRENT_VERSION_OLD\"/\"$MANIFEST_VERSION\"/" "$JAVA_FILE"
            fi

            # Verify the update
            CURRENT_VERSION_NEW=$(grep -A 1 "private static final String CURRENT_VERSION =" "$JAVA_FILE" | tail -1 | sed 's/.*"\([^"]*\)".*/\1/')

            if [ "$CURRENT_VERSION_NEW" = "$MANIFEST_VERSION" ]; then
                echo "✓ Updated CURRENT_VERSION: $CURRENT_VERSION_OLD → $MANIFEST_VERSION"
            else
                echo "⚠ Warning: Failed to update CURRENT_VERSION in $JAVA_FILE"
                echo "  Expected: $MANIFEST_VERSION"
                echo "  Found: $CURRENT_VERSION_NEW"
                echo "  Please manually update to: $MANIFEST_VERSION"
            fi
        fi
    else
        echo "⚠ Warning: Could not find $JAVA_FILE"
        echo "  Please manually update CURRENT_VERSION to: $MANIFEST_VERSION"
    fi
else
    echo ""
    echo "Skipping CURRENT_VERSION update (sample data unchanged)"
fi

echo ""
echo "================================================================"
echo "✓ Sample data regeneration complete!"
echo "================================================================"
echo ""
echo "Summary:"
echo "  - Sample data: datahub-upgrade/src/main/resources/boot/sample_data_mcp.json"
echo "  - New manifest: $MANIFEST_OUTPUT"
echo "  - Version: $MANIFEST_VERSION"
if [ "$SAMPLE_DATA_CHANGED" = true ]; then
    echo "  - CURRENT_VERSION updated in: $JAVA_FILE"
    echo "  - Sample data changed: YES"
else
    echo "  - CURRENT_VERSION: No update needed"
    echo "  - Sample data changed: NO"
fi
echo ""

# Check for and delete old manifests (keeping 001, current committed, and new one)
# We need to keep the current committed manifest so instances can diff against it during upgrade.
# Get the CURRENT_VERSION from IngestSampleDataStep.java (this is the currently committed version)
COMMITTED_VERSION=$(grep -A 1 "private static final String CURRENT_VERSION =" "$JAVA_FILE" 2>/dev/null | tail -1 | sed 's/.*"\([^"]*\)".*/\1/' || echo "")

# Build exclusion list: 001, new version, and committed version (if different from new)
EXCLUDE_ARGS="! -name \"001_manifest.json\" ! -name \"${MANIFEST_VERSION}_manifest.json\""
if [ -n "$COMMITTED_VERSION" ] && [ "$COMMITTED_VERSION" != "$MANIFEST_VERSION" ]; then
    EXCLUDE_ARGS="$EXCLUDE_ARGS ! -name \"${COMMITTED_VERSION}_manifest.json\""
    echo "Keeping manifests: 001, $COMMITTED_VERSION (current), $MANIFEST_VERSION (new)"
else
    echo "Keeping manifests: 001, $MANIFEST_VERSION"
fi

OLD_MANIFESTS=$(eval "find \"$MANIFEST_DIR\" -name \"*_manifest.json\" $EXCLUDE_ARGS" 2>/dev/null)

if [ -n "$OLD_MANIFESTS" ]; then
    echo "Old manifests detected (will be deleted):"
    echo "$OLD_MANIFESTS" | while read -r old_manifest; do
        echo "  - $old_manifest"
        rm "$old_manifest"
        echo "    ✓ Deleted"
    done
    echo ""
    echo "✓ Old manifests cleaned up"
else
    echo "✓ No old manifests to clean up"
fi

echo ""
echo "Next steps:"
echo "  1. Review changes: git diff"
echo "  2. Commit changes"
echo "================================================================"
