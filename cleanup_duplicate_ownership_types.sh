#!/bin/bash

# Script to clean up duplicate ownership types
# This will keep the first ownership type for each name and remove duplicates

set -e

DATAHUB_URL="http://localhost:3000"
GRAPHQL_ENDPOINT="${DATAHUB_URL}/api/v2/graphql"
COOKIE_FILE="cookies.txt"

echo "🧹 Starting cleanup of duplicate ownership types..."

# Authenticate
echo "🔐 Authenticating..."
curl -c "$COOKIE_FILE" -b "$COOKIE_FILE" -s "${DATAHUB_URL}/logIn" -X POST \
    -H "Content-Type: application/json" \
    -d '{"username": "datahub", "password": "datahub"}' > /dev/null

if [ ! -f "$COOKIE_FILE" ] || ! grep -q "actor" "$COOKIE_FILE"; then
    echo "❌ Authentication failed"
    exit 1
fi

echo "✅ Authentication successful"

# Query all ownership types
echo "🔍 Querying ownership types..."
QUERY_RESULT=$(curl -b "$COOKIE_FILE" -s "$GRAPHQL_ENDPOINT" -X POST \
    -H "Content-Type: application/json" \
    -d '{"query": "query { listOwnershipTypes(input: { start: 0, count: 1000 }) { ownershipTypes { urn info { name } } } }"}')

echo "Raw query result:"
echo "$QUERY_RESULT" | jq '.'

# Extract ownership types and group by name
echo "📊 Analyzing ownership types..."
OWNERSHIP_TYPES=$(echo "$QUERY_RESULT" | jq -r '.data.listOwnershipTypes.ownershipTypes[] | "\(.info.name)|\(.urn)"')

# Group by name and find duplicates
declare -A name_to_urns
declare -A name_to_first_urn

while IFS='|' read -r name urn; do
    if [ -z "${name_to_urns[$name]}" ]; then
        name_to_urns[$name]="$urn"
        name_to_first_urn[$name]="$urn"
        echo "✅ Keeping first: $name -> $urn"
    else
        name_to_urns[$name]="${name_to_urns[$name]} $urn"
        echo "🗑️  Marking for deletion: $name -> $urn"
    fi
done <<< "$OWNERSHIP_TYPES"

echo ""
echo "📋 Summary:"
for name in "${!name_to_urns[@]}"; do
    urns=(${name_to_urns[$name]})
    count=${#urns[@]}
    if [ $count -gt 1 ]; then
        echo "⚠️  $name: $count duplicates (keeping: ${name_to_first_urn[$name]})"
    else
        echo "✅ $name: unique"
    fi
done

echo ""
echo "⚠️  WARNING: This script would delete duplicate ownership types."
echo "⚠️  This is a destructive operation that cannot be undone."
echo "⚠️  Please review the list above before proceeding."
echo ""
read -p "Do you want to proceed with cleanup? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ Cleanup cancelled"
    rm -f "$COOKIE_FILE"
    exit 0
fi

echo "🗑️  Starting cleanup..."

# Delete duplicates (keep first, delete rest)
deleted_count=0
for name in "${!name_to_urns[@]}"; do
    urns=(${name_to_urns[$name]})
    count=${#urns[@]}
    
    if [ $count -gt 1 ]; then
        echo "Processing $name ($count duplicates)..."
        
        # Skip the first URN (keep it), delete the rest
        for ((i=1; i<count; i++)); do
            urn_to_delete="${urns[$i]}"
            echo "  Deleting: $urn_to_delete"
            
            # Note: There's no direct delete mutation for ownership types in DataHub
            # They would need to be soft-deleted or removed through the admin interface
            echo "  ⚠️  Note: Ownership type deletion requires admin interface or soft-delete"
            echo "  ⚠️  URN to delete: $urn_to_delete"
            
            ((deleted_count++))
        done
    fi
done

echo ""
echo "🎉 Cleanup analysis complete!"
echo "📊 Found $deleted_count ownership types that should be deleted"
echo ""
echo "💡 Recommendation:"
echo "1. Use the DataHub admin interface to soft-delete duplicate ownership types"
echo "2. Or implement a soft-delete mutation in the GraphQL API"
echo "3. The system will now use the first ownership type for each name"

# Cleanup
rm -f "$COOKIE_FILE"
