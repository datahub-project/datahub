#!/bin/bash
#
# Removes VIEW_ANALYTICS privilege from "All Users" policy for existing users
#

set -e

echo "🔒 DataHub Analytics Authorization Security Fix"
echo "This script removes VIEW_ANALYTICS from the default 'All Users' policy"
echo "to prevent unauthorized access to the analytics dashboard."
echo ""

# Check if required tools are available
command -v curl >/dev/null 2>&1 || { echo "❌ curl is required but not installed. Aborting." >&2; exit 1; }

# Configuration
DATAHUB_GMS_URL=${DATAHUB_GMS_URL:-"http://localhost:8080"}
ADMIN_TOKEN=${DATAHUB_ADMIN_TOKEN:-""}

if [ -z "$ADMIN_TOKEN" ]; then
    echo "🔑 Admin token required. Please provide it via:"
    echo "   export DATAHUB_ADMIN_TOKEN='your-admin-token'"
    echo "   or get it from DataHub UI -> Settings -> Access Tokens"
    exit 1
fi

echo "🔍 Checking current policy state..."

# Step 1: Get current policy
CURRENT_POLICY=$(curl -s -X POST "${DATAHUB_GMS_URL}/api/graphql" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -d '{
    "query": "query { getPolicy(urn: \"urn:li:dataHubPolicy:7\") { urn displayName privileges actors { allUsers } } }"
  }')

if echo "$CURRENT_POLICY" | grep -q "VIEW_ANALYTICS"; then
    echo "⚠️  Found VIEW_ANALYTICS in All Users policy - applying fix..."
    
    # Step 2: Update policy to remove VIEW_ANALYTICS
    UPDATE_RESULT=$(curl -s -X POST "${DATAHUB_GMS_URL}/api/graphql" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      -d '{
        "query": "mutation { updatePolicy(urn: \"urn:li:dataHubPolicy:7\", input: { privileges: [\"GENERATE_PERSONAL_ACCESS_TOKENS\", \"VIEW_METADATA_PROPOSALS\"] }) }"
      }')
    
    if echo "$UPDATE_RESULT" | grep -q "error"; then
        echo "❌ Failed to update policy:"
        echo "$UPDATE_RESULT"
        exit 1
    fi
    
    echo "✅ Successfully removed VIEW_ANALYTICS from All Users policy"
    
    # Step 3: Verify the fix
    echo "🔍 Verifying fix..."
    UPDATED_POLICY=$(curl -s -X POST "${DATAHUB_GMS_URL}/api/graphql" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${ADMIN_TOKEN}" \
      -d '{
        "query": "query { getPolicy(urn: \"urn:li:dataHubPolicy:7\") { privileges } }"
      }')
    
    if echo "$UPDATED_POLICY" | grep -q "VIEW_ANALYTICS"; then
        echo "❌ Verification failed - VIEW_ANALYTICS still present"
        exit 1
    else
        echo "✅ Verification successful - VIEW_ANALYTICS removed"
    fi
    
    echo ""
    echo "🎉 Analytics authorization fix applied successfully!"
    echo "📋 Summary:"
    echo "   - All users no longer have default analytics access"
    echo "   - Only users with explicit VIEW_ANALYTICS privilege can access /analytics"
    echo "   - Admins retain analytics access via admin policies"
    echo ""
    echo "🔄 Restart DataHub frontend to ensure route protection is active."
    
else
    echo "✅ Policy already fixed - no VIEW_ANALYTICS found in All Users policy"
fi