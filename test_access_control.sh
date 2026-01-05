#!/bin/bash

# DataHub GMS GraphQL Endpoint
GMS_URL="http://localhost:8080/api/graphql"

# Admin Actor
ADMIN_ACTOR="urn:li:corpuser:datahub"
# Test User Actor
TEST_USER_ACTOR="urn:li:corpuser:access-test-user"
# Test Org
TEST_ORG_ID="access-test-org-$(date +%s)"
TEST_ORG_URN="urn:li:organization:$TEST_ORG_ID"

echo "1. Creating Organization as Admin..."
curl -s -X POST $GMS_URL \
  -H "Content-Type: application/json" \
  -H "X-DataHub-Actor: $ADMIN_ACTOR" \
  -d "{
    \"query\": \"mutation createOrganization(\$input: CreateOrganizationInput!) { createOrganization(input: \$input) }\",
    \"variables\": {
      \"input\": {
        \"id\": \"$TEST_ORG_ID\",
        \"name\": \"Access Test Organization\",
        \"description\": \"An organization for testing access control\"
      }
    }
  }" | jq .

echo -e "\n2. Verifying Organization exists (as Admin)..."
curl -s -X POST $GMS_URL \
  -H "Content-Type: application/json" \
  -H "X-DataHub-Actor: $ADMIN_ACTOR" \
  -d "{
    \"query\": \"{ organization(urn: \\\"$TEST_ORG_URN\\\") { urn properties { name } } }\"
  }" | jq .

echo -e "\n3. Verifying Organization is NOT visible to Test User (Strict Isolation)..."
# The Test User has no organizations assigned yet, so they should see NOTHING.
curl -s -X POST $GMS_URL \
  -H "Content-Type: application/json" \
  -H "X-DataHub-Actor: $TEST_USER_ACTOR" \
  -d "{
    \"query\": \"{ organization(urn: \\\"$TEST_ORG_URN\\\") { urn properties { name } } }\"
  }" | jq .

echo -e "\n4. Adding Test User to Organization..."
curl -s -X POST $GMS_URL \
  -H "Content-Type: application/json" \
  -H "X-DataHub-Actor: $ADMIN_ACTOR" \
  -d "{
    \"query\": \"mutation addUserToOrganizations(\$userUrn: String!, \$organizationUrns: [String!]!) { addUserToOrganizations(userUrn: \$userUrn, organizationUrns: \$organizationUrns) }\",
    \"variables\": {
      \"userUrn\": \"$TEST_USER_ACTOR\",
      \"organizationUrns\": [\"$TEST_ORG_URN\"]
    }
  }" | jq .

echo -e "\n5. Verifying Organization IS visible to Test User now..."
curl -s -X POST $GMS_URL \
  -H "Content-Type: application/json" \
  -H "X-DataHub-Actor: $TEST_USER_ACTOR" \
  -d "{
    \"query\": \"{ organization(urn: \\\"$TEST_ORG_URN\\\") { urn properties { name } } }\"
  }" | jq .

echo -e "\n6. Verifying Test User CANNOT manage Organization (Authorization Check)..."
curl -s -X POST $GMS_URL \
  -H "Content-Type: application/json" \
  -H "X-DataHub-Actor: $TEST_USER_ACTOR" \
  -d "{
    \"query\": \"mutation updateOrganization(\$urn: String!, \$input: OrganizationUpdateInput!) { updateOrganization(urn: \$urn, input: \$input) }\",
    \"variables\": {
      \"urn\": \"$TEST_ORG_URN\",
      \"input\": {
        \"name\": \"Unauthorized Update\"
      }
    }
  }" | jq .
