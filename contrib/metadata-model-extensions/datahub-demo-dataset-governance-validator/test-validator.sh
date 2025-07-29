#!/bin/bash

# Test script for DataHub Dataset Governance Validator
TOKEN="eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjM4N2M3ZGFjLWJiYmUtNDlkYi05ZWQ1LTQzYzgyMDQ4M2UyMyIsInN1YiI6ImRhdGFodWIiLCJleHAiOjE3NTU5ODY5NDAsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.oZqlOgZAFE29lt79Bb7EbUJ0UucWGEcR4oAIB0frphA"
DATAHUB_URL="http://localhost:8080"

echo "🧪 Testing DataHub Dataset Governance Validator"
echo "================================================"

# Test 1: Try to ingest a dataset WITHOUT governance metadata (should fail if validator works)
echo ""
echo "📋 Test 1: Ingesting dataset WITHOUT governance metadata (should fail)..."

response1=$(curl -s -w "HTTP_STATUS:%{http_code}" \
  -X POST "${DATAHUB_URL}/aspects?action=ingestProposal" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "proposal": {
      "entityType": "dataset", 
      "entityUrn": "urn:li:dataset:(mysql,test_db.bad_table,PROD)",
      "aspectName": "datasetProperties",
      "aspect": {
        "json": "{\"name\":\"Bad Test Table\",\"description\":\"A test table without any governance metadata\"}"
      },
      "changeType": "UPSERT"
    }
  }')

http_status1=$(echo "$response1" | grep -o "HTTP_STATUS:[0-9]*" | cut -d: -f2)
response_body1=$(echo "$response1" | sed 's/HTTP_STATUS:[0-9]*$//')

echo "Status: $http_status1"
echo "Response: $response_body1"

if [ "$http_status1" -eq 400 ] || [ "$http_status1" -eq 422 ]; then
    echo "✅ EXPECTED: Validation failed (this means the validator is working!)"
elif [ "$http_status1" -eq 200 ]; then
    echo "❌ UNEXPECTED: Ingestion succeeded (validator may not be loaded)"
else
    echo "⚠️  UNEXPECTED: Got status $http_status1"
fi

# Test 2: Try to ingest the same dataset WITH governance metadata (should succeed)
echo ""
echo "📋 Test 2: Ingesting dataset WITH governance metadata (should succeed)..."

# First add ownership
echo "  📝 Adding ownership..."
curl -s -X POST "${DATAHUB_URL}/aspects?action=ingestProposal" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "proposal": {
      "entityType": "dataset",
      "entityUrn": "urn:li:dataset:(mysql,test_db.good_table,PROD)", 
      "aspectName": "ownership",
      "aspect": {
        "json": "{\"owners\":[{\"owner\":\"urn:li:corpuser:datahub\",\"type\":\"DATAOWNER\"}]}"
      },
      "changeType": "UPSERT"
    }
  }' > /dev/null

# Then add tags
echo "  🏷️  Adding tags..."
curl -s -X POST "${DATAHUB_URL}/aspects?action=ingestProposal" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "proposal": {
      "entityType": "dataset",
      "entityUrn": "urn:li:dataset:(mysql,test_db.good_table,PROD)",
      "aspectName": "globalTags", 
      "aspect": {
        "json": "{\"tags\":[{\"tag\":\"urn:li:tag:test-tag\"}]}"
      },
      "changeType": "UPSERT"
    }
  }' > /dev/null

# Then add domain
echo "  🏢 Adding domain..."
curl -s -X POST "${DATAHUB_URL}/aspects?action=ingestProposal" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "proposal": {
      "entityType": "dataset",
      "entityUrn": "urn:li:dataset:(mysql,test_db.good_table,PROD)",
      "aspectName": "domains",
      "aspect": {
        "json": "{\"domains\":[\"urn:li:domain:test-domain\"]}"
      },
      "changeType": "UPSERT"
    }
  }' > /dev/null

# Finally add dataset properties
echo "  📊 Adding dataset properties..."
response2=$(curl -s -w "HTTP_STATUS:%{http_code}" \
  -X POST "${DATAHUB_URL}/aspects?action=ingestProposal" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "proposal": {
      "entityType": "dataset",
      "entityUrn": "urn:li:dataset:(mysql,test_db.good_table,PROD)",
      "aspectName": "datasetProperties",
      "aspect": {
        "json": "{\"name\":\"Good Test Table\",\"description\":\"A test table with proper governance metadata\"}"
      },
      "changeType": "UPSERT"
    }
  }')

http_status2=$(echo "$response2" | grep -o "HTTP_STATUS:[0-9]*" | cut -d: -f2)
response_body2=$(echo "$response2" | sed 's/HTTP_STATUS:[0-9]*$//')

echo "Status: $http_status2"
echo "Response: $response_body2"

if [ "$http_status2" -eq 200 ]; then
    echo "✅ EXPECTED: Ingestion succeeded (dataset has all required governance metadata)"
elif [ "$http_status2" -eq 400 ] || [ "$http_status2" -eq 422 ]; then
    echo "⚠️  UNEXPECTED: Validation failed even with governance metadata"
else
    echo "⚠️  UNEXPECTED: Got status $http_status2"
fi

echo ""
echo "🏁 Test Summary:"
echo "================"
echo "Test 1 (no governance): HTTP $http_status1 - $([ "$http_status1" -eq 400 ] || [ "$http_status1" -eq 422 ] && echo "✅ PASS" || echo "❌ FAIL")"
echo "Test 2 (with governance): HTTP $http_status2 - $([ "$http_status2" -eq 200 ] && echo "✅ PASS" || echo "❌ FAIL")"

if ([ "$http_status1" -eq 400 ] || [ "$http_status1" -eq 422 ]) && [ "$http_status2" -eq 200 ]; then
    echo ""
    echo "🎉 SUCCESS: Validator is working correctly!"
    echo "   - Blocked dataset without governance metadata"
    echo "   - Allowed dataset with proper governance metadata" 
elif [ "$http_status1" -eq 200 ] && [ "$http_status2" -eq 200 ]; then
    echo ""
    echo "⚠️  VALIDATOR NOT ACTIVE: Both tests passed"
    echo "   - The validator is likely not loaded in the classpath"
    echo "   - Check GMS logs for ClassNotFoundException"
else
    echo ""
    echo "❓ MIXED RESULTS: Check responses above for details" 
fi