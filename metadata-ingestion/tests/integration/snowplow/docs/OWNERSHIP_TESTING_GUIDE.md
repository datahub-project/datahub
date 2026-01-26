# Snowplow Ownership Feature - Testing Guide

**Date**: 2025-12-12
**Feature**: Schema-Level Ownership Tracking
**Status**: Ready for Testing

---

## Quick Test (No DataHub Required)

This test verifies ownership extraction using the integration test infrastructure.

### Option 1: Run Integration Tests

The fastest way to verify ownership is working:

```bash
cd /Users/treff7es/shadow/datahub/metadata-ingestion
python -m pytest tests/integration/snowplow/test_snowplow.py::test_snowplow_ingest -v
```

**Expected Result**: Test passes and creates golden file with 3 ownership aspects.

**Verify Ownership Output**:

```bash
cd /Users/treff7es/shadow/datahub/metadata-ingestion
python3 -c "
import json
with open('tests/integration/snowplow/snowplow_mces_golden.json') as f:
    data = json.load(f)
    ownership_count = sum(1 for item in data if item.get('aspectName') == 'ownership')
    print(f'✅ Found {ownership_count} ownership aspects in golden file')

    for item in data:
        if item.get('aspectName') == 'ownership':
            owners = item['aspect']['json']['owners']
            print(f'\nOwnership:')
            for owner in owners:
                print(f'  - {owner[\"owner\"]} ({owner[\"type\"]})')
"
```

**Expected Output**:

```
✅ Found 3 ownership aspects in golden file

Ownership:
  - urn:li:corpuser:ryan@company.com (DATAOWNER)
  - urn:li:corpuser:jane@company.com (PRODUCER)

Ownership:
  - urn:li:corpuser:alice@company.com (DATAOWNER)

Ownership:
  - urn:li:corpuser:bob@company.com (DATAOWNER)
```

---

## Option 2: Test with Mock BDP Server

Test the connector with the local mock server (simulates real BDP API).

### Step 1: Start Mock BDP Server

```bash
cd /Users/treff7es/shadow/datahub/metadata-ingestion/tests/integration/snowplow
python mock_bdp_server.py --port 8081
```

**Expected Output**:

```
Mock Snowplow BDP API Server
============================
Loaded 3 data structures from fixtures/data_structures_with_ownership.json
Server running on http://localhost:8081
Press Ctrl+C to stop
```

### Step 2: Create Test Recipe

Create `test_mock_bdp.yml`:

```yaml
source:
  type: snowplow
  config:
    bdp_connection:
      organization_id: "test-org-uuid"
      api_key_id: "test-key-id"
      api_key: "test-secret"
      base_url: "http://localhost:8081" # Point to mock server

    extract_event_specifications: false
    extract_tracking_scenarios: false

sink:
  type: file
  config:
    filename: "/tmp/snowplow_ownership_mock_test.json"
```

### Step 3: Run Ingestion

```bash
cd /Users/treff7es/shadow/datahub/metadata-ingestion
datahub ingest -c tests/integration/snowplow/test_mock_bdp.yml
```

### Step 4: Verify Output

```bash
python3 -c "
import json
with open('/tmp/snowplow_ownership_mock_test.json') as f:
    data = json.load(f)
    ownership_count = sum(1 for item in data if item.get('aspectName') == 'ownership')
    print(f'✅ Extracted {ownership_count} ownership aspects')

    for item in data:
        if item.get('aspectName') == 'ownership':
            entity = item['entityUrn']
            owners = item['aspect']['json']['owners']
            print(f'\n{entity}:')
            for owner in owners:
                owner_email = owner['owner'].split(':')[-1]
                print(f'  - {owner_email} ({owner[\"type\"]})')
"
```

---

## Option 3: Test with Real DataHub (UI Testing)

To see ownership in the DataHub UI, you'll need a running DataHub instance.

### Prerequisites

1. **Docker and Docker Compose** installed
2. **DataHub repository** (you already have this)

### Step 1: Start DataHub

```bash
cd /Users/treff7es/shadow/datahub
docker-compose -f docker-compose-without-neo4j-m1.yml up -d

# Wait for DataHub to start (2-3 minutes)
# Check health:
curl http://localhost:8080/health
```

**Expected**: Returns `{"status":"ok"}`

### Step 2: Get Access Token

**Option A - Via UI**:

1. Open http://localhost:9002
2. Login (default: datahub / datahub)
3. Go to Settings → Access Tokens → Generate New Token
4. Copy the token

**Option B - Via CLI** (only for localhost):

```bash
datahub user get-access-token --user datahub
```

### Step 3: Create Ingestion Recipe

Update `test_ownership_recipe.yml` with DataHub sink:

```yaml
source:
  type: snowplow
  config:
    bdp_connection:
      organization_id: "YOUR_ORG_ID"
      api_key_id: "YOUR_KEY_ID"
      api_key: "${SNOWPLOW_API_KEY}" # Use env var for security

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}" # Use env var
```

### Step 4: Set Environment Variables

```bash
export SNOWPLOW_API_KEY="your-snowplow-api-key"
export DATAHUB_TOKEN="your-datahub-token"
```

### Step 5: Run Ingestion

```bash
cd /Users/treff7es/shadow/datahub/metadata-ingestion
datahub ingest -c tests/integration/snowplow/test_ownership_recipe.yml
```

### Step 6: Verify in UI

1. Open http://localhost:9002
2. Search for your Snowplow schemas (e.g., "checkout_started")
3. Click on a schema
4. Look for **"Owners"** section in the right sidebar or schema details

**Expected to See**:

- **Data Owner**: Creator's email (e.g., ryan@company.com)
- **Producer**: Last modifier's email (e.g., jane@company.com)
- **Source**: Link to BDP Console

### Step 7: Verify via GraphQL

Query DataHub's GraphQL API to confirm ownership:

```bash
curl -X POST http://localhost:8080/api/graphql \
  -H "Authorization: Bearer ${DATAHUB_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout_started.1-1-0,PROD)\") { urn ownership { owners { owner { ... on CorpUser { username } } type } } } }"
  }'
```

**Expected Response**:

```json
{
  "data": {
    "dataset": {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout_started.1-1-0,PROD)",
      "ownership": {
        "owners": [
          {
            "owner": {
              "username": "ryan@company.com"
            },
            "type": "DATAOWNER"
          },
          {
            "owner": {
              "username": "jane@company.com"
            },
            "type": "PRODUCER"
          }
        ]
      }
    }
  }
}
```

---

## Troubleshooting

### No Ownership Aspects Found

**Possible Causes**:

1. **No deployments in API response** - BDP API must return deployments array
2. **No initiator information** - Deployments must have initiator or initiatorId
3. **Users API not accessible** - Falls back to using names directly

**Debug Steps**:

```bash
# Check if deployments are in the fixture
cat tests/integration/snowplow/fixtures/data_structures_with_ownership.json | grep -A 5 deployments

# Check ingestion logs for warnings
datahub ingest -c recipe.yml --debug
```

### Users Not Resolved to Emails

**Issue**: Seeing names instead of emails as owners

**Cause**: Users API not returning data or initiatorId missing

**Solution**: Verify mock users are configured in tests:

```python
# In test_snowplow.py
mock_users = [
    User(id="user1", email="ryan@company.com", name="Ryan Smith"),
    # ... more users
]
mock_client.get_users.return_value = mock_users
```

### DataHub Not Starting

**Check Ports**:

```bash
lsof -i :8080  # GMS port
lsof -i :9002  # Frontend port
```

**Check Docker**:

```bash
docker ps
docker-compose logs -f
```

---

## What to Verify

When testing, verify these aspects:

### ✅ Ownership Extraction

- [ ] Schemas with multiple deployments show both DATAOWNER and PRODUCER
- [ ] Schemas with single deployment show only DATAOWNER
- [ ] Creator is oldest deployment initiator
- [ ] Modifier is newest deployment initiator

### ✅ User Resolution

- [ ] Users resolved to emails when initiatorId available
- [ ] Falls back to names when ID not available
- [ ] Logs warnings for ambiguous name matches

### ✅ DataHub Integration

- [ ] Ownership aspects emitted with correct structure
- [ ] SOURCE_CONTROL source type set
- [ ] BDP Console URLs included
- [ ] Ownership visible in DataHub UI

---

## Success Criteria

Your testing is successful when:

1. ✅ **Integration tests pass** (test_snowplow_ingest)
2. ✅ **3 ownership aspects** found in output
3. ✅ **Correct owner types** (DATAOWNER for creators, PRODUCER for modifiers)
4. ✅ **Email resolution works** (users resolved to emails, not just names)
5. ✅ **UI displays ownership** (if testing with DataHub)

---

## Next Steps After Testing

Once you've verified ownership works:

1. **Provide Feedback**: What works well? Any issues?
2. **Optional Enhancements**:
   - Field-level authorship (who added each field)
   - Column-level lineage (schema → warehouse mapping)
   - Data products extraction
3. **Production Deployment**: Configure with real BDP credentials

---

## Quick Reference: Test Commands

```bash
# Quick test (integration)
pytest tests/integration/snowplow/test_snowplow.py::test_snowplow_ingest -v

# Start mock server
cd tests/integration/snowplow && python mock_bdp_server.py

# Check DataHub health
curl http://localhost:8080/health

# Run ingestion to file
datahub ingest -c recipe.yml

# Verify ownership count
python3 -c "import json; data=json.load(open('/tmp/output.json')); print(sum(1 for x in data if x.get('aspectName')=='ownership'))"
```

---

**Ready to Test!** Start with Option 1 (integration tests) for immediate verification.
