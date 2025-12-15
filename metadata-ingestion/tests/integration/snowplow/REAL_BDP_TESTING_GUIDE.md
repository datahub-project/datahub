# Testing Snowplow Ownership with Real BDP Credentials

This guide walks you through testing the ownership feature with your actual Snowplow BDP deployment.

---

## Prerequisites

1. **Snowplow BDP Account** with Console access
2. **BDP API Credentials** (Organization ID, API Key ID, API Key)
3. **DataHub running** locally (already confirmed ✅)

---

## Step 1: Gather BDP Credentials

### Finding Your Organization ID

1. Log into BDP Console: https://console.snowplowanalytics.com
2. Look at the URL after login: `https://console.snowplowanalytics.com/organizations/{ORG_ID}/...`
3. Copy the UUID (e.g., `a1b2c3d4-e5f6-7890-abcd-ef1234567890`)

### Creating API Credentials

1. In BDP Console, go to: **Settings → API Credentials**
2. Click **"Create New Credentials"**
3. Give it a name: `DataHub Ingestion`
4. Copy the **API Key ID** and **API Key Secret**
5. **IMPORTANT**: Save the secret immediately - it won't be shown again!

---

## Step 2: Set Environment Variables

**Securely set your credentials as environment variables**:

```bash
# Set BDP credentials
export SNOWPLOW_ORG_ID="your-org-id-here"
export SNOWPLOW_API_KEY_ID="your-api-key-id-here"
export SNOWPLOW_API_KEY="your-api-key-secret-here"

# DataHub token (already set from ~/.datahubenv)
export DATAHUB_TOKEN="eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImFkbWluIiwidHlwZSI6IlBFUlNPTkFMIiwidmVyc2lvbiI6IjIiLCJqdGkiOiI5Yjg4YzU4NS1iY2RkLTQ1Y2YtYTBlYi1kYmZjZThhZTA0YjEiLCJzdWIiOiJhZG1pbiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.3c0YvNLCLLoMCnOHD6jUmyJy7PHIK8vR49FfYuxTB20"

# Verify environment variables are set
echo "Organization ID: $SNOWPLOW_ORG_ID"
echo "API Key ID: $SNOWPLOW_API_KEY_ID"
echo "API Key: ${SNOWPLOW_API_KEY:0:10}..." # Show first 10 chars only
```

**Security Notes**:
- ❌ Never commit credentials to git
- ❌ Never hardcode credentials in config files
- ✅ Always use environment variables
- ✅ Use separate credentials for testing vs production

---

## Step 3: Test BDP Connection

Before running full ingestion, test the connection:

```bash
cd /Users/treff7es/shadow/datahub/metadata-ingestion

# Test connection (will validate credentials and connection)
datahub ingest -c tests/integration/snowplow/test_real_bdp.yml --dry-run --preview
```

**Expected Output**:
```
✅ Successfully authenticated with Snowplow BDP Console API
✅ Fetching users from Snowplow BDP
✅ Cached N users for ownership resolution
✅ Found X data structures
```

**If you see errors**:
- `401 Unauthorized` → Check API credentials
- `404 Not Found` → Verify organization ID
- `Connection refused` → Check network/firewall

---

## Step 4: Run Ingestion (Dry Run First)

First, do a dry run to see what would be ingested:

```bash
cd /Users/treff7es/shadow/datahub/metadata-ingestion

# Dry run - won't write to DataHub
datahub ingest -c tests/integration/snowplow/test_real_bdp.yml --dry-run
```

Review the output:
- How many schemas found?
- Are ownership aspects included?
- Any warnings or errors?

---

## Step 5: Run Full Ingestion

If dry run looks good, run the full ingestion:

```bash
cd /Users/treff7es/shadow/datahub/metadata-ingestion

datahub ingest -c tests/integration/snowplow/test_real_bdp.yml
```

**What to Watch For**:
- ✅ Authentication successful
- ✅ Users cached (should see count > 0)
- ✅ Data structures fetched
- ✅ Ownership aspects emitted
- ✅ Events written to DataHub

---

## Step 6: Verify in DataHub UI

1. **Open DataHub**: http://localhost:9002
2. **Search for your schemas**: Use your actual schema names
3. **Check ownership**:
   - Click on a schema
   - Look for "Owners" section
   - Verify real user emails appear
   - Check DATAOWNER and PRODUCER assignments

---

## Step 7: Verify via GraphQL

```bash
# Replace with your actual schema URN
python3 << 'EOF'
import requests
import os

token = os.environ.get('DATAHUB_TOKEN')
org_id = os.environ.get('SNOWPLOW_ORG_ID')

# First, search for your schemas
query = """
{
  search(input: {type: DATASET, query: "snowplow", start: 0, count: 10}) {
    total
    searchResults {
      entity {
        urn
        ... on Dataset {
          name
        }
      }
    }
  }
}
"""

response = requests.post(
    "http://localhost:8080/api/graphql",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json={"query": query}
)

data = response.json()
if 'data' in data:
    print(f"Found {data['data']['search']['total']} Snowplow schemas")
    for result in data['data']['search']['searchResults']:
        print(f"  - {result['entity']['urn']}")
EOF
```

---

## Troubleshooting

### Issue: No ownership data appears

**Check**:
1. Does your BDP deployment have deployment history?
   - Log into BDP Console
   - Go to Data Structures
   - Click on a schema → Version History
   - Verify deployments exist with initiator information

2. Are users being fetched?
   - Look for log line: `Cached N users for ownership resolution`
   - If N=0, check Users API access permissions

3. Do schemas have initiatorId or initiator?
   - Older BDP versions may not include this data
   - Contact Snowplow support if missing

### Issue: Wrong users appearing as owners

**Possible causes**:
1. **Name collisions**: Multiple users with same name
   - Solution: BDP should include `initiatorId` for unique resolution
   - Check logs for warnings about ambiguous names

2. **Cached user data outdated**: Users changed in BDP
   - Solution: Re-run ingestion (users are fetched fresh each run)

### Issue: Missing some schemas

**Check**:
1. Schema filters in recipe (schema_pattern)
2. Hidden schemas (set `include_hidden_schemas: true` if needed)
3. Schema types (check `schema_types_to_extract: ["event", "entity"]`)

---

## Expected Results

With real BDP data, you should see:

1. **Real user emails** as owners (not test data)
2. **Actual deployment history** reflected in ownership
3. **Multiple schemas** from your BDP deployment
4. **Correct DATAOWNER/PRODUCER** assignments based on who created/modified

---

## Security Cleanup

After testing, unset credentials:

```bash
unset SNOWPLOW_API_KEY
unset SNOWPLOW_API_KEY_ID
unset SNOWPLOW_ORG_ID
```

Or close your terminal session.

---

## Success Criteria

✅ BDP authentication successful
✅ Users fetched and cached (count > 0)
✅ Schemas extracted with ownership
✅ Real user emails appear in DataHub
✅ Ownership visible in DataHub UI
✅ DATAOWNER = schema creator
✅ PRODUCER = last modifier (if different from creator)

---

**Ready to test?** Set your environment variables and run Step 3!
