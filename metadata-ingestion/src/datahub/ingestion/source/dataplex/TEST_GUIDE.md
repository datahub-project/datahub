# Dataplex Connector Testing Guide

This guide explains how to test the Dataplex connector without writing metadata to DataHub.

## Quick Start

### Option 1: Using a Configuration File (Recommended)

1. **Copy the sample config:**

   ```bash
   cp test_config_sample.yml my_test_config.yml
   ```

2. **Edit the configuration:**

   ```yaml
   source:
     type: dataplex
     config:
       project_ids:
         - "your-gcp-project-id"
       location: "us-central1"
   ```

3. **Run the test:**
   ```bash
   python test_dataplex_connector.py --config my_test_config.yml
   ```

### Option 2: Quick Test with Command-Line Args

```bash
# Using Application Default Credentials (ADC)
python test_dataplex_connector.py --project my-gcp-project --location us-central1

# Using a service account key file
python test_dataplex_connector.py \
  --project my-gcp-project \
  --location us-central1 \
  --credential-file /path/to/service-account-key.json
```

## Test Output

The test script performs the following checks:

### 1. Authentication Test ✅

- Validates GCP credentials
- Checks whether using service account or ADC

### 2. Configuration Validation ✅

- Validates all config parameters
- Shows active settings

### 3. API Access Test ✅

- Initializes Dataplex API clients
- Verifies connectivity

### 4. Project Listing ✅

- Confirms access to configured projects

### 5. Lake Extraction ✅

- Lists all lakes in each project
- Shows which lakes pass/fail filters
- Displays lake metadata

### 6. Zone Extraction ✅

- Lists all zones in each lake
- Shows zone types (RAW/CURATED)
- Displays which zones pass/fail filters

### 7. Workunit Generation ✅

- Generates DataHub workunits (dry run)
- Counts workunits by type
- **Does NOT write to DataHub**

## Example Output

```
================================================================================
Starting Dataplex Connector Tests
================================================================================

================================================================================
Test: Authentication
================================================================================
✓ Using Application Default Credentials (ADC)
Authentication: ✅ PASSED

================================================================================
Test: Lake Extraction
================================================================================
Scanning lakes in project: my-project

  ✅ INCLUDED Lake: sales-lake (Sales Data Lake)
  ✅ INCLUDED Lake: analytics-lake (Analytics Data Lake)
  🚫 FILTERED Lake: test-lake (Test Lake)

✓ Total lakes found: 3
Lake Extraction: ✅ PASSED

================================================================================
TEST SUMMARY
================================================================================

Authentication: adc
API Access: True
Projects Scanned: 1
Lakes Found: 3
Zones Found: 5
Workunits Generated: 15
```

## Export Results to JSON

Save test results for analysis:

```bash
python test_dataplex_connector.py \
  --config my_test_config.yml \
  --output test_results.json
```

The JSON output includes:

```json
{
  "authentication": "adc",
  "api_access": true,
  "projects": [...],
  "lakes": [
    {
      "id": "sales-lake",
      "display_name": "Sales Data Lake",
      "state": "ACTIVE",
      "filtered": false
    }
  ],
  "zones": [...],
  "workunits_generated": 15,
  "workunit_types": {
    "containerProperties": 5,
    "domainProperties": 10
  },
  "errors": []
}
```

## Advanced Usage

### Verbose Logging

Enable debug-level logging:

```bash
python test_dataplex_connector.py --config my_test_config.yml --verbose
```

### Testing Multiple Projects

Edit your config to include multiple projects:

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "prod-data-project"
      - "dev-data-project"
      - "analytics-project"
    location: "us-central1"
```

### Testing Filters

Test different filter patterns:

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-project"
    filter_config:
      lake_pattern:
        allow:
          - "prod-.*"
          - "analytics-.*"
        deny:
          - ".*-test"
      zone_pattern:
        allow:
          - ".*"
        deny:
          - "deprecated-.*"
```

## Troubleshooting

### Authentication Errors

**Error:** `Permission denied accessing lakes`

**Solution:** Ensure your service account has these IAM roles:

```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:SA_EMAIL" \
  --role="roles/dataplex.viewer"
```

Required permissions:

- `dataplex.lakes.list`
- `dataplex.lakes.get`
- `dataplex.zones.list`
- `dataplex.zones.get`

### API Not Enabled

**Error:** `Dataplex API has not been used in project`

**Solution:** Enable the API:

```bash
gcloud services enable dataplex.googleapis.com --project=PROJECT_ID
```

### No Resources Found

**Error:** `Total lakes found: 0`

**Possible causes:**

1. No Dataplex resources exist in the project/location
2. Filter patterns are too restrictive
3. Service account lacks list permissions
4. Wrong location specified

**Debug steps:**

```bash
# Check if lakes exist
gcloud dataplex lakes list --location=us-central1 --project=PROJECT_ID

# Try with verbose logging
python test_dataplex_connector.py --config my_test_config.yml --verbose
```

### Configuration Validation Failed

**Error:** `At least one project must be specified`

**Solution:** Make sure you have either:

- `project_ids: ["my-project"]` in config, OR
- `project_id: "my-project"` in config (deprecated but supported)

## Integration with CI/CD

Use the test script in your CI pipeline:

```bash
#!/bin/bash
# ci-test-dataplex.sh

python test_dataplex_connector.py \
  --config $CONFIG_FILE \
  --output test_results.json

# Check exit code
if [ $? -eq 0 ]; then
  echo "✅ All tests passed"
  exit 0
else
  echo "❌ Tests failed"
  cat test_results.json
  exit 1
fi
```

## Next Steps

Once tests pass:

1. **Run actual ingestion** to DataHub:

   ```bash
   datahub ingest -c my_test_config.yml
   ```

2. **Add a sink configuration** for real ingestion:

   ```yaml
   source:
     type: dataplex
     config:
       # ... your config ...

   sink:
     type: datahub-rest
     config:
       server: http://localhost:8080
   ```

3. **Schedule regular ingestion** using cron or your orchestration tool

## Support

For issues or questions:

- Check the main [README.md](./README.md)
- Review the [implementation spec](./dataplex_implementation.md)
- Open an issue on GitHub
