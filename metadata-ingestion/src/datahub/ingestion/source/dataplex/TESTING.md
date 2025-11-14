# Quick Testing Guide

## Test Without Writing to DataHub

The test script allows you to validate the connector before running actual ingestion.

### Prerequisites

```bash
# 1. Authenticate with Google Cloud
gcloud auth application-default login

# 2. Install the package in development mode
cd metadata-ingestion
pip install -e .

# OR using gradle (from repository root)
./gradlew :metadata-ingestion:installDev

# 3. Enable Dataplex API
gcloud services enable dataplex.googleapis.com --project=YOUR_PROJECT
```

### Basic Usage

```bash
cd metadata-ingestion/src/datahub/ingestion/source/dataplex/

# Quick test with project ID (uses ADC)
python test_dataplex_connector.py --project my-gcp-project

# Test with config file
python test_dataplex_connector.py --config my_config.yml

# Save results to JSON
python test_dataplex_connector.py --config my_config.yml --output results.json

# Verbose output
python test_dataplex_connector.py --config my_config.yml --verbose
```

### What Gets Tested

- ✅ **Authentication**: Validates GCP credentials
- ✅ **API Access**: Confirms Dataplex API connectivity
- ✅ **Projects**: Lists configured projects
- ✅ **Lakes**: Extracts and filters lakes
- ✅ **Zones**: Extracts and filters zones (with types)
- ✅ **Workunits**: Generates metadata (dry run, not written)

### Example Output

```
================================================================================
Test: Lake Extraction
================================================================================
Scanning lakes in project: my-project

  ✅ INCLUDED Lake: sales-data (Sales Data Lake)
  ✅ INCLUDED Lake: analytics (Analytics Lake)
  🚫 FILTERED Lake: test-lake (Test Environment)

✓ Total lakes found: 3

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

### Common Issues

**Permission Denied:**

```bash
# Grant required permissions
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:YOUR_SA@PROJECT.iam.gserviceaccount.com" \
  --role="roles/dataplex.viewer"
```

**No Resources Found:**

- Check if Dataplex resources exist in the location
- Verify filter patterns aren't too restrictive
- Try with `--verbose` flag

**API Not Enabled:**

```bash
gcloud services enable dataplex.googleapis.com --project=YOUR_PROJECT
```

### After Testing Succeeds

Run actual ingestion to DataHub:

```bash
# From repository root
datahub ingest -c your_config.yml
```

See [TEST_GUIDE.md](./TEST_GUIDE.md) for detailed documentation.
