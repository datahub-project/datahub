# Google Drive Integration Tests

Integration tests for the DataHub Google Drive connector that validate real-world ingestion against the Google Drive API.

## Overview

These tests run a real ingestion pipeline against a Google Drive folder to validate:

1. **End-to-end ingestion** - Documents in a real folder are discovered, exported, and emitted as DataHub `document` entities.
2. **Test connection** - The `test_connection` capability succeeds with valid credentials.

## Prerequisites

### 1. Create a Service Account

1. Go to the [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts).
2. Create a new service account (or reuse an existing one).
3. Enable the **Google Drive API** for the service account's project.
4. Create and download a JSON key for the service account.

### 2. Grant Access to a Test Folder

Choose one of the following:

**Option A: Share a folder directly with the service account**

1. Create a folder in Google Drive containing at least one Google Doc.
2. Share the folder with the service account's email address (found in the JSON key file, e.g. `my-sa@my-project.iam.gserviceaccount.com`), granting at least **Viewer** access.
3. Copy the folder ID from its URL: `https://drive.google.com/drive/folders/{FOLDER_ID}`.

**Option B: Use domain-wide delegation**

1. Configure [domain-wide delegation](https://developers.google.com/identity/protocols/oauth2/service-account#delegatingauthority) for the service account in your Workspace admin console, authorizing the Drive read-only scope.
2. Pick a Workspace user whose Drive contains (or can access) the test folder.
3. Set `GOOGLE_DRIVE_DELEGATED_USER_EMAIL` to that user's email so the service account impersonates them.

## Running the Tests

### Set Environment Variables

```bash
# Required
export GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE="/path/to/service-account.json"
export GOOGLE_DRIVE_TEST_FOLDER_ID="1A2B3C4D5E6F7G8H9I0J"

# Optional: only needed for domain-wide delegation
export GOOGLE_DRIVE_DELEGATED_USER_EMAIL="test-user@example.com"
```

### Run All Google Drive Integration Tests

From the repository root:

```bash
cd metadata-ingestion
../gradlew :metadata-ingestion:testQuick -PtestFile=tests/integration/google_drive/test_google_drive_integration.py
```

Or using pytest directly (after activating the venv):

```bash
pytest -m integration tests/integration/google_drive/
```

### Run a Specific Test

```bash
pytest -m integration tests/integration/google_drive/test_google_drive_integration.py::test_google_drive_ingestion -v
```

### Skip Tests Without Credentials

If `GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE` or `GOOGLE_DRIVE_TEST_FOLDER_ID` are not set, all tests in this directory are automatically skipped at collection time:

```
SKIPPED [2] tests/integration/google_drive/conftest.py:11: GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE and GOOGLE_DRIVE_TEST_FOLDER_ID environment variables must be set for Google Drive integration tests
```

## What Gets Tested

### 1. End-to-End Ingestion

**Test**: `test_google_drive_ingestion`

- Runs a real `Pipeline` against the configured test folder (`max_documents: 10`).
- Verifies the ingestion completes without failures.
- Verifies at least one `document` entity MCP is present in the file-sink output.

### 2. Test Connection

**Test**: `test_google_drive_test_connection`

- Calls `GoogleDriveSource.test_connection` with the configured credentials.
- Verifies basic connectivity to the Google Drive API succeeds.

## Troubleshooting

### Tests are skipped

**Cause**: Environment variables not set.

**Solution**: Export `GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE` and `GOOGLE_DRIVE_TEST_FOLDER_ID`.

### "File not found" or permission errors

**Cause**: The test folder is not shared with the service account (or delegation is not configured correctly).

**Solution**: Re-check the sharing settings on the test folder, or verify domain-wide delegation is authorized for the Drive scope.

### No `document` entities in the output

**Cause**: The test folder doesn't contain any Google Docs, or `include_docs`/`include_slides`/`include_sheets` filtering excludes the only files present.

**Solution**: Ensure the test folder contains at least one native Google Doc.

## CI/CD Integration

### GitHub Actions Example

```yaml
- name: Run Google Drive Integration Tests
  env:
    GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE: ${{ secrets.GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE }}
    GOOGLE_DRIVE_TEST_FOLDER_ID: ${{ secrets.GOOGLE_DRIVE_TEST_FOLDER_ID }}
  run: |
    cd metadata-ingestion
    ../gradlew :metadata-ingestion:testQuick -PtestFile=tests/integration/google_drive/test_google_drive_integration.py
```

### Important Notes for CI

- Store the service account key as a secret file (never commit it to the repo).
- Use a dedicated test folder/Workspace to avoid touching production Drive content.
- Tests will automatically skip if credentials are not provided, so CI without secrets configured stays green.

## Related Documentation

- [Google Drive API Documentation](https://developers.google.com/drive/api/guides/about-sdk)
- [DataHub Google Drive Connector](../../../src/datahub/ingestion/source/google_drive/google_drive_source.py)
- [Integration Test Patterns](../../README.md)
