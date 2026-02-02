### Setup

This source pulls metadata directly from Google Sheets and enables data profiling, lineage extraction, and usage statistics collection.

#### Steps to Get the Required Information

1. **Create a Google Cloud Project**:

   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one

2. **Enable Required APIs**:

   - Go to [API Library](https://console.cloud.google.com/apis/library)
   - Enable the following APIs:
     - Google Drive API
     - Google Sheets API
     - Google Drive Activity API (optional, for usage statistics)

3. **Create Service Account Credentials**:

   - Go to [Credentials](https://console.cloud.google.com/apis/credentials)
   - Click "Create Credentials" and select "Service Account"
   - Fill in the required fields and create the service account
   - Generate a JSON key file for the service account
   - Download and securely store the JSON key file

4. **Grant Access to Your Google Sheets**:
   - Share your Google Sheets or Drive folders with the service account email address
   - The service account email will look like: `service-account-name@project-id.iam.gserviceaccount.com`
   - For proper lineage extraction, ensure the service account has access to all linked sheets

#### Configuration Tips

- **Shared Drives**: Use `shared_drive_patterns` to filter specific Shared Drives by name when `scan_shared_drives=true`
- **Header Detection**: Use `auto_detect` mode for sheets with varying header positions, or `header_row_index` for fixed positions
- **Incremental Ingestion**: Enable `enable_incremental_ingestion` to only process modified sheets on subsequent runs
- **Cross-Platform Lineage**: Enable `enable_cross_platform_lineage` to extract lineage from QUERY() formulas connecting to databases

#### Troubleshooting

- **Permission Issues**: Ensure the service account has at least "Viewer" access to all sheets you want to catalog
- **Rate Limiting**: Configure `requests_per_second` to avoid hitting Google API rate limits. The connector includes retry logic with `max_retries` and `retry_delay`
- **Lineage Extraction Failures**: Check that all referenced sheets are accessible to the service account. For cross-platform lineage, ensure `parse_sql_for_lineage` is enabled
- **Shared Drive Access**: If Shared Drives aren't appearing, verify the service account has been added to the Shared Drive with at least "Viewer" permission
- **Header Detection Issues**: If auto-detection isn't working, use explicit `header_row_index` or examine sheets with `skip_empty_leading_rows=false`
