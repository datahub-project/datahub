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

#### Troubleshooting

- **Permission Issues**: Ensure the service account has at least "Viewer" access to all sheets you want to catalog
- **Rate Limiting**: The connector includes retry logic, but Google API rate limits may still apply
- **Lineage Extraction Failures**: Check that all referenced sheets are accessible to the service account
