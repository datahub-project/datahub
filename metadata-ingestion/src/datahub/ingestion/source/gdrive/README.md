# Google Drive Source Connector for DataHub

## Overview
This connector enables DataHub to ingest metadata from Google Drive, including files and folders, and represent them as datasets and containers in DataHub's metadata graph.

## Features
- Ingests files and folders from Google Drive
- Supports Google Drive shared drives and shared-with-me files
- File and folder filtering by name and MIME type
- Extracts metadata such as file size, type, creation/modification time, and web link
- Supports DataHub secrets for secure credential management
- Uses DataHub's modern SDK Dataset interface
- Supports platform instance and environment configuration

## Configuration
Example YAML configuration:

```yaml
source:
  type: gdrive
  config:
    credentials_json: "${GOOGLE_CREDENTIALS_JSON}"
    env: "PROD"
    platform_instance: "google-drive-main"
    root_folder_id: null  # Set to null to scan entire My Drive
    include_files:
      allow:
        - ".*"
      deny:
        - ".*\\.tmp$"
        - ".*\\.cache$"
    include_mime_types:
      allow:
        - "application/vnd.google-apps.folder"
        - "application/pdf"
        - "application/vnd.openxmlformats-officedocument.*"
      deny: []
    include_shared_drives: true
    include_trashed: false
    max_file_size_mb: 100
    max_recursion_depth: 10
```

## Required Permissions
- Service account with `drive.readonly` scope
- Access to the target Google Drive files/folders

## Usage
1. Set your Google service account credentials as a DataHub secret (e.g., `GOOGLE_CREDENTIALS_JSON`).
2. Configure your YAML as above.
3. Run ingestion:
   ```bash
   datahub ingest -c gdrive_config.yml
   ```

## Capabilities
- Descriptions
- Platform Instance
- Schema Metadata
- Containers (folder hierarchy)

## Development
- Source code: `src/datahub/ingestion/source/gdrive/`
- Main class: `GoogleDriveSource`
- Config: `GoogleDriveConfig`


## License
See the root LICENSE file.
