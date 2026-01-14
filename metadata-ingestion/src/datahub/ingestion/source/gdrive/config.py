import json
import os
from typing import Optional

from pydantic import ConfigDict, Field, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)


class GoogleDriveConfig(EnvConfigMixin, PlatformInstanceConfigMixin, ConfigModel):
    """Configuration for Google Drive source"""
    # DataHub secret containing full service account JSON (preferred approach)
    credentials_json: Optional[str] = Field(
        default=None,
        description="Google service account credentials as JSON string. "
        "This should be stored as a DataHub secret (e.g., ${GOOGLE_CREDENTIALS_JSON}). "
        "Preferred over credentials_path for DataHub secrets integration.",
    )

    # Folder filtering
    include_folders: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for folders to include",
    )

    # File filtering
    include_files: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for files to include",
    )

    # MIME type filtering
    include_mime_types: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="MIME type patterns to include/exclude. Examples: "
        "application/vnd.google-apps.spreadsheet (Google Sheets), "
        "application/pdf (PDF files), text/csv (CSV files)",
    )

    # File size limits
    max_file_size_mb: int = Field(
        default=100,
        description="Maximum file size in MB to process (default: 100MB)",
    )

    # Recursion depth
    max_recursion_depth: int = Field(
        default=10,
        description="Maximum depth to recurse into subdirectories (default: 10)",
    )

    # Include shared drives
    include_shared_drives: bool = Field(
        default=True,
        description="Whether to include shared drives (default: True)",
    )

    # Include trashed files
    include_trashed: bool = Field(
        default=False,
        description="Whether to include trashed files (default: False)",
    )

    # Specific folder ID to start from
    root_folder_id: Optional[str] = Field(
        default=None,
        description="Specific Google Drive folder ID to start scanning from. "
        "If not provided, will scan entire My Drive.",
    )

    @field_validator("credentials_json")
    @classmethod
    def validate_credentials_json(cls, v):
        if v is None:
            # Check for environment variable fallback
            env_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if env_creds and os.path.exists(env_creds):
                # Read from environment variable file path
                with open(env_creds, 'r') as f:
                    return f.read()
            else:
                raise ValueError(
                    "credentials_json must be provided as a DataHub secret (e.g., ${GOOGLE_CREDENTIALS_JSON}) "
                    "or GOOGLE_APPLICATION_CREDENTIALS environment variable must point to a valid file. "
                    "Note: Google Drive API requires service account authentication (API keys are not supported)."
                )
        
        # Validate that it's valid JSON
        try:
            json.loads(v)
        except json.JSONDecodeError as e:
            raise ValueError(f"credentials_json must be valid JSON: {e}")
            
        return v

    def get_credentials_dict(self):
        """Get credentials as a dictionary for Google API client"""
        if self.credentials_json:
            # Use credentials from JSON string (DataHub secret)
            return json.loads(self.credentials_json)
        else:
            # Fallback to environment variable
            env_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if env_creds and os.path.exists(env_creds):
                with open(env_creds, 'r') as f:
                    return json.load(f)
            else:
                raise ValueError("No valid credentials configuration found")

    @field_validator("max_file_size_mb")
    @classmethod
    def validate_max_file_size(cls, v):
        if v <= 0:
            raise ValueError("max_file_size_mb must be greater than 0")
        return v

    @field_validator("max_recursion_depth")
    @classmethod
    def validate_max_recursion_depth(cls, v):
        if v <= 0:
            raise ValueError("max_recursion_depth must be greater than 0")
        return v
