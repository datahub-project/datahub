import os
from typing import Optional

from pydantic import Field, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel


class GoogleDriveConfig(ConfigModel):
    """Configuration for Google Drive source"""

    # Google Drive API requires service account credentials (API keys not supported)
    credentials_path: Optional[str] = Field(
        default=None,
        description="Path to the Google service account credentials JSON file. "
        "If not provided, will look for GOOGLE_APPLICATION_CREDENTIALS environment variable. "
        "Note: Google Drive API does not support API key authentication.",
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

    @field_validator("credentials_path")
    @classmethod
    def validate_credentials_path(cls, v):
        if v is None:
            # Check for environment variable
            env_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if env_creds:
                return env_creds
            else:
                raise ValueError(
                    "credentials_path must be provided or GOOGLE_APPLICATION_CREDENTIALS environment variable must be set. "
                    "Note: Google Drive API requires service account authentication (API keys are not supported)."
                )

        if not os.path.exists(v):
            raise ValueError(f"Credentials file not found at path: {v}")

        return v

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
