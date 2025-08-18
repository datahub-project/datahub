import json
import logging
import tempfile
from pathlib import Path
from typing import Dict, Optional

import requests
from pydantic import Field

from datahub.configuration.git import GitInfo
from datahub.ingestion.source.data_lake_common.connections import (
    DataLakeConnectionConfig,
    get_data_lake_uri_type as get_cloud_uri_type,
    is_data_lake_uri as is_cloud_uri,
)
from datahub.ingestion.source.data_lake_common.file_loader import load_file_as_json
from datahub.ingestion.source.git.git_import import GitClone

logger = logging.getLogger(__name__)


def is_git_uri(uri: str) -> bool:
    """
    Check if a URI is a Git repository URI.

    Args:
        uri: The URI to check

    Returns:
        bool: True if the URI is a Git repository URI, False otherwise
    """
    if not uri:
        return False

    return (
        uri.startswith("git@")
        or uri.startswith("https://github.com/")
        or uri.startswith("https://gitlab.com/")
        or uri.startswith("git+")
        or ".git" in uri
    )


def is_external_uri(uri: str) -> bool:
    """
    Check if a URI is for an external source (cloud storage, Git, or HTTP).

    Args:
        uri: The URI to check

    Returns:
        bool: True if the URI is for an external source, False otherwise
    """
    if not uri:
        return False

    return (
        is_cloud_uri(uri) or is_git_uri(uri) or uri.startswith(("http://", "https://"))
    )


def get_external_uri_type(uri: str) -> Optional[str]:
    """
    Get the type of external URI.

    Args:
        uri: The URI to check

    Returns:
        str: The URI type ('s3', 'gcs', 'azure', 'git', 'http') or None if not an external URI
    """
    if not uri:
        return None

    cloud_type = get_cloud_uri_type(uri)
    if cloud_type:
        return cloud_type
    elif is_git_uri(uri):
        return "git"
    elif uri.startswith(("http://", "https://")):
        return "http"
    else:
        return None


class ExternalConnectionConfig(DataLakeConnectionConfig):
    """
    Configuration for external connections supporting cloud storage and Git repositories.

    This configuration allows dbt sources to fetch manifest, catalog, and run result files
    from various external sources including:
    - Cloud storage: S3, GCS, Azure Blob Storage (inherited from DataLakeConnectionConfig)
    - Git repositories: GitHub (including Enterprise), GitLab, and other Git hosts
    - HTTP/HTTPS endpoints
    """

    git_info: Optional[GitInfo] = Field(
        default=None,
        description="When fetching manifest files from Git repositories (GitHub, GitLab, etc.), configuration for Git connection details including deploy keys",
    )

    def validate_connections_for_uris(self, uris: list) -> None:
        """
        Validate that appropriate connections are configured for the given URIs.

        Args:
            uris: List of URIs to validate connections for

        Raises:
            ValueError: If required connections are missing for any URI type
        """
        cloud_uris = []
        git_uris = []

        for uri in uris:
            if not uri:
                continue

            # Check Git URIs first, since they might also match data lake patterns
            if is_git_uri(uri):
                git_uris.append(uri)
            elif is_data_lake_uri(uri):
                cloud_uris.append(uri)

        # Validate cloud storage connections (inherited from DataLakeConnectionConfig)
        if cloud_uris:
            # Use the inherited validate_connections_for_uris method from DataLakeConnectionConfig
            super().validate_connections_for_uris(cloud_uris)

        # Validate Git connections
        if git_uris and self.git_info is None:
            raise ValueError(
                f"Please provide git_info configuration, since Git URIs have been provided: {git_uris}"
            )

    def load_file_as_json(self, uri: str) -> Dict:
        """
        Load a JSON file from various external sources.

        Args:
            uri: The URI of the file to load. Supports:
                - Cloud storage: S3, GCS, Azure Blob Storage (via data_lake_common)
                - Git repositories: GitHub, GitLab, etc.
                - HTTP/HTTPS URLs
                - Local file paths

        Returns:
            Dict: The parsed JSON content

        Raises:
            ValueError: If the URI format is not supported or required connections are missing
            Exception: If file loading or JSON parsing fails
        """
        if is_git_uri(uri):
            return self._load_git_file_as_json(uri)
        elif is_data_lake_uri(uri):
            # Delegate to the inherited data lake file loading functionality
            return load_file_as_json(uri, self)
        elif uri.startswith(("http://", "https://")):
            # Regular HTTP/HTTPS URL (not detected as cloud URI)
            response = requests.get(uri)
            response.raise_for_status()
            return response.json()
        else:
            # Local file path
            with open(uri, "r", encoding="utf-8") as f:
                return json.load(f)

    def _load_git_file_as_json(self, uri: str) -> Dict:
        """
        Load a JSON file from a Git repository.

        Args:
            uri: The Git URI to load from

        Returns:
            Dict: The parsed JSON content

        Raises:
            ValueError: If Git connection is not configured
            Exception: If Git operations or file loading fails
        """
        if not self.git_info:
            raise ValueError(f"Git connection required for Git URI: {uri}")

        # Parse the Git URI to extract repository and file path
        # For now, assume format like: git@github.com:owner/repo.git/path/to/file.json
        # or https://github.com/owner/repo/blob/main/path/to/file.json

        # This is a simplified implementation - in practice, you might want more sophisticated parsing
        if "@" in uri and ":" in uri:
            # SSH format: git@github.com:owner/repo.git/path/to/file.json
            parts = uri.split("/", 1)
            if len(parts) == 2:
                repo_part = parts[0]
                file_path = parts[1]
            else:
                raise ValueError(f"Invalid Git URI format: {uri}")
        elif uri.startswith(("https://github.com/", "https://gitlab.com/")):
            # HTTPS format: https://github.com/owner/repo/blob/main/path/to/file.json
            # Extract repository and file path
            # This is a simplified parser - you might want to use a more robust implementation
            raise NotImplementedError("HTTPS Git URI parsing not yet implemented")
        else:
            raise ValueError(f"Unsupported Git URI format: {uri}")

        # Clone the repository to a temporary directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            git_clone = GitClone(tmp_dir)
            cloned_path = git_clone.clone(
                ssh_key=self.git_info.deploy_key,
                repo_url=repo_part,
                branch=getattr(self.git_info, "branch", None),
            )

            # Load the file from the cloned repository
            full_file_path = Path(cloned_path) / file_path
            with open(full_file_path, "r", encoding="utf-8") as f:
                return json.load(f)


# Keep these for backwards compatibility, but they now delegate to the external functions
def is_data_lake_uri(uri: str) -> bool:
    """
    Check if a URI is an external URI (cloud storage, Git, or HTTP).

    Note: This function name is kept for backwards compatibility but now includes Git URIs.
    """
    return is_external_uri(uri)


def get_data_lake_uri_type(uri: str) -> Optional[str]:
    """
    Get the type of external URI.

    Note: This function name is kept for backwards compatibility but now includes Git URIs.
    """
    return get_external_uri_type(uri)
