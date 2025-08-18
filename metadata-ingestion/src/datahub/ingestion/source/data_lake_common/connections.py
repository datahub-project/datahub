"""
Data lake connection configurations for cloud storage access.

This module provides unified connection configurations that can be used across
different DataHub connectors to access files from cloud storage including:
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage

This implementation leverages the data_lake_common module for consistent handling
of object store operations.
"""

import logging
from typing import Any, Dict, Optional

from pydantic import Field, validator

from datahub.configuration import ConfigModel
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential

# NOTE: Object store classes are imported locally in functions to avoid loading
# cloud dependencies until actually needed. While the classes themselves don't
# have cloud dependencies, importing this module loads AwsConnectionConfig,
# AzureConnectionConfig, and GCPCredential which do load cloud SDKs.

logger = logging.getLogger(__name__)


def is_data_lake_uri(uri: str) -> bool:
    """
    Check if a URI is for a supported data lake (cloud storage).

    Uses the centralized object store detection from data_lake_common.

    Args:
        uri: The URI to check

    Returns:
        bool: True if the URI is for a supported data lake, False otherwise
    """
    if not uri:
        return False

    # Local imports to avoid loading unnecessary cloud dependencies
    from datahub.ingestion.source.data_lake_common.object_store import (
        ABSObjectStore,
        GCSObjectStore,
        S3ObjectStore,
    )

    # Check if any object store can handle this URI
    object_stores = [S3ObjectStore, GCSObjectStore, ABSObjectStore]
    return any(store.is_uri(uri) for store in object_stores)


def get_data_lake_uri_type(uri: str) -> Optional[str]:
    """
    Get the type of data lake URI.

    Uses the centralized object store detection from data_lake_common.

    Args:
        uri: The URI to check

    Returns:
        str: The URI type ('s3', 'gcs', 'azure') or None if not a data lake URI
    """
    if not uri:
        return None

    # Local imports to avoid loading unnecessary cloud dependencies
    from datahub.ingestion.source.data_lake_common.object_store import (
        ABSObjectStore,
        GCSObjectStore,
        S3ObjectStore,
    )

    # Check each object store type
    if S3ObjectStore.is_uri(uri):
        return "s3"
    elif GCSObjectStore.is_uri(uri):
        return "gcs"
    elif ABSObjectStore.is_uri(uri):
        return "azure"
    else:
        return None


# Individual URI detection functions for specific connectors
def is_s3_uri(uri: str) -> bool:
    """
    Check if a URI is for Amazon S3.

    Uses the centralized S3ObjectStore detection from data_lake_common.
    Local import to avoid loading unnecessary cloud dependencies.

    Args:
        uri: The URI to check

    Returns:
        bool: True if the URI is for S3, False otherwise
    """
    from datahub.ingestion.source.data_lake_common.object_store import S3ObjectStore

    return S3ObjectStore.is_uri(uri)


def is_gcs_uri(uri: str) -> bool:
    """
    Check if a URI is for Google Cloud Storage.

    Uses the centralized GCSObjectStore detection from data_lake_common.
    Local import to avoid loading unnecessary cloud dependencies.

    Args:
        uri: The URI to check

    Returns:
        bool: True if the URI is for GCS, False otherwise
    """
    from datahub.ingestion.source.data_lake_common.object_store import GCSObjectStore

    return GCSObjectStore.is_uri(uri)


def is_abs_uri(uri: str) -> bool:
    """
    Check if a URI is for Azure Blob Storage.

    Uses the centralized ABSObjectStore detection from data_lake_common.
    Local import to avoid loading unnecessary cloud dependencies.

    Args:
        uri: The URI to check

    Returns:
        bool: True if the URI is for ABS, False otherwise
    """
    from datahub.ingestion.source.data_lake_common.object_store import ABSObjectStore

    return ABSObjectStore.is_uri(uri)


class DataLakeConnectionConfig(ConfigModel):
    """
    Configuration for data lake connections supporting cloud storage.

    This configuration allows connectors to fetch files from various cloud storage systems:
    - Amazon S3
    - Google Cloud Storage
    - Azure Blob Storage

    This is a reusable component that can be used across different DataHub connectors.
    """

    aws_connection: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="Configuration for AWS S3 connection details",
    )

    gcs_connection: Optional[GCPCredential] = Field(
        default=None,
        description="Configuration for Google Cloud Storage connection details",
    )

    azure_connection: Optional[AzureConnectionConfig] = Field(
        default=None,
        description="Configuration for Azure Blob Storage connection details",
    )

    @validator("aws_connection", always=True)
    def aws_connection_validator(
        cls, aws_connection: Optional[AwsConnectionConfig], values: Dict, **kwargs: Any
    ) -> Optional[AwsConnectionConfig]:
        """Validate AWS connection configuration."""
        return aws_connection

    @validator("gcs_connection", always=True)
    def gcs_connection_validator(
        cls, gcs_connection: Optional[GCPCredential], values: Dict, **kwargs: Any
    ) -> Optional[GCPCredential]:
        """Validate GCS connection configuration."""
        return gcs_connection

    @validator("azure_connection", always=True)
    def azure_connection_validator(
        cls,
        azure_connection: Optional[AzureConnectionConfig],
        values: Dict,
        **kwargs: Any,
    ) -> Optional[AzureConnectionConfig]:
        """Validate Azure connection configuration."""
        return azure_connection

    def validate_connections_for_uris(self, uris: list) -> None:
        """
        Validate that appropriate connections are configured for the given URIs.

        Args:
            uris: List of URIs to validate connections for

        Raises:
            ValueError: If required connections are missing for any URI type
        """
        s3_uris = []
        gcs_uris = []
        azure_uris = []

        # Local imports to avoid loading unnecessary cloud dependencies
        from datahub.ingestion.source.data_lake_common.object_store import (
            ABSObjectStore,
            GCSObjectStore,
            S3ObjectStore,
        )

        for uri in uris:
            if not uri:
                continue

            # Check each object store type
            if S3ObjectStore.is_uri(uri):
                s3_uris.append(uri)
            elif GCSObjectStore.is_uri(uri):
                gcs_uris.append(uri)
            elif ABSObjectStore.is_uri(uri):
                azure_uris.append(uri)

        # Check for missing connections
        if s3_uris and self.aws_connection is None:
            raise ValueError(
                f"Please provide aws_connection configuration, since S3 URIs have been provided: {s3_uris}"
            )

        if gcs_uris and self.gcs_connection is None:
            raise ValueError(
                f"Please provide gcs_connection configuration, since GCS URIs have been provided: {gcs_uris}"
            )

        if azure_uris and self.azure_connection is None:
            raise ValueError(
                f"Please provide azure_connection configuration, since Azure URIs have been provided: {azure_uris}"
            )
