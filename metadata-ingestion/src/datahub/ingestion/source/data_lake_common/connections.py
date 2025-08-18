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
from datahub.ingestion.source.data_lake_common.object_store import (
    ABSObjectStore,
    GCSObjectStore,
    S3ObjectStore,
    get_object_store_for_uri,
)

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

    return get_object_store_for_uri(uri) is not None


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

    object_store = get_object_store_for_uri(uri)
    if object_store == S3ObjectStore:
        return "s3"
    elif object_store == GCSObjectStore:
        return "gcs"
    elif object_store == ABSObjectStore:
        return "azure"
    else:
        return None


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

        for uri in uris:
            if not uri:
                continue

            object_store = get_object_store_for_uri(uri)
            if object_store == S3ObjectStore:
                s3_uris.append(uri)
            elif object_store == GCSObjectStore:
                gcs_uris.append(uri)
            elif object_store == ABSObjectStore:
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
