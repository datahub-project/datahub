import logging
from typing import Optional

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential

# Object store classes imported locally to avoid loading cloud dependencies

logger = logging.getLogger(__name__)


def is_data_lake_uri(uri: str) -> bool:
    """Check if a URI is for a supported data lake (cloud storage)."""
    if not uri:
        return False

    from datahub.ingestion.source.data_lake_common.object_store import (
        ABSObjectStore,
        GCSObjectStore,
        S3ObjectStore,
    )

    object_stores = [S3ObjectStore, GCSObjectStore, ABSObjectStore]
    return any(store.is_uri(uri) for store in object_stores)


def get_data_lake_uri_type(uri: str) -> Optional[str]:
    """Get the type of data lake URI ('s3', 'gcs', 'azure')."""
    if not uri:
        return None

    from datahub.ingestion.source.data_lake_common.object_store import (
        ABSObjectStore,
        GCSObjectStore,
        S3ObjectStore,
    )

    if S3ObjectStore.is_uri(uri):
        return "s3"
    elif GCSObjectStore.is_uri(uri):
        return "gcs"
    elif ABSObjectStore.is_uri(uri):
        return "azure"
    else:
        return None


def is_s3_uri(uri: str) -> bool:
    """Check if a URI is for Amazon S3."""
    from datahub.ingestion.source.data_lake_common.object_store import S3ObjectStore

    return S3ObjectStore.is_uri(uri)


def is_gcs_uri(uri: str) -> bool:
    """Check if a URI is for Google Cloud Storage."""
    from datahub.ingestion.source.data_lake_common.object_store import GCSObjectStore

    return GCSObjectStore.is_uri(uri)


def is_abs_uri(uri: str) -> bool:
    """Check if a URI is for Azure Blob Storage."""
    from datahub.ingestion.source.data_lake_common.object_store import ABSObjectStore

    return ABSObjectStore.is_uri(uri)


class DataLakeConnectionConfig(ConfigModel):
    """Configuration for data lake connections supporting cloud storage."""

    aws_connection: Optional[AwsConnectionConfig] = Field(
        default=None, description="Configuration for AWS S3 connection details"
    )

    gcs_connection: Optional[GCPCredential] = Field(
        default=None,
        description="Configuration for Google Cloud Storage connection details",
    )

    azure_connection: Optional[AzureConnectionConfig] = Field(
        default=None,
        description="Configuration for Azure Blob Storage connection details",
    )

    def validate_connections_for_uris(self, uris: list) -> None:
        """Validate that appropriate connections are configured for the given URIs."""
        s3_uris = []
        gcs_uris = []
        azure_uris = []

        from datahub.ingestion.source.data_lake_common.object_store import (
            ABSObjectStore,
            GCSObjectStore,
            S3ObjectStore,
        )

        for uri in uris:
            if not uri:
                continue
            if S3ObjectStore.is_uri(uri):
                s3_uris.append(uri)
            elif GCSObjectStore.is_uri(uri):
                gcs_uris.append(uri)
            elif ABSObjectStore.is_uri(uri):
                azure_uris.append(uri)

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
