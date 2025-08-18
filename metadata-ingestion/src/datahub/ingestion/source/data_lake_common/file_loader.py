import json
import logging
from typing import Any, Dict

import requests
from google.cloud import storage

from datahub.ingestion.source.data_lake_common.connections import (
    DataLakeConnectionConfig,
    get_data_lake_uri_type,
    is_data_lake_uri,
)
from datahub.ingestion.source.data_lake_common.object_store import (
    get_object_key,
    get_object_store_bucket_name,
)

logger = logging.getLogger(__name__)


def load_file_as_json(
    uri: str, data_lake_connections: DataLakeConnectionConfig
) -> Dict[str, Any]:
    """
    Load a file from a URI as JSON, supporting various cloud storage systems.

    Args:
        uri: The URI of the file to load
        data_lake_connections: Configuration for data lake connections

    Returns:
        Dict[str, Any]: The loaded JSON content

    Raises:
        ValueError: If the URI type is not supported or connections are missing
        Exception: If file loading fails
    """
    if not uri:
        raise ValueError("URI cannot be empty")

    # Handle local file paths (but not unsupported schemes like ftp://)
    if (
        not is_data_lake_uri(uri)
        and not uri.startswith(("http://", "https://"))
        and "://" not in uri
    ):
        logger.debug(f"Loading local file: {uri}")
        with open(uri, "r") as file:
            return json.load(file)

    # Handle data lake URIs first (before HTTP/HTTPS check)
    uri_type = get_data_lake_uri_type(uri)
    if uri_type:
        logger.debug(f"Loading file from {uri_type.upper()}: {uri}")

        if uri_type == "s3":
            if not data_lake_connections.aws_connection:
                raise ValueError(
                    f"Please provide aws_connection configuration, since S3 URI has been provided: {uri}"
                )
            return _load_s3_file_as_json(uri, data_lake_connections.aws_connection)
        elif uri_type == "gcs":
            if not data_lake_connections.gcs_connection:
                raise ValueError(
                    f"Please provide gcs_connection configuration, since GCS URI has been provided: {uri}"
                )
            return _load_gcs_file_as_json(uri, data_lake_connections.gcs_connection)
        elif uri_type == "azure":
            if not data_lake_connections.azure_connection:
                raise ValueError(
                    f"Please provide azure_connection configuration, since Azure URI has been provided: {uri}"
                )
            return _load_azure_file_as_json(uri, data_lake_connections.azure_connection)

    # Handle HTTP/HTTPS URLs (that are not data lake URIs)
    if uri.startswith(("http://", "https://")):
        logger.debug(f"Loading file from HTTP/HTTPS: {uri}")
        response = requests.get(uri)
        response.raise_for_status()
        return response.json()

    # If we get here, it's an unsupported URI format
    raise ValueError(f"Unsupported URI format: {uri}")


def _load_s3_file_as_json(uri: str, aws_connection: Any) -> Dict[str, Any]:
    """Load a file from S3 as JSON."""

    s3_client = aws_connection.get_s3_client()
    bucket_name = get_object_store_bucket_name(uri)
    key = get_object_key(uri)

    logger.debug(f"Loading S3 file: s3://{bucket_name}/{key}")

    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return json.loads(response["Body"].read())


def _load_gcs_file_as_json(uri: str, gcs_connection: Any) -> Dict[str, Any]:
    """Load a file from GCS as JSON."""

    client = storage.Client.from_service_account_info(
        gcs_connection.get_service_account_info()
    )
    bucket_name = get_object_store_bucket_name(uri)
    blob_name = get_object_key(uri)

    logger.debug(f"Loading GCS file: gs://{bucket_name}/{blob_name}")

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return json.loads(blob.download_as_text())


def _load_azure_file_as_json(uri: str, azure_connection: Any) -> Dict[str, Any]:
    """Load a file from Azure Blob Storage as JSON."""

    blob_service_client = azure_connection.get_blob_service_client()
    container_name = get_object_store_bucket_name(uri)
    blob_name = get_object_key(uri)

    logger.debug(f"Loading Azure file: {container_name}/{blob_name}")

    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    return json.loads(blob_client.download_blob().readall())
