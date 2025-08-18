import json
import logging
from typing import Any, Dict

import requests

from datahub.ingestion.source.data_lake_common.connections import (
    DataLakeConnectionConfig,
    is_data_lake_uri,
)
from datahub.ingestion.source.data_lake_common.object_store_client import (
    ObjectStoreClient,
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
        # Prevent path traversal attacks
        if "../" in uri or "..\\" in uri:
            raise ValueError("Invalid file path")
        with open(uri, "r") as file:
            return json.load(file)

    # Handle data lake URIs using object store client
    client = ObjectStoreClient(data_lake_connections)
    if client.is_supported_uri(uri):
        platform = client.get_platform_for_uri(uri)
        logger.debug(f"Loading file from {platform}: {uri}")
        return client.load_file_as_json(uri)

    # Handle HTTP/HTTPS URLs (that are not data lake URIs)
    if uri.startswith(("http://", "https://")):
        logger.debug(f"Loading file from HTTP/HTTPS: {uri}")
        response = requests.get(uri)
        response.raise_for_status()
        return response.json()

    # If we get here, it's an unsupported URI format
    raise ValueError(f"Unsupported URI format: {uri}")
