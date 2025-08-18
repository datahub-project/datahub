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
    """Load a file from a URI as JSON, supporting various cloud storage systems."""
    if not uri:
        raise ValueError("URI cannot be empty")

    if (
        not is_data_lake_uri(uri)
        and not uri.startswith(("http://", "https://"))
        and "://" not in uri
    ):
        logger.debug(f"Loading local file: {uri}")
        if "../" in uri or "..\\" in uri:
            raise ValueError("Invalid file path")
        with open(uri, "r") as file:
            return json.load(file)

    client = ObjectStoreClient(data_lake_connections)
    if client.is_supported_uri(uri):
        platform = client.get_platform_for_uri(uri)
        logger.debug(f"Loading file from {platform}: {uri}")
        return client.load_file_as_json(uri)

    if uri.startswith(("http://", "https://")):
        logger.debug(f"Loading file from HTTP/HTTPS: {uri}")
        response = requests.get(uri)
        response.raise_for_status()
        return response.json()

    raise ValueError(f"Unsupported URI format: {uri}")
