import logging
from typing import Any, Dict, Optional, Type

from datahub.ingestion.source.data_lake_common.connections import (
    DataLakeConnectionConfig,
)
from datahub.ingestion.source.data_lake_common.object_store import (
    ABSObjectStore,
    GCSObjectStore,
    ObjectStoreInterface,
    S3ObjectStore,
)

logger = logging.getLogger(__name__)


class ObjectStoreClient:
    """High-level client that automatically dispatches operations to the correct cloud provider."""

    _OBJECT_STORE_REGISTRY: Dict[str, Type[ObjectStoreInterface]] = {
        "s3": S3ObjectStore,
        "gcs": GCSObjectStore,
        "abs": ABSObjectStore,
    }

    def __init__(self, connections: DataLakeConnectionConfig):
        self.connections = connections

    def _get_connection_for_uri(self, uri: str) -> Any:
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)

        if object_store == S3ObjectStore:
            if not self.connections.aws_connection:
                raise ValueError(f"AWS connection required for S3 URI: {uri}")
            return self.connections.aws_connection

        elif object_store == GCSObjectStore:
            if not self.connections.gcs_connection:
                raise ValueError(f"GCS connection required for GCS URI: {uri}")
            return self.connections.gcs_connection

        elif object_store == ABSObjectStore:
            if not self.connections.azure_connection:
                raise ValueError(f"Azure connection required for ABS URI: {uri}")
            return self.connections.azure_connection

        else:
            raise ValueError(f"Unsupported URI format: {uri}")

    def load_file_as_json(self, uri: str) -> Dict[str, Any]:
        """Load a file from any supported object store as JSON."""
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if not object_store:
            raise ValueError(f"Unsupported URI format: {uri}")

        connection = self._get_connection_for_uri(uri)
        return object_store.load_file_as_json(uri, connection)

    def load_file_as_text(self, uri: str) -> str:
        """Load a file from any supported object store as text."""
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if not object_store:
            raise ValueError(f"Unsupported URI format: {uri}")

        connection = self._get_connection_for_uri(uri)
        return object_store.load_file_as_text(uri, connection)

    def load_file_as_bytes(self, uri: str) -> bytes:
        """Load a file from any supported object store as bytes."""
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if not object_store:
            raise ValueError(f"Unsupported URI format: {uri}")

        connection = self._get_connection_for_uri(uri)
        return object_store.load_file_as_bytes(uri, connection)

    @staticmethod
    def get_object_store_for_uri(uri: str) -> Optional[Type[ObjectStoreInterface]]:
        """Get the appropriate object store implementation for the given URI."""
        for object_store in ObjectStoreClient._OBJECT_STORE_REGISTRY.values():
            if object_store.is_uri(uri):
                return object_store
        return None

    @staticmethod
    def get_bucket_name(uri: str) -> str:
        """Get the bucket/container name from any supported object store URI."""
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if object_store:
            return object_store.get_bucket_name(uri)

        raise ValueError(f"Unsupported URI format: {uri}")

    @staticmethod
    def get_object_key(uri: str) -> str:
        """Get the object key/path from any supported object store URI."""
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if object_store:
            return object_store.get_object_key(uri)

        raise ValueError(f"Unsupported URI format: {uri}")

    def get_bucket_name_with_connection(self, uri: str) -> str:
        """Get the bucket/container name from any supported URI (instance method)."""
        self._get_connection_for_uri(uri)
        return ObjectStoreClient.get_bucket_name(uri)

    def get_object_key_with_connection(self, uri: str) -> str:
        """Get the object key/path from any supported URI (instance method)."""
        self._get_connection_for_uri(uri)
        return ObjectStoreClient.get_object_key(uri)

    def is_supported_uri(self, uri: str) -> bool:
        """Check if a URI is supported by any object store."""
        return ObjectStoreClient.get_object_store_for_uri(uri) is not None

    def get_platform_for_uri(self, uri: str) -> Optional[str]:
        """Get the platform name for a URI."""
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)

        if object_store == S3ObjectStore:
            return "s3"
        elif object_store == GCSObjectStore:
            return "gcs"
        elif object_store == ABSObjectStore:
            return "abs"
        else:
            return None
