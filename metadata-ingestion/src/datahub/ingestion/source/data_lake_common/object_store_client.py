"""
Object Store Client for unified cloud storage operations.

This module provides a high-level client interface that automatically dispatches
operations to the appropriate cloud provider based on URI patterns, eliminating
the need for callers to know which specific object store implementation to use.
"""

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
    """
    High-level client that automatically dispatches operations to the correct cloud provider.

    This class provides a unified interface for all object store operations,
    automatically determining which cloud provider to use based on the URI
    and routing the operation to the appropriate implementation.

    This is the main entry point for data lake operations - other modules should
    use this client rather than calling object store implementations directly.
    """

    # Object store registry - centralized in the client
    _OBJECT_STORE_REGISTRY: Dict[str, Type[ObjectStoreInterface]] = {
        "s3": S3ObjectStore,
        "gcs": GCSObjectStore,
        "abs": ABSObjectStore,
    }

    def __init__(self, connections: DataLakeConnectionConfig):
        """
        Initialize the object store client with connection configurations.

        Args:
            connections: DataLakeConnectionConfig containing all cloud provider connections
        """
        self.connections = connections

    def _get_connection_for_uri(self, uri: str) -> Any:
        """
        Get the appropriate connection configuration for a URI.

        Args:
            uri: The URI to get connection for

        Returns:
            The connection configuration for the URI's cloud provider

        Raises:
            ValueError: If no appropriate connection is configured
        """
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
        """
        Load a file from any supported object store as JSON.

        Args:
            uri: The URI of the file to load

        Returns:
            Dict[str, Any]: The loaded JSON content

        Raises:
            ValueError: If the URI format is not supported or connections are missing
        """
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if not object_store:
            raise ValueError(f"Unsupported URI format: {uri}")

        connection = self._get_connection_for_uri(uri)
        return object_store.load_file_as_json(uri, connection)

    def load_file_as_text(self, uri: str) -> str:
        """
        Load a file from any supported object store as text.

        Args:
            uri: The URI of the file to load

        Returns:
            str: The loaded text content
        """
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if not object_store:
            raise ValueError(f"Unsupported URI format: {uri}")

        connection = self._get_connection_for_uri(uri)
        return object_store.load_file_as_text(uri, connection)

    def load_file_as_bytes(self, uri: str) -> bytes:
        """
        Load a file from any supported object store as bytes.

        Args:
            uri: The URI of the file to load

        Returns:
            bytes: The loaded file content
        """
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if not object_store:
            raise ValueError(f"Unsupported URI format: {uri}")

        connection = self._get_connection_for_uri(uri)
        return object_store.load_file_as_bytes(uri, connection)

    # Static methods - these are the main entry points for data lake operations
    @staticmethod
    def get_object_store_for_uri(uri: str) -> Optional[Type[ObjectStoreInterface]]:
        """
        Get the appropriate object store implementation for the given URI.

        Args:
            uri: The URI to get the object store for

        Returns:
            The object store implementation, or None if no matching implementation is found
        """
        for object_store in ObjectStoreClient._OBJECT_STORE_REGISTRY.values():
            if object_store.is_uri(uri):
                return object_store
        return None

    @staticmethod
    def get_bucket_name(uri: str) -> str:
        """
        Get the bucket/container name from any supported object store URI.

        This function acts as a central dispatcher that identifies the appropriate
        object store implementation and uses it to extract the bucket name.

        Args:
            uri: The URI to get the bucket name from

        Returns:
            The bucket/container name

        Raises:
            ValueError: If the URI is not supported by any registered object store
        """
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if object_store:
            return object_store.get_bucket_name(uri)

        raise ValueError(f"Unsupported URI format: {uri}")

    @staticmethod
    def get_object_key(uri: str) -> str:
        """
        Get the object key/path from any supported object store URI.

        Args:
            uri: The URI to get the object key from

        Returns:
            The object key/path

        Raises:
            ValueError: If the URI is not supported by any registered object store
        """
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)
        if object_store:
            return object_store.get_object_key(uri)

        raise ValueError(f"Unsupported URI format: {uri}")

    # Instance methods that use connections
    def get_bucket_name_with_connection(self, uri: str) -> str:
        """
        Get the bucket/container name from any supported URI (instance method).

        This is an instance method that can validate connections are available.
        For most use cases, use the static get_bucket_name() method instead.

        Args:
            uri: The URI to get the bucket name from

        Returns:
            str: The bucket/container name
        """
        # Validate connection is available (but don't require it for bucket name extraction)
        self._get_connection_for_uri(uri)  # This will raise if connection is missing
        return ObjectStoreClient.get_bucket_name(uri)

    def get_object_key_with_connection(self, uri: str) -> str:
        """
        Get the object key/path from any supported URI (instance method).

        This is an instance method that can validate connections are available.
        For most use cases, use the static get_object_key() method instead.

        Args:
            uri: The URI to get the object key from

        Returns:
            str: The object key/path
        """
        # Validate connection is available (but don't require it for key extraction)
        self._get_connection_for_uri(uri)  # This will raise if connection is missing
        return ObjectStoreClient.get_object_key(uri)

    def is_supported_uri(self, uri: str) -> bool:
        """
        Check if a URI is supported by any object store.

        Args:
            uri: The URI to check

        Returns:
            bool: True if the URI is supported, False otherwise
        """
        return ObjectStoreClient.get_object_store_for_uri(uri) is not None

    def get_platform_for_uri(self, uri: str) -> Optional[str]:
        """
        Get the platform name for a URI.

        Args:
            uri: The URI to get the platform for

        Returns:
            Optional[str]: The platform name ('s3', 'gcs', 'abs') or None if unsupported
        """
        object_store = ObjectStoreClient.get_object_store_for_uri(uri)

        if object_store == S3ObjectStore:
            return "s3"
        elif object_store == GCSObjectStore:
            return "gcs"
        elif object_store == ABSObjectStore:
            return "abs"
        else:
            return None

    # Future methods can be added here and will automatically work with all providers
    # Example:
    # def list_objects(self, uri: str, prefix: str = "") -> List[str]:
    #     """List objects in a bucket/container with optional prefix."""
    #     object_store = ObjectStoreClient.get_object_store_for_uri(uri)
    #     if not object_store:
    #         raise ValueError(f"Unsupported URI format: {uri}")
    #
    #     connection = self._get_connection_for_uri(uri)
    #     return object_store.list_objects(uri, connection, prefix)
    #
    # def delete_object(self, uri: str) -> None:
    #     """Delete an object from any supported object store."""
    #     object_store = ObjectStoreClient.get_object_store_for_uri(uri)
    #     if not object_store:
    #         raise ValueError(f"Unsupported URI format: {uri}")
    #
    #     connection = self._get_connection_for_uri(uri)
    #     object_store.delete_object(uri, connection)
