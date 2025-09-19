import json
import re
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, TypeVar
from urllib.parse import unquote

# Don't import TableData at the module level to avoid circular imports
# from datahub.ingestion.source.s3.source import TableData
# Don't import ObjectStoreClient at the module level to avoid circular imports
# from datahub.ingestion.source.data_lake_common.object_store_client import ObjectStoreClient

T = TypeVar("T")


class ObjectStoreInterface(ABC):
    """Abstract interface for object store operations."""

    @classmethod
    @abstractmethod
    def is_uri(cls, uri: str) -> bool:
        """Check if the given URI is for this object store."""
        pass

    @classmethod
    @abstractmethod
    def get_prefix(cls, uri: str) -> Optional[str]:
        """Get the prefix for this object store URI (e.g., 's3://', 'gs://')."""
        pass

    @classmethod
    @abstractmethod
    def strip_prefix(cls, uri: str) -> str:
        """Remove the object store prefix from the URI."""
        pass

    @classmethod
    @abstractmethod
    def get_bucket_name(cls, uri: str) -> str:
        """Get the bucket name from the URI."""
        pass

    @classmethod
    @abstractmethod
    def get_object_key(cls, uri: str) -> str:
        """Get the object key/path (excluding the bucket) from the URI."""
        pass

    @classmethod
    @abstractmethod
    def load_file_as_json(cls, uri: str, connection_config: Any) -> Dict[str, Any]:
        """Load a file from the object store as JSON."""
        pass

    @classmethod
    @abstractmethod
    def load_file_as_text(cls, uri: str, connection_config: Any) -> str:
        """Load a file from the object store as text."""
        pass

    @classmethod
    @abstractmethod
    def load_file_as_bytes(cls, uri: str, connection_config: Any) -> bytes:
        """Load a file from the object store as bytes."""
        pass


class S3ObjectStore(ObjectStoreInterface):
    """Implementation of ObjectStoreInterface for Amazon S3."""

    PREFIXES = ["s3://", "s3n://", "s3a://"]

    @classmethod
    def is_uri(cls, uri: str) -> bool:
        return any(uri.startswith(prefix) for prefix in cls.PREFIXES)

    @classmethod
    def get_prefix(cls, uri: str) -> Optional[str]:
        for prefix in cls.PREFIXES:
            if uri.startswith(prefix):
                return prefix
        return None

    @classmethod
    def strip_prefix(cls, uri: str) -> str:
        prefix = cls.get_prefix(uri)
        if not prefix:
            raise ValueError(
                f"Not an S3 URI. Must start with one of the following prefixes: {str(cls.PREFIXES)}"
            )
        return uri[len(prefix) :]

    @classmethod
    def get_bucket_name(cls, uri: str) -> str:
        if not cls.is_uri(uri):
            raise ValueError(
                f"Not an S3 URI. Must start with one of the following prefixes: {str(cls.PREFIXES)}"
            )
        return cls.strip_prefix(uri).split("/")[0]

    @classmethod
    def get_object_key(cls, uri: str) -> str:
        if not cls.is_uri(uri):
            raise ValueError(
                f"Not an S3 URI. Must start with one of the following prefixes: {str(cls.PREFIXES)}"
            )
        parts = cls.strip_prefix(uri).split("/", 1)
        if len(parts) < 2:
            return ""
        return parts[1]

    @classmethod
    def load_file_as_json(cls, uri: str, connection_config: Any) -> Dict[str, Any]:
        s3_client = connection_config.get_s3_client()
        bucket_name = cls.get_bucket_name(uri)
        key = cls.get_object_key(uri)

        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return json.loads(response["Body"].read())

    @classmethod
    def load_file_as_text(cls, uri: str, connection_config: Any) -> str:
        s3_client = connection_config.get_s3_client()
        bucket_name = cls.get_bucket_name(uri)
        key = cls.get_object_key(uri)

        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return response["Body"].read().decode("utf-8")

    @classmethod
    def load_file_as_bytes(cls, uri: str, connection_config: Any) -> bytes:
        s3_client = connection_config.get_s3_client()
        bucket_name = cls.get_bucket_name(uri)
        key = cls.get_object_key(uri)

        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return response["Body"].read()


class GCSObjectStore(ObjectStoreInterface):
    """Implementation of ObjectStoreInterface for Google Cloud Storage."""

    PREFIX = "gs://"

    @classmethod
    def is_uri(cls, uri: str) -> bool:
        return uri.startswith(cls.PREFIX)

    @classmethod
    def get_prefix(cls, uri: str) -> Optional[str]:
        if uri.startswith(cls.PREFIX):
            return cls.PREFIX
        return None

    @classmethod
    def strip_prefix(cls, uri: str) -> str:
        prefix = cls.get_prefix(uri)
        if not prefix:
            raise ValueError(f"Not a GCS URI. Must start with prefix: {cls.PREFIX}")
        return uri[len(prefix) :]

    @classmethod
    def get_bucket_name(cls, uri: str) -> str:
        if not cls.is_uri(uri):
            raise ValueError(f"Not a GCS URI. Must start with prefix: {cls.PREFIX}")
        return cls.strip_prefix(uri).split("/")[0]

    @classmethod
    def get_object_key(cls, uri: str) -> str:
        if not cls.is_uri(uri):
            raise ValueError(f"Not a GCS URI. Must start with prefix: {cls.PREFIX}")
        parts = cls.strip_prefix(uri).split("/", 1)
        if len(parts) < 2:
            return ""
        return parts[1]

    @classmethod
    def load_file_as_json(cls, uri: str, connection_config: Any) -> Dict[str, Any]:
        from google.cloud import storage

        client = storage.Client.from_service_account_info(connection_config.to_dict())
        bucket_name = cls.get_bucket_name(uri)
        blob_name = cls.get_object_key(uri)

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return json.loads(blob.download_as_text())

    @classmethod
    def load_file_as_text(cls, uri: str, connection_config: Any) -> str:
        from google.cloud import storage

        client = storage.Client.from_service_account_info(connection_config.to_dict())
        bucket_name = cls.get_bucket_name(uri)
        blob_name = cls.get_object_key(uri)

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text()

    @classmethod
    def load_file_as_bytes(cls, uri: str, connection_config: Any) -> bytes:
        from google.cloud import storage

        client = storage.Client.from_service_account_info(connection_config.to_dict())
        bucket_name = cls.get_bucket_name(uri)
        blob_name = cls.get_object_key(uri)

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_bytes()


class ABSObjectStore(ObjectStoreInterface):
    """Implementation of ObjectStoreInterface for Azure Blob Storage."""

    PREFIX = "abfss://"
    HTTPS_REGEX = re.compile(r"(https?://[a-z0-9]{3,24}\.blob\.core\.windows\.net/)")

    @classmethod
    def is_uri(cls, uri: str) -> bool:
        return uri.startswith(cls.PREFIX) or bool(cls.HTTPS_REGEX.match(uri))

    @classmethod
    def get_prefix(cls, uri: str) -> Optional[str]:
        if uri.startswith(cls.PREFIX):
            return cls.PREFIX

        # Check for HTTPS format
        match = cls.HTTPS_REGEX.match(uri)
        if match:
            return match.group(1)

        return None

    @classmethod
    def strip_prefix(cls, uri: str) -> str:
        if uri.startswith(cls.PREFIX):
            return uri[len(cls.PREFIX) :]

        # Handle HTTPS format
        match = cls.HTTPS_REGEX.match(uri)
        if match:
            return uri[len(match.group(1)) :]

        raise ValueError(
            f"Not an ABS URI. Must start with prefix: {cls.PREFIX} or match Azure Blob Storage HTTPS pattern"
        )

    @classmethod
    def get_bucket_name(cls, uri: str) -> str:
        if not cls.is_uri(uri):
            raise ValueError(
                f"Not an ABS URI. Must start with prefix: {cls.PREFIX} or match Azure Blob Storage HTTPS pattern"
            )

        if uri.startswith(cls.PREFIX):
            # abfss://container@account.dfs.core.windows.net/path
            return cls.strip_prefix(uri).split("@")[0]
        else:
            # https://account.blob.core.windows.net/container/path
            return cls.strip_prefix(uri).split("/")[0]

    @classmethod
    def get_object_key(cls, uri: str) -> str:
        if not cls.is_uri(uri):
            raise ValueError(
                f"Not an ABS URI. Must start with prefix: {cls.PREFIX} or match Azure Blob Storage HTTPS pattern"
            )

        if uri.startswith(cls.PREFIX):
            # abfss://container@account.dfs.core.windows.net/path
            parts = cls.strip_prefix(uri).split("@", 1)
            if len(parts) < 2:
                return ""
            account_path = parts[1]
            path_parts = account_path.split("/", 1)
            if len(path_parts) < 2:
                return ""
            return path_parts[1]
        else:
            # https://account.blob.core.windows.net/container/path
            stripped = cls.strip_prefix(uri)
            parts = stripped.split("/", 1)
            if len(parts) < 2:
                return ""
            return parts[1]

    @classmethod
    def load_file_as_json(cls, uri: str, connection_config: Any) -> Dict[str, Any]:
        blob_service_client = connection_config.get_blob_service_client()
        container_name = cls.get_bucket_name(uri)
        blob_name = cls.get_object_key(uri)

        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        return json.loads(blob_client.download_blob().readall())

    @classmethod
    def load_file_as_text(cls, uri: str, connection_config: Any) -> str:
        blob_service_client = connection_config.get_blob_service_client()
        container_name = cls.get_bucket_name(uri)
        blob_name = cls.get_object_key(uri)

        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        return blob_client.download_blob().readall().decode("utf-8")

    @classmethod
    def load_file_as_bytes(cls, uri: str, connection_config: Any) -> bytes:
        blob_service_client = connection_config.get_blob_service_client()
        container_name = cls.get_bucket_name(uri)
        blob_name = cls.get_object_key(uri)

        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        return blob_client.download_blob().readall()


class ObjectStoreSourceAdapter:
    """Adapter for customizing object store source implementations."""

    @staticmethod
    def create_s3_path(bucket_name: str, key: str) -> str:
        return unquote(f"s3://{bucket_name}/{key}")

    @staticmethod
    def create_gcs_path(bucket_name: str, key: str) -> str:
        return unquote(f"gs://{bucket_name}/{key}")

    @staticmethod
    def create_abs_path(container_name: str, key: str, account_name: str) -> str:
        return unquote(
            f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{key}"
        )

    @staticmethod
    def get_s3_external_url(
        table_data: Any, region: Optional[str] = None
    ) -> Optional[str]:
        """Get the AWS S3 console URL for the given table."""
        if not S3ObjectStore.is_uri(table_data.table_path):
            return None

        from datahub.ingestion.source.data_lake_common.object_store_client import (
            ObjectStoreClient,
        )

        bucket_name = ObjectStoreClient.get_bucket_name(table_data.table_path)
        key = ObjectStoreClient.get_object_key(table_data.table_path)

        # Use the provided region or default to us-east-1
        aws_region = region or "us-east-1"

        return f"https://{aws_region}.console.aws.amazon.com/s3/buckets/{bucket_name}?prefix={key}"

    @staticmethod
    def get_gcs_external_url(table_data: Any) -> Optional[str]:
        """Get the GCS console URL for the given table."""
        if not GCSObjectStore.is_uri(table_data.table_path):
            return None

        from datahub.ingestion.source.data_lake_common.object_store_client import (
            ObjectStoreClient,
        )

        bucket_name = ObjectStoreClient.get_bucket_name(table_data.table_path)
        key = ObjectStoreClient.get_object_key(table_data.table_path)

        return f"https://console.cloud.google.com/storage/browser/{bucket_name}/{key}"

    @staticmethod
    def get_abs_external_url(table_data: Any) -> Optional[str]:
        """Get the Azure Storage browser URL for the given table."""
        if not ABSObjectStore.is_uri(table_data.table_path):
            return None

        try:
            if table_data.table_path.startswith("abfss://"):
                path_without_prefix = ABSObjectStore.strip_prefix(table_data.table_path)
                parts = path_without_prefix.split("@", 1)
                if len(parts) < 2:
                    return None

                container_name = parts[0]
                account_parts = parts[1].split("/", 1)
                account_domain = account_parts[0]
                account_name = account_domain.split(".")[0]
            else:
                container_name = ABSObjectStore.get_bucket_name(table_data.table_path)
                if "blob.core.windows.net" in table_data.table_path:
                    account_name = table_data.table_path.split("//")[1].split(".")[0]
                else:
                    return None

            return f"https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/{account_name}/containerName/{container_name}"
        except Exception:
            return None

    def __init__(
        self,
        platform: str,
        platform_name: str,
        aws_region: Optional[str] = None,
        azure_storage_account: Optional[str] = None,
    ):
        self.platform = platform
        self.platform_name = platform_name
        self.aws_region = aws_region
        self.azure_storage_account = azure_storage_account
        self.customizations: Dict[str, Callable[..., Any]] = {}

        if platform == "gcs":
            self.register_customization("is_s3_platform", lambda: True)
            self.register_customization("create_s3_path", self.create_gcs_path)
            self.register_customization(
                "get_external_url",
                lambda table_data: self.get_gcs_external_url(table_data),
            )

            self.register_customization(
                "_normalize_uri_for_pattern_matching",
                self._normalize_gcs_uri_for_pattern_matching,
            )

            self.register_customization("strip_s3_prefix", self._strip_gcs_prefix)
        elif platform == "s3":
            self.register_customization("is_s3_platform", lambda: True)
            self.register_customization("create_s3_path", self.create_s3_path)
            self.register_customization(
                "get_external_url",
                lambda table_data: self.get_s3_external_url(
                    table_data, self.aws_region
                ),
            )
        elif platform == "abs":
            self.register_customization("is_s3_platform", lambda: True)

            if self.azure_storage_account:
                storage_account = self.azure_storage_account
                self.register_customization(
                    "create_s3_path",
                    lambda bucket, key: self.create_abs_path(
                        bucket, key, storage_account
                    ),
                )
            else:
                self.register_customization(
                    "create_s3_path", lambda bucket, key: f"abfss://{bucket}@{key}"
                )
            self.register_customization("get_external_url", self.get_abs_external_url)

    def register_customization(
        self, method_name: str, implementation: Callable[..., Any]
    ) -> None:
        self.customizations[method_name] = implementation

    def apply_customizations(self, source: Any) -> Any:
        if hasattr(source, "source_config") and hasattr(
            source.source_config, "platform"
        ):
            source.source_config.platform = self.platform

        for method_name, implementation in self.customizations.items():
            if (
                hasattr(implementation, "__self__")
                and implementation.__self__ is not None
            ):
                setattr(source, method_name, implementation.__get__(source))
            else:
                setattr(source, method_name, implementation)

        return source

    def get_external_url(self, table_data: Any) -> Optional[str]:
        """Get the external URL for a table based on the platform type."""
        if self.platform == "s3":
            return self.get_s3_external_url(table_data, self.aws_region)
        elif self.platform == "gcs":
            return self.get_gcs_external_url(table_data)
        elif self.platform == "abs":
            return self.get_abs_external_url(table_data)
        return None

    def _normalize_gcs_uri_for_pattern_matching(self, uri: str) -> str:
        if uri.startswith("gs://"):
            return uri.replace("gs://", "s3://", 1)
        return uri

    def _strip_gcs_prefix(self, uri: str) -> str:
        if uri.startswith("gs://"):
            return uri[5:]  # Remove "gs://" prefix
        return uri


def create_object_store_adapter(
    platform: str,
    aws_region: Optional[str] = None,
    azure_storage_account: Optional[str] = None,
) -> ObjectStoreSourceAdapter:
    platform_names = {
        "s3": "Amazon S3",
        "gcs": "Google Cloud Storage",
        "abs": "Azure Blob Storage",
    }

    return ObjectStoreSourceAdapter(
        platform=platform,
        platform_name=platform_names.get(platform, f"Unknown ({platform})"),
        aws_region=aws_region,
        azure_storage_account=azure_storage_account,
    )
