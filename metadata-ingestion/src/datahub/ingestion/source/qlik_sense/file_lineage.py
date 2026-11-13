import base64
import hashlib
import re
from typing import Dict, List, Optional

from pydantic import BaseModel

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.azure.abs_utils import is_azure_path, parse_azure_path
from datahub.ingestion.source.data_lake_common.object_store import (
    GCSObjectStore,
    S3ObjectStore,
)

# Qlik Cloud names a file a data connection reads as
# qri:file:<scheme>://<connection>#<path>#<item>, and its lineage graph hides
# each part behind the base64url SHA-256 of its text (a secure QRI).
FILE_QRI_RE = re.compile(
    r"^qri:file:(?P<scheme>[a-z0-9-]+)://(?P<connection>[^#]+)#(?P<path>[^#]+)"
)
CONNECT_PROPERTY_RE = re.compile(r"(?P<key>[A-Za-z][\w.]*)=(?P<value>[^;\"]*)")


class FileConnector(BaseModel):
    """A Qlik file connector, as `GET /api/v1/data-sources` describes it: the
    DataHub platform its files are on, and the connection properties its
    `qriDefinition` builds the connection part of a file's QRI from."""

    platform: str
    qri_scheme: str
    connection_properties: List[str]
    # The connection property that names the bucket or container, with any
    # folder inside it, which a file's DataHub name starts with.
    location_property: str


FILE_CONNECTORS: Dict[str, FileConnector] = {
    "File_AzureStorageConnector": FileConnector(
        platform="abs",
        qri_scheme="adls",
        connection_properties=["blobContainerPath"],
        location_property="blobContainerPath",
    ),
    "File_AmazonS3Connector": FileConnector(
        platform="s3",
        qri_scheme="s3",
        connection_properties=["region", "bucketName"],
        location_property="bucketName",
    ),
    "File_AmazonS3ConnectorV2": FileConnector(
        platform="s3",
        qri_scheme="s3",
        connection_properties=["region", "bucketName"],
        location_property="bucketName",
    ),
    "File_GoogleCloudStorageConnector": FileConnector(
        platform="gcs",
        qri_scheme="gcs",
        connection_properties=["bucketName"],
        location_property="bucketName",
    ),
}


# The DataHub platform of the files each file QRI scheme names.
PLATFORM_BY_QRI_SCHEME = {
    connector.qri_scheme: connector.platform for connector in FILE_CONNECTORS.values()
}


class FileReference(BaseModel):
    connection: str
    path: str


class FileCandidates(BaseModel):
    """The script's references to the file a secure QRI names, with the
    platform and the id of the connection part the QRI gives."""

    references: List[FileReference] = []
    platform: Optional[str] = None
    connection_id: str = ""


class FileStorage(BaseModel):
    """Where a file data connection's files are: a DataHub platform and the
    bucket or container, with any folder inside it, file paths are under."""

    platform: str
    location: str
    # The id of the connection part of its files' secure QRIs, when the
    # connection's properties are known.
    qri_connection_id: Optional[str] = None
    # That id as a connection holding exactly this location would have it,
    # when only a storage URI is known. A match only tells candidates apart,
    # as the connection's own property may be spelled differently.
    likely_qri_connection_id: Optional[str] = None


def qri_part_id(text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def connect_properties(connect_statement: str) -> Dict[str, str]:
    return {
        match.group("key"): match.group("value").strip()
        for match in CONNECT_PROPERTY_RE.finditer(connect_statement)
    }


def file_connector_platform(connect_statement: str) -> Optional[str]:
    """The DataHub platform of the files a data connection reads, or None
    when it is not an Azure Storage, Amazon S3 or Google Cloud Storage one."""
    connector = FILE_CONNECTORS.get(
        connect_properties(connect_statement).get("sourceType", "")
    )
    return connector.platform if connector else None


def file_storage_of_connection(connect_statement: str) -> Optional[FileStorage]:
    """The storage a Qlik Cloud file data connection reads, from its connect
    statement, or None when it is not a known file connector or lacks the
    properties its files' QRIs are built from."""
    properties = connect_properties(connect_statement)
    connector = FILE_CONNECTORS.get(properties.get("sourceType", ""))
    if connector is None:
        return None
    values = [properties.get(name, "") for name in connector.connection_properties]
    location = properties.get(connector.location_property, "").strip("/")
    if not all(values) or not location:
        return None
    return FileStorage(
        platform=connector.platform,
        location=location,
        qri_connection_id=qri_part_id("/".join(values)),
    )


def file_storage_of_uri(uri: str) -> Optional[FileStorage]:
    """The storage an Azure (`abfss://`, `abfs://` or Azure HTTPS), `s3://` or
    `gs://` URI names."""
    uri = uri.strip()
    scheme, separator, rest = uri.partition("://")
    if separator:
        # Schemes are case-insensitive; the helpers below expect lower case.
        uri = f"{scheme.lower()}://{rest}"
    if is_azure_path(uri):
        try:
            azure = parse_azure_path(uri)
        except ValueError:
            # az:// and adl:// need an account name, which the container
            # location does not include.
            return None
        if not azure.container_name:
            return None
        location = "/".join(
            filter(None, [azure.container_name, azure.object_path.strip("/")])
        )
        return FileStorage(
            platform="abs",
            location=location,
            likely_qri_connection_id=qri_part_id(location),
        )
    for platform, store in (("s3", S3ObjectStore), ("gcs", GCSObjectStore)):
        if store.is_uri(uri):
            bucket = store.get_bucket_name(uri)
            if bucket:
                return FileStorage(
                    platform=platform,
                    location="/".join(
                        filter(None, [bucket, store.get_object_key(uri).strip("/")])
                    ),
                    # An S3 connection's QRI part also holds its region.
                    likely_qri_connection_id=qri_part_id(bucket)
                    if platform == "gcs"
                    else None,
                )
    return None


def normalized_file_path(path: str) -> str:
    # Qlik builds a file's QRI from its path inside the data connection, with
    # no leading or doubled separators, which a path assembled from variables
    # such as `$(vFolder)/file.qvd` often has.
    return "/".join(part for part in path.replace("\\", "/").split("/") if part)


class FileSourceResolver:
    """Resolves the files in an app's lineage graph to datasets, from the
    `lib://` files its load script reads. A file is resolved only when the
    hash of a path the script reads is the path part of the file's QRI, and
    the connection the script reads it through maps to a storage whose
    connection part, when known, is the QRI's too."""

    def __init__(self, references: List[FileReference]) -> None:
        self._by_path_id: Dict[str, List[FileReference]] = {}
        for reference in references:
            path = normalized_file_path(reference.path)
            if path:
                self._by_path_id.setdefault(qri_part_id(path), []).append(
                    FileReference(connection=reference.connection, path=path)
                )

    def candidates(self, file_qri: str) -> FileCandidates:
        match = FILE_QRI_RE.match(file_qri)
        if not match:
            return FileCandidates()
        references = self._by_path_id.get(match.group("path"), [])
        # Several reads of the same file are one reference.
        unique = {(ref.connection, ref.path): ref for ref in references}
        return FileCandidates(
            references=list(unique.values()),
            platform=PLATFORM_BY_QRI_SCHEME.get(match.group("scheme")),
            connection_id=match.group("connection"),
        )


def file_dataset_urn(
    storage: FileStorage,
    path: str,
    platform_instance: Optional[str],
    env: str,
    lowercase: bool = False,
) -> str:
    name = f"{storage.location}/{path}"
    return make_dataset_urn_with_platform_instance(
        platform=storage.platform,
        name=name.lower() if lowercase else name,
        platform_instance=platform_instance,
        env=env,
    )
