import base64
import hashlib
import posixpath
import re
from typing import Dict, List, Optional

from pydantic import BaseModel

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.azure.abs_utils import is_azure_path, parse_azure_path
from datahub.ingestion.source.data_lake_common.object_store import (
    GCSObjectStore,
    S3ObjectStore,
)
from datahub.utilities.str_enum import StrEnum

# Qlik Cloud names a file a data connection reads as
# qri:file:<scheme>://<connection>#<path>#<item>, and its lineage graph hides
# each part behind the base64url SHA-256 of its text (a secure QRI).
FILE_QRI_RE = re.compile(
    r"^qri:file:(?P<scheme>[a-z0-9-]+)://(?P<connection>[^#]+)#(?P<path>[^#]+)"
)
CONNECT_PROPERTY_RE = re.compile(r"(?P<key>[A-Za-z][\w.]*)=(?P<value>[^;\"]*)")


class FilePlatform(StrEnum):
    ABS = "abs"
    S3 = "s3"
    GCS = "gcs"
    # Files on a Windows share, which Qlik Sense Enterprise ingests as Qlik
    # Sense datasets named by their path.
    WINDOWS_SHARE = "qlik-sense"


class FileConnector(BaseModel):
    """A Qlik file connector, as `GET /api/v1/data-sources` describes it: the
    DataHub platform its files are on, and the connection properties its
    `qriDefinition` builds the connection part of a file's QRI from."""

    platform: FilePlatform
    qri_scheme: str
    connection_properties: List[str]
    # The connection property that names the bucket or container, with any
    # folder inside it, which a file's DataHub name starts with.
    location_property: str


FILE_CONNECTORS: Dict[str, FileConnector] = {
    "File_AzureStorageConnector": FileConnector(
        platform=FilePlatform.ABS,
        qri_scheme="adls",
        connection_properties=["blobContainerPath"],
        location_property="blobContainerPath",
    ),
    "File_AmazonS3Connector": FileConnector(
        platform=FilePlatform.S3,
        qri_scheme="s3",
        connection_properties=["region", "bucketName"],
        location_property="bucketName",
    ),
    "File_AmazonS3ConnectorV2": FileConnector(
        platform=FilePlatform.S3,
        qri_scheme="s3",
        connection_properties=["region", "bucketName"],
        location_property="bucketName",
    ),
    "File_GoogleCloudStorageConnector": FileConnector(
        platform=FilePlatform.GCS,
        qri_scheme="gcs",
        connection_properties=["bucketName"],
        location_property="bucketName",
    ),
    # A Data Gateway file connection, which reads a Windows share.
    "DG_File_local": FileConnector(
        platform=FilePlatform.WINDOWS_SHARE,
        qri_scheme="local",
        connection_properties=["path"],
        location_property="path",
    ),
}


# The DataHub platform of the files each file QRI scheme names.
PLATFORM_BY_QRI_SCHEME = {
    connector.qri_scheme: connector.platform for connector in FILE_CONNECTORS.values()
}


class FileReference(BaseModel):
    connection: str
    path: str
    qvd: bool = False


class FileCandidates(BaseModel):
    """The script's references to the file a secure QRI names, with the
    platform and the id of the connection part the QRI gives."""

    references: List[FileReference] = []
    platform: Optional[FilePlatform] = None
    connection_id: str = ""


class FileStorage(BaseModel):
    """Where a file data connection's files are: a DataHub platform and the
    bucket or container, with any folder inside it, file paths are under."""

    platform: FilePlatform
    location: str
    # The id of the connection part of its files' secure QRIs, when the
    # connection's properties are known.
    qri_connection_id: Optional[str] = None
    # That id as a connection holding exactly this location would have it,
    # when only a storage URI is known. A match only tells candidates apart,
    # as the connection's own property may be spelled differently.
    likely_qri_connection_id: Optional[str] = None


def canonical_path(path: str) -> str:
    """One spelling per file: forward slashes, no `.` or `..`, lower case, as
    Windows paths are case-insensitive. `//server/share` keeps its UNC prefix."""
    return posixpath.normpath(path.strip().replace("\\", "/")).lower()


def qri_part_id(text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def connect_properties(connect_statement: str) -> Dict[str, str]:
    return {
        match.group("key"): match.group("value").strip()
        for match in CONNECT_PROPERTY_RE.finditer(connect_statement)
    }


def file_connector_platform(connect_statement: str) -> Optional[FilePlatform]:
    """The DataHub platform of the files a data connection reads, or None
    when it is not an Azure Storage, Amazon S3, Google Cloud Storage or Data
    Gateway file one."""
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
    if connector.platform == FilePlatform.WINDOWS_SHARE:
        storage = file_storage_of_uri(properties[connector.location_property])
        if storage is None:
            return None
        storage.qri_connection_id = qri_part_id("/".join(values))
        return storage
    # Qlik Cloud saves an Azure container as an HTTPS URL, but DataHub names
    # files on storage from the container or bucket on.
    if "://" in location:
        storage = file_storage_of_uri(location)
        if storage is None or storage.platform != connector.platform:
            return None
        location = storage.location
    return FileStorage(
        platform=connector.platform,
        location=location,
        qri_connection_id=qri_part_id("/".join(values)),
    )


def file_storage_of_uri(uri: str) -> Optional[FileStorage]:
    """The storage an Azure (`abfss://`, `abfs://` or Azure HTTPS), `s3://` or
    `gs://` URI, or a `\\\\server\\share` path, names."""
    uri = uri.strip()
    if uri.startswith(("\\\\", "//")) and "://" not in uri:
        location = canonical_path(uri)
        # `//` alone, or a server without a share, is no share.
        if len([part for part in location.split("/") if part]) < 2:
            return None
        return FileStorage(
            platform=FilePlatform.WINDOWS_SHARE,
            location=location,
            # A Data Gateway connection's QRI part is its path as written.
            likely_qri_connection_id=qri_part_id(uri),
        )
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
            platform=FilePlatform.ABS,
            location=location,
            # Qlik Cloud saves an Azure connection's container as its blob
            # HTTPS URL, which its files' QRIs are built from.
            likely_qri_connection_id=qri_part_id(
                f"https://{azure.account_name}.blob.core.windows.net/{location}"
            ),
        )
    for platform, store in (
        (FilePlatform.S3, S3ObjectStore),
        (FilePlatform.GCS, GCSObjectStore),
    ):
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
                    if platform == FilePlatform.GCS
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
                    FileReference(
                        connection=reference.connection, path=path, qvd=reference.qvd
                    )
                )

    def references(self) -> List[FileReference]:
        return [ref for refs in self._by_path_id.values() for ref in refs]

    def candidates(self, file_qri: str) -> FileCandidates:
        match = FILE_QRI_RE.match(file_qri)
        if not match:
            return FileCandidates()
        references = self._by_path_id.get(match.group("path"), [])
        # Several reads of the same file are one reference.
        unique = {(ref.connection, ref.path, ref.qvd): ref for ref in references}
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
    if storage.platform == FilePlatform.WINDOWS_SHARE:
        # As Qlik Sense Enterprise names a file on a share, so both join.
        return make_dataset_urn_with_platform_instance(
            platform=storage.platform,
            name=canonical_path(f"{storage.location}/{path}"),
            platform_instance=platform_instance,
            env=env,
        )
    name = f"{storage.location}/{path}"
    return make_dataset_urn_with_platform_instance(
        platform=storage.platform,
        name=name.lower() if lowercase else name,
        platform_instance=platform_instance,
        env=env,
    )
