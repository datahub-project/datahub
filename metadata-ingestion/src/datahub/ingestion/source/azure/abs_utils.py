import os
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

# This file should not import any abs spectific modules as we import it in path_spec.py in datat_lake_common.py

ABS_PREFIXES_REGEX = re.compile(
    r"(http[s]?://[a-z0-9]{3,24}\.blob\.core\.windows\.net/)"
)
AZURE_FILESYSTEM_SCHEMES = ("abfs", "abfss")
AZURE_CONTAINER_SCHEMES = ("az", "adl")
AZURE_URI_SCHEMES = (*AZURE_FILESYSTEM_SCHEMES, *AZURE_CONTAINER_SCHEMES)
AZURE_HTTP_HOST_SUFFIXES = (".blob.core.windows.net", ".dfs.core.windows.net")
AZURE_SUPPORTED_FORMATS_HINT = "abfss://, abfs://, az://, adl://, and Azure HTTPS"


@dataclass(frozen=True)
class AzureBlobPath:
    account_name: str
    container_name: str
    object_path: str


def is_azure_http_netloc(netloc: str) -> bool:
    lowered = netloc.lower()
    return any(lowered.endswith(suffix) for suffix in AZURE_HTTP_HOST_SUFFIXES)


def is_azure_path(path: str) -> bool:
    parsed = urlparse(path or "")
    scheme = parsed.scheme.lower()
    return scheme in AZURE_URI_SCHEMES or (
        scheme in {"http", "https"} and is_azure_http_netloc(parsed.netloc)
    )


def parse_azure_path(path: str, account_name: Optional[str] = None) -> AzureBlobPath:
    parsed = urlparse(path)
    scheme = parsed.scheme.lower()

    if scheme in AZURE_FILESYSTEM_SCHEMES:
        if "@" not in parsed.netloc:
            raise ValueError(
                f"Unsupported Azure path format: {path}. Supported formats: {AZURE_SUPPORTED_FORMATS_HINT}."
            )
        container_name, account_domain = parsed.netloc.split("@", 1)
        return AzureBlobPath(
            account_name=account_domain.split(".")[0],
            container_name=container_name,
            object_path=parsed.path.lstrip("/"),
        )

    if scheme in AZURE_CONTAINER_SCHEMES:
        if not account_name:
            raise ValueError(
                "For `az://` and `adl://` paths, set `source.config.azure.account_name`."
            )
        return AzureBlobPath(
            account_name=account_name,
            container_name=parsed.netloc,
            object_path=parsed.path.lstrip("/"),
        )

    if scheme in {"http", "https"} and is_azure_http_netloc(parsed.netloc):
        stripped_path = parsed.path.lstrip("/")
        parts = stripped_path.split("/", 1)
        return AzureBlobPath(
            account_name=parsed.netloc.split(".")[0],
            container_name=parts[0],
            object_path=parts[1] if len(parts) > 1 else "",
        )

    raise ValueError(
        f"Unsupported Azure path format: {path}. Supported formats: {AZURE_SUPPORTED_FORMATS_HINT}."
    )


def to_blob_https_uri(path: str, account_name: Optional[str] = None) -> str:
    parsed = urlparse(path)
    if parsed.scheme in {"http", "https"} and is_azure_http_netloc(parsed.netloc):
        blob_netloc = parsed.netloc.replace(
            ".dfs.core.windows.net", ".blob.core.windows.net"
        )
        return f"{parsed.scheme}://{blob_netloc}{parsed.path}"

    azure_path = parse_azure_path(path, account_name=account_name)
    if azure_path.object_path:
        return (
            f"https://{azure_path.account_name}.blob.core.windows.net/"
            f"{azure_path.container_name}/{azure_path.object_path}"
        )
    return (
        f"https://{azure_path.account_name}.blob.core.windows.net/"
        f"{azure_path.container_name}"
    )


def is_abs_uri(uri: str) -> bool:
    return bool(ABS_PREFIXES_REGEX.match(uri))


def get_abs_prefix(abs_uri: str) -> Optional[str]:
    result = re.search(ABS_PREFIXES_REGEX, abs_uri)
    if result and result.groups():
        return result.group(1)
    return None


def strip_abs_prefix(abs_uri: str) -> str:
    # remove abs prefix https://<storage-account>.blob.core.windows.net
    abs_prefix = get_abs_prefix(abs_uri)
    if not abs_prefix:
        raise ValueError(
            f"Not an Azure Blob Storage URI. Must match the following regular expression: {str(ABS_PREFIXES_REGEX)}"
        )
    length_abs_prefix = len(abs_prefix)
    return abs_uri[length_abs_prefix:]


def make_abs_urn(abs_uri: str, env: str) -> str:
    abs_name = strip_abs_prefix(abs_uri)

    if abs_name.endswith("/"):
        abs_name = abs_name[:-1]

    name, extension = os.path.splitext(abs_name)

    if extension != "":
        extension = extension[1:]  # remove the dot
        return f"urn:li:dataset:(urn:li:dataPlatform:abs,{name}_{extension},{env})"

    return f"urn:li:dataset:(urn:li:dataPlatform:abs,{abs_name},{env})"


def get_container_name(abs_uri: str) -> str:
    if not is_abs_uri(abs_uri):
        raise ValueError(
            f"Not an Azure Blob Storage URI. Must match the following regular expression: {str(ABS_PREFIXES_REGEX)}"
        )
    return strip_abs_prefix(abs_uri).split("/")[0]


def get_key_prefix(abs_uri: str) -> str:
    if not is_abs_uri(abs_uri):
        raise ValueError(
            f"Not an Azure Blob Storage URI. Must match the following regular expression: {str(ABS_PREFIXES_REGEX)}"
        )
    return strip_abs_prefix(abs_uri).split("/", maxsplit=1)[1]


def get_container_relative_path(abs_uri: str) -> str:
    return "/".join(strip_abs_prefix(abs_uri).split("/")[1:])
