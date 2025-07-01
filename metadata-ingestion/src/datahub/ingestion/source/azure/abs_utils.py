import os
import re
from typing import Optional

# This file should not import any abs spectific modules as we import it in path_spec.py in datat_lake_common.py

ABS_PREFIXES_REGEX = re.compile(
    r"(http[s]?://[a-z0-9]{3,24}\.blob\.core\.windows\.net/)"
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
