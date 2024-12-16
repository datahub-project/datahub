import datetime
import hashlib
import random
import string
from urllib.parse import urlparse

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    RemoteExecutorKeyClass,
    RemoteExecutorStatusClass,
)

from datahub_executor.common.constants import DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME


def get_hostname_from_url(url: str) -> str:
    if "//" not in url:
        url = f"http://{url}"

    hostname = urlparse(url).hostname
    if hostname is None:
        return "undefined"

    return hostname


def get_string_hash(addr: str) -> str:
    return hashlib.sha256(bytes(addr, encoding="ascii")).hexdigest()[0:8]


def get_random_string(length: int) -> str:
    letters = string.ascii_lowercase + string.digits
    return "".join(random.choice(letters) for i in range(length))


def get_remote_executor_id_from_urn(urn: str) -> str:
    return urn.replace(f"urn:li:{DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME}:", "")


def send_remote_executor_status(
    graph: DataHubGraph, instance_id: str, status: RemoteExecutorStatusClass
) -> None:
    key = RemoteExecutorKeyClass(id=instance_id)
    mcpw = MetadataChangeProposalWrapper(
        entityType=DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME,
        entityKeyAspect=key,
        aspect=status,
    )
    graph.emit_mcp(mcpw, async_flag=False)


def get_utc_timestamp() -> int:
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp())
