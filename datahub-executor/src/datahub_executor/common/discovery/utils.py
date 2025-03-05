import datetime

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    RemoteExecutorKeyClass,
    RemoteExecutorStatusClass,
)

from datahub_executor.common.constants import DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME


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
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp()*1000)


def get_backend_revision(graph: DataHubGraph) -> int:
    server_config = graph.get_config()
    if isinstance(server_config, dict):
        backend_config = server_config.get("remoteExecutorBackend", {})
        revision = backend_config.get("revision", 0)
        return revision
