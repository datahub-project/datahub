#!/usr/bin/env python3

import logging
import os
import sys
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)
from datahub.metadata.schema_classes import (
    RemoteExecutorPoolInfoClass,
    RemoteExecutorPoolKeyClass,
    RemoteExecutorPoolStateClass,
    RemoteExecutorPoolStatusClass,
)

logger = logging.getLogger(__name__)


def create_executor_pool(graph: DataHubGraph, name: str) -> None:
    aspect = RemoteExecutorPoolInfoClass(
        createdAt=int(time.time() * 1000),
        isEmbedded=False,
        state=RemoteExecutorPoolStateClass(
            status=RemoteExecutorPoolStatusClass.PROVISIONING_PENDING
        ),
    )
    mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=RemoteExecutorPoolKeyClass(id=name),
        entityUrn=f"urn:li:dataHubRemoteExecutorPool:{name}",
        entityType="dataHubRemoteExecutorPool",
        aspectName="dataHubRemoteExecutorPoolInfo",
        aspect=aspect,
        changeType="UPSERT",
    )
    graph.emit_mcp(mcpw)


def get_pool_status(graph: DataHubGraph, name: str) -> RemoteExecutorPoolInfoClass:
    request_urn = f"urn:li:dataHubRemoteExecutorPool:{name}"
    result = graph.get_aspect(request_urn, RemoteExecutorPoolInfoClass)
    return result


def wait_for_pool_creation(graph: DataHubGraph, name: str, timeout=60) -> bool:
    start_time = time.time()
    while True:
        res = get_pool_status(graph, name)
        if res is not None:
            if res.state.status == "READY":
                return True
            elif res.state.status in ["FAILED"]:
                return False
        if time.time() > (start_time + timeout):
            return False
        time.sleep(1.0)


def delete_executor_pool(graph: DataHubGraph, name: str) -> None:
    graph.hard_delete_entity(f"urn:li:dataHubRemoteExecutorPool:{name}")


executor_id = os.environ.get("DATAHUB_SMOKETEST_EXECUTOR_ID")
if executor_id is None or executor_id == "":
    logger.error("DATAHUB_SMOKETEST_EXECUTOR_ID is not set. Skipping setup")
    sys.exit(0)

graph = get_default_graph()
create_executor_pool(graph, executor_id)
wait_for_pool_creation(graph, executor_id)
