import os
import time
import uuid
from typing import Any

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataHubIngestionSourceConfigClass,
    DataHubIngestionSourceInfoClass,
    DataHubIngestionSourceKeyClass,
    ExecutionRequestInputClass,
    ExecutionRequestKeyClass,
    ExecutionRequestResultClass,
    ExecutionRequestSourceClass,
)


def create_ingestion_request(graph, name, executor_id) -> str:
    aspect = ExecutionRequestInputClass(
        task="RUN_INGEST",
        args={
            "recipe": """{
        "source": {
          "type": "demo-data",
          "config": {}
        }
      }""",
            "version": "1.0.0.2",
            "debug_mode": "false",
        },
        executorId=executor_id,
        attempts=0,
        actorUrn=None,
        requestedAt=int(time.time() * 1000),
        source=ExecutionRequestSourceClass(
            type="MANUAL_INGESTION_SOURCE",
            ingestionSource=f"urn:li:dataHubIngestionSource:{name}",
        ),
    )

    request_id = str(uuid.uuid4())
    mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=ExecutionRequestKeyClass(id=request_id),
        entityUrn=f"urn:li:dataHubExecutionRequest:{request_id}",
        entityType="dataHubExecutionRequest",
        aspectName="dataHubExecutionRequestInput",
        aspect=aspect,
        changeType="UPSERT",
    )
    graph.emit_mcp(mcpw)
    return request_id


def get_ingestion_request_result(graph, request_id) -> Any:
    request_urn = f"urn:li:dataHubExecutionRequest:{request_id}"
    result = graph.get_aspect(request_urn, ExecutionRequestResultClass)
    return result


def create_ingestion_source(graph, name, executor_id) -> None:
    aspect = DataHubIngestionSourceInfoClass(
        name=name,
        type="demo-data",
        config=DataHubIngestionSourceConfigClass(
            recipe="""{
        "source": {
          "type": "demo-data",
          "config": {}
        }
      }""",
            executorId=executor_id,
            extraArgs={},
            debugMode=False,
        ),
    )
    mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=DataHubIngestionSourceKeyClass(id=name),
        entityUrn=f"urn:li:dataHubIngestionSource:{name}",
        entityType="dataHubIngestionSource",
        aspectName="dataHubIngestionSourceInfo",
        aspect=aspect,
        changeType="UPSERT",
    )
    graph.emit_mcp(mcpw)


def wait_for_ingestion_request_completion(graph_client, request_id, timeout=60) -> bool:
    start_time = time.time()
    while True:
        res = get_ingestion_request_result(graph_client, request_id)
        if res is not None:
            if res.status == "SUCCESS":
                return True
            elif res.status in ["CANCELLED", "DUPLICATE", "ABORTED", "FAILURE"]:
                return False
        if time.time() > (start_time + timeout):
            return False
        time.sleep(1.0)


@pytest.mark.remote_executor
def test_ingestion_embedded(auth_session, graph_client):
    create_ingestion_source(graph_client, "demo-data-embedded", "default")
    request = create_ingestion_request(graph_client, "demo-data-embedded", "default")
    assert wait_for_ingestion_request_completion(graph_client, request)


@pytest.mark.remote_executor
def test_ingestion_remote(auth_session, graph_client):
    executor_id = os.environ.get("DATAHUB_SMOKETEST_EXECUTOR_ID", "remote-ci")
    create_ingestion_source(graph_client, "demo-data-remote", executor_id)

    result = False
    for _ in range(3):
        request = create_ingestion_request(
            graph_client, "demo-data-remote", executor_id
        )
        result = wait_for_ingestion_request_completion(graph_client, request)
        if result:
            break
    assert result
