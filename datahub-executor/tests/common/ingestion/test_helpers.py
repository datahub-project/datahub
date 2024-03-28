import json
from unittest.mock import Mock

from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import GenericAspectClass, MetadataChangeLogClass

from datahub_executor.common.constants import (
    DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
    DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
    RUN_INGEST_TASK_NAME,
)
from datahub_executor.common.ingestion.helpers import (
    extract_execution_request,
    fetch_execution_signal_requests,
)


class TestExtractExecutionRequest:
    def setup_method(self) -> None:
        self.aspect_dict = {
            "executorId": "default",
            "task": RUN_INGEST_TASK_NAME,
            "source": {"ingestionSource": "my-ingestion-urn"},
            "args": {"recipe": "recipe-string", "version": "v1", "debug_mode": False},
        }
        self.event = MetadataChangeLogClass(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            changeType="UPSERT",
            aspectName=DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            aspect=GenericAspectClass(
                contentType=JSON_CONTENT_TYPE,
                value=json.dumps(self.aspect_dict).encode(),
            ),
            entityKeyAspect=GenericAspectClass(
                contentType=JSON_CONTENT_TYPE,
                value=json.dumps(
                    {
                        "id": "some-id",
                    }
                ).encode(),
            ),
            entityUrn="urn:li:dataset:test",
        )

    def test_with_no_aspect(self) -> None:
        self.event.aspect = None
        execution_request = extract_execution_request(self.event)
        assert execution_request is None

    def test_with_empty_aspect(self) -> None:
        self.event.aspect = GenericAspectClass(
            contentType=JSON_CONTENT_TYPE,
            value=json.dumps({}).encode(),
        )
        execution_request = extract_execution_request(self.event)
        assert execution_request is None

    def test_with_no_entity_key(self) -> None:
        self.event.entityKeyAspect = None
        execution_request = extract_execution_request(self.event)
        assert execution_request is None

    def test_with_empty_entity_key(self) -> None:
        self.event.entityKeyAspect = GenericAspectClass(
            contentType=JSON_CONTENT_TYPE,
            value=json.dumps({}).encode(),
        )
        execution_request = extract_execution_request(self.event)
        assert execution_request is None

    def test_with_wrong_executor_id(self) -> None:
        self.aspect_dict["executorId"] = "remote"
        self.event.aspect = GenericAspectClass(
            contentType=JSON_CONTENT_TYPE,
            value=json.dumps(self.aspect_dict).encode(),
        )
        execution_request = extract_execution_request(self.event)
        assert execution_request is None

    def test_successful_extract(self) -> None:
        execution_request = extract_execution_request(self.event)
        assert execution_request is not None
        assert execution_request.executor_id == "default"


class TestFetchExecutionSignals:
    def setup_method(self) -> None:
        self.graph = Mock(spec=DataHubGraph)

        # Configure the mock object to return a specific result when its execute_graphql method is called
        self.graph.execute_graphql.return_value = {
            "listSignalRequests": {
                "total": 1,
                "signalRequests": [
                    {
                        "execId": "urn:li:dataHubExecutionRequest:my-exec-id",
                        "executorId": "default",
                        "signal": "KILL",
                    },
                    {
                        "executorId": "default",
                        "signal": "KILL",
                    },
                ],
            }
        }

    def test_no_list_signal_requests(self) -> None:
        self.graph.execute_graphql.return_value = {}
        signal_requests = fetch_execution_signal_requests(self.graph, ["my-exec-id"])
        assert len(signal_requests) == 0

    def test_no_signal_requests(self) -> None:
        self.graph.execute_graphql.return_value = {"listSignalRequests": {}}
        signal_requests = fetch_execution_signal_requests(self.graph, ["my-exec-id"])
        assert len(signal_requests) == 0

    def test_fetch_successful(self) -> None:
        signal_requests = fetch_execution_signal_requests(self.graph, ["my-exec-id"])
        assert len(signal_requests) == 1
