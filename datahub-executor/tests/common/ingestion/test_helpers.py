import json

from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.metadata.schema_classes import GenericAspectClass, MetadataChangeLogClass

from datahub_executor.common.constants import (
    DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
    DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
    DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
    RUN_INGEST_TASK_NAME,
)
from datahub_executor.common.ingestion.helpers import extract_execution_request


class TestExtractExecutionRequest:
    def setup_method(self) -> None:
        self.aspect_dict = {
            "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
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
        assert execution_request.executor_id == DATAHUB_EXECUTOR_EMBEDDED_POOL_ID
