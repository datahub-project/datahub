import json
from unittest import mock
from unittest.mock import Mock

import pytest
from acryl.executor.execution.reporting_executor import ReportingExecutor
from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import GenericAspectClass, MetadataChangeLogClass
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE

from datahub_executor.common.constants import (
    DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
)
from datahub_executor.common.discovery.discovery import DatahubExecutorDiscovery
from datahub_executor.common.types import ExecutorConfig

with mock.patch(
    "datahub_executor.worker.celery_sqs.config.update_celery_credentials"
) as mock_func:
    from datahub_executor.common.constants import (
        DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
        DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
        DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME,
    )
    from datahub_executor.coordinator.ingestion import IngestionAction


class TestIngestionAction:
    def setup_method(self) -> None:
        self.discovery = Mock(spec=DatahubExecutorDiscovery)
        self.graph = Mock(spec=DataHubGraph)
        self.action = IngestionAction(
            self.graph, self.discovery, False, True, DATAHUB_EXECUTOR_EMBEDDED_POOL_ID
        )
        self.change_event = MetadataChangeLogClass(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            changeType="UPSERT",
            aspectName=DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            aspect=GenericAspectClass(
                contentType=JSON_CONTENT_TYPE,
                value=json.dumps(
                    {
                        "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                    }
                ).encode(),
            ),
            entityUrn="urn:li:dataset:test",
        )
        self.event = EventEnvelope(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            self.change_event,
            {},
        )
        self.credentials = ExecutorConfig.model_validate(
            {
                "region": "us-west-2",
                "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                "queueUrl": "",
                "accessKeyId": "",
                "secretKeyId": "",
                "sessionToken": "",
            }
        )

    @mock.patch("datahub_executor.worker.remote.update_celery_credentials")
    @mock.patch("datahub_executor.worker.celery_sqs.app.ingestion_request.apply_async")
    def test_ingestion_request(
        self, apply_async_mock: Mock, update_creds_mock: Mock
    ) -> None:
        self.action.act(self.event)

        assert update_creds_mock.call_count == 1
        assert apply_async_mock.call_count == 1

    @mock.patch("datahub_executor.worker.remote.update_celery_credentials")
    @mock.patch("datahub_executor.worker.celery_sqs.app.ingestion_request.apply_async")
    def test_ingestion_request_no_aspect(
        self, apply_async_mock: Mock, update_creds_mock: Mock
    ) -> None:
        self.change_event.aspect = None
        self.action.act(self.event)

        assert update_creds_mock.call_count == 0
        assert apply_async_mock.call_count == 0

    @mock.patch("datahub_executor.coordinator.ingestion.setup_ingestion_executor")
    @mock.patch("threading.Thread")
    @pytest.mark.skip(reason="This never worked")
    def test_execution_request_input_local(
        self, thread_mock: Mock, setup_mock: Mock
    ) -> None:
        ingestion_executor_mock = Mock(spec=ReportingExecutor)
        setup_mock.return_value = ingestion_executor_mock
        self.action = IngestionAction(
            self.graph, self.discovery, True, True, DATAHUB_EXECUTOR_EMBEDDED_POOL_ID
        )
        self.action.act(self.event)

        assert thread_mock.call_count == 1
        assert thread_mock.return_value.start.call_count == 1

    @mock.patch("datahub_executor.coordinator.ingestion.setup_ingestion_executor")
    @mock.patch("threading.Thread")
    @pytest.mark.skip(reason="This never worked")
    def test_request_signal_local(self, thread_mock: Mock, setup_mock: Mock) -> None:
        self.change_event.aspectName = DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME
        self.change_event.aspect = GenericAspectClass(
            contentType=JSON_CONTENT_TYPE,
            value=json.dumps(
                {"executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID, "signal": "KILL"}
            ).encode(),
        )

        ingestion_executor_mock = Mock(spec=ReportingExecutor)
        setup_mock.return_value = ingestion_executor_mock
        self.action = IngestionAction(
            self.graph, self.discovery, True, True, DATAHUB_EXECUTOR_EMBEDDED_POOL_ID
        )
        self.action.act(self.event)

        assert thread_mock.call_count == 1
        assert thread_mock.return_value.start.call_count == 1

    @mock.patch("datahub_executor.coordinator.ingestion.setup_ingestion_executor")
    @mock.patch("threading.Thread")
    @pytest.mark.skip(reason="This never worked")
    def test_request_signal_local_no_aspect(
        self, thread_mock: Mock, setup_mock: Mock
    ) -> None:
        self.change_event.aspectName = DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME
        self.change_event.aspect = None

        ingestion_executor_mock = Mock(spec=ReportingExecutor)
        setup_mock.return_value = ingestion_executor_mock
        self.action = IngestionAction(
            self.graph, self.discovery, True, True, DATAHUB_EXECUTOR_EMBEDDED_POOL_ID
        )
        self.action.act(self.event)

        assert thread_mock.call_count == 1
        assert thread_mock.return_value.start.call_count == 1
