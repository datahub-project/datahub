import concurrent.futures
import contextlib
import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from unittest.mock import MagicMock

import pytest
import requests
import time_machine

import datahub.metadata.schema_classes as models
from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter, EmitMode
from datahub.ingestion.api.common import RUN_REPORTER_RECORD_KEY, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.sink.datahub_rest import (
    DatahubRestSink,
    DatahubRestSinkConfig,
    DataHubRestSinkReport,
    RestSinkMode,
    _http_status,
)
from datahub.utilities.partition_executor import (
    BatchPartitionExecutor,
    PartitionExecutor,
)

MOCK_GMS_ENDPOINT = "http://fakegmshost:8080"
_REPORTER_URN = "urn:li:dataHubIngestionSource:cli-0123456789abcdef0123456789abcdef"
_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)"

FROZEN_TIME = 1618987484580
basicAuditStamp = models.AuditStampClass(
    time=1618987484580,
    actor="urn:li:corpuser:datahub",
    impersonator=None,
)


@pytest.mark.parametrize(
    "record,path,snapshot",
    [
        (
            # Simple test.
            models.MetadataChangeEventClass(
                proposedSnapshot=models.DatasetSnapshotClass(
                    urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,downstream,PROD)",
                    aspects=[
                        models.UpstreamLineageClass(
                            upstreams=[
                                models.UpstreamClass(
                                    auditStamp=basicAuditStamp,
                                    dataset="urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream1,PROD)",
                                    type="TRANSFORMED",
                                ),
                                models.UpstreamClass(
                                    auditStamp=basicAuditStamp,
                                    dataset="urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream2,PROD)",
                                    type="TRANSFORMED",
                                ),
                            ]
                        )
                    ],
                ),
            ),
            "/entities?action=ingest",
            {
                "entity": {
                    "value": {
                        "com.linkedin.metadata.snapshot.DatasetSnapshot": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,downstream,PROD)",
                            "aspects": [
                                {
                                    "com.linkedin.dataset.UpstreamLineage": {
                                        "upstreams": [
                                            {
                                                "auditStamp": {
                                                    "time": 1618987484580,
                                                    "actor": "urn:li:corpuser:datahub",
                                                },
                                                "dataset": "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream1,PROD)",
                                                "type": "TRANSFORMED",
                                            },
                                            {
                                                "auditStamp": {
                                                    "time": 1618987484580,
                                                    "actor": "urn:li:corpuser:datahub",
                                                },
                                                "dataset": "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream2,PROD)",
                                                "type": "TRANSFORMED",
                                            },
                                        ]
                                    }
                                }
                            ],
                        }
                    }
                },
                "systemMetadata": {
                    "lastObserved": FROZEN_TIME,
                    "lastRunId": "no-run-id-provided",
                    "properties": {
                        "clientId": "acryl-datahub",
                        "clientVersion": "1!0.0.0.dev0",
                    },
                    "runId": "no-run-id-provided",
                },
            },
        ),
        (
            # Verify the serialization behavior with chart type enums.
            models.MetadataChangeEventClass(
                proposedSnapshot=models.ChartSnapshotClass(
                    urn="urn:li:chart:(superset,227)",
                    aspects=[
                        models.ChartInfoClass(
                            title="Weekly Messages",
                            description="",
                            lastModified=models.ChangeAuditStampsClass(
                                created=basicAuditStamp,
                                lastModified=basicAuditStamp,
                            ),
                            type=models.ChartTypeClass.SCATTER,
                        ),
                    ],
                )
            ),
            "/entities?action=ingest",
            {
                "entity": {
                    "value": {
                        "com.linkedin.metadata.snapshot.ChartSnapshot": {
                            "urn": "urn:li:chart:(superset,227)",
                            "aspects": [
                                {
                                    "com.linkedin.chart.ChartInfo": {
                                        "customProperties": {},
                                        "title": "Weekly Messages",
                                        "description": "",
                                        "lastModified": {
                                            "created": {
                                                "time": 1618987484580,
                                                "actor": "urn:li:corpuser:datahub",
                                            },
                                            "lastModified": {
                                                "time": 1618987484580,
                                                "actor": "urn:li:corpuser:datahub",
                                            },
                                        },
                                        "type": "SCATTER",
                                    }
                                }
                            ],
                        }
                    }
                },
                "systemMetadata": {
                    "lastObserved": FROZEN_TIME,
                    "lastRunId": "no-run-id-provided",
                    "properties": {
                        "clientId": "acryl-datahub",
                        "clientVersion": "1!0.0.0.dev0",
                    },
                    "runId": "no-run-id-provided",
                },
            },
        ),
        (
            # Verify that DataJobInfo is serialized properly (particularly it's union type).
            models.MetadataChangeEventClass(
                proposedSnapshot=models.DataJobSnapshotClass(
                    urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)",
                    aspects=[
                        models.DataJobInfoClass(
                            name="User Deletions",
                            description="Constructs the fct_users_deleted from logging_events",
                            type=models.AzkabanJobTypeClass.SQL,
                        )
                    ],
                )
            ),
            "/entities?action=ingest",
            {
                "entity": {
                    "value": {
                        "com.linkedin.metadata.snapshot.DataJobSnapshot": {
                            "urn": "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)",
                            "aspects": [
                                {
                                    "com.linkedin.datajob.DataJobInfo": {
                                        "customProperties": {},
                                        "name": "User Deletions",
                                        "description": "Constructs the fct_users_deleted from logging_events",
                                        "type": {"string": "SQL"},
                                    }
                                }
                            ],
                        }
                    }
                },
                "systemMetadata": {
                    "lastObserved": FROZEN_TIME,
                    "lastRunId": "no-run-id-provided",
                    "properties": {
                        "clientId": "acryl-datahub",
                        "clientVersion": "1!0.0.0.dev0",
                    },
                    "runId": "no-run-id-provided",
                },
            },
        ),
        (
            # Usage stats ingestion test.
            models.UsageAggregationClass(
                bucket=1623826800000,
                duration="DAY",
                resource="urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
                metrics=models.UsageAggregationMetricsClass(
                    uniqueUserCount=2,
                    users=[
                        models.UserUsageCountsClass(
                            user="urn:li:corpuser:jdoe",
                            count=5,
                        ),
                        models.UserUsageCountsClass(
                            user="urn:li:corpuser:unknown",
                            count=3,
                            userEmail="foo@example.com",
                        ),
                    ],
                    totalSqlQueries=1,
                    topSqlQueries=["SELECT * FROM foo"],
                ),
            ),
            "/usageStats?action=batchIngest",
            {
                "buckets": [
                    {
                        "bucket": 1623826800000,
                        "duration": "DAY",
                        "resource": "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
                        "metrics": {
                            "uniqueUserCount": 2,
                            "users": [
                                {"count": 5, "user": "urn:li:corpuser:jdoe"},
                                {
                                    "count": 3,
                                    "user": "urn:li:corpuser:unknown",
                                    "userEmail": "foo@example.com",
                                },
                            ],
                            "totalSqlQueries": 1,
                            "topSqlQueries": ["SELECT * FROM foo"],
                        },
                    }
                ]
            },
        ),
        (
            MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
                aspect=models.OwnershipClass(
                    owners=[
                        models.OwnerClass(
                            owner="urn:li:corpuser:fbar",
                            type=models.OwnershipTypeClass.DATAOWNER,
                        )
                    ],
                    lastModified=models.AuditStampClass(
                        time=0,
                        actor="urn:li:corpuser:fbar",
                    ),
                ),
            ),
            "/aspects?action=ingestProposal",
            {
                "async": "false",
                "proposal": {
                    "entityType": "dataset",
                    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
                    "changeType": "UPSERT",
                    "aspectName": "ownership",
                    "aspect": {
                        "value": '{"owners": [{"owner": "urn:li:corpuser:fbar", "type": "DATAOWNER"}], "ownerTypes": {}, "lastModified": {"time": 0, "actor": "urn:li:corpuser:fbar"}}',
                        "contentType": "application/json",
                    },
                    "systemMetadata": {
                        "lastObserved": FROZEN_TIME,
                        "lastRunId": "no-run-id-provided",
                        "properties": {
                            "clientId": "acryl-datahub",
                            "clientVersion": "1!0.0.0.dev0",
                        },
                        "runId": "no-run-id-provided",
                    },
                },
            },
        ),
    ],
)
@time_machine.travel(
    datetime.fromtimestamp(FROZEN_TIME / 1000, tz=timezone.utc), tick=False
)
def test_datahub_rest_emitter(requests_mock, record, path, snapshot):
    def match_request_text(request: requests.Request) -> bool:
        requested_snapshot = request.json()
        assert requested_snapshot == snapshot, (
            f"Expected snapshot to be {json.dumps(snapshot)}, got {json.dumps(requested_snapshot)}"
        )
        return True

    requests_mock.post(
        f"{MOCK_GMS_ENDPOINT}{path}",
        request_headers={"X-RestLi-Protocol-Version": "2.0.0"},
        additional_matcher=match_request_text,
    )

    with contextlib.ExitStack() as stack:
        if isinstance(record, models.UsageAggregationClass):
            stack.enter_context(
                pytest.warns(
                    DeprecationWarning,
                    match="Use emit with a datasetUsageStatistics aspect instead",
                )
            )

        # This test specifically exercises the restli emitter endpoints.
        # We have additional tests specifically for the OpenAPI emitter
        # and its request format.
        emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=False)
        stack.enter_context(emitter)
        emitter.emit(record)


@pytest.mark.parametrize(
    "sink_mode,configured_emit_mode,expected_emit_mode",
    [
        # Compatible: async sink + async emit mode → use configured
        (RestSinkMode.ASYNC_BATCH, EmitMode.ASYNC, EmitMode.ASYNC),
        (RestSinkMode.ASYNC_BATCH, EmitMode.ASYNC_WAIT, EmitMode.ASYNC_WAIT),
        (RestSinkMode.ASYNC, EmitMode.ASYNC, EmitMode.ASYNC),
        (RestSinkMode.ASYNC, EmitMode.ASYNC_WAIT, EmitMode.ASYNC_WAIT),
        # Incompatible: async sink + sync emit mode → override to ASYNC
        (RestSinkMode.ASYNC_BATCH, EmitMode.SYNC_PRIMARY, EmitMode.ASYNC),
        (RestSinkMode.ASYNC, EmitMode.SYNC_PRIMARY, EmitMode.ASYNC),
        # Compatible: sync sink + sync emit mode → use configured
        (RestSinkMode.SYNC, EmitMode.SYNC_PRIMARY, EmitMode.SYNC_PRIMARY),
        (RestSinkMode.SYNC, EmitMode.SYNC_WAIT, EmitMode.SYNC_WAIT),
        # Incompatible: sync sink + async emit mode → override to SYNC_PRIMARY
        (RestSinkMode.SYNC, EmitMode.ASYNC, EmitMode.SYNC_PRIMARY),
    ],
)
def test_resolve_gms_emit_mode(sink_mode, configured_emit_mode, expected_emit_mode):
    """Guard rail: _resolve_gms_emit_mode picks a compatible EmitMode for the sink mode."""
    from datahub.ingestion.sink.datahub_rest import _resolve_gms_emit_mode

    assert _resolve_gms_emit_mode(sink_mode, configured_emit_mode) == expected_emit_mode


class TestDatahubRestSinkTcpKeepalive:
    """Regression tests covering end-to-end tcp_keepalive propagation through
    the REST sink: from the recipe config, through the sink config, into the
    per-thread DataHubRestEmitter, and finally into the underlying
    requests.Session adapter. We verify the full chain here so a future
    refactor can't silently regress it.
    """

    def test_sink_make_emitter_passes_tcp_keepalive(self):
        """DatahubRestSink.make_emitter must hand tcp_keepalive to the emitter."""
        from requests.adapters import HTTPAdapter

        from datahub.emitter.rest_emitter import _KeepAliveHTTPAdapter
        from datahub.ingestion.sink.datahub_rest import (
            DatahubRestSink,
            DatahubRestSinkConfig,
        )

        cfg = DatahubRestSinkConfig(server="http://localhost:8080", tcp_keepalive=True)
        emitter = DatahubRestSink.make_emitter(cfg)
        assert isinstance(
            emitter._session.get_adapter("https://example.com"), _KeepAliveHTTPAdapter
        )

        cfg = DatahubRestSinkConfig(server="http://localhost:8080", tcp_keepalive=False)
        emitter = DatahubRestSink.make_emitter(cfg)
        assert type(emitter._session.get_adapter("https://example.com")) is HTTPAdapter


def test_emit_batch_wrapper_uses_resolved_emit_mode():
    """Regression test: _emit_batch_wrapper must pass self._gms_emit_mode to emit_mcps."""

    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
        aspect=models.StatusClass(removed=False),
    )

    mock_emitter = MagicMock()
    mock_emitter.emit_mcps.return_value = [MagicMock()]

    sink = DatahubRestSink.__new__(DatahubRestSink)
    sink._emitter_thread_local = threading.local()
    sink._emitter_thread_local.emitter = mock_emitter
    sink._gms_emit_mode = EmitMode.ASYNC
    sink.report = MagicMock()

    sink._emit_batch_wrapper([(mcp,)])

    mock_emitter.emit_mcps.assert_called_once()
    call_kwargs = mock_emitter.emit_mcps.call_args
    actual_mode = call_kwargs[1]["emit_mode"]
    assert actual_mode == EmitMode.ASYNC, (
        f"Expected self._gms_emit_mode (ASYNC) but got {actual_mode}. "
        "The batch wrapper must use the resolved emit mode."
    )


class TestDataHubRestSinkBatchEmission:
    """Tests for DatahubRestSink._emit_batch_wrapper behavior."""

    def test_emit_batch_wrapper_logs_when_batches_split(self, caplog):
        """Test that _emit_batch_wrapper logs info when emit_mcps returns multiple chunks."""
        from unittest.mock import MagicMock, PropertyMock, patch

        from datahub.emitter.response_helper import TraceData
        from datahub.ingestion.sink.datahub_rest import (
            DatahubRestSink,
            DataHubRestSinkReport,
        )

        # Create mock emitter that returns multiple TraceData objects
        mock_emitter = MagicMock()
        mock_emitter.emit_mcps.return_value = [
            TraceData(trace_id="trace-1", data={"urn:li:dataset:1": ["status"]}),
            TraceData(trace_id="trace-2", data={"urn:li:dataset:2": ["status"]}),
        ]

        # Create sink with mocked emitter property
        with (
            patch.object(
                DatahubRestSink, "__init__", lambda self, *args, **kwargs: None
            ),
            patch.object(
                DatahubRestSink, "emitter", new_callable=PropertyMock
            ) as mock_emitter_prop,
        ):
            mock_emitter_prop.return_value = mock_emitter
            sink = DatahubRestSink.__new__(DatahubRestSink)
            sink._gms_emit_mode = EmitMode.ASYNC
            sink.report = DataHubRestSinkReport()

            # Create test MCPs
            mcps: list = [
                (
                    MetadataChangeProposalWrapper(
                        entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:test,table{i},PROD)",
                        aspect=models.StatusClass(removed=False),
                    ),
                )
                for i in range(2)
            ]

            # Call _emit_batch_wrapper
            with caplog.at_level("INFO"):
                sink._emit_batch_wrapper(mcps)

            # Verify report was updated
            assert sink.report.async_batches_prepared == 1
            assert sink.report.async_batches_split == 2

            # Verify log message
            assert "payload was split into 2 batches" in caplog.text


def test_sync_origin_opt_in_passed_through_in_all_modes():
    from datahub.ingestion.sink.datahub_rest import (
        DatahubRestSinkConfig,
        RestSinkMode,
    )

    # Marker-aware sync routing only ever upgrades a batch to sync, never
    # downgrades it, so it is a no-op in SYNC mode (already synchronous) and the
    # opt-in is passed through unconditionally regardless of sink mode.
    for mode in (RestSinkMode.SYNC, RestSinkMode.ASYNC, RestSinkMode.ASYNC_BATCH):
        emitter = DatahubRestSink.make_emitter(
            DatahubRestSinkConfig(
                server="http://localhost:8080",
                mode=mode,
                respect_mcp_sync_marker=True,
            )
        )
        assert emitter.respect_mcp_sync_marker is True


def test_rest_sink_config_accepts_client_config_dump():
    # The Kafka default sink builds its REST fallback via
    # DatahubRestSinkConfig(**DatahubClientConfig(...).model_dump()). Under
    # ConfigModel's extra="forbid", that round-trip breaks the moment a base
    # client-config field isn't also a DatahubRestSinkConfig field. Guard it so
    # such a drift fails here (a targeted unit test) instead of at runtime on
    # every Kafka-default ingestion run.
    client = DatahubClientConfig(server="http://localhost:8080")
    cfg = DatahubRestSinkConfig(**client.model_dump())
    assert cfg.server == "http://localhost:8080"


@pytest.mark.parametrize(
    "status,body",
    [
        pytest.param(
            403, {"json": {"status": 403, "message": "unauthorized"}}, id="403-json"
        ),
        pytest.param(401, {"text": "<html>Unauthorized</html>"}, id="401-non-json"),
    ],
)
def test_emitter_http_error_keeps_http_cause(requests_mock, status, body):
    requests_mock.post(
        f"{MOCK_GMS_ENDPOINT}/aspects?action=ingestProposal",
        status_code=status,
        **body,
    )
    emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=False)
    mcp = MetadataChangeProposalWrapper(
        entityUrn=_REPORTER_URN, aspect=models.StatusClass(removed=False)
    )

    with pytest.raises(OperationalError) as exc_info:
        emitter.emit(mcp)

    cause = exc_info.value.__cause__
    assert isinstance(cause, requests.HTTPError)
    assert cause.response.status_code == status


def _gms_error(
    status: Optional[int], info: Optional[Dict[str, Any]] = None
) -> OperationalError:
    """The OperationalError DataHubRestEmitter raises; status=None models a connection error."""
    if status is None:
        return OperationalError(
            "Unable to emit metadata to DataHub GMS", {"message": "Connection refused"}
        )
    response = requests.Response()
    response.status_code = status
    error = OperationalError(
        "Unable to emit metadata to DataHub GMS: denied",
        info if info is not None else {"status": status, "message": "denied"},
    )
    error.__cause__ = requests.HTTPError(f"{status} Error", response=response)
    return error


def _bare_sink(
    mode: RestSinkMode = RestSinkMode.ASYNC_BATCH,
) -> Tuple[DatahubRestSink, MagicMock, MagicMock]:
    """A sink with a mocked emitter and executor; returns (sink, emitter, executor)."""
    emitter = MagicMock()
    executor = MagicMock(
        spec=BatchPartitionExecutor
        if mode == RestSinkMode.ASYNC_BATCH
        else PartitionExecutor
    )
    sink = DatahubRestSink.__new__(DatahubRestSink)
    sink.config = DatahubRestSinkConfig(server=MOCK_GMS_ENDPOINT, mode=mode)
    sink.report = DataHubRestSinkReport()
    sink._emitter_thread_local = threading.local()
    sink._emitter_thread_local.emitter = emitter
    sink._gms_emit_mode = EmitMode.ASYNC
    sink.executor = executor
    return sink, emitter, executor


def _envelope(urn: str, reporter: bool) -> RecordEnvelope:
    mcp = MetadataChangeProposalWrapper(
        entityUrn=urn, aspect=models.StatusClass(removed=False)
    )
    return RecordEnvelope(
        mcp, metadata={RUN_REPORTER_RECORD_KEY: True} if reporter else {}
    )


def _complete_with_error(
    sink: DatahubRestSink, envelope: RecordEnvelope, error: Exception
) -> None:
    future: concurrent.futures.Future = concurrent.futures.Future()
    future.set_exception(error)
    sink.report.pending_requests += 1
    sink._write_done_callback(envelope, NoopWriteCallback(), future)


@pytest.mark.parametrize("status", [401, 403])
def test_reporter_record_denied_logs_warning_without_failure(caplog, status):
    sink, _, _ = _bare_sink()

    with caplog.at_level("WARNING"):
        _complete_with_error(
            sink, _envelope(_REPORTER_URN, reporter=True), _gms_error(status)
        )

    # The exit code and --strict-warnings both read these two lists.
    assert len(sink.report.failures) == 0
    assert len(sink.report.warnings) == 0
    assert _REPORTER_URN in caplog.text
    assert f"HTTP {status}" in caplog.text


def test_reporter_record_denied_under_treat_errors_as_warnings_adds_no_warning():
    sink, _, _ = _bare_sink()
    sink.treat_errors_as_warnings = True

    _complete_with_error(sink, _envelope(_REPORTER_URN, reporter=True), _gms_error(403))

    assert len(sink.report.failures) == 0
    assert len(sink.report.warnings) == 0


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(_gms_error(500), id="server-error"),
        pytest.param(_gms_error(None), id="no-http-response"),
    ],
)
def test_reporter_record_other_errors_still_fail(error):
    sink, _, _ = _bare_sink()

    _complete_with_error(sink, _envelope(_REPORTER_URN, reporter=True), error)

    assert len(sink.report.failures) == 1


def test_non_reporter_record_denied_still_fails():
    sink, _, _ = _bare_sink()

    _complete_with_error(sink, _envelope(_DATASET_URN, reporter=False), _gms_error(403))

    assert len(sink.report.failures) == 1


def test_denial_detected_from_info_status_without_http_cause():
    # An error re-raised per record may lose the HTTPError but keeps GMS's body.
    error = OperationalError("denied", {"status": 403, "message": "denied"})

    assert _http_status(error) == 403


def test_async_batch_sends_reporter_record_on_its_own():
    sink, emitter, executor = _bare_sink(RestSinkMode.ASYNC_BATCH)
    emitter.emit.side_effect = _gms_error(403)

    sink.write_record_async(
        _envelope(_REPORTER_URN, reporter=True), NoopWriteCallback()
    )
    sink.write_record_async(
        _envelope(_DATASET_URN, reporter=False), NoopWriteCallback()
    )

    # Never batched, so a refused run report can't fail the metadata batched with it.
    emitter.emit.assert_called_once()
    assert emitter.emit.call_args.args[0].entityUrn == _REPORTER_URN
    executor.submit.assert_called_once()
    assert executor.submit.call_args.args[0] == _DATASET_URN
    assert len(sink.report.failures) == 0
    assert sink.report.pending_requests == 1  # only the batched metadata record


def test_async_batch_reporter_record_success_is_counted():
    sink, _, _ = _bare_sink(RestSinkMode.ASYNC_BATCH)

    sink.write_record_async(
        _envelope(_REPORTER_URN, reporter=True), NoopWriteCallback()
    )

    assert sink.report.total_records_written == 1
    assert sink.report.pending_requests == 0
