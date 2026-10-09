import concurrent.futures
import contextlib
import json
import threading
from datetime import datetime, timezone
from typing import List, Optional, Union
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests
import time_machine

import datahub.metadata.schema_classes as models
from datahub.configuration.common import OperationalError
from datahub.emitter import rest_emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import ChunkedEmitError, DatahubRestEmitter, EmitMode
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.sink.datahub_rest import (
    _MAX_CONSECUTIVE_ZERO_RECOVERY_ISOLATIONS,
    DatahubRestSink,
    DatahubRestSinkConfig,
    DataHubRestSinkReport,
    RestSinkMode,
    _parse_denied_urns,
)
from datahub.utilities.partition_executor import BatchItemFailures

MOCK_GMS_ENDPOINT = "http://fakegmshost:8080"

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
        from unittest.mock import MagicMock, PropertyMock

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


def _make_sink(mock_emitter: Union[MagicMock, DatahubRestEmitter]) -> DatahubRestSink:
    sink = DatahubRestSink.__new__(DatahubRestSink)
    sink._emitter_thread_local = threading.local()
    sink._emitter_thread_local.emitter = mock_emitter
    sink._gms_emit_mode = EmitMode.ASYNC
    sink.report = DataHubRestSinkReport()
    sink._isolation_lock = threading.Lock()
    sink._consecutive_zero_recovery_isolations = 0
    sink._isolation_suppressed = False
    return sink


def _status_mcp(name: str) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:foo,{name},PROD)",
        aspect=models.StatusClass(removed=False),
    )


def _scripted_emitter(*outcomes: Optional[Exception]) -> MagicMock:
    """An emitter whose successive emit_mcps calls raise the given errors (None = accepted)."""
    mock_emitter = MagicMock()
    mock_emitter.emit_mcps.side_effect = [
        outcome if outcome is not None else [MagicMock()] for outcome in outcomes
    ]
    return mock_emitter


def _chunked(error: OperationalError, not_landed: List[int]) -> ChunkedEmitError:
    return ChunkedEmitError(error.message, error.info, not_landed)


_JOB = "urn:li:dataJob:(urn:li:dataFlow:(mysql,inst.db.stored_procedures,PROD),proc)"
_CONTAINER = "urn:li:container:0123456789abcdef"
_DENIAL_PREFIX = "User urn:li:corpuser:svc_ingest is unauthorized to modify entity: "


def _denial(*urns: str, entry_status: int = 403) -> OperationalError:
    """What DataHubRestEmitter raises when the token may not write some entities of a batch."""
    message = _DENIAL_PREFIX + ", ".join(
        f"HttpStatus: {entry_status} Urn: {urn}" for urn in urns
    )
    return OperationalError(
        f"Unable to emit metadata to DataHub GMS: {message}",
        {
            "exceptionClass": "com.linkedin.restli.server.RestLiServiceException",
            "message": message,
            "status": 403,
        },
    )


def _entity_mcp(urn: str) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=urn, aspect=models.StatusClass(removed=False)
    )


def test_emit_batch_wrapper_isolates_the_record_that_caused_the_rejection():
    """One invalid record must not take the valid records batched with it down."""
    good1 = _status_mcp("good1")
    poison = _status_mcp("poison")
    good2 = _status_mcp("good2")

    batch_error = OperationalError("batch rejected", {"status": 422})
    poison_error = OperationalError("record rejected", {"status": 422})

    def emit_mcps(events, emit_mode=None):
        if len(events) > 1:
            raise batch_error
        if events[0] is poison:
            raise poison_error
        return [MagicMock()]

    mock_emitter = MagicMock()
    mock_emitter.emit_mcps.side_effect = emit_mcps
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(good1,), (poison,), (good2,)])

    outcomes = exc_info.value.outcomes
    assert outcomes[0] is None
    assert outcomes[1] is poison_error
    assert outcomes[2] is None

    assert sink.report.batches_rejected == 1
    assert sink.report.records_isolated_after_batch_rejection == 3
    assert sink.report.records_recovered_after_batch_rejection == 2


def test_emit_batch_wrapper_does_not_isolate_a_single_record_batch():
    """With one record the error is already precise, so re-emitting it is pure waste."""
    poison = _status_mcp("poison")
    poison_error = OperationalError("record rejected", {"status": 422})

    mock_emitter = MagicMock()
    mock_emitter.emit_mcps.side_effect = poison_error
    sink = _make_sink(mock_emitter)

    with pytest.raises(OperationalError):
        sink._emit_batch_wrapper([(poison,)])

    assert mock_emitter.emit_mcps.call_count == 1
    assert sink.report.batches_rejected == 0


def test_emit_batch_wrapper_isolation_when_every_record_fails():
    """A block applied to the whole request, not one poison record.

    Isolation cannot recover anything here, but it must still attribute an error to
    every record rather than sharing one, and uniform failure is itself diagnostic:
    it proves no single record was at fault.
    """
    records = [_status_mcp(f"rec{i}") for i in range(3)]
    error = OperationalError(
        "forbidden", {"status": 403, "message": "403 Client Error: Forbidden"}
    )

    mock_emitter = MagicMock()
    mock_emitter.emit_mcps.side_effect = error
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    outcomes = exc_info.value.outcomes
    assert len(outcomes) == 3
    assert all(outcome is error for outcome in outcomes)

    assert sink.report.batches_rejected == 1
    assert sink.report.records_isolated_after_batch_rejection == 3
    assert sink.report.records_recovered_after_batch_rejection == 0


def test_emit_batch_wrapper_isolation_keeps_an_mce_record_whole():
    """An MCE expands to several MCPs; isolation must re-emit them together as one record."""
    mce = models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:foo,mce,PROD)",
            aspects=[
                models.StatusClass(removed=False),
                models.DatasetPropertiesClass(name="mce"),
            ],
        )
    )
    good = _status_mcp("good")

    single_record_calls = []

    def emit_mcps(events, emit_mode=None):
        if len(events) > 2:  # the whole batch: 2 MCPs from the MCE + 1 MCP
            raise OperationalError("batch rejected", {"status": 422})
        single_record_calls.append(list(events))
        return [MagicMock()]

    mock_emitter = MagicMock()
    mock_emitter.emit_mcps.side_effect = emit_mcps
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(mce,), (good,)])

    assert exc_info.value.outcomes == [None, None]
    # Two isolation calls: the MCE's two MCPs together, then the lone MCP.
    assert [len(call) for call in single_record_calls] == [2, 1]
    assert sink.report.records_recovered_after_batch_rejection == 2


def test_isolation_stops_after_consecutive_zero_recovery_batches(caplog):
    """A systemic failure (e.g. an expired token) must not turn one rejected batch
    into max_per_batch sequential failures forever; isolation should give up."""
    records = [_status_mcp(f"rec{i}") for i in range(3)]
    error = OperationalError(
        "forbidden", {"status": 403, "message": "403 Client Error: Forbidden"}
    )

    mock_emitter = MagicMock()
    mock_emitter.emit_mcps.side_effect = error
    sink = _make_sink(mock_emitter)

    for _ in range(_MAX_CONSECUTIVE_ZERO_RECOVERY_ISOLATIONS):
        with pytest.raises(BatchItemFailures):
            sink._emit_batch_wrapper([(record,) for record in records])

    assert sink.report.batches_rejected == _MAX_CONSECUTIVE_ZERO_RECOVERY_ISOLATIONS
    assert sink.report.batches_rejected_while_isolation_suppressed == 0

    mock_emitter.emit_mcps.reset_mock()
    with caplog.at_level("WARNING"):
        with pytest.raises(BatchItemFailures) as exc_info:
            sink._emit_batch_wrapper([(record,) for record in records])

    assert exc_info.value.outcomes == [error, error, error]

    # Only the single failed batch call — no per-record isolation attempts.
    assert mock_emitter.emit_mcps.call_count == 1
    assert sink.report.batches_rejected == _MAX_CONSECUTIVE_ZERO_RECOVERY_ISOLATIONS
    assert sink.report.batches_rejected_while_isolation_suppressed == 1
    assert "disabling per-record isolation" in caplog.text


def test_isolation_continues_when_a_pass_recovers_at_least_one_record():
    """One recovered record per pass must reset the zero-recovery streak, so an
    unlucky run of partial failures never trips the circuit breaker."""
    poison = _status_mcp("poison")
    good = _status_mcp("good")
    poison_error = OperationalError("record rejected", {"status": 422})

    def emit_mcps(events, emit_mode=None):
        if len(events) > 1:
            raise OperationalError("batch rejected", {"status": 422})
        if events[0] is poison:
            raise poison_error
        return [MagicMock()]

    mock_emitter = MagicMock()
    mock_emitter.emit_mcps.side_effect = emit_mcps
    sink = _make_sink(mock_emitter)

    passes = _MAX_CONSECUTIVE_ZERO_RECOVERY_ISOLATIONS + 2
    for _ in range(passes):
        with pytest.raises(BatchItemFailures):
            sink._emit_batch_wrapper([(poison,), (good,)])

    # Isolation ran on every pass — it was never suppressed, because `good` was
    # recovered each time.
    assert sink.report.batches_rejected == passes
    assert sink.report.batches_rejected_while_isolation_suppressed == 0


def test_isolation_skips_records_whose_chunk_already_landed():
    """Re-emitting a landed chunk re-applies PATCH and CREATE writes, so only records
    that did not land may be re-sent."""
    records = [_status_mcp(f"rec{i}") for i in range(4)]
    record_error = OperationalError("record rejected", {"status": 422})
    mock_emitter = _scripted_emitter(
        _chunked(OperationalError("rejected", {"status": 422}), [2, 3]),
        record_error,
        None,
    )
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    assert exc_info.value.outcomes == [None, None, record_error, None]
    sent = [call.args[0] for call in mock_emitter.emit_mcps.call_args_list]
    assert sent[1:] == [[records[2]], [records[3]]]
    assert sink.report.records_isolated_after_batch_rejection == 2
    assert sink.report.records_recovered_after_batch_rejection == 1


def test_isolation_resends_only_the_unlanded_part_of_a_split_record():
    """A chunk boundary can fall inside an MCE's MCPs; its landed half is not re-sent."""
    mce = models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:foo,mce,PROD)",
            aspects=[
                models.StatusClass(removed=False),
                models.DatasetPropertiesClass(name="mce"),
            ],
        )
    )
    good = _status_mcp("good")
    # Flattened events: [mce/status, mce/datasetProperties, good]; the first landed.
    mock_emitter = _scripted_emitter(
        _chunked(OperationalError("rejected", {"status": 422}), [1, 2]), None, None
    )
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(mce,), (good,)])

    assert exc_info.value.outcomes == [None, None]
    sent = [call.args[0] for call in mock_emitter.emit_mcps.call_args_list]
    assert [[event.aspectName for event in call] for call in sent[1:]] == [
        ["datasetProperties"],
        ["status"],
    ]


def test_suppressed_isolation_still_credits_records_that_landed():
    records = [_status_mcp(f"rec{i}") for i in range(3)]
    error = _chunked(OperationalError("rejected", {"status": 422}), [1, 2])
    mock_emitter = _scripted_emitter(error)
    sink = _make_sink(mock_emitter)
    sink._isolation_suppressed = True

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    assert exc_info.value.outcomes == [None, error, error]
    assert mock_emitter.emit_mcps.call_count == 1
    assert sink.report.batches_rejected_while_isolation_suppressed == 1


def _http_error(status: int) -> OperationalError:
    response = requests.Response()
    response.status_code = status
    error = OperationalError("rejected", {"message": "no status in the body"})
    error.__cause__ = requests.HTTPError(response=response)
    return error


@pytest.mark.parametrize(
    "batch_error",
    [
        pytest.param(OperationalError("throttled", {"status": 429}), id="429"),
        pytest.param(OperationalError("unavailable", {"status": 503}), id="503"),
        pytest.param(OperationalError("unauthorized", {"status": 401}), id="401"),
        pytest.param(OperationalError("timeout", {"status": 408}), id="408"),
        pytest.param(_http_error(500), id="500-from-cause"),
        pytest.param(
            OperationalError("unreachable", {"message": "Connection refused"}),
            id="no-status",
        ),
        pytest.param(RuntimeError("unexpected"), id="not-operational"),
    ],
)
def test_isolation_skipped_when_the_rejection_is_not_record_attributable(
    batch_error: Exception,
) -> None:
    """An outage or throttle fails every record alike; re-sending each one with its own
    retry ladder would only stall the run."""
    records = [_status_mcp(f"rec{i}") for i in range(3)]
    mock_emitter = _scripted_emitter(batch_error)
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    assert exc_info.value.outcomes == [batch_error, batch_error, batch_error]
    assert mock_emitter.emit_mcps.call_count == 1
    assert sink.report.batches_rejected == 0
    assert sink.report.batches_rejected_not_record_attributable == 1


def test_isolation_skipped_still_credits_records_that_landed():
    records = [_status_mcp(f"rec{i}") for i in range(3)]
    error = _chunked(OperationalError("unavailable", {"status": 503}), [2])
    mock_emitter = _scripted_emitter(error)
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    assert exc_info.value.outcomes == [None, None, error]
    assert mock_emitter.emit_mcps.call_count == 1


def test_isolation_runs_for_a_client_error_status_read_from_the_http_cause():
    records = [_status_mcp(f"rec{i}") for i in range(2)]
    mock_emitter = _scripted_emitter(_http_error(400), None, None)
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    assert exc_info.value.outcomes == [None, None]
    assert mock_emitter.emit_mcps.call_count == 3


def test_isolation_stops_emitting_once_suppressed_mid_pass():
    """Another worker can trip the circuit breaker while this pass is running; the
    remaining records then fail with the batch error instead of being re-sent."""
    records = [_status_mcp(f"rec{i}") for i in range(4)]
    batch_error = OperationalError("rejected", {"status": 422})
    record_error = OperationalError("record rejected", {"status": 422})
    mock_emitter = MagicMock()
    sink = _make_sink(mock_emitter)

    def emit_mcps(events, emit_mode=None):
        if len(events) > 1:
            raise batch_error
        sink._isolation_suppressed = True
        raise record_error

    mock_emitter.emit_mcps.side_effect = emit_mcps

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    assert exc_info.value.outcomes == [
        record_error,
        batch_error,
        batch_error,
        batch_error,
    ]
    assert mock_emitter.emit_mcps.call_count == 2
    assert sink.report.records_isolated_after_batch_rejection == 1


def test_isolation_stops_when_the_server_starts_failing_mid_pass():
    """An outage that begins during isolation fails every remaining record alike, so
    re-sending each with its own retry ladder would only stall the run."""
    records = [_status_mcp(f"rec{i}") for i in range(4)]
    batch_error = OperationalError("rejected", {"status": 422})
    outage = OperationalError("unavailable", {"status": 503})
    mock_emitter = _scripted_emitter(batch_error, outage)
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    assert exc_info.value.outcomes == [outage, batch_error, batch_error, batch_error]
    assert mock_emitter.emit_mcps.call_count == 2


def test_shared_batch_error_reports_each_record_with_its_own_urn():
    """One batch error fanned out to several records must not leak the last record's
    URN into every report entry."""
    sink = _make_sink(MagicMock())
    sink.treat_errors_as_warnings = False
    sink.report.pending_requests = 2
    shared = OperationalError("rejected", {"status": 422})
    future: concurrent.futures.Future = concurrent.futures.Future()
    future.set_exception(shared)
    write_callback = MagicMock()

    for name in ("a", "b"):
        envelope = RecordEnvelope(_status_mcp(name), metadata={})
        sink._write_done_callback(envelope, write_callback, future)

    assert [failure["info"]["urn"] for failure in sink.report.failures] == [
        _status_mcp("a").entityUrn,
        _status_mcp("b").entityUrn,
    ]
    assert "urn" not in shared.info


def test_parse_denied_urns_splits_on_the_entry_token_not_on_commas():
    dataset = _status_mcp("a").entityUrn
    assert dataset is not None
    assert _parse_denied_urns(_denial(_JOB, dataset, _CONTAINER, _JOB)) == {
        _JOB,
        dataset,
        _CONTAINER,
    }


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            OperationalError(
                "x",
                {
                    "status": 401,
                    "message": _DENIAL_PREFIX + f"HttpStatus: 403 Urn: {_JOB}",
                },
            ),
            id="401",
        ),
        pytest.param(
            OperationalError("x", {"status": 403, "message": "Actor is not active"}),
            id="other-403",
        ),
        pytest.param(_denial(_CONTAINER, entry_status=500), id="non-403-entry"),
        pytest.param(
            OperationalError("x", {"status": 403, "message": _DENIAL_PREFIX}),
            id="empty-list",
        ),
        pytest.param(
            OperationalError(
                "x", {"status": 403, "message": _DENIAL_PREFIX + "HttpStatus: 403 Urn"}
            ),
            id="truncated",
        ),
        pytest.param(OperationalError("x", {"status": 403}), id="no-message"),
    ],
)
def test_parse_denied_urns_rejects_anything_but_a_complete_per_entity_denial(
    error: OperationalError,
) -> None:
    assert _parse_denied_urns(error) is None


def test_authorization_denial_fails_only_denied_records_and_resends_the_rest_once_in_order():
    a_status = _status_mcp("a")
    a_props = MetadataChangeProposalWrapper(
        entityUrn=a_status.entityUrn, aspect=models.DatasetPropertiesClass(name="a")
    )
    job = _entity_mcp(_JOB)
    container = _entity_mcp(_CONTAINER)
    b_status = _status_mcp("b")
    records = [a_status, job, a_props, container, b_status]
    mock_emitter = _scripted_emitter(_denial(_JOB, _CONTAINER), None)
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    assert [o is None for o in exc_info.value.outcomes] == [
        True,
        False,
        True,
        False,
        True,
    ]
    sent = [call.args[0] for call in mock_emitter.emit_mcps.call_args_list]
    # Exactly one resend, with the allowed events in their original order, so the two
    # aspects of entity "a" still arrive in the order the source produced them.
    assert sent[1:] == [[a_status, a_props, b_status]]
    assert sink.report.batches_split_on_authorization_denial == 1
    assert sink.report.records_denied_by_authorization == 2
    assert set(sink.report.authorization_denied_urns) == {_JOB, _CONTAINER}
    assert sink.report.batches_rejected == 0


def test_each_denied_record_gets_its_own_error_naming_its_entity():
    """The done callback writes the record's urn into error.info, so a shared error
    object would make every report entry name the same entity."""
    container_status = _entity_mcp(_CONTAINER)
    container_props = MetadataChangeProposalWrapper(
        entityUrn=_CONTAINER, aspect=models.ContainerPropertiesClass(name="c")
    )
    job = _entity_mcp(_JOB)
    allowed = _status_mcp("ok")
    sink = _make_sink(_scripted_emitter(_denial(_CONTAINER, _CONTAINER, _JOB), None))

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper(
            [(container_status,), (container_props,), (job,), (allowed,)]
        )

    outcomes = exc_info.value.outcomes
    denied = [o for o in outcomes[:3] if isinstance(o, OperationalError)]
    assert len(denied) == 3
    assert len({id(error) for error in denied}) == 3
    assert [error.info["status"] for error in denied] == [403, 403, 403]
    assert denied[0].message.endswith(f"HttpStatus: 403 Urn: {_CONTAINER}")
    assert denied[2].message.endswith(f"HttpStatus: 403 Urn: {_JOB}")
    assert outcomes[3] is None


def test_denial_naming_an_entity_outside_the_batch_falls_back_to_isolation():
    """If the server renders a URN differently from the client, the fast path cannot
    tell which records were denied, so it must not guess."""
    records = [_status_mcp("a"), _status_mcp("b")]
    mock_emitter = _scripted_emitter(
        _denial("urn:li:dataset:(urn:li:dataPlatform:foo,elsewhere,PROD)"), None, None
    )
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(record,) for record in records])

    assert exc_info.value.outcomes == [None, None]
    assert mock_emitter.emit_mcps.call_count == 3
    assert sink.report.batches_split_on_authorization_denial == 0
    assert sink.report.batches_rejected == 1


@pytest.mark.parametrize(
    "batch_error,expected_calls,expected_isolations",
    [
        # 401 and 5xx are not record-attributable, so the batch fails as a whole.
        pytest.param(
            OperationalError(
                "Unable to emit metadata to DataHub GMS",
                {"message": "401 Client Error: Unauthorized"},
            ),
            1,
            0,
            id="401",
        ),
        pytest.param(
            OperationalError(
                "Unable to emit metadata to DataHub GMS: Actor is not active",
                {"status": 403, "message": "Actor is not active"},
            ),
            4,
            1,
            id="other-403",
        ),
        pytest.param(
            OperationalError(
                "Unable to emit metadata to DataHub GMS: boom",
                {"status": 500, "message": "boom"},
            ),
            1,
            0,
            id="500",
        ),
    ],
)
def test_other_rejections_do_not_take_the_authorization_fast_path(
    batch_error: OperationalError, expected_calls: int, expected_isolations: int
) -> None:
    records = [_status_mcp(f"rec{i}") for i in range(3)]
    mock_emitter = _scripted_emitter(batch_error, None, None, None)
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures):
        sink._emit_batch_wrapper([(record,) for record in records])

    assert mock_emitter.emit_mcps.call_count == expected_calls
    assert sink.report.batches_split_on_authorization_denial == 0
    assert sink.report.batches_rejected == expected_isolations


def test_failed_resend_falls_back_to_isolating_the_remainder():
    a, job, b = _status_mcp("a"), _entity_mcp(_JOB), _status_mcp("b")
    b_error = OperationalError("record rejected", {"status": 422})
    mock_emitter = _scripted_emitter(
        _denial(_JOB), OperationalError("rejected", {"status": 422}), None, b_error
    )
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(a,), (job,), (b,)])

    outcomes = exc_info.value.outcomes
    assert outcomes[0] is None
    assert isinstance(outcomes[1], OperationalError)
    assert outcomes[2] is b_error
    sent = [call.args[0] for call in mock_emitter.emit_mcps.call_args_list]
    assert sent[1:] == [[a, b], [a], [b]]
    assert sink.report.batches_split_on_authorization_denial == 1
    assert sink.report.batches_rejected == 1


def test_failed_resend_on_an_outage_is_not_isolated():
    a, job, b = _status_mcp("a"), _entity_mcp(_JOB), _status_mcp("b")
    outage = OperationalError("unavailable", {"status": 503})
    mock_emitter = _scripted_emitter(_denial(_JOB), outage)
    sink = _make_sink(mock_emitter)

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(a,), (job,), (b,)])

    outcomes = exc_info.value.outcomes
    assert outcomes[0] is outage and outcomes[2] is outage
    assert isinstance(outcomes[1], OperationalError) and outcomes[1] is not outage
    assert mock_emitter.emit_mcps.call_count == 2
    assert sink.report.batches_rejected == 0
    assert sink.report.batches_rejected_not_record_attributable == 1


def test_authorization_fast_path_still_runs_when_isolation_is_suppressed():
    a, job = _status_mcp("a"), _entity_mcp(_JOB)
    mock_emitter = _scripted_emitter(_denial(_JOB), None)
    sink = _make_sink(mock_emitter)
    sink._isolation_suppressed = True

    with pytest.raises(BatchItemFailures) as exc_info:
        sink._emit_batch_wrapper([(a,), (job,)])

    assert exc_info.value.outcomes[0] is None
    assert isinstance(exc_info.value.outcomes[1], OperationalError)
    assert mock_emitter.emit_mcps.call_count == 2
    assert sink.report.batches_rejected_while_isolation_suppressed == 0


def test_denial_in_a_later_chunk_does_not_resend_the_chunk_that_landed(monkeypatch):
    """End to end through the real REST.li emitter: chunk 1 lands, chunk 2 is denied
    for one entity, and only chunk 2's allowed record is re-sent."""
    monkeypatch.setattr(rest_emitter, "BATCH_INGEST_MAX_PAYLOAD_LENGTH", 2)
    emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=False)
    records = [
        _status_mcp("c1a"),
        _status_mcp("c1b"),
        _entity_mcp(_JOB),
        _status_mcp("c2b"),
    ]
    ok = Mock(spec=requests.Response)
    ok.status_code = 200
    ok.headers = {}
    script: List[Union[requests.Response, Exception]] = [ok, _denial(_JOB), ok]

    def emit_generic(url: str, payload: str, method: str = "POST") -> requests.Response:
        outcome = script.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    sink = _make_sink(emitter)
    with patch.object(emitter, "_emit_generic", side_effect=emit_generic) as mock_emit:
        with pytest.raises(BatchItemFailures) as exc_info:
            sink._emit_batch_wrapper([(record,) for record in records])

    sent = [
        [p["entityUrn"] for p in json.loads(call.args[1])["proposals"]]
        for call in mock_emit.call_args_list
    ]
    assert sent == [
        [records[0].entityUrn, records[1].entityUrn],
        [_JOB, records[3].entityUrn],
        [records[3].entityUrn],
    ]
    assert [o is None for o in exc_info.value.outcomes] == [True, True, False, True]
