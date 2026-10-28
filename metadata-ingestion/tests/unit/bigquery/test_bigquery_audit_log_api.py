import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence
from unittest.mock import MagicMock

import pytest
from google.cloud.bigquery.table import Row

from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
    QueryEvent,
    ReadEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_audit_log_api import (
    BigQueryAuditLogApi,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQueryAuditLogApiPerfReport,
    BigQueryV2Report,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_DATE_SHARD_FORMAT,
    BQ_DATETIME_FORMAT,
    BigQueryIdentifierBuilder,
)
from datahub.ingestion.source.bigquery_v2.queries import (
    bigquery_audit_metadata_query_template_lineage,
    bigquery_audit_metadata_query_template_usage,
)

PROJECT = "my-project"
DATASET = "my_dataset"
ACTOR = "user@example.com"
TS = datetime(2023, 1, 2, 12, 0, tzinfo=timezone.utc)
START_TIME = "2023-01-02T12:00:00Z"
END_TIME = "2023-01-02T12:00:01Z"


# ---------------------------------------------------------------------------
# Raw audit-log builders. These are shared with the lineage and usage
# extractor tests so every test parses the same shapes that GCP emits.
# ---------------------------------------------------------------------------


@dataclass
class FakeLogEntry:
    """The subset of a google.cloud.logging LogEntry the parsers read."""

    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = TS


def table_ref(table: str, project: str = PROJECT, dataset: str = DATASET) -> str:
    return f"projects/{project}/datasets/{dataset}/tables/{table}"


def _table_spec(table: str) -> Dict[str, str]:
    return {"projectId": PROJECT, "datasetId": DATASET, "tableId": table}


def v1_query_entry(
    query: str = "SELECT 1",
    statement_type: str = "SELECT",
    destination: Optional[str] = None,
    referenced_tables: Sequence[str] = (),
    referenced_views: Sequence[str] = (),
    job_id: str = "job_1",
    project: str = PROJECT,
) -> FakeLogEntry:
    """A legacy AuditData (serviceData.jobCompletedEvent) entry."""
    query_conf: Dict[str, Any] = {
        "query": query,
        "statementType": statement_type,
        "defaultDataset": {},
    }
    if destination:
        query_conf["destinationTable"] = _table_spec(destination)
    return FakeLogEntry(
        payload={
            "authenticationInfo": {"principalEmail": ACTOR},
            "serviceData": {
                "jobCompletedEvent": {
                    "job": {
                        "jobName": {"projectId": project, "jobId": job_id},
                        "jobConfiguration": {"query": query_conf},
                        "jobStatistics": {
                            "startTime": START_TIME,
                            "endTime": END_TIME,
                            "totalBilledBytes": "2048",
                            "queryOutputRowCount": "7",
                            "referencedTables": [
                                _table_spec(t) for t in referenced_tables
                            ],
                            "referencedViews": [
                                _table_spec(v) for v in referenced_views
                            ],
                        },
                    }
                }
            },
        }
    )


def v2_job_metadata(
    query: str = "SELECT 1",
    statement_type: str = "SELECT",
    destination: Optional[str] = None,
    referenced_tables: Sequence[str] = (),
    referenced_views: Sequence[str] = (),
    job_id: str = "job_1",
    project: str = PROJECT,
) -> Dict[str, Any]:
    """The BigQueryAuditMetadata `jobChange` payload shared by v2 log entries
    and exported audit rows."""
    return {
        "jobChange": {
            "job": {
                "jobName": f"projects/{project}/jobs/{job_id}",
                "jobConfig": {
                    "queryConfig": {
                        "query": query,
                        "statementType": statement_type,
                        "destinationTable": table_ref(destination)
                        if destination
                        else None,
                    }
                },
                "jobStats": {
                    "startTime": START_TIME,
                    "endTime": END_TIME,
                    "queryStats": {
                        "referencedTables": [table_ref(t) for t in referenced_tables],
                        "referencedViews": [table_ref(v) for v in referenced_views],
                        "outputRowCount": "7",
                        "totalBilledBytes": "2048",
                    },
                },
            }
        }
    }


def v2_query_entry(**kwargs: Any) -> FakeLogEntry:
    return FakeLogEntry(
        payload={
            "authenticationInfo": {"principalEmail": ACTOR},
            "metadata": v2_job_metadata(**kwargs),
        }
    )


def read_metadata(
    fields: Sequence[str] = ("id",),
    reason: str = "JOB",
    job_name: Optional[str] = f"projects/{PROJECT}/jobs/job_1",
) -> Dict[str, Any]:
    read_info: Dict[str, Any] = {"fields": list(fields), "reason": reason}
    if job_name:
        read_info["jobName"] = job_name
    return {"tableDataRead": read_info}


def read_entry(
    table: str,
    fields: Sequence[str] = ("id",),
    reason: str = "JOB",
    dataset: str = DATASET,
    timestamp: datetime = TS,
) -> FakeLogEntry:
    return FakeLogEntry(
        payload={
            "authenticationInfo": {"principalEmail": ACTOR},
            "resourceName": table_ref(table, dataset=dataset),
            "metadata": read_metadata(fields=fields, reason=reason),
        },
        timestamp=timestamp,
    )


def exported_row(metadata: Dict[str, Any], resource_name: Optional[str] = None) -> Row:
    """A row as returned by the exported-audit-log query templates."""
    proto_payload: Dict[str, Any] = {"authenticationInfo": {"principalEmail": ACTOR}}
    if resource_name:
        proto_payload["resourceName"] = resource_name
    return Row(
        (
            TS,
            "projects/p/logs/data_access",
            "insert-1",
            proto_payload,
            json.dumps(metadata),
        ),
        {"timestamp": 0, "logName": 1, "insertId": 2, "protoPayload": 3, "metadata": 4},
    )


def bq_ref(table: str) -> BigQueryTableRef:
    return BigQueryTableRef.from_string_name(table_ref(table))


@pytest.fixture
def shard_suffix() -> Iterator[str]:
    """The shard suffix is process-wide state set by whichever
    BigQueryIdentifierBuilder was constructed last, so pin it through the
    public config knob for this test and restore it afterwards."""
    previous = BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX
    BigQueryIdentifierBuilder(
        BigQueryV2Config(enable_legacy_sharded_table_support=False),
        BigQueryV2Report(),
    )
    yield BigqueryTableIdentifier.DEFAULT_BQ_SHARDED_TABLE_SUFFIX
    BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX = previous


# ---------------------------------------------------------------------------
# Query templates (queries.py)
# ---------------------------------------------------------------------------

TEMPLATES = [
    bigquery_audit_metadata_query_template_lineage,
    bigquery_audit_metadata_query_template_usage,
]


class TestAuditMetadataQueryTemplates:
    @pytest.mark.parametrize("template", TEMPLATES)
    def test_date_sharded_tables_use_wildcard_and_suffix_filter(
        self, template: Callable[[str, bool, Optional[int]], str]
    ) -> None:
        query = template("p.audit", True, None)

        assert "`p.audit.cloudaudit_googleapis_com_data_access_*`" in query
        assert "_TABLE_SUFFIX BETWEEN" in query
        assert "limit" not in query.lower().split("qualify")[-1]

    @pytest.mark.parametrize("template", TEMPLATES)
    def test_partitioned_table_with_limit(
        self, template: Callable[[str, bool, Optional[int]], str]
    ) -> None:
        query = template("p.audit", False, 5)

        assert "`p.audit.cloudaudit_googleapis_com_data_access`" in query
        assert "_TABLE_SUFFIX" not in query
        assert "limit 5" in query


# ---------------------------------------------------------------------------
# BigQueryAuditLogApi
# ---------------------------------------------------------------------------


def _api(rate_limit: bool = False) -> BigQueryAuditLogApi:
    return BigQueryAuditLogApi(
        report=BigQueryAuditLogApiPerfReport(),
        rate_limit=rate_limit,
        requests_per_min=600,
    )


class TestExportedAuditMetadataApi:
    def test_no_datasets_configured_yields_nothing(self) -> None:
        client = MagicMock()

        rows = list(
            _api().get_exported_bigquery_audit_metadata(
                bigquery_client=client,
                bigquery_audit_metadata_query_template=bigquery_audit_metadata_query_template_lineage,
                bigquery_audit_metadata_datasets=None,
                use_date_sharded_audit_log_tables=False,
                start_time=TS,
                end_time=TS,
            )
        )

        assert rows == []
        client.query.assert_not_called()

    def test_queries_each_dataset_with_formatted_time_window(self) -> None:
        api = _api()
        client = MagicMock()
        client.query.side_effect = [["row1"], ["row2", "row3"]]
        start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end = datetime(2023, 1, 3, tzinfo=timezone.utc)

        rows = list(
            api.get_exported_bigquery_audit_metadata(
                bigquery_client=client,
                bigquery_audit_metadata_query_template=bigquery_audit_metadata_query_template_lineage,
                bigquery_audit_metadata_datasets=["p.a", "p.b"],
                use_date_sharded_audit_log_tables=True,
                start_time=start,
                end_time=end,
                limit=10,
            )
        )

        assert rows == ["row1", "row2", "row3"]
        queries: List[str] = [call.args[0] for call in client.query.call_args_list]
        assert "`p.a.cloudaudit_googleapis_com_data_access_*`" in queries[0]
        assert "`p.b.cloudaudit_googleapis_com_data_access_*`" in queries[1]
        for query in queries:
            assert start.strftime(BQ_DATETIME_FORMAT) in query
            assert end.strftime(BQ_DATETIME_FORMAT) in query
            assert start.strftime(BQ_DATE_SHARD_FORMAT) in query
            assert end.strftime(BQ_DATE_SHARD_FORMAT) in query
            assert "{" not in query, "unfilled template placeholder"
            assert "limit 10" in query
        assert api.report.num_get_exported_log_entries_api_requests == 1

    def test_rate_limited_iteration_yields_every_row(self) -> None:
        client = MagicMock()
        client.query.return_value = ["r1", "r2", "r3"]

        rows = list(
            _api(rate_limit=True).get_exported_bigquery_audit_metadata(
                bigquery_client=client,
                bigquery_audit_metadata_query_template=bigquery_audit_metadata_query_template_usage,
                bigquery_audit_metadata_datasets=["p.a"],
                use_date_sharded_audit_log_tables=False,
                start_time=TS,
                end_time=TS,
            )
        )

        assert rows == ["r1", "r2", "r3"]


class TestGcpLoggingApi:
    def test_passes_filter_page_size_and_limit_to_client(self) -> None:
        api = _api()
        client = MagicMock()
        entries = [read_entry("t1"), read_entry("t2")]
        client.list_entries.return_value = iter(entries)

        yielded = list(
            api.get_bigquery_log_entries_via_gcp_logging(
                client, filter="my-filter", log_page_size=500, limit=1
            )
        )

        assert yielded == entries
        client.list_entries.assert_called_once_with(
            filter_="my-filter", page_size=500, max_results=1
        )
        assert api.report.num_list_log_entries_api_requests == 1


# ---------------------------------------------------------------------------
# Entry parsing (bigquery_audit.py)
# ---------------------------------------------------------------------------


class TestQueryEventParsing:
    def test_v1_entry_parses_job_tables_and_sanitizes_shards(
        self, shard_suffix: str
    ) -> None:
        entry = v1_query_entry(
            query="INSERT INTO t SELECT 1",
            statement_type="INSERT",
            destination="events_20230101",
            referenced_tables=["src_20230101", "src_20230102"],
        )
        assert QueryEvent.get_missing_key_entry(entry) is None

        event = QueryEvent.from_entry(entry)

        assert event.project_id == PROJECT
        assert event.job_name == f"projects/{PROJECT}/jobs/job_1"
        assert event.actor_email == ACTOR
        assert event.statementType == "INSERT"
        assert event.numAffectedRows == 7
        assert event.start_time == datetime(2023, 1, 2, 12, 0, tzinfo=timezone.utc)
        assert event.end_time == datetime(2023, 1, 2, 12, 0, 1, tzinfo=timezone.utc)
        # Shard suffixes collapse to the logical table so lineage/usage merge
        # across daily shards.
        assert event.destinationTable == bq_ref("events_yyyymmdd")
        assert event.referencedTables == [
            bq_ref("src_yyyymmdd"),
            bq_ref("src_yyyymmdd"),
        ]
        assert not event.query_on_view
        assert event.payload is None

    def test_v2_entry_parses_views_and_project_from_job_name(self) -> None:
        entry = v2_query_entry(
            destination="out",
            referenced_tables=["t"],
            referenced_views=["v"],
            project="other-project",
        )
        assert QueryEvent.get_missing_key_entry(entry) == "serviceData"
        assert QueryEvent.get_missing_key_entry_v2(entry) is None

        event = QueryEvent.from_entry_v2(entry, debug_include_full_payloads=True)

        assert event.project_id == "other-project"
        assert event.job_name == "projects/other-project/jobs/job_1"
        assert event.destinationTable == bq_ref("out")
        assert event.referencedTables == [bq_ref("t")]
        assert event.referencedViews == [bq_ref("v")]
        assert event.query_on_view
        assert event.billed_bytes == 2048
        assert event.payload is entry.payload

    def test_exported_row_parses_and_reports_missing_keys(self) -> None:
        row = exported_row(
            v2_job_metadata(statement_type="CREATE_TABLE_AS_SELECT", destination="out")
        )
        assert QueryEvent.get_missing_key_exported_bigquery_audit_metadata(row) is None

        event = QueryEvent.from_exported_bigquery_audit_metadata(row)

        assert event.project_id == PROJECT
        assert event.timestamp == TS
        assert event.statementType == "CREATE_TABLE_AS_SELECT"
        assert event.destinationTable == bq_ref("out")
        assert event.end_time == datetime(2023, 1, 2, 12, 0, 1, tzinfo=timezone.utc)

        read_row = exported_row(read_metadata(), resource_name=table_ref("t"))
        assert (
            QueryEvent.get_missing_key_exported_bigquery_audit_metadata(read_row)
            == "jobChange"
        )


class TestReadEventParsing:
    def test_job_read_keeps_job_name_and_strips_partition_decorator(self) -> None:
        entry = read_entry("t$__UNPARTITIONED__", fields=["a", "b"], reason="JOB")
        assert ReadEvent.get_missing_key_entry(entry) is None

        event = ReadEvent.from_entry(entry)

        assert event.resource == bq_ref("t")
        assert event.fieldsRead == ["a", "b"]
        assert event.readReason == "JOB"
        assert event.jobName == f"projects/{PROJECT}/jobs/job_1"
        assert event.actor_email == ACTOR
        assert not event.from_query

    def test_non_job_read_has_no_job_name(self) -> None:
        event = ReadEvent.from_entry(read_entry("t", reason="TABLEDATA_LIST_REQUEST"))

        assert event.readReason == "TABLEDATA_LIST_REQUEST"
        assert event.jobName is None

    def test_query_entries_are_not_read_events(self) -> None:
        assert ReadEvent.get_missing_key_entry(v1_query_entry()) == "metadata"
        assert ReadEvent.get_missing_key_entry(v2_query_entry()) == "tableDataRead"

    def test_exported_read_row(self) -> None:
        row = exported_row(read_metadata(fields=["x"]), resource_name=table_ref("t"))
        assert ReadEvent.get_missing_key_exported_bigquery_audit_metadata(row) is None

        event = ReadEvent.from_exported_bigquery_audit_metadata(row)

        assert event.resource == bq_ref("t")
        assert event.fieldsRead == ["x"]
        assert event.timestamp == TS
        assert event.jobName == f"projects/{PROJECT}/jobs/job_1"

        query_row = exported_row(v2_job_metadata())
        assert (
            ReadEvent.get_missing_key_exported_bigquery_audit_metadata(query_row)
            == "tableDataRead"
        )
