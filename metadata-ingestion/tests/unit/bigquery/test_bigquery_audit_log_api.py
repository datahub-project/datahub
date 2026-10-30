"""Audit-log access (queries.py templates, BigQueryAuditLogApi) and parsing of
the raw entries into QueryEvent / ReadEvent (bigquery_audit.py)."""

from datetime import datetime, timezone
from typing import Callable, List, Optional
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.bigquery_v2.bigquery_audit import QueryEvent, ReadEvent
from datahub.ingestion.source.bigquery_v2.bigquery_audit_log_api import (
    BigQueryAuditLogApi,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQueryAuditLogApiPerfReport,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_DATE_SHARD_FORMAT,
    BQ_DATETIME_FORMAT,
)
from datahub.ingestion.source.bigquery_v2.queries import (
    bigquery_audit_metadata_query_template_lineage,
    bigquery_audit_metadata_query_template_usage,
)
from tests.unit.bigquery.conftest import (
    ACTOR,
    PROJECT,
    TS,
    bq_ref,
    exported_row,
    read_entry,
    read_metadata,
    table_ref,
    v1_query_entry,
    v2_job_metadata,
    v2_query_entry,
)

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

    @pytest.mark.xfail(
        strict=True,
        raises=pytest.fail.Exception,
        reason="validate_bigquery_audit_metadata_datasets is a field validator on "
        "bigquery_audit_metadata_datasets that reads use_exported_bigquery_audit_metadata "
        "from info.data, but that field is declared after it and so is never present "
        "when the validator runs; the guard cannot fire.",
    )
    def test_exported_audit_metadata_requires_datasets(self) -> None:
        with pytest.raises(ValidationError):
            BigQueryV2Config(use_exported_bigquery_audit_metadata=True)

    def test_queries_each_dataset_with_formatted_time_window(self) -> None:
        """Each configured dataset gets its own query with the window rendered in
        both the timestamp and the _TABLE_SUFFIX formats, and no template
        placeholder is left behind."""
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


class TestQueryEventParsing:
    def test_v1_entry_parses_job_tables_and_sanitizes_shards(
        self, shard_suffix: str
    ) -> None:
        """Shard suffixes collapse to the logical table so lineage and usage
        merge across daily shards."""
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
        assert event.destinationTable == bq_ref(f"events{shard_suffix}")
        assert event.referencedTables == [
            bq_ref(f"src{shard_suffix}"),
            bq_ref(f"src{shard_suffix}"),
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
    def test_job_read_keeps_job_name_and_strips_non_date_partition_decorator(
        self,
    ) -> None:
        entry = read_entry("t$__UNPARTITIONED__", fields=["a", "b"], reason="JOB")
        assert ReadEvent.get_missing_key_entry(entry) is None

        event = ReadEvent.from_entry(entry)

        assert event.resource == bq_ref("t")
        assert event.fieldsRead == ["a", "b"]
        assert event.readReason == "JOB"
        assert event.jobName == f"projects/{PROJECT}/jobs/job_1"
        assert event.actor_email == ACTOR
        assert not event.from_query

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="BigqueryTableIdentifier.is_sharded_table matches the date in a "
        "partition decorator (t$20230101) as a shard suffix, so the sanitized "
        "ref becomes t_yyyymmdd instead of t and the read is attributed to a "
        "table that was never ingested (dropped from usage).",
    )
    def test_date_partition_decorator_is_stripped_not_treated_as_shard(
        self, shard_suffix: str
    ) -> None:
        event = ReadEvent.from_entry(read_entry("t$20230101"))

        assert event.resource == bq_ref("t")

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
