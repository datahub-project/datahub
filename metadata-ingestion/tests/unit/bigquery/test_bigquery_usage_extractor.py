from datetime import datetime, timedelta, timezone
from typing import Any, Collection, Optional, Sequence
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    AuditEvent,
    BigQueryTableRef,
    QueryEvent,
    ReadEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryUsageConfig,
    BigQueryV2Config,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder
from datahub.ingestion.source.bigquery_v2.usage import (
    BigQueryUsageExtractor,
    BigQueryUsageState,
)
from datahub.metadata.schema_classes import OperationClass, OperationTypeClass
from datahub.sql_parsing.schema_resolver import SchemaResolver
from tests.unit.bigquery.test_bigquery_audit_log_api import (
    ACTOR,
    DATASET,
    PROJECT,
    FakeLogEntry,
    bq_ref,
    exported_row,
    read_entry,
    read_metadata,
    table_ref,
    v1_query_entry,
    v2_job_metadata,
    v2_query_entry,
)

START = datetime(2023, 1, 1, tzinfo=timezone.utc)
END = datetime(2023, 1, 2, tzinfo=timezone.utc)
IN_WINDOW = START + timedelta(hours=6)


def make_config(**usage: Any) -> BigQueryV2Config:
    return BigQueryV2Config(
        start_time=START,
        end_time=END,
        max_query_duration=timedelta(hours=1),
        table_pattern={"deny": [".*\\.denied_table"]},
        usage=BigQueryUsageConfig(**usage),
    )


def make_extractor(config: Optional[BigQueryV2Config] = None) -> BigQueryUsageExtractor:
    config = config or make_config()
    report = BigQueryV2Report()
    return BigQueryUsageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )


def read_event(
    table: str,
    fields: Sequence[str] = ("id",),
    timestamp: datetime = IN_WINDOW,
    dataset: str = DATASET,
    reason: str = "JOB",
) -> ReadEvent:
    return ReadEvent(
        timestamp=timestamp,
        actor_email=ACTOR,
        resource=BigQueryTableRef.from_string_name(table_ref(table, dataset=dataset)),
        fieldsRead=list(fields),
        readReason=reason,
        jobName=f"projects/{PROJECT}/jobs/job_1",
        payload=None,
    )


def query_event(
    statement_type: str = "INSERT",
    destination: Optional[str] = "dest",
    referenced_tables: Sequence[str] = ("src",),
    job_name: Optional[str] = f"projects/{PROJECT}/jobs/job_1",
    query: str = "INSERT INTO dest SELECT * FROM src",
    query_on_view: bool = False,
) -> QueryEvent:
    return QueryEvent(
        timestamp=IN_WINDOW,
        actor_email=ACTOR,
        query=query,
        statementType=statement_type,
        project_id=PROJECT,
        job_name=job_name,
        destinationTable=bq_ref(destination) if destination else None,
        referencedTables=[bq_ref(t) for t in referenced_tables],
        start_time=IN_WINDOW,
        end_time=IN_WINDOW + timedelta(seconds=2),
        billed_bytes=4096,
        default_dataset=DATASET,
        numAffectedRows=12,
        query_on_view=query_on_view,
    )


def urn_for(table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{PROJECT}.{DATASET}.{table},PROD)"


class TestLogEntryParsing:
    def test_read_events_are_counted_and_filtered_by_table_pattern(self) -> None:
        extractor = make_extractor()

        allowed = extractor._parse_bigquery_log_entry(read_entry("t1", reason="JOB"))
        denied = extractor._parse_bigquery_log_entry(read_entry("denied_table"))

        assert allowed is not None and allowed.read_event is not None
        assert allowed.read_event.resource == bq_ref("t1")
        assert allowed.query_event is None
        assert denied is None
        assert extractor.report.num_read_events == 1
        assert extractor.report.num_filtered_read_events == 1
        assert extractor.report.read_reasons_stat["JOB"] == 1

    def test_query_entries_of_both_formats_and_unknown_entries(self) -> None:
        extractor = make_extractor()

        v1 = extractor._parse_bigquery_log_entry(v1_query_entry(destination="a"))
        v2 = extractor._parse_bigquery_log_entry(v2_query_entry(destination="b"))
        unknown = extractor._parse_bigquery_log_entry(FakeLogEntry(payload={"x": 1}))

        assert v1 is not None and v1.query_event is not None
        assert v1.query_event.destinationTable == bq_ref("a")
        assert v2 is not None and v2.query_event is not None
        assert v2.query_event.destinationTable == bq_ref("b")
        assert unknown is None
        assert extractor.report.num_query_events == 2

    def test_exported_audit_rows(self) -> None:
        extractor = make_extractor()

        read = extractor._parse_exported_bigquery_audit_metadata(
            exported_row(read_metadata(fields=["a"]), resource_name=table_ref("t1"))
        )
        query = extractor._parse_exported_bigquery_audit_metadata(
            exported_row(v2_job_metadata(destination="d"))
        )
        unknown = extractor._parse_exported_bigquery_audit_metadata(
            exported_row({"somethingElse": {}})
        )

        assert read is not None and read.read_event is not None
        assert read.read_event.fieldsRead == ["a"]
        assert query is not None and query.query_event is not None
        assert query.query_event.destinationTable == bq_ref("d")
        assert unknown is None


class TestAuditLogFetching:
    def test_entries_are_counted_and_fetched_with_padded_window(self) -> None:
        extractor = make_extractor()
        logging_client = MagicMock()
        broken = v1_query_entry(destination="a")
        del broken.payload["serviceData"]["jobCompletedEvent"]["job"]["jobStatistics"]
        logging_client.list_entries.return_value = [
            read_entry("t1"),
            FakeLogEntry(payload={"unrelated": True}),
            broken,
        ]

        with patch.object(
            BigQueryV2Config, "make_gcp_logging_client", return_value=logging_client
        ):
            events = list(extractor._get_parsed_bigquery_log_events(PROJECT))

        assert len(events) == 1 and events[0].read_event is not None
        assert extractor.report.num_usage_total_log_entries[PROJECT] == 3
        assert extractor.report.num_usage_parsed_log_entries[PROJECT] == 1
        assert len(extractor.report.warnings) == 1
        # max_query_duration pads the window so query/read events still join.
        assert extractor.report.audit_start_time == START - timedelta(hours=1)
        assert extractor.report.audit_end_time == END + timedelta(hours=1)
        log_filter = logging_client.list_entries.call_args.kwargs["filter_"]
        assert "2022-12-31T23:00:00Z" in log_filter
        assert "2023-01-02T01:00:00Z" in log_filter

    def test_capability_probe_requests_a_single_entry(self) -> None:
        extractor = make_extractor()
        logging_client = MagicMock()
        logging_client.list_entries.return_value = [read_entry("t1")]

        with patch.object(
            BigQueryV2Config, "make_gcp_logging_client", return_value=logging_client
        ):
            extractor.test_capability(PROJECT)

        assert logging_client.list_entries.call_args.kwargs["max_results"] == 1

    def test_failed_project_is_reported_and_other_projects_continue(self) -> None:
        extractor = make_extractor()
        good_client = MagicMock()
        good_client.list_entries.return_value = [read_entry("t1")]

        def logging_client_for(project_id: str) -> MagicMock:
            if project_id == "broken-project":
                raise RuntimeError("logging api disabled")
            return good_client

        with patch.object(
            BigQueryV2Config, "make_gcp_logging_client", side_effect=logging_client_for
        ):
            events = list(extractor._get_usage_events(["broken-project", PROJECT]))

        assert len(events) == 1
        assert list(extractor.report.usage_failed_extraction) == ["broken-project"]
        assert set(extractor.report.usage_extraction_sec) == {"broken-project", PROJECT}


class TestStoreUsageEvent:
    def test_only_known_non_temporary_reads_inside_the_window_are_stored(
        self,
    ) -> None:
        config = make_config()
        extractor = make_extractor(config)
        table_refs: Collection[str] = {table_ref("t1")}

        with BigQueryUsageState(config) as state:
            stored = extractor._store_usage_event(
                AuditEvent(read_event=read_event("t1", fields=["a", "b"])),
                state,
                table_refs,
            )
            too_early = extractor._store_usage_event(
                AuditEvent(read_event=read_event("t1", timestamp=START - timedelta(1))),
                state,
                table_refs,
            )
            unknown = extractor._store_usage_event(
                AuditEvent(read_event=read_event("not_ingested")), state, table_refs
            )
            temporary = extractor._store_usage_event(
                AuditEvent(read_event=read_event("t1", dataset="_tmp_dataset")),
                state,
                {table_ref("t1"), table_ref("t1", dataset="_tmp_dataset")},
            )
            query_without_job = extractor._store_usage_event(
                AuditEvent(query_event=query_event(job_name=None)), state, table_refs
            )

            assert stored
            assert not too_early and not unknown and not temporary
            assert not query_without_job
            assert len(state.read_events) == 1
            assert len(state.column_accesses) == 2
        assert extractor.report.num_usage_resources_dropped == 1


class TestOperationWorkunits:
    def _operation(
        self, extractor: BigQueryUsageExtractor, event: AuditEvent
    ) -> Optional[OperationClass]:
        wu = extractor._create_operation_workunit(
            event, {table_ref("dest"), table_ref("t1")}
        )
        if wu is None:
            return None
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert isinstance(wu.metadata.aspect, OperationClass)
        assert wu.metadata.entityUrn == urn_for(
            "dest"
        ) or wu.metadata.entityUrn == urn_for("t1")
        return wu.metadata.aspect

    def test_insert_with_read_stats_carries_query_details(self) -> None:
        extractor = make_extractor(
            make_config(
                include_operational_stats=True, include_read_operational_stats=True
            )
        )

        operation = self._operation(extractor, AuditEvent(query_event=query_event()))

        assert operation is not None
        assert operation.operationType == OperationTypeClass.INSERT
        assert operation.customOperationType is None
        assert operation.actor == extractor.identifiers.gen_user_urn(ACTOR)
        assert operation.lastUpdatedTimestamp == int(IN_WINDOW.timestamp() * 1000)
        assert operation.affectedDatasets == [urn_for("src")]
        assert operation.numAffectedRows == 12
        assert operation.customProperties == {
            "millisecondsTaken": "2000",
            "sessionId": f"projects/{PROJECT}/jobs/job_1",
            "text": "INSERT INTO dest SELECT * FROM src",
            "bytesProcessed": "4096",
            "defaultDatabase": DATASET,
        }
        assert extractor.report.operation_types_stat["INSERT"] == 1

    def test_insert_without_read_stats_has_no_custom_properties(self) -> None:
        extractor = make_extractor(make_config(include_operational_stats=True))

        operation = self._operation(extractor, AuditEvent(query_event=query_event()))

        assert operation is not None
        assert operation.operationType == OperationTypeClass.INSERT
        assert not operation.customProperties
        assert operation.numAffectedRows is None

    def test_queryless_read_is_a_custom_read_only_when_read_stats_enabled(
        self,
    ) -> None:
        event = AuditEvent(read_event=read_event("t1", fields=["a", "b"]))

        with_reads = make_extractor(
            make_config(
                include_operational_stats=True, include_read_operational_stats=True
            )
        )
        operation = self._operation(with_reads, event)
        assert operation is not None
        assert operation.operationType == OperationTypeClass.CUSTOM
        assert operation.customOperationType == "CUSTOM_READ"
        assert operation.customProperties == {"readReason": "JOB", "fieldsRead": "a,b"}

        without_reads = make_extractor(make_config(include_operational_stats=True))
        assert self._operation(without_reads, event) is None

    def test_select_is_not_an_operation_unless_read_stats_enabled(self) -> None:
        event = AuditEvent(
            query_event=query_event(statement_type="SELECT", destination="dest")
        )

        without_reads = make_extractor(make_config(include_operational_stats=True))
        assert self._operation(without_reads, event) is None

        with_reads = make_extractor(
            make_config(
                include_operational_stats=True, include_read_operational_stats=True
            )
        )
        operation = self._operation(with_reads, event)
        assert operation is not None
        assert operation.operationType == OperationTypeClass.CUSTOM
        assert operation.customOperationType == "SELECT"

    def test_operations_on_unknown_or_denied_destinations_are_dropped(self) -> None:
        extractor = make_extractor(make_config(include_operational_stats=True))

        unknown = self._operation(
            extractor, AuditEvent(query_event=query_event(destination="not_ingested"))
        )
        denied = extractor._create_operation_workunit(
            AuditEvent(query_event=query_event(destination="denied_table")),
            {table_ref("denied_table")},
        )
        no_destination = self._operation(
            extractor, AuditEvent(query_event=query_event(destination=None))
        )

        assert unknown is None and denied is None and no_destination is None
        assert extractor.report.num_usage_operations_dropped == 2


class TestViewUsage:
    def test_read_events_are_generated_for_each_table_in_a_view_query(self) -> None:
        extractor = make_extractor()
        event = query_event(
            statement_type="SELECT",
            destination=None,
            query=f"SELECT a.id FROM `{PROJECT}.{DATASET}.v1` a JOIN `{PROJECT}.{DATASET}.t2` b ON a.id = b.id",
            query_on_view=True,
        )

        generated = list(extractor.generate_read_events_from_query(event))

        resources = {e.read_event.resource for e in generated if e.read_event}
        assert resources == {bq_ref("v1"), bq_ref("t2")}
        assert all(e.read_event and e.read_event.from_query for e in generated)
        assert extractor.report.num_view_query_events_failed_sql_parsing == 0

    def test_unparseable_view_query_yields_nothing_and_is_counted(self) -> None:
        extractor = make_extractor()
        event = query_event(
            statement_type="SELECT",
            destination=None,
            query="this is not sql",
            query_on_view=True,
        )

        assert list(extractor.generate_read_events_from_query(event)) == []
        assert extractor.report.num_view_query_events_failed_sql_parsing == 1


@pytest.mark.parametrize("skip", [True, False])
def test_redundant_run_handling(skip: bool) -> None:
    config = make_config()
    report = BigQueryV2Report()
    handler = MagicMock()
    handler.suggest_run_time_window.return_value = (START, END)
    handler.should_skip_this_run.return_value = skip
    extractor = BigQueryUsageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
        redundant_run_skip_handler=handler,
    )
    logging_client = MagicMock()
    logging_client.list_entries.return_value = []

    with patch.object(
        BigQueryV2Config, "make_gcp_logging_client", return_value=logging_client
    ):
        list(extractor.get_usage_workunits([PROJECT], [table_ref("t1")]))

    assert logging_client.list_entries.called is not skip
    assert handler.update_state.called is not skip


class TestExportedAuditMetadata:
    def test_exported_rows_are_fetched_from_the_configured_datasets(self) -> None:
        config = BigQueryV2Config(
            start_time=START,
            end_time=END,
            max_query_duration=timedelta(hours=1),
            use_exported_bigquery_audit_metadata=True,
            bigquery_audit_metadata_datasets=["audit_project.audit_dataset"],
        )
        extractor = make_extractor(config)
        bq_client = MagicMock()
        bq_client.query.return_value = [
            exported_row(v2_job_metadata(destination="dest", referenced_tables=["src"]))
        ]

        with patch.object(
            BigQueryV2Config, "get_bigquery_client", return_value=bq_client
        ):
            events = list(extractor._get_usage_events([PROJECT]))

        assert len(events) == 1 and events[0].query_event is not None
        query_sql = bq_client.query.call_args.args[0]
        assert "audit_project.audit_dataset" in query_sql
        # With exported metadata the project list is irrelevant: counts are keyed on "*".
        assert extractor.report.num_usage_total_log_entries["*"] == 1


def test_query_text_hash_collisions_are_kept_apart() -> None:
    config = make_config()
    extractor = make_extractor(config)
    collision_hash = "deadbeef"

    with BigQueryUsageState(config) as state:
        state.queries[collision_hash] = "a different query with the same hash"
        with patch("datahub.ingestion.source.bigquery_v2.usage.hashlib.md5") as md5:
            md5.return_value.hexdigest.return_value = collision_hash
            stored = extractor._store_usage_event(
                AuditEvent(query_event=query_event(query="SELECT 1")),
                state,
                {table_ref("dest"), table_ref("src")},
            )

        assert stored
        assert extractor.report.num_usage_query_hash_collisions == 1
        key = next(iter(state.query_events.values())).query
        assert key != collision_hash
        assert extractor.uuid_to_query[key] == "SELECT 1"


def test_empty_query_on_view_has_no_tables() -> None:
    assert make_extractor().get_tables_from_query("", default_project=PROJECT) == []
