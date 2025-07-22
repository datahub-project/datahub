from datetime import datetime

from datahub.ingestion.source.snowflake.stored_proc_lineage import (
    StoredProcCall,
    StoredProcLineageTracker,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import PreparsedQuery

# Reused test constants
_PLATFORM = "snowflake"
_ROOT_QUERY_ID = "sp_root_123"

_STORED_PROC_CALL = StoredProcCall(
    snowflake_root_query_id=_ROOT_QUERY_ID,
    query_text="CALL analytics.user_analysis_proc();",
    timestamp=datetime(2023, 1, 1, 12, 0, 0),
    user=CorpUserUrn("analyst"),
    default_db="ANALYTICS_DB",
    default_schema="PUBLIC",
)

_RELATED_QUERY = PreparsedQuery(
    query_id="query_456",
    query_text="SELECT * FROM users",
    upstreams=["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"],
    downstream="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.results,PROD)",
    query_count=1,
    user=CorpUserUrn("analyst"),
    timestamp=datetime(2023, 1, 1, 12, 0, 0),
    extra_info={"snowflake_root_query_id": _ROOT_QUERY_ID},
)


def test_stored_procedure_call_tracking_increments_counter() -> None:
    with StoredProcLineageTracker(platform=_PLATFORM) as tracker:
        tracker.add_stored_proc_call(_STORED_PROC_CALL)

        assert tracker.report.num_stored_proc_calls == 1
        assert _ROOT_QUERY_ID in tracker._stored_proc_execution_lineage


def test_related_query_with_matching_root_id_links_successfully() -> None:
    with StoredProcLineageTracker(platform=_PLATFORM) as tracker:
        tracker.add_stored_proc_call(_STORED_PROC_CALL)

        result = tracker.add_related_query(_RELATED_QUERY)

        assert result is True
        execution = tracker._stored_proc_execution_lineage[_ROOT_QUERY_ID]
        assert execution.inputs == _RELATED_QUERY.upstreams
        assert execution.outputs == [_RELATED_QUERY.downstream]


def test_unrelated_query_returns_false() -> None:
    with StoredProcLineageTracker(platform=_PLATFORM) as tracker:
        result = tracker.add_related_query(_RELATED_QUERY)
        assert result is False


def test_query_without_extra_info_returns_false() -> None:
    with StoredProcLineageTracker(platform=_PLATFORM) as tracker:
        tracker.add_stored_proc_call(_STORED_PROC_CALL)

        query = PreparsedQuery(
            query_id="query_789",
            query_text="SELECT 1",
            upstreams=[],
            downstream=None,
            query_count=1,
            user=CorpUserUrn("test"),
            timestamp=datetime(2023, 1, 1, 12, 0, 0),
            extra_info=None,
        )

        result = tracker.add_related_query(query)
        assert result is False


def test_lineage_entries_built_with_correct_attributes() -> None:
    with StoredProcLineageTracker(platform=_PLATFORM) as tracker:
        tracker.add_stored_proc_call(_STORED_PROC_CALL)
        tracker.add_related_query(_RELATED_QUERY)

        entries = list(tracker.build_merged_lineage_entries())

        assert len(entries) == 1
        entry = entries[0]
        assert entry.query_text == _STORED_PROC_CALL.query_text
        assert entry.upstreams == _RELATED_QUERY.upstreams
        assert entry.downstream == _RELATED_QUERY.downstream


def test_stored_procedure_without_inputs_skipped() -> None:
    with StoredProcLineageTracker(platform=_PLATFORM) as tracker:
        tracker.add_stored_proc_call(_STORED_PROC_CALL)

        entries = list(tracker.build_merged_lineage_entries())

        assert len(entries) == 0
        assert tracker.report.num_stored_proc_calls_with_no_inputs == 1


def test_stored_procedure_without_outputs_skipped() -> None:
    with StoredProcLineageTracker(platform=_PLATFORM) as tracker:
        tracker.add_stored_proc_call(_STORED_PROC_CALL)

        query = PreparsedQuery(
            query_id="query_no_output",
            query_text="SELECT * FROM source",
            upstreams=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source,PROD)"
            ],
            downstream=None,
            query_count=1,
            user=CorpUserUrn("test"),
            timestamp=datetime(2023, 1, 1, 12, 0, 0),
            extra_info={"snowflake_root_query_id": _ROOT_QUERY_ID},
        )

        tracker.add_related_query(query)
        entries = list(tracker.build_merged_lineage_entries())

        assert len(entries) == 0
        assert tracker.report.num_stored_proc_calls_with_no_outputs == 1
