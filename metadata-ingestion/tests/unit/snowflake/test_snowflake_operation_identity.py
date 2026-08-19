from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeJoinedAccessEvent,
    SnowflakeUsageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import OperationClass

TABLE = "test_db.test_schema.events"


def _make_extractor() -> SnowflakeUsageExtractor:
    config = SnowflakeV2Config(account_id="test-account", email_domain="example.com")
    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=report
    )
    return SnowflakeUsageExtractor(
        config=config,
        report=report,
        connection=MagicMock(),
        filter=SnowflakeFilter(filter_config=config, structured_reporter=report),
        identifiers=identifiers,
        redundant_run_skip_handler=None,
    )


def _event(query_id: str, minute: int) -> SnowflakeJoinedAccessEvent:
    """Two COPY executions into the same table, minutes apart."""
    return SnowflakeJoinedAccessEvent(
        query_id=query_id,
        query_start_time=datetime(2026, 8, 19, 10, minute, 0, tzinfo=timezone.utc),
        query_text="COPY INTO events FROM @my_stage",
        query_type="COPY",
        base_objects_accessed=[],
        direct_objects_accessed=[],
        objects_modified=[
            {"objectDomain": "Table", "objectName": TABLE.upper(), "columns": []}
        ],
        user_name="svc_loader",
        email="svc_loader@example.com",
        role_name="LOADER",
    )


def _operations_for(events: List[SnowflakeJoinedAccessEvent]) -> List[OperationClass]:
    extractor = _make_extractor()
    aspects: List[OperationClass] = []
    for event in events:
        for wu in extractor._get_operation_aspect_work_unit(
            event, discovered_datasets=[TABLE]
        ):
            mcp = wu.metadata
            assert isinstance(mcp, MetadataChangeProposalWrapper)
            assert isinstance(mcp.aspect, OperationClass)
            aspects.append(mcp.aspect)
    return aspects


def test_operations_for_same_table_are_distinct_timeseries_documents() -> None:
    """Two distinct write operations on one table must not collapse into one ES doc.

    GMS derives the timeseries docId from
    (timestampMillis, eventGranularity, urn, collectionId, messageId, partitionSpec).
    Every Snowflake operation aspect in a run is stamped with `timestampMillis =
    now()`, so without a messageId two operations on the same table that are emitted
    within the same millisecond overwrite each other and the write is lost.
    """
    aspects = _operations_for([_event("q1", minute=0), _event("q2", minute=30)])

    assert len(aspects) == 2
    # Same table, same run -> identical urn, and timestampMillis may well be identical.
    doc_keys = {(a.timestampMillis, a.messageId) for a in aspects}
    assert len(doc_keys) == 2, (
        f"Both operations collapse to the same timeseries document: {doc_keys}"
    )


def test_operation_message_id_is_stable_across_runs() -> None:
    """Re-ingesting the same window must not duplicate the write.

    messageId has to be derived from the operation itself (query id + table), not
    from the ingestion run, or an overlapping window double counts writes.
    """
    first = _operations_for([_event("q1", minute=0)])
    second = _operations_for([_event("q1", minute=0)])

    assert first[0].messageId is not None
    assert first[0].messageId == second[0].messageId
