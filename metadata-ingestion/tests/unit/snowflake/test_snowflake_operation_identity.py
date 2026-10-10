from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock, patch

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
    Both queries start in the same millisecond and write the same table, so urn and
    timestampMillis are identical and messageId is the only thing keeping them apart.
    Two concurrent loads into one table is the ordinary case for this, not a corner.
    """
    aspects = _operations_for([_event("q1", minute=0), _event("q2", minute=0)])

    assert len(aspects) == 2
    assert len({a.timestampMillis for a in aspects}) == 1, (
        "precondition: both operations must share a timestamp for this to test anything"
    )
    doc_keys = {(a.timestampMillis, a.messageId) for a in aspects}
    assert len(doc_keys) == 2, (
        f"Both operations collapse to the same timeseries document: {doc_keys}"
    )


def test_operation_timestamp_is_the_write_time_not_the_ingestion_time() -> None:
    """timestampMillis must carry the time of the write itself.

    It is part of the timeseries docId, and it is the field the 30-day write window
    is evaluated against -- so stamping it with the ingestion time makes the window
    mean "last 30 days of ingestion" rather than "last 30 days of writes".
    """
    event = _event("q1", minute=0)
    assert event.query_start_time is not None
    expected = int(event.query_start_time.timestamp() * 1000)

    (aspect,) = _operations_for([event])

    assert aspect.timestampMillis == expected
    assert aspect.lastUpdatedTimestamp == expected


def test_operation_document_identity_is_stable_across_runs() -> None:
    """Re-ingesting the same window must not duplicate the write.

    Both halves of the docId key that we control -- timestampMillis and messageId --
    have to be derived from the operation itself, not from the ingestion run. A
    stable messageId alone is not enough: timestampMillis is hashed into the docId
    too, so an ingestion-time stamp gives the same write a fresh document on every
    overlapping run.
    """
    with patch("time.time", return_value=1_000.0):
        first = _operations_for([_event("q1", minute=0)])
    with patch("time.time", return_value=999_999.0):
        second = _operations_for([_event("q1", minute=0)])

    assert first[0].messageId is not None
    assert (first[0].timestampMillis, first[0].messageId) == (
        second[0].timestampMillis,
        second[0].messageId,
    )
