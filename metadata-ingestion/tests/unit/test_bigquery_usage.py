import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Iterable
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from datahub.configuration.time_window_config import BucketDuration
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    AuditEvent,
    BigqueryTableIdentifier,
    BigQueryTableRef,
    QueryEvent,
    ReadEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryUsageConfig,
    BigQueryV2Config,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.usage import (
    OPERATION_STATEMENT_TYPES,
    BigQueryUsageExtractor,
)
from datahub.metadata.schema_classes import (
    DatasetFieldUsageCountsClass,
    DatasetUsageStatisticsClass,
    DatasetUserUsageCountsClass,
    OperationClass,
    TimeWindowSizeClass,
)
from datahub.testing.compare_metadata_json import diff_metadata_json
from tests.performance.bigquery.bigquery_events import generate_events, ref_from_table
from tests.performance.data_generation import generate_data, generate_queries
from tests.performance.data_model import Container, FieldAccess, Query, Table, View

PROJECT_1 = "project-1"
PROJECT_2 = "project-2"
ACTOR_1, ACTOR_1_URN = "a@acryl.io", "urn:li:corpuser:a"
ACTOR_2, ACTOR_2_URN = "b@acryl.io", "urn:li:corpuser:b"
DATABASE_1 = Container("database_1")
DATABASE_2 = Container("database_2")
TABLE_1 = Table("table_1", DATABASE_1, columns=["id", "name", "age"], upstreams=[])
TABLE_2 = Table(
    "table_2", DATABASE_1, columns=["id", "table_1_id", "value"], upstreams=[]
)
VIEW_1 = View(
    name="view_1",
    container=DATABASE_1,
    columns=["id", "name", "total"],
    definition="VIEW DEFINITION 1",
    upstreams=[TABLE_1, TABLE_2],
)
ALL_TABLES = [TABLE_1, TABLE_2, VIEW_1]

TABLE_TO_PROJECT = {
    TABLE_1.name: PROJECT_1,
    TABLE_2.name: PROJECT_2,
    VIEW_1.name: PROJECT_1,
}
TABLE_REFS = {
    table.name: str(ref_from_table(table, TABLE_TO_PROJECT)) for table in ALL_TABLES
}

FROZEN_TIME = datetime(year=2023, month=2, day=1, tzinfo=timezone.utc)
TS_1 = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)
TS_2 = datetime(year=2023, month=1, day=2, tzinfo=timezone.utc)


def query_table_1_a(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT * FROM table_1",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[
            FieldAccess("id", TABLE_1),
            FieldAccess("name", TABLE_1),
            FieldAccess("age", TABLE_1),
        ],
    )


def query_table_1_b(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT name FROM table_1",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[FieldAccess("name", TABLE_1)],
    )


def query_table_2(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT * FROM table_2",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[
            FieldAccess("id", TABLE_2),
            FieldAccess("table_1_id", TABLE_2),
            FieldAccess("value", TABLE_2),
        ],
    )


def query_tables_1_and_2(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT t1.id, t1.name, t2.id, t2.value FROM table_1 t1 JOIN table_2 t2 ON table_1.id = table_2.table_1_id",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[
            FieldAccess("id", TABLE_1),
            FieldAccess("name", TABLE_1),
            FieldAccess("id", TABLE_2),
            FieldAccess("value", TABLE_2),
        ],
    )


def query_view_1(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT * FROM project-1.database_1.view_1",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[
            FieldAccess("id", VIEW_1),
            FieldAccess("name", VIEW_1),
            FieldAccess("total", VIEW_1),
        ],
    )


def query_view_1_and_table_1(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="""SELECT v.id, v.name, v.total, t.name as name1
        FROM
            `project-1.database_1.view_1` as v
        inner join
            `project-1.database_1.table_1` as t
        on
            v.id=t.id""",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[
            FieldAccess("id", VIEW_1),
            FieldAccess("name", VIEW_1),
            FieldAccess("total", VIEW_1),
            FieldAccess("name", TABLE_1),
        ],
    )


def make_usage_workunit(
    table: Table, dataset_usage_statistics: DatasetUsageStatisticsClass
) -> MetadataWorkUnit:
    resource = BigQueryTableRef.from_string_name(TABLE_REFS[table.name])
    return MetadataChangeProposalWrapper(
        entityUrn=resource.to_urn("PROD"),
        aspectName=dataset_usage_statistics.get_aspect_name(),
        aspect=dataset_usage_statistics,
    ).as_workunit()


def make_operational_workunit(
    resource: str, operation: OperationClass
) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=BigQueryTableRef.from_string_name(resource).to_urn("PROD"),
        aspectName=operation.get_aspect_name(),
        aspect=operation,
    ).as_workunit()


@pytest.fixture
def config() -> BigQueryV2Config:
    return BigQueryV2Config(
        file_backed_cache_size=1,
        start_time=TS_1,
        end_time=TS_2 + timedelta(minutes=1),
        usage=BigQueryUsageConfig(
            include_top_n_queries=True,
            top_n_queries=3,
            bucket_duration=BucketDuration.DAY,
            include_operational_stats=False,
        ),
    )


@pytest.fixture
def usage_extractor(config: BigQueryV2Config) -> BigQueryUsageExtractor:
    report = BigQueryV2Report()
    return BigQueryUsageExtractor(
        config,
        report,
        lambda ref: make_dataset_urn("bigquery", str(ref.table_identifier)),
    )


def make_zero_usage_workunit(
    table: Table, time: datetime, bucket_duration: BucketDuration = BucketDuration.DAY
) -> MetadataWorkUnit:
    return make_usage_workunit(
        table=table,
        dataset_usage_statistics=DatasetUsageStatisticsClass(
            timestampMillis=int(time.timestamp() * 1000),
            eventGranularity=TimeWindowSizeClass(unit=bucket_duration, multiple=1),
            totalSqlQueries=0,
            uniqueUserCount=0,
            topSqlQueries=[],
            userCounts=[],
            fieldCounts=[],
        ),
    )


def compare_workunits(
    output: Iterable[MetadataWorkUnit], expected: Iterable[MetadataWorkUnit]
) -> None:
    assert not diff_metadata_json(
        [wu.metadata.to_obj() for wu in output],
        [wu.metadata.to_obj() for wu in expected],
    )


def test_usage_counts_single_bucket_resource_project(
    usage_extractor: BigQueryUsageExtractor,
    config: BigQueryV2Config,
) -> None:
    queries = [
        query_table_1_a(TS_1, ACTOR_1),
        query_table_1_a(TS_1, ACTOR_1),
        query_table_1_a(TS_1, ACTOR_2),
        query_table_1_b(TS_1, ACTOR_1),
        query_table_1_b(TS_1, ACTOR_2),
    ]
    events = generate_events(
        queries,
        [PROJECT_1, PROJECT_2],
        TABLE_TO_PROJECT,
        config=config,
        proabability_of_project_mismatch=0.5,
    )

    workunits = usage_extractor._get_workunits_internal(events, TABLE_REFS.values())
    expected = [
        make_usage_workunit(
            table=TABLE_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=len(queries),
                topSqlQueries=[query_table_1_a().text, query_table_1_b().text],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=3,
                        userEmail=ACTOR_1,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=2,
                        userEmail=ACTOR_2,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=5,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="age",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=3,
                    ),
                ],
            ),
        ),
        make_zero_usage_workunit(TABLE_2, TS_1),
        make_zero_usage_workunit(VIEW_1, TS_1),
    ]
    compare_workunits(workunits, expected)


def test_usage_counts_multiple_buckets_and_resources_view_usage(
    usage_extractor: BigQueryUsageExtractor,
    config: BigQueryV2Config,
) -> None:
    queries = [
        # TS 1
        query_table_1_a(TS_1, ACTOR_1),
        query_table_1_a(TS_1, ACTOR_2),
        query_table_1_b(TS_1, ACTOR_1),
        query_tables_1_and_2(TS_1, ACTOR_1),
        query_tables_1_and_2(TS_1, ACTOR_1),
        query_tables_1_and_2(TS_1, ACTOR_1),
        query_view_1(TS_1, ACTOR_1),
        query_view_1(TS_1, ACTOR_2),
        query_view_1(TS_1, ACTOR_2),
        # TS 2
        query_table_1_a(TS_2, ACTOR_1),
        query_table_1_a(TS_2, ACTOR_2),
        query_table_1_b(TS_2, ACTOR_2),
        query_tables_1_and_2(TS_2, ACTOR_2),
        query_table_2(TS_2, ACTOR_2),
        query_view_1(TS_2, ACTOR_1),
        query_view_1_and_table_1(TS_2, ACTOR_1),
    ]
    events = generate_events(
        queries,
        [PROJECT_1, PROJECT_2],
        TABLE_TO_PROJECT,
        config=config,
        proabability_of_project_mismatch=0.5,
    )

    workunits = usage_extractor._get_workunits_internal(events, TABLE_REFS.values())
    expected = [
        # TS 1
        make_usage_workunit(
            table=TABLE_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=6,
                topSqlQueries=[
                    query_tables_1_and_2().text,
                    query_table_1_a().text,
                    query_table_1_b().text,
                ],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=5,
                        userEmail=ACTOR_1,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=1,
                        userEmail=ACTOR_2,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=6,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=5,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="age",
                        count=2,
                    ),
                ],
            ),
        ),
        make_usage_workunit(
            table=VIEW_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=3,
                topSqlQueries=[
                    query_view_1().text,
                ],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=2,
                        userEmail=ACTOR_2,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=1,
                        userEmail=ACTOR_1,
                    ),
                ],
                fieldCounts=[],
            ),
        ),
        make_usage_workunit(
            table=TABLE_2,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=3,
                topSqlQueries=[
                    query_tables_1_and_2().text,
                ],
                uniqueUserCount=1,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=3,
                        userEmail=ACTOR_1,
                    )
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="value",
                        count=3,
                    ),
                ],
            ),
        ),
        # TS 2
        make_usage_workunit(
            table=TABLE_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_2.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=5,
                topSqlQueries=[
                    query_table_1_a().text,
                    query_tables_1_and_2().text,
                    query_table_1_b().text,
                    query_view_1_and_table_1().text,
                ],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=3,
                        userEmail=ACTOR_2,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=2,
                        userEmail=ACTOR_1,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=4,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="age",
                        count=2,
                    ),
                ],
            ),
        ),
        make_usage_workunit(
            table=VIEW_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_2.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=2,
                topSqlQueries=[query_view_1().text, query_view_1_and_table_1().text],
                uniqueUserCount=1,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=2,
                        userEmail=ACTOR_1,
                    ),
                ],
                fieldCounts=[],
            ),
        ),
        make_usage_workunit(
            table=TABLE_2,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_2.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=2,
                topSqlQueries=[query_tables_1_and_2().text, query_table_2().text],
                uniqueUserCount=1,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=2,
                        userEmail=ACTOR_2,
                    )
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=2,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="value",
                        count=2,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="table_1_id",
                        count=1,
                    ),
                ],
            ),
        ),
    ]
    compare_workunits(workunits, expected)
    assert usage_extractor.report.num_view_query_events == 5
    assert usage_extractor.report.num_view_query_events_failed_sql_parsing == 0
    assert usage_extractor.report.num_view_query_events_failed_table_identification == 0


def test_usage_counts_multiple_buckets_and_resources_no_view_usage(
    usage_extractor: BigQueryUsageExtractor,
    config: BigQueryV2Config,
) -> None:
    config.usage.apply_view_usage_to_tables = True
    queries = [
        # TS 1
        query_table_1_a(TS_1, ACTOR_1),
        query_table_1_a(TS_1, ACTOR_2),
        query_table_1_b(TS_1, ACTOR_1),
        query_tables_1_and_2(TS_1, ACTOR_1),
        query_tables_1_and_2(TS_1, ACTOR_1),
        query_tables_1_and_2(TS_1, ACTOR_1),
        query_view_1(TS_1, ACTOR_1),
        query_view_1(TS_1, ACTOR_2),
        query_view_1(TS_1, ACTOR_2),
        # TS 2
        query_table_1_a(TS_2, ACTOR_1),
        query_table_1_a(TS_2, ACTOR_2),
        query_table_1_b(TS_2, ACTOR_2),
        query_tables_1_and_2(TS_2, ACTOR_2),
        query_table_2(TS_2, ACTOR_2),
        query_view_1(TS_2, ACTOR_1),
        query_view_1_and_table_1(TS_2, ACTOR_1),
    ]
    events = generate_events(
        queries,
        [PROJECT_1, PROJECT_2],
        TABLE_TO_PROJECT,
        config=config,
        proabability_of_project_mismatch=0.5,
    )

    workunits = usage_extractor._get_workunits_internal(events, TABLE_REFS.values())
    expected = [
        # TS 1
        make_usage_workunit(
            table=TABLE_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=9,
                topSqlQueries=[
                    query_tables_1_and_2().text,
                    query_view_1().text,
                    query_table_1_a().text,
                    query_table_1_b().text,
                ],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=6,
                        userEmail=ACTOR_1,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=3,
                        userEmail=ACTOR_2,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=9,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=8,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="total",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="age",
                        count=2,
                    ),
                ],
            ),
        ),
        make_usage_workunit(
            table=TABLE_2,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=6,
                topSqlQueries=[query_tables_1_and_2().text, query_view_1().text],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=4,
                        userEmail=ACTOR_1,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=2,
                        userEmail=ACTOR_2,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=6,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="total",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="value",
                        count=3,
                    ),
                ],
            ),
        ),
        # TS 2
        make_usage_workunit(
            table=TABLE_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_2.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=6,
                topSqlQueries=[
                    query_table_1_a().text,
                    query_tables_1_and_2().text,
                    query_view_1().text,
                    query_table_1_b().text,
                    query_view_1_and_table_1().text,
                ],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=3,
                        userEmail=ACTOR_1,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=3,
                        userEmail=ACTOR_2,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=6,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=5,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="age",
                        count=2,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="total",
                        count=2,
                    ),
                ],
            ),
        ),
        make_usage_workunit(
            table=TABLE_2,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_2.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=4,
                topSqlQueries=[
                    query_tables_1_and_2().text,
                    query_view_1().text,
                    query_table_2().text,
                    query_view_1_and_table_1().text,
                ],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=2,
                        userEmail=ACTOR_1,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=2,
                        userEmail=ACTOR_2,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=4,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=2,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="total",
                        count=2,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="value",
                        count=2,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="table_1_id",
                        count=1,
                    ),
                ],
            ),
        ),
        make_zero_usage_workunit(VIEW_1, TS_1),
        # TS_2 not included as only 1 minute of it was ingested
    ]
    compare_workunits(workunits, expected)
    assert usage_extractor.report.num_view_query_events == 0


def test_usage_counts_no_query_event(
    caplog: pytest.LogCaptureFixture,
    usage_extractor: BigQueryUsageExtractor,
    config: BigQueryV2Config,
) -> None:
    with caplog.at_level(logging.WARNING):
        ref = BigQueryTableRef(BigqueryTableIdentifier("project", "dataset", "table"))
        event = AuditEvent.create(
            ReadEvent(
                jobName="job_name",
                timestamp=TS_1,
                actor_email=ACTOR_1,
                resource=ref,
                fieldsRead=["id", "name", "total"],
                readReason="JOB",
                payload=None,
            )
        )
        workunits = usage_extractor._get_workunits_internal([event], [str(ref)])
        expected = [
            MetadataChangeProposalWrapper(
                entityUrn=ref.to_urn("PROD"),
                aspect=DatasetUsageStatisticsClass(
                    timestampMillis=int(TS_1.timestamp() * 1000),
                    eventGranularity=TimeWindowSizeClass(
                        unit=BucketDuration.DAY, multiple=1
                    ),
                    totalSqlQueries=0,
                    uniqueUserCount=0,
                    topSqlQueries=[],
                    userCounts=[],
                    fieldCounts=[],
                ),
            ).as_workunit()
        ]
        compare_workunits(workunits, expected)
        assert not caplog.records


def test_usage_counts_no_columns(
    caplog: pytest.LogCaptureFixture,
    usage_extractor: BigQueryUsageExtractor,
    config: BigQueryV2Config,
) -> None:
    job_name = "job_name"
    ref = BigQueryTableRef(
        BigqueryTableIdentifier(PROJECT_1, DATABASE_1.name, TABLE_1.name)
    )
    events = [
        AuditEvent.create(
            ReadEvent(
                jobName=job_name,
                timestamp=TS_1,
                actor_email=ACTOR_1,
                resource=ref,
                fieldsRead=[],
                readReason="JOB",
                payload=None,
            ),
        ),
        AuditEvent.create(
            QueryEvent(
                job_name=job_name,
                timestamp=TS_1,
                actor_email=ACTOR_1,
                query="SELECT * FROM table_1",
                statementType="SELECT",
                project_id=PROJECT_1,
                destinationTable=None,
                referencedTables=[ref],
                referencedViews=[],
                payload=None,
            )
        ),
    ]
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        workunits = usage_extractor._get_workunits_internal(
            events, [TABLE_REFS[TABLE_1.name]]
        )
        expected = [
            make_usage_workunit(
                table=TABLE_1,
                dataset_usage_statistics=DatasetUsageStatisticsClass(
                    timestampMillis=int(TS_1.timestamp() * 1000),
                    eventGranularity=TimeWindowSizeClass(
                        unit=BucketDuration.DAY, multiple=1
                    ),
                    totalSqlQueries=1,
                    topSqlQueries=["SELECT * FROM table_1"],
                    uniqueUserCount=1,
                    userCounts=[
                        DatasetUserUsageCountsClass(
                            user=ACTOR_1_URN,
                            count=1,
                            userEmail=ACTOR_1,
                        ),
                    ],
                    fieldCounts=[],
                ),
            )
        ]
        compare_workunits(workunits, expected)
        assert not caplog.records


@freeze_time(FROZEN_TIME)
@patch.object(BigQueryUsageExtractor, "_generate_usage_workunits")
def test_operational_stats(
    mock: MagicMock,
    usage_extractor: BigQueryUsageExtractor,
    config: BigQueryV2Config,
) -> None:
    mock.return_value = []
    config.usage.apply_view_usage_to_tables = True
    config.usage.include_operational_stats = True
    seed_metadata = generate_data(
        num_containers=3,
        num_tables=5,
        num_views=2,
        time_range=timedelta(days=1),
    )
    all_tables = seed_metadata.tables + seed_metadata.views

    num_projects = 2
    projects = [f"project-{i}" for i in range(num_projects)]
    table_to_project = {table.name: random.choice(projects) for table in all_tables}
    table_refs = {
        table.name: str(ref_from_table(table, table_to_project)) for table in all_tables
    }

    queries = list(
        generate_queries(
            seed_metadata,
            num_selects=10,
            num_operations=20,
            num_unique_queries=10,
            num_users=3,
        )
    )

    events = generate_events(queries, projects, table_to_project, config=config)
    workunits = usage_extractor._get_workunits_internal(events, table_refs.values())
    expected = [
        make_operational_workunit(
            table_refs[query.object_modified.name],
            OperationClass(
                timestampMillis=int(FROZEN_TIME.timestamp() * 1000),
                lastUpdatedTimestamp=int(query.timestamp.timestamp() * 1000),
                actor=f"urn:li:corpuser:{query.actor.split('@')[0]}",
                operationType=query.type
                if query.type in OPERATION_STATEMENT_TYPES.values()
                else "CUSTOM",
                customOperationType=None
                if query.type in OPERATION_STATEMENT_TYPES.values()
                else query.type,
                affectedDatasets=list(
                    dict.fromkeys(  # Preserve order
                        BigQueryTableRef.from_string_name(
                            table_refs[field.table.name]
                        ).to_urn("PROD")
                        for field in query.fields_accessed
                        if not field.table.is_view()
                    )
                )
                + list(
                    dict.fromkeys(  # Preserve order
                        BigQueryTableRef.from_string_name(
                            table_refs[parent.name]
                        ).to_urn("PROD")
                        for field in query.fields_accessed
                        if field.table.is_view()
                        for parent in field.table.upstreams
                    )
                ),
            ),
        )
        for query in queries
        if query.object_modified and query.type in OPERATION_STATEMENT_TYPES.values()
    ]
    compare_workunits(
        [
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, OperationClass)
        ],
        expected,
    )


def test_get_tables_from_query(usage_extractor):
    assert usage_extractor.get_tables_from_query(
        PROJECT_1, "SELECT * FROM project-1.database_1.view_1"
    ) == [
        BigQueryTableRef(BigqueryTableIdentifier("project-1", "database_1", "view_1"))
    ]

    assert usage_extractor.get_tables_from_query(
        PROJECT_1, "SELECT * FROM database_1.view_1"
    ) == [
        BigQueryTableRef(BigqueryTableIdentifier("project-1", "database_1", "view_1"))
    ]

    assert sorted(
        usage_extractor.get_tables_from_query(
            PROJECT_1,
            "SELECT v.id, v.name, v.total, t.name as name1 FROM database_1.view_1 as v inner join database_1.table_1 as t on v.id=t.id",
        )
    ) == [
        BigQueryTableRef(BigqueryTableIdentifier("project-1", "database_1", "table_1")),
        BigQueryTableRef(BigqueryTableIdentifier("project-1", "database_1", "view_1")),
    ]

    assert sorted(
        usage_extractor.get_tables_from_query(
            PROJECT_1,
            "CREATE TABLE database_1.new_table AS SELECT v.id, v.name, v.total, t.name as name1 FROM database_1.view_1 as v inner join database_1.table_1 as t on v.id=t.id",
        )
    ) == [
        BigQueryTableRef(BigqueryTableIdentifier("project-1", "database_1", "table_1")),
        BigQueryTableRef(BigqueryTableIdentifier("project-1", "database_1", "view_1")),
    ]
