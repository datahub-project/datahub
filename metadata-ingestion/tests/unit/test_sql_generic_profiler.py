"""The rowCount contract for profiles that measured only part of a dataset.

A non-FULL_TABLE partition spec means the profiler scanned a sample or a single
partition. `rowCount` then comes from the source metadata rather than from what
was scanned, while the column statistics still describe only the scanned rows.
Previously untested: no golden in this repo contains a non-FULL_TABLE profile.
"""

from typing import Iterable, List, Optional, Tuple

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.profiling.common import ProfilerRequest
from datahub.ingestion.source.sql.sql_generic import BaseTable, SQLAlchemyGenericConfig
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    PartitionSpecClass,
    PartitionTypeClass,
)

FULL_TABLE = PartitionSpecClass(
    type=PartitionTypeClass.FULL_TABLE, partition="FULL_TABLE_SNAPSHOT"
)
SAMPLED = PartitionSpecClass(type=PartitionTypeClass.QUERY, partition="SAMPLE")
SAMPLED_PARTITION = PartitionSpecClass(
    type=PartitionTypeClass.PARTITION, partition="20230906 SAMPLE"
)


class _StubProfiler(GenericProfiler):
    """A GenericProfiler whose column profiler hands back one canned profile."""

    def __init__(self, profile: DatasetProfileClass) -> None:
        super().__init__(
            config=SQLAlchemyGenericConfig(platform="mydb", connect_uri="sqlite://"),
            report=SQLSourceReport(),
            platform="mydb",
        )
        self.profile = profile

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        return f"{db_name}.{schema_name}.{table_name}"

    def get_profiler_instance(self, db_name: Optional[str] = None) -> "_StubProfiler":  # type: ignore[override]
        return self

    def generate_profiles(
        self,
        requests: List[ProfilerRequest],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[dict] = None,
    ) -> Iterable[Tuple[ProfilerRequest, DatasetProfileClass]]:
        return [(request, self.profile) for request in requests]


def _emit(
    partition_spec: PartitionSpecClass,
    *,
    measured_row_count: Optional[int],
    metadata_rows_count: Optional[int],
) -> DatasetProfileClass:
    """Run one profile through generate_profile_workunits and return the aspect."""
    table = BaseTable(
        name="t",
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=None,
        rows_count=metadata_rows_count,
        column_count=2,
    )
    request = TableProfilerRequest(pretty_name="db.sch.t", batch_kwargs={}, table=table)
    profile = DatasetProfileClass(
        timestampMillis=0,
        rowCount=measured_row_count,
        columnCount=2,
        partitionSpec=partition_spec,
    )

    workunits = list(
        _StubProfiler(profile).generate_profile_workunits([request], max_workers=1)
    )

    assert len(workunits) == 1
    mcp = workunits[0].metadata
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert isinstance(mcp.aspect, DatasetProfileClass)
    return mcp.aspect


def test_sampled_profile_reports_the_datasets_row_count() -> None:
    # The profiler scanned ~1000 sampled rows; the table has 2000.
    assert (
        _emit(SAMPLED, measured_row_count=1000, metadata_rows_count=2000).rowCount
        == 2000
    )


def test_sampled_partition_reports_the_whole_tables_row_count() -> None:
    # Deliberate, not a bug: the count describes the dataset, not the partition.
    assert (
        _emit(
            SAMPLED_PARTITION, measured_row_count=1000, metadata_rows_count=1610
        ).rowCount
        == 1610
    )


def test_no_metadata_row_count_emits_no_row_count() -> None:
    # rowCount is the dataset's total, so the sample's size is not a fallback for
    # it. Unknown is expressible -- the field is optional -- and that is what a
    # sampled view or external table gets.
    assert (
        _emit(SAMPLED, measured_row_count=1000, metadata_rows_count=None).rowCount
        is None
    )


def test_full_table_profile_keeps_the_measured_count() -> None:
    # Metadata deliberately disagrees: a full scan is authoritative over a
    # possibly stale crawl, so the override must not fire here.
    assert (
        _emit(FULL_TABLE, measured_row_count=3, metadata_rows_count=999).rowCount == 3
    )
