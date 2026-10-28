"""The rowCount contract for profiles that measured only part of a dataset.

A non-FULL_TABLE partition spec means the profiler scanned a sample or a single
partition. `rowCount` then comes from the source metadata rather than from what
was scanned, while the column statistics still describe only the scanned rows.
Previously untested: no golden in this repo contains a non-FULL_TABLE profile.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Optional, Tuple, cast

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


# ---------------------------------------------------------------------------
# Eligibility and request building: the guardrails that keep a profiler from
# scanning tables the recipe excluded, or that are too big or too stale.
# ---------------------------------------------------------------------------


class _FakeProfilingState:
    """Stands in for the stateful profiling checkpoint."""

    def __init__(self, last_profiled: Optional[int]) -> None:
        self.last_profiled = last_profiled
        self.added: List[Tuple[str, int]] = []

    def get_last_profiled(self, urn: str) -> Optional[int]:
        return self.last_profiled

    def add_to_state(self, urn: str, timestamp: int) -> None:
        self.added.append((urn, timestamp))


class _PlainProfiler(GenericProfiler):
    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        return f"{db_name}.{schema_name}.{table_name}"


def _profiler(
    *, state: Optional[_FakeProfilingState] = None, **overrides: Any
) -> _PlainProfiler:
    config = SQLAlchemyGenericConfig.model_validate(
        {"platform": "mydb", "connect_uri": "sqlite://", **overrides}
    )
    return _PlainProfiler(
        config=config,
        report=SQLSourceReport(),
        platform="mydb",
        state_handler=cast(Any, state),
    )


def _table(
    *,
    rows: Optional[int] = 10,
    size: Optional[int] = 1024,
    columns: Optional[int] = 2,
    last_altered: Optional[datetime] = None,
) -> BaseTable:
    return BaseTable(
        name="t",
        comment=None,
        created=None,
        last_altered=last_altered,
        size_in_bytes=size,
        rows_count=rows,
        column_count=columns,
    )


def test_eligible_table_yields_a_column_profiling_request() -> None:
    request = _profiler().get_profile_request(_table(), "sch", "db")

    assert request is not None
    assert request.pretty_name == "db.sch.t"
    assert request.batch_kwargs == {"schema": "sch", "table": "t"}
    assert not request.profile_table_level_only


def test_oversized_table_is_profiled_at_table_level_only() -> None:
    profiler = _profiler(profiling={"enabled": True, "profile_table_size_limit": 1})

    request = profiler.get_profile_request(_table(size=3 * 2**30), "sch", "db")

    assert request is not None
    assert request.profile_table_level_only
    assert profiler.report.profiling_skipped_size_limit["db.sch"] == 1


def test_stale_table_is_skipped_entirely() -> None:
    profiler = _profiler(
        profiling={
            "enabled": True,
            "profile_if_updated_since_days": 1,
            "report_dropped_profiles": True,
        }
    )
    stale = datetime.now(timezone.utc) - timedelta(days=30)

    assert profiler.get_profile_request(_table(last_altered=stale), "sch", "db") is None
    assert profiler.report.profiling_skipped_not_updated["db.sch"]
    assert profiler.report.num_tables_not_eligible_profiling["db.sch"] == 1
    assert "profile of db.sch.t" in profiler.report.filtered


def test_table_without_columns_is_skipped() -> None:
    assert _profiler().get_profile_request(_table(columns=0), "sch", "db") is None


def test_row_limit_and_patterns_gate_eligibility() -> None:
    profiler = _profiler(
        table_pattern={"deny": [".*denied"]},
        profile_pattern={"deny": [".*unprofiled"]},
        profiling={"enabled": True, "profile_table_row_limit": 100},
    )

    assert profiler.is_dataset_eligible_for_profiling("db.sch.t", rows_count=50)
    assert not profiler.is_dataset_eligible_for_profiling("db.sch.t", rows_count=500)
    assert profiler.report.profiling_skipped_row_limit["db.sch"] == 1
    assert not profiler.is_dataset_eligible_for_profiling("db.sch.denied")
    assert not profiler.is_dataset_eligible_for_profiling("db.sch.unprofiled")
    assert profiler.report.profiling_skipped_table_profile_pattern["db.sch"] == 1


def test_last_profiled_time_is_carried_over_and_used_as_the_threshold() -> None:
    last_profiled = int(
        (datetime.now(timezone.utc) - timedelta(days=2)).timestamp() * 1000
    )
    state = _FakeProfilingState(last_profiled)
    profiler = _profiler(state=state)

    altered_before_last_run = datetime.now(timezone.utc) - timedelta(days=5)
    altered_after_last_run = datetime.now(timezone.utc) - timedelta(days=1)

    assert not profiler.is_dataset_eligible_for_profiling(
        "db.sch.t", last_altered=altered_before_last_run
    )
    assert profiler.is_dataset_eligible_for_profiling(
        "db.sch.t", last_altered=altered_after_last_run
    )
    # Existing state is carried into the new checkpoint even when nothing is profiled.
    assert (
        state.added
        == [("urn:li:dataset:(urn:li:dataPlatform:mydb,db.sch.t,PROD)", last_profiled)]
        * 2
    )


# --- generate_profile_workunits -------------------------------------------


class _RecordingProfiler(_PlainProfiler):
    def __init__(self, profile: Optional[DatasetProfileClass], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.profile = profile
        self.seen: List[ProfilerRequest] = []

    def get_profiler_instance(  # type: ignore[override]
        self, db_name: Optional[str] = None
    ) -> "_RecordingProfiler":
        return self

    def generate_profiles(
        self,
        requests: List[ProfilerRequest],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[dict] = None,
    ) -> Iterable[Tuple[ProfilerRequest, Optional[DatasetProfileClass]]]:
        self.seen.extend(requests)
        return [(request, self.profile) for request in requests]


def _recording(
    profile: Optional[DatasetProfileClass], state: Optional[_FakeProfilingState] = None
) -> _RecordingProfiler:
    return _RecordingProfiler(
        profile,
        config=SQLAlchemyGenericConfig(platform="mydb", connect_uri="sqlite://"),
        report=SQLSourceReport(),
        platform="mydb",
        state_handler=cast(Any, state),
    )


def _request(table: BaseTable, *, table_level_only: bool) -> TableProfilerRequest:
    return TableProfilerRequest(
        pretty_name=f"db.sch.{table.name}",
        batch_kwargs={},
        table=table,
        profile_table_level_only=table_level_only,
    )


def test_table_level_requests_are_answered_from_metadata_without_scanning() -> None:
    profiler = _recording(profile=None)
    with_stats = _request(_table(rows=7, size=99, columns=3), table_level_only=True)
    nothing_known = _request(
        _table(rows=None, size=None, columns=None), table_level_only=True
    )

    workunits = list(
        profiler.generate_profile_workunits([with_stats, nothing_known], max_workers=1)
    )

    assert len(workunits) == 1
    mcp = workunits[0].metadata
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert isinstance(mcp.aspect, DatasetProfileClass)
    assert (mcp.aspect.rowCount, mcp.aspect.sizeInBytes, mcp.aspect.columnCount) == (
        7,
        99,
        3,
    )
    # No column profiler was ever consulted.
    assert profiler.seen == []


def test_column_profiles_record_state_and_skip_failed_requests() -> None:
    state = _FakeProfilingState(None)
    profile = DatasetProfileClass(timestampMillis=0, rowCount=5, columnCount=2)
    profiler = _recording(profile, state)
    request = _request(_table(size=2048), table_level_only=False)

    workunits = list(profiler.generate_profile_workunits([request], max_workers=1))

    assert len(workunits) == 1
    mcp = workunits[0].metadata
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert isinstance(mcp.aspect, DatasetProfileClass)
    assert mcp.aspect.sizeInBytes == 2048
    assert [urn for urn, _ in state.added] == [
        "urn:li:dataset:(urn:li:dataPlatform:mydb,db.sch.t,PROD)"
    ]

    # A profiler that could not profile the table emits nothing for it.
    assert not list(
        _recording(None).generate_profile_workunits([request], max_workers=1)
    )


def test_profiler_instance_is_built_on_the_configured_connection() -> None:
    profiler = _profiler()

    inspectors = list(profiler.get_inspectors())
    assert len(inspectors) == 1
    assert inspectors[0].engine.dialect.name == "sqlite"

    column_profiler = profiler.get_profiler_instance()
    assert column_profiler.platform == "mydb"
    assert column_profiler.report is profiler.report
    assert profiler.get_profile_args() == {}
