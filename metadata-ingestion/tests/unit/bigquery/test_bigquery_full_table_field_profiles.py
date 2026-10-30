"""Copying partition column stats onto the full-table profile.

Off unless profiling.copy_partition_field_profiles_to_full_table_profile is set.
The full-table profile is what latestFullTableProfile reads; column stats for a
partitioned BigQuery table otherwise exist only on the partition profile.
"""

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryTable,
    ShardedTableStats,
)
from datahub.ingestion.source.bigquery_v2.profiling.profiler import BigqueryProfiler
from datahub.ingestion.source.profiling.common import ProfilerRequest
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import TableProfilerRequest
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    PartitionSpecClass,
    PartitionTypeClass,
)


def _config(*, copy_fields: bool) -> BigQueryV2Config:
    profiling: Dict[str, object] = {"enabled": True}
    if copy_fields:
        profiling["copy_partition_field_profiles_to_full_table_profile"] = True
    return BigQueryV2Config.parse_obj(
        {"project_id": "test-project", "profiling": profiling}
    )


class _StubProfiler(BigqueryProfiler):
    def __init__(self, profile: DatasetProfileClass, *, copy_fields: bool) -> None:
        super().__init__(
            config=_config(copy_fields=copy_fields), report=BigQueryV2Report()
        )
        self.profile = profile

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


def _partition(partition: str = "20261010") -> PartitionSpecClass:
    return PartitionSpecClass(type=PartitionTypeClass.PARTITION, partition=partition)


def _field_profiles() -> List[DatasetFieldProfileClass]:
    return [DatasetFieldProfileClass(fieldPath="id", uniqueCount=3)]


def _run(
    partition_spec: Optional[PartitionSpecClass],
    *,
    copy_fields: bool,
    field_profiles: Optional[List[DatasetFieldProfileClass]] = None,
    table_names: Tuple[str, ...] = ("events",),
) -> Tuple[List[MetadataWorkUnit], BigQueryV2Report]:
    requests = [
        TableProfilerRequest(
            pretty_name=f"test-project.ds.{name}",
            batch_kwargs={},
            table=BaseTable(
                name=name,
                comment=None,
                created=None,
                last_altered=None,
                size_in_bytes=100,
                rows_count=50,
                column_count=1,
            ),
        )
        for name in table_names
    ]
    profile = DatasetProfileClass(
        timestampMillis=1_000,
        rowCount=4,
        columnCount=1,
        partitionSpec=partition_spec,
        fieldProfiles=field_profiles,
    )
    profiler = _StubProfiler(profile, copy_fields=copy_fields)
    workunits = list(profiler.generate_profile_workunits(requests, max_workers=1))
    return workunits, profiler.report


def _profiles(workunits: List[MetadataWorkUnit]) -> List[DatasetProfileClass]:
    profiles = []
    for workunit in workunits:
        metadata = workunit.metadata
        assert isinstance(metadata, MetadataChangeProposalWrapper)
        assert isinstance(metadata.aspect, DatasetProfileClass)
        profiles.append(metadata.aspect)
    return profiles


def test_partition_field_profiles_are_not_copied_when_disabled() -> None:
    workunits, report = _run(
        _partition(), copy_fields=False, field_profiles=_field_profiles()
    )

    profiles = _profiles(workunits)
    assert len(profiles) == 1
    assert profiles[0].partitionSpec == _partition()
    assert report.num_full_table_profiles_copied_from_partition == 0


def test_partition_field_profiles_are_copied_onto_the_full_table_profile() -> None:
    workunits, report = _run(
        _partition(), copy_fields=True, field_profiles=_field_profiles()
    )

    partition_profile, full_table_profile = _profiles(workunits)
    assert partition_profile.partitionSpec == _partition()
    assert full_table_profile.partitionSpec == PartitionSpecClass(
        type=PartitionTypeClass.FULL_TABLE,
        partition="FULL_TABLE_SNAPSHOT (from partition 20261010)",
    )
    assert full_table_profile.timestampMillis == partition_profile.timestampMillis
    assert full_table_profile.fieldProfiles == _field_profiles()
    # Table totals from source metadata, not the partition scan's row count.
    assert full_table_profile.rowCount == 50
    assert full_table_profile.columnCount == 1
    assert full_table_profile.sizeInBytes == 100
    # Both profiles are emitted for the same dataset under different workunit ids.
    assert workunits[0].get_urn() == workunits[1].get_urn()
    assert workunits[1].id == f"{workunits[0].id}-full-table"
    assert report.num_full_table_profiles_copied_from_partition == 1
    assert not report.full_table_profile_copy_skipped


def test_copy_is_unaffected_by_in_place_edits_to_the_yielded_partition_profile() -> (
    None
):
    profiler = _StubProfiler(
        DatasetProfileClass(
            timestampMillis=1_000,
            partitionSpec=_partition(),
            fieldProfiles=_field_profiles(),
        ),
        copy_fields=True,
    )
    request = TableProfilerRequest(
        pretty_name="test-project.ds.events",
        batch_kwargs={},
        table=BaseTable(
            name="events",
            comment=None,
            created=None,
            last_altered=None,
            size_in_bytes=None,
            rows_count=None,
        ),
    )
    profiles: List[DatasetProfileClass] = []
    for workunit in profiler.generate_profile_workunits([request], max_workers=1):
        profile = workunit.get_aspect_of_type(DatasetProfileClass)
        assert profile is not None
        if not profiles:
            # Mimics a transformer stripping column stats from the partition profile.
            profile.fieldProfiles = None
        profiles.append(profile)

    partition_profile, full_table_profile = profiles
    assert partition_profile.fieldProfiles is None
    assert full_table_profile.fieldProfiles == _field_profiles()


def test_sharded_table_copy_reports_totals_across_shards() -> None:
    profiler = _StubProfiler(
        DatasetProfileClass(
            timestampMillis=1_000,
            rowCount=4,
            partitionSpec=_partition("20261010"),
            fieldProfiles=_field_profiles(),
        ),
        copy_fields=True,
    )
    table = BigqueryTable(
        name="events_20261010",
        comment=None,
        rows_count=4,
        size_in_bytes=10,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        max_shard_id="20261010",
        sharded_table_stats=ShardedTableStats(rows_count=40, size_in_bytes=100),
    )

    workunits = list(profiler.get_workunits("test-project", {"ds": [table]}))

    partition_profile, full_table_profile = _profiles(workunits)
    assert partition_profile.partitionSpec == _partition("20261010")
    assert full_table_profile.partitionSpec == PartitionSpecClass(
        type=PartitionTypeClass.FULL_TABLE,
        partition="FULL_TABLE_SNAPSHOT (from partition 20261010)",
    )
    assert full_table_profile.fieldProfiles == _field_profiles()
    assert (full_table_profile.rowCount, full_table_profile.sizeInBytes) == (40, 100)


@pytest.mark.parametrize(
    "partition_spec",
    [
        pytest.param(
            PartitionSpecClass(
                type=PartitionTypeClass.FULL_TABLE, partition="FULL_TABLE_SNAPSHOT"
            ),
            id="full_table",
        ),
        pytest.param(
            PartitionSpecClass(type=PartitionTypeClass.QUERY, partition="SAMPLE"),
            id="sampled_table",
        ),
        pytest.param(
            PartitionSpecClass(
                type=PartitionTypeClass.QUERY, partition='{"limit": 10, "offset": 0}'
            ),
            id="limit_offset",
        ),
    ],
)
def test_non_partition_profiles_are_not_copied(
    partition_spec: Optional[PartitionSpecClass],
) -> None:
    workunits, report = _run(
        partition_spec, copy_fields=True, field_profiles=_field_profiles()
    )

    assert len(workunits) == 1
    assert report.num_full_table_profiles_copied_from_partition == 0
    assert not report.full_table_profile_copy_skipped


@pytest.mark.parametrize(
    ("partition_spec", "field_profiles", "reason"),
    [
        pytest.param(
            _partition("20261010 SAMPLE"),
            _field_profiles(),
            "partition was sampled",
            id="sampled_partition",
        ),
        pytest.param(
            _partition(), None, "partition profile has no column stats", id="no_stats"
        ),
    ],
)
def test_skipped_partition_profiles_are_reported(
    partition_spec: PartitionSpecClass,
    field_profiles: Optional[List[DatasetFieldProfileClass]],
    reason: str,
) -> None:
    workunits, report = _run(
        partition_spec, copy_fields=True, field_profiles=field_profiles
    )

    assert len(workunits) == 1
    assert report.num_full_table_profiles_copied_from_partition == 0
    assert dict(report.full_table_profile_copy_skipped) == {
        workunits[0].get_urn(): reason
    }
