"""Copying partition column stats onto the full-table profile.

Off unless profiling.copy_partition_field_profiles_to_full_table_profile is set.
The full-table profile is what latestFullTableProfile reads; column stats for a
partitioned BigQuery table otherwise exist only on the partition profile.
"""

from typing import Iterable, List, Optional, Tuple

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
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
    profiling: dict = {"enabled": True}
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


def _emit(
    partition_spec: Optional[PartitionSpecClass],
    *,
    copy_fields: bool,
    field_profiles: Optional[List[DatasetFieldProfileClass]] = None,
) -> List[DatasetProfileClass]:
    table = BaseTable(
        name="events",
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=100,
        rows_count=50,
        column_count=1,
    )
    request = TableProfilerRequest(
        pretty_name="test-project.ds.events", batch_kwargs={}, table=table
    )
    profile = DatasetProfileClass(
        timestampMillis=1_000,
        rowCount=4,
        columnCount=1,
        partitionSpec=partition_spec,
        fieldProfiles=field_profiles,
    )
    workunits = list(
        _StubProfiler(profile, copy_fields=copy_fields).generate_profile_workunits(
            [request], max_workers=1
        )
    )
    aspects: List[DatasetProfileClass] = []
    for workunit in workunits:
        metadata = workunit.metadata
        assert isinstance(metadata, MetadataChangeProposalWrapper)
        assert isinstance(metadata.aspect, DatasetProfileClass)
        aspects.append(metadata.aspect)
    return aspects


def test_copy_option_defaults_to_off() -> None:
    assert (
        _config(
            copy_fields=False
        ).profiling.copy_partition_field_profiles_to_full_table_profile
        is False
    )


def test_partition_field_profiles_are_not_copied_when_disabled() -> None:
    profiles = _emit(
        PartitionSpecClass(type=PartitionTypeClass.PARTITION, partition="20261010"),
        copy_fields=False,
        field_profiles=[DatasetFieldProfileClass(fieldPath="id", uniqueCount=3)],
    )
    assert len(profiles) == 1
    assert profiles[0].partitionSpec is not None
    assert profiles[0].partitionSpec.partition == "20261010"


def test_partition_field_profiles_are_copied_onto_the_full_table_profile() -> None:
    profiles = _emit(
        PartitionSpecClass(type=PartitionTypeClass.PARTITION, partition="20261010"),
        copy_fields=True,
        field_profiles=[DatasetFieldProfileClass(fieldPath="id", uniqueCount=3)],
    )
    assert len(profiles) == 2

    partition_profile, full_table_profile = profiles
    assert partition_profile.partitionSpec is not None
    assert partition_profile.partitionSpec.partition == "20261010"
    assert full_table_profile.partitionSpec == PartitionSpecClass(
        type=PartitionTypeClass.FULL_TABLE, partition="FULL_TABLE_SNAPSHOT"
    )
    assert full_table_profile.fieldProfiles is not None
    assert full_table_profile.fieldProfiles[0].fieldPath == "id"
    assert full_table_profile.fieldProfiles[0].uniqueCount == 3
    # Table total from source metadata, not the partition scan's row count.
    assert full_table_profile.rowCount == 50
    assert full_table_profile.columnCount == 1
    assert full_table_profile.sizeInBytes == 100
    # Distinct timestamp so the timeseries workunit id does not collide.
    assert full_table_profile.timestampMillis == partition_profile.timestampMillis + 1
    assert full_table_profile.fieldProfiles is not partition_profile.fieldProfiles


def test_full_table_profile_is_not_copied_again() -> None:
    profiles = _emit(
        PartitionSpecClass(
            type=PartitionTypeClass.FULL_TABLE, partition="FULL_TABLE_SNAPSHOT"
        ),
        copy_fields=True,
        field_profiles=[DatasetFieldProfileClass(fieldPath="id", uniqueCount=3)],
    )
    assert len(profiles) == 1


def test_partition_profile_without_field_profiles_is_not_copied() -> None:
    profiles = _emit(
        PartitionSpecClass(type=PartitionTypeClass.PARTITION, partition="20261010"),
        copy_fields=True,
        field_profiles=None,
    )
    assert len(profiles) == 1
