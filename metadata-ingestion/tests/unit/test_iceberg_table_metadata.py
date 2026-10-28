"""Iceberg table-level metadata and profiling: how snapshot/manifest statistics,
table properties and catalog connection settings end up in the emitted aspects."""

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast
from unittest.mock import MagicMock, patch

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.conversions import to_bytes
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import (
    DateType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)
from requests import Request
from requests.adapters import HTTPAdapter

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.iceberg.iceberg import IcebergSource, IcebergSourceConfig
from datahub.ingestion.source.iceberg.iceberg_common import (
    IcebergProfilingConfig,
    IcebergSourceReport,
    TimeoutHTTPAdapter,
    TimingClass,
    TopTableTimings,
)
from datahub.ingestion.source.iceberg.iceberg_profiler import IcebergProfiler
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DatasetProfileClass,
    DatasetPropertiesClass,
    OwnershipClass,
)

GET_CATALOG = "datahub.ingestion.source.iceberg.iceberg.IcebergSourceConfig.get_catalog"
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:iceberg,ns.tbl,PROD)"
SNAPSHOT_TS_MS = 1700000000000

PROFILE_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "event_ts", TimestampType(), required=False),
    NestedField(3, "name", StringType(), required=False),
    schema_id=0,
)


class _Catalog:
    """Just enough of a pyiceberg Catalog for IcebergSource to walk one table."""

    def __init__(self, table: Table) -> None:
        self._table = table
        table.catalog = self  # type: ignore[assignment]

    def table_name_from(self, identifier: Tuple[str, ...]) -> str:
        return ".".join(identifier)

    def list_namespaces(self) -> Iterable[Tuple[str]]:
        return [("ns",)]

    def list_tables(self, namespace: Tuple[str, ...]) -> Iterable[Tuple[str, str]]:
        return [("ns", "tbl")]

    def load_table(self, identifier: Tuple[str, str]) -> Table:
        return self._table

    def load_namespace_properties(self, namespace: Tuple[str, ...]) -> Dict[str, str]:
        return {}


def _table(
    properties: Optional[Dict[str, str]] = None,
    last_updated_ms: int = 0,
) -> Table:
    snapshot = Snapshot(
        snapshot_id=1,
        timestamp_ms=SNAPSHOT_TS_MS,
        manifest_list="s3://bucket/ns/tbl/metadata/snap-1.avro",
        summary=Summary(
            operation=Operation.APPEND,
            **{"total-records": "10", "total-files-size": "2048"},
        ),
        schema_id=0,
    )
    return Table(
        identifier=("ns", "tbl"),
        metadata=TableMetadataV2(
            partition_specs=[PartitionSpec(spec_id=0)],
            location="s3://bucket/ns/tbl",
            last_column_id=3,
            schemas=[PROFILE_SCHEMA],
            current_schema_id=0,
            snapshots=[snapshot],
            current_snapshot_id=1,
            last_updated_ms=last_updated_ms,
            properties=properties or {},
            # Needed because the partition-spec warning path repr()s the table.
            sort_orders=[UNSORTED_SORT_ORDER],
            default_sort_order_id=0,
        ),
        metadata_location="s3://bucket/ns/tbl/metadata/v1.metadata.json",
        io=PyArrowFileIO(),
        catalog=None,  # type: ignore[arg-type]
    )


def _ingest(
    table: Table, **config_overrides: Any
) -> Tuple[IcebergSource, Dict[str, Dict[str, Any]]]:
    config = IcebergSourceConfig(
        catalog={"test": {"type": "rest"}},
        processing_threads=1,
        **config_overrides,
    )
    source = IcebergSource(ctx=PipelineContext(run_id="iceberg-test"), config=config)
    with patch(GET_CATALOG, return_value=_Catalog(table)):
        workunits: List[MetadataWorkUnit] = list(source.get_workunits_internal())

    aspects: Dict[str, Dict[str, Any]] = defaultdict(dict)
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert wu.metadata.entityUrn and wu.metadata.aspectName
        aspects[wu.metadata.entityUrn][wu.metadata.aspectName] = wu.metadata.aspect
    return source, aspects


# --------------------------------------------------------------------------- #
# Dataset properties / ownership / browse paths
# --------------------------------------------------------------------------- #


def test_snapshot_drives_last_modified_and_custom_properties() -> None:
    table = _table(properties={"created-at": "2024-01-02T03:04:05Z"})

    _, aspects = _ingest(table)

    props = aspects[DATASET_URN]["datasetProperties"]
    assert isinstance(props, DatasetPropertiesClass)
    # Metadata has no last-updated time, so the current snapshot's is used.
    assert props.lastModified is not None
    assert props.lastModified.time == SNAPSHOT_TS_MS
    assert props.created is not None
    assert props.created.time == 1704164645000
    assert props.customProperties["snapshot-id"] == "1"
    assert props.customProperties["manifest-list"] == (
        "s3://bucket/ns/tbl/metadata/snap-1.avro"
    )


def test_unparseable_created_at_is_ignored() -> None:
    table = _table(properties={"created-at": "yesterday-ish"})

    _, aspects = _ingest(table)

    props = aspects[DATASET_URN]["datasetProperties"]
    assert props.created is None
    assert props.customProperties["created-at"] == "yesterday-ish"


def test_partition_spec_failure_degrades_to_none_and_warns() -> None:
    table = _table()

    with patch(
        "datahub.ingestion.source.iceberg.iceberg.partition_spec_json",
        side_effect=Exception("unsupported transform"),
    ):
        source, aspects = _ingest(table)

    props = aspects[DATASET_URN]["datasetProperties"]
    assert props.customProperties["partition-spec"] == "None"
    assert source.report.warnings.total_elements == 1


def test_user_and_group_ownership_properties_become_owners() -> None:
    table = _table(properties={"owner": "alice", "team": "data-eng"})

    _, aspects = _ingest(
        table, user_ownership_property="owner", group_ownership_property="team"
    )

    ownership = aspects[DATASET_URN]["ownership"]
    assert isinstance(ownership, OwnershipClass)
    assert {o.owner for o in ownership.owners} == {
        "urn:li:corpuser:alice",
        "urn:li:corpGroup:data-eng",
    }


def test_platform_instance_leads_the_browse_path() -> None:
    urn = "urn:li:dataset:(urn:li:dataPlatform:iceberg,prod_catalog.ns.tbl,PROD)"

    _, aspects = _ingest(_table(), platform_instance="prod_catalog")

    browse = aspects[urn]["browsePathsV2"]
    assert isinstance(browse, BrowsePathsV2Class)
    assert browse.path[0].urn == (
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:iceberg,prod_catalog)"
    )
    assert browse.path[1].urn is not None
    assert browse.path[1].urn.startswith("urn:li:container:")


def test_profiling_enabled_emits_profile_even_when_manifests_unreadable() -> None:
    """The manifest list lives in object storage that is not reachable here; the
    profile must still be emitted from the snapshot summary with a warning."""
    source, aspects = _ingest(_table(), profiling={"enabled": True})

    profile = aspects[DATASET_URN]["datasetProfile"]
    assert isinstance(profile, DatasetProfileClass)
    assert profile.rowCount == 10
    assert profile.sizeInBytes == 2048
    assert source.report.warnings.total_elements == 1


def test_catalog_initialisation_failure_is_reported_and_stops_ingestion() -> None:
    config = IcebergSourceConfig(catalog={"test": {"type": "rest"}})
    source = IcebergSource(ctx=PipelineContext(run_id="iceberg-test"), config=config)

    with patch(GET_CATALOG, side_effect=Exception("cannot reach catalog")):
        assert list(source.get_workunits_internal()) == []

    assert source.report.failures.total_elements == 1


# --------------------------------------------------------------------------- #
# Profiler: manifest statistics aggregation
# --------------------------------------------------------------------------- #


def _data_file(
    record_count: int,
    null_counts: Dict[int, int],
    lower: Dict[int, bytes],
    upper: Dict[int, bytes],
) -> MagicMock:
    entry = MagicMock()
    entry.data_file.record_count = record_count
    entry.data_file.null_value_counts = null_counts
    entry.data_file.lower_bounds = lower
    entry.data_file.upper_bounds = upper
    return entry


def _profiler(**config: Any) -> IcebergProfiler:
    return IcebergProfiler(
        IcebergSourceReport(), IcebergProfilingConfig(enabled=True, **config)
    )


def _mock_table_with_entries(entries: List[MagicMock], total_records: str) -> MagicMock:
    table = MagicMock()
    table.schema.return_value = PROFILE_SCHEMA
    table.metadata_location = "s3://bucket/ns/tbl/metadata/v1.metadata.json"
    snapshot = MagicMock()
    snapshot.summary.additional_properties = {"total-records": total_records}
    manifest = MagicMock()
    manifest.fetch_manifest_entry.return_value = entries
    snapshot.manifests.return_value = [manifest]
    table.current_snapshot.return_value = snapshot
    return table


def test_profile_aggregates_bounds_and_null_counts_across_data_files() -> None:
    ts_a = 1688559488157000  # 2023-07-05T12:18:08.157
    ts_b = 1688645888157000  # 2023-07-06T12:18:08.157
    entries = [
        _data_file(
            record_count=2,
            null_counts={1: 0, 2: 1},
            lower={
                1: to_bytes(LongType(), 5),
                2: to_bytes(TimestampType(), ts_b),
                3: to_bytes(StringType(), "a"),
            },
            upper={1: to_bytes(LongType(), 9), 2: to_bytes(TimestampType(), ts_b)},
        ),
        _data_file(
            record_count=2,
            null_counts={1: 0, 2: 1},
            lower={1: to_bytes(LongType(), 3), 2: to_bytes(TimestampType(), ts_a)},
            # Field 99 belonged to an older schema version; it must be ignored.
            upper={1: to_bytes(LongType(), 7), 99: to_bytes(LongType(), 1)},
        ),
    ]
    table = _mock_table_with_entries(entries, total_records="4")

    profiles = list(_profiler().profile_table("ns.tbl", table))

    assert len(profiles) == 1
    profile = profiles[0]
    assert isinstance(profile, DatasetProfileClass)
    assert profile.rowCount == 4
    assert profile.columnCount == 3
    by_path = {f.fieldPath: f for f in profile.fieldProfiles or []}

    assert by_path["id"].min == "3"
    assert by_path["id"].max == "9"
    assert by_path["id"].nullCount == 0
    assert by_path["id"].nullProportion == 0.0

    assert by_path["event_ts"].min == "2023-07-05T12:18:08.157000"
    assert by_path["event_ts"].max == "2023-07-06T12:18:08.157000"
    assert by_path["event_ts"].nullCount == 2
    assert by_path["event_ts"].nullProportion == 0.5

    # Strings are not numeric so manifests bounds are not used for them.
    assert by_path["name"].min is None
    assert by_path["name"].max is None


def test_profile_respects_disabled_field_metrics() -> None:
    entries = [
        _data_file(
            record_count=1,
            null_counts={1: 1},
            lower={1: to_bytes(LongType(), 1)},
            upper={1: to_bytes(LongType(), 1)},
        )
    ]
    table = _mock_table_with_entries(entries, total_records="1")
    profiler = _profiler(
        include_field_null_count=False,
        include_field_min_value=False,
        include_field_max_value=False,
    )

    profile = next(iter(profiler.profile_table("ns.tbl", table)))

    assert isinstance(profile, DatasetProfileClass)
    field = next(f for f in profile.fieldProfiles or [] if f.fieldPath == "id")
    assert field.nullCount is None
    assert field.min is None
    assert field.max is None


def test_profile_skips_table_without_snapshot() -> None:
    table = MagicMock()
    table.current_snapshot.return_value = None

    assert list(_profiler().profile_table("ns.tbl", table)) == []


def test_profile_tolerates_non_numeric_file_size() -> None:
    table = _mock_table_with_entries([], total_records="3")
    table.current_snapshot.return_value.summary.additional_properties[
        "total-files-size"
    ] = "unknown"

    profile = next(iter(_profiler().profile_table("ns.tbl", table)))

    assert isinstance(profile, DatasetProfileClass)
    assert profile.sizeInBytes is None
    assert profile.rowCount == 3


def test_render_value_failure_yields_none_and_warns() -> None:
    profiler = _profiler()

    rendered = profiler._render_value("ns.tbl", DateType(), "not-a-day-count")

    assert rendered is None
    assert profiler.report.warnings.total_elements == 1


# --------------------------------------------------------------------------- #
# REST catalog connection settings
# --------------------------------------------------------------------------- #


def test_timeout_adapter_applies_default_timeout_only_when_unset() -> None:
    adapter = TimeoutHTTPAdapter(timeout=7)
    request = Request("GET", "http://catalog.test/v1/config").prepare()

    with patch.object(HTTPAdapter, "send", return_value="resp") as send:
        adapter.send(request)
        adapter.send(request, timeout=1)

    assert send.call_args_list[0].kwargs["timeout"] == 7
    assert send.call_args_list[1].kwargs["timeout"] == 1


def test_rest_catalog_gets_configured_timeout_and_retries() -> None:
    config = IcebergSourceConfig(
        catalog={
            "test": {
                "type": "rest",
                "uri": "http://catalog.test",
                "connection": {"timeout": 30, "retry": {"total": 5}},
            }
        }
    )
    rest_catalog = MagicMock(spec=RestCatalog)
    rest_catalog._session = MagicMock()

    with patch(
        "datahub.ingestion.source.iceberg.iceberg_common.load_catalog",
        return_value=rest_catalog,
    ):
        assert config.get_catalog() is rest_catalog

    session = cast(MagicMock, rest_catalog._session)
    mounted = {c.args[0]: c.args[1] for c in session.mount.call_args_list}
    assert set(mounted) == {"http://", "https://"}
    for adapter in mounted.values():
        assert isinstance(adapter, TimeoutHTTPAdapter)
        assert adapter.timeout == 30
        assert adapter.max_retries.total == 5


# --------------------------------------------------------------------------- #
# Report timing summaries
# --------------------------------------------------------------------------- #


def test_top_table_timings_keeps_only_the_slowest() -> None:
    timings = TopTableTimings(size=2)
    timings.add({"table": "fast", "timing": 1.0})
    timings.add({"table": "slow", "timing": 10.0})
    timings.add({"table": "medium", "timing": 5.0})
    timings.add({"table": "untimed"})

    assert [t["table"] for t in timings.top_entites] == ["slow", "medium"]
    assert "fast" not in str(timings)


def test_timing_summary_reports_average_and_bounds() -> None:
    timing = TimingClass()
    assert str(timing) == "no timings reported"

    timing.add_timing(1.0)
    timing.add_timing(3.0)

    summary = str(timing)
    assert "average_time" in summary
    assert "min_time" in summary
    assert "max_time" in summary
