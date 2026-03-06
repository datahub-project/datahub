import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, cast

from pyiceberg.conversions import from_bytes
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    PrimitiveType,
    TimestampType,
    TimestamptzType,
    TimeType,
)
from pyiceberg.utils.datetime import (
    days_to_date,
    to_human_time,
    to_human_timestamp,
    to_human_timestamptz,
)
from typing_extensions import TypeGuard

from datahub.emitter.mce_builder import get_sys_time
from datahub.ingestion.source.iceberg.iceberg_common import (
    IcebergProfilingConfig,
    IcebergSourceReport,
)
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    LatestPartitionProfilesClass,
    PartitionProfileClass,
    PartitionSpecClass,
    PartitionTypeClass,
    _Aspect,
)
from datahub.utilities.perf_timer import PerfTimer

LOGGER = logging.getLogger(__name__)


class IcebergProfiler:
    def __init__(
        self,
        report: IcebergSourceReport,
        config: IcebergProfilingConfig,
    ) -> None:
        self.report: IcebergSourceReport = report
        self.config: IcebergProfilingConfig = config
        self.platform: str = "iceberg"

    def _aggregate_counts(
        self,
        aggregated_count: Dict[int, int],
        manifest_counts: Dict[int, int],
    ) -> Dict[int, int]:
        return {
            k: aggregated_count.get(k, 0) + manifest_counts.get(k, 0)
            for k in set(aggregated_count) | set(manifest_counts)
        }

    def _aggregate_bounds(
        self,
        schema: Schema,
        aggregator: Callable,
        aggregated_values: Dict[int, Any],
        manifest_values: Dict[int, bytes],
    ) -> None:
        for field_id, value_encoded in manifest_values.items():
            try:
                field = schema.find_field(field_id)
            except ValueError:
                # Bounds in manifests can reference historical field IDs that are not part of the current schema.
                # We simply not profile those since we only care about the current snapshot.
                continue
            if IcebergProfiler._is_numeric_type(field.field_type):
                value_decoded = from_bytes(field.field_type, value_encoded)
                if value_decoded is not None:
                    agg_value = aggregated_values.get(field_id)
                    aggregated_values[field_id] = (
                        aggregator(agg_value, value_decoded)
                        if agg_value
                        else value_decoded
                    )

    def profile_table(
        self,
        dataset_name: str,
        table: Table,
    ) -> Iterable[_Aspect]:
        """This method will profile the supplied Iceberg table by looking at the table's manifest.

        The overall profile of the table is aggregated from the individual manifest files.
        We can extract the following from those manifests:
          - "field minimum values"
          - "field maximum values"
          - "field null occurences"

        "field distinct value occurences" cannot be computed since the 'value_counts' only apply for
        a manifest, making those values innacurate.  For example, if manifest A has 2 unique values
        and manifest B has 1, it is possible that the value in B is also in A, hence making the total
        number of unique values 2 and not 3.

        Args:
            dataset_name (str): dataset name of the table to profile, mainly used in error reporting
            dataset_urn (str): dataset urn of the table to profile
            table (Table): Iceberg table to profile.

        Raises:
            Exception: Occurs when a table manifest cannot be loaded.

        Yields:
            Iterator[Iterable[MetadataWorkUnit]]: Workunits related to datasetProfile.
        """
        with PerfTimer() as timer:
            LOGGER.debug(f"Starting profiling of dataset: {dataset_name}")
            current_snapshot = table.current_snapshot()
            if not current_snapshot:
                # Table has no data, cannot profile, or we can't get current_snapshot.
                return

            row_count = (
                int(current_snapshot.summary.additional_properties["total-records"])
                if current_snapshot.summary
                else 0
            )
            column_count = len(
                [
                    field.field_id
                    for field in table.schema().fields
                    if field.field_type.is_primitive
                ]
            )
            dataset_profile = DatasetProfileClass(
                timestampMillis=get_sys_time(),
                rowCount=row_count,
                columnCount=column_count,
            )
            dataset_profile.partitionSpec = PartitionSpecClass(
                partition="FULL_TABLE_SNAPSHOT",
                type=PartitionTypeClass.FULL_TABLE,
            )
            needs_field_profiles = (
                self.config.include_field_null_count
                or self.config.include_field_min_value
                or self.config.include_field_max_value
            )
            if needs_field_profiles:
                dataset_profile.fieldProfiles = []

            total_size_bytes = 0
            total_file_count = 0
            total_delete_file_count = 0
            total_small_file_count = 0
            total_file_count_with_size = 0
            partition_keys: Set[str] = set()
            partition_stats: Dict[str, Dict[str, int]] = {}
            summary_stats: Dict[str, Dict[str, int]] = {}
            summary_members: Dict[str, Set[str]] = {}
            null_counts: Dict[int, int] = {}
            min_bounds: Dict[int, Any] = {}
            max_bounds: Dict[int, Any] = {}
            manifests: List[Any] = []

            try:
                time_partition_fields = self._get_time_partition_fields(table)
                manifests = list(current_snapshot.manifests(table.io))
                small_file_threshold_bytes = 100 * 1024 * 1024

                def _is_delete_file(data_file: Any) -> bool:
                    content = getattr(data_file, "content", None)
                    if content is None:
                        return False
                    content_name = getattr(content, "name", None)
                    if isinstance(content_name, str):
                        return "DELETE" in content_name.upper()
                    if isinstance(content, int):
                        return content != 0
                    return "DELETE" in str(content).upper()

                for manifest in manifests:
                    for manifest_entry in manifest.fetch_manifest_entry(table.io):
                        data_file = manifest_entry.data_file
                        partition_mapping = self._partition_mapping(
                            table, data_file.partition
                        )
                        partition_key = self._format_partition_key(
                            partition_mapping
                        )
                        summary_key = self._format_summary_partition_key(
                            time_partition_fields, partition_mapping
                        )

                        stats = None
                        if summary_key:
                            stats = summary_stats.setdefault(
                                summary_key, {"row_count": 0, "size_bytes": 0}
                            )
                            if partition_key:
                                summary_members.setdefault(
                                    summary_key, set()
                                ).add(partition_key)
                        elif partition_key:
                            stats = partition_stats.setdefault(
                                partition_key, {"row_count": 0, "size_bytes": 0}
                            )

                        if stats is not None:
                            stats["row_count"] += int(
                                data_file.record_count or 0
                            )
                            file_size_bytes = getattr(
                                data_file, "file_size_in_bytes", None
                            )
                            if file_size_bytes is not None:
                                stats["size_bytes"] += int(file_size_bytes)

                        total_file_count += 1
                        if _is_delete_file(data_file):
                            total_delete_file_count += 1
                        if partition_key:
                            partition_keys.add(partition_key)

                        if self.config.include_field_null_count:
                            null_counts = self._aggregate_counts(
                                null_counts, data_file.null_value_counts
                            )
                        if self.config.include_field_min_value:
                            self._aggregate_bounds(
                                table.schema(),
                                min,
                                min_bounds,
                                data_file.lower_bounds,
                            )
                        if self.config.include_field_max_value:
                            self._aggregate_bounds(
                                table.schema(),
                                max,
                                max_bounds,
                                data_file.upper_bounds,
                            )

                        file_size_bytes = getattr(
                            data_file, "file_size_in_bytes", None
                        )
                        if file_size_bytes is not None:
                            total_size_bytes += int(file_size_bytes)
                            total_file_count_with_size += 1
                            if int(file_size_bytes) < small_file_threshold_bytes:
                                total_small_file_count += 1

                for key, stats in summary_stats.items():
                    stats["partition_count"] = len(
                        summary_members.get(key, set())
                    )
                for key in partition_stats.keys():
                    partition_stats[key]["partition_count"] = 1
            except Exception as e:
                self.report.warning(
                    title="Error when profiling a table",
                    message="Skipping profiling of the table due to errors",
                    context=dataset_name,
                    exc=e,
                )

            if row_count and needs_field_profiles:
                # Iterating through fieldPaths introduces unwanted stats for list element fields...
                for field_path, field_id in table.schema()._name_to_id.items():
                    field = table.schema().find_field(field_id)
                    column_profile = DatasetFieldProfileClass(fieldPath=field_path)
                    if self.config.include_field_null_count:
                        column_profile.nullCount = cast(
                            int, null_counts.get(field_id, 0)
                        )
                        column_profile.nullProportion = float(
                            column_profile.nullCount / row_count
                        )

                    if self.config.include_field_min_value:
                        column_profile.min = (
                            self._render_value(
                                dataset_name,
                                field.field_type,
                                min_bounds.get(field_id),
                            )
                            if field_id in min_bounds
                            else None
                        )
                    if self.config.include_field_max_value:
                        column_profile.max = (
                            self._render_value(
                                dataset_name,
                                field.field_type,
                                max_bounds.get(field_id),
                            )
                            if field_id in max_bounds
                            else None
                        )
                    dataset_profile.fieldProfiles.append(column_profile)

            if total_size_bytes:
                dataset_profile.sizeInBytes = total_size_bytes
            if manifests:
                dataset_profile.totalManifestFiles = len(manifests)
            if total_file_count:
                dataset_profile.totalFiles = total_file_count
            if total_delete_file_count:
                dataset_profile.totalDeleteFiles = total_delete_file_count
            if total_small_file_count:
                dataset_profile.totalFilesLessThan100Mb = total_small_file_count
            if partition_keys:
                dataset_profile.totalPartitions = len(partition_keys)
            if total_file_count_with_size:
                dataset_profile.avgFileSizeBytes = (
                    total_size_bytes / total_file_count_with_size
                )

            time_taken = timer.elapsed_seconds()
            self.report.report_table_profiling_time(
                time_taken, dataset_name, table.metadata_location
            )
            LOGGER.debug(
                f"Finished profiling of dataset: {dataset_name} in {time_taken}"
            )

        yield dataset_profile
        stats_to_emit = summary_stats if summary_stats else partition_stats
        snapshot_timestamp = get_sys_time()
        latest_profiles: List[PartitionProfileClass] = []
        for partition_key, stats in stats_to_emit.items():
            if not stats["row_count"] and not stats["size_bytes"]:
                continue
            profile = PartitionProfileClass(
                partition=partition_key,
                rowCount=stats["row_count"],
                columnCount=column_count,
            )
            if stats["size_bytes"]:
                profile.sizeInBytes = stats["size_bytes"]
            if stats.get("partition_count") is not None:
                profile.partitionCount = stats["partition_count"]
            latest_profiles.append(profile)
        yield LatestPartitionProfilesClass(
            timestampMillis=snapshot_timestamp,
            partitionProfiles=latest_profiles,
        )

    def _partition_mapping(self, table: Table, partition: Any) -> Dict[str, Any]:
        if partition is None:
            return {}
        for attr in ("as_dict", "to_dict"):
            if hasattr(partition, attr):
                try:
                    mapping = getattr(partition, attr)()
                    if isinstance(mapping, dict) and mapping:
                        return mapping
                except Exception:
                    pass
        if isinstance(partition, dict) and partition:
            return partition

        spec_fields = getattr(table.spec(), "fields", []) or []
        if spec_fields and hasattr(partition, "__getitem__"):
            mapping: Dict[str, Any] = {}
            for idx, field in enumerate(spec_fields):
                name = getattr(field, "name", None) or f"field_{idx}"
                try:
                    mapping[name] = partition[idx]
                except Exception:
                    mapping[name] = None
            return mapping
        return {}

    @staticmethod
    def _format_partition_key(partition_mapping: Dict[str, Any]) -> Optional[str]:
        if not partition_mapping:
            return None
        return "/".join(f"{key}={partition_mapping[key]}" for key in partition_mapping)

    @staticmethod
    def _get_time_partition_fields(table: Table) -> List[str]:
        spec_fields = getattr(table.spec(), "fields", []) or []
        time_fields: List[str] = []
        for field in spec_fields:
            transform = getattr(field, "transform", None)
            transform_name = str(transform).lower() if transform is not None else ""
            if transform_name in {"hour", "day", "month", "year"}:
                name = getattr(field, "name", None)
                if name:
                    time_fields.append(name)
        return time_fields

    @staticmethod
    def _format_summary_partition_key(
        time_fields: List[str], partition_mapping: Dict[str, Any]
    ) -> Optional[str]:
        if not time_fields or not partition_mapping:
            return None
        parts = [
            f"{name}={partition_mapping[name]}"
            for name in time_fields
            if name in partition_mapping
        ]
        return "/".join(parts) if parts else None

    def _render_value(
        self, dataset_name: str, value_type: IcebergType, value: Any
    ) -> Optional[str]:
        try:
            if isinstance(value_type, TimestampType):
                return to_human_timestamp(value)
            if isinstance(value_type, TimestamptzType):
                return to_human_timestamptz(value)
            elif isinstance(value_type, DateType):
                return days_to_date(value).strftime("%Y-%m-%d")
            elif isinstance(value_type, TimeType):
                return to_human_time(value)
            return str(value)
        except Exception as e:
            self.report.warning(
                title="Couldn't render value when profiling a table",
                message="Encountered error, when trying to redner a value for table profile.",
                context=str(
                    {
                        "value": value,
                        "value_type": value_type,
                        "dataset_name": dataset_name,
                    }
                ),
                exc=e,
            )
            return None

    @staticmethod
    def _is_numeric_type(type: IcebergType) -> TypeGuard[PrimitiveType]:
        return isinstance(
            type,
            (
                DateType,
                DecimalType,
                DoubleType,
                FloatType,
                IntegerType,
                LongType,
                TimestampType,
                TimestamptzType,
                TimeType,
            ),
        )
