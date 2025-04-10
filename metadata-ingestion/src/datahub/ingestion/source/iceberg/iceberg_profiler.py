import logging
from typing import Any, Callable, Dict, Iterable, Optional, cast

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

from datahub.emitter.mce_builder import get_sys_time
from datahub.ingestion.source.iceberg.iceberg_common import (
    IcebergProfilingConfig,
    IcebergSourceReport,
)
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
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
        for field_id, value_encoded in manifest_values.items():  # type: int, Any
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
            dataset_profile.fieldProfiles = []

            total_count = 0
            null_counts: Dict[int, int] = {}
            min_bounds: Dict[int, Any] = {}
            max_bounds: Dict[int, Any] = {}
            try:
                for manifest in current_snapshot.manifests(table.io):
                    for manifest_entry in manifest.fetch_manifest_entry(table.io):
                        data_file = manifest_entry.data_file
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
                        total_count += data_file.record_count
            except Exception as e:
                self.report.warning(
                    title="Error when profiling a table",
                    message="Skipping profiling of the table due to errors",
                    context=dataset_name,
                    exc=e,
                )
            if row_count:
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
                                dataset_name, field.field_type, min_bounds.get(field_id)
                            )
                            if field_id in min_bounds
                            else None
                        )
                    if self.config.include_field_max_value:
                        column_profile.max = (
                            self._render_value(
                                dataset_name, field.field_type, max_bounds.get(field_id)
                            )
                            if field_id in max_bounds
                            else None
                        )
                    dataset_profile.fieldProfiles.append(column_profile)
            time_taken = timer.elapsed_seconds()
            self.report.report_table_profiling_time(
                time_taken, dataset_name, table.metadata_location
            )
            LOGGER.debug(
                f"Finished profiling of dataset: {dataset_name} in {time_taken}"
            )

        yield dataset_profile

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
    def _is_numeric_type(type: IcebergType) -> bool:
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
