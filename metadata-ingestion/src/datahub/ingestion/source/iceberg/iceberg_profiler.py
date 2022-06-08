from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, Union, cast

from iceberg.api import types as IcebergTypes
from iceberg.api.data_file import DataFile
from iceberg.api.manifest_file import ManifestFile
from iceberg.api.schema import Schema
from iceberg.api.snapshot import Snapshot
from iceberg.api.table import Table
from iceberg.api.types import Conversions, NestedField, Type, TypeID
from iceberg.core.base_table import BaseTable
from iceberg.core.filesystem import FileSystemInputFile
from iceberg.core.manifest_reader import ManifestReader
from iceberg.exceptions.exceptions import FileSystemNotFound

from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.iceberg.iceberg_common import (
    IcebergProfilingConfig,
    IcebergSourceReport,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
)


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
        manifest_values: Dict[int, Any],
    ) -> None:
        for field_id, value_encoded in manifest_values.items():  # type: int, Any
            field: NestedField = schema.find_field(field_id)
            # Bounds in manifests can reference historical field IDs that are not part of the current schema.
            # We simply not profile those since we only care about the current snapshot.
            if field and IcebergProfiler._is_numeric_type(field.type):
                value_decoded = Conversions.from_byte_buffer(field.type, value_encoded)
                if value_decoded:
                    agg_value = aggregated_values.get(field_id)
                    aggregated_values[field_id] = (
                        aggregator(agg_value, value_decoded)
                        if agg_value
                        else value_decoded
                    )

    def profile_table(
        self,
        dataset_name: str,
        dataset_urn: str,
        table: Table,
    ) -> Iterable[MetadataWorkUnit]:
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
        if not table.snapshots() or not isinstance(table, BaseTable):
            # Table has no data, cannot profile, or we can't get current_snapshot.
            return

        row_count: int = int(table.current_snapshot().summary["total-records"])
        column_count: int = len(table.schema()._id_to_name)
        dataset_profile = DatasetProfileClass(
            timestampMillis=get_sys_time(),
            rowCount=row_count,
            columnCount=column_count,
        )
        dataset_profile.fieldProfiles = []

        field_paths: Dict[int, str] = table.schema()._id_to_name
        current_snapshot: Snapshot = table.current_snapshot()
        total_count: int = 0
        null_counts: Dict[int, int] = {}
        min_bounds: Dict[int, Any] = {}
        max_bounds: Dict[int, Any] = {}
        manifest: ManifestFile
        try:
            for manifest in current_snapshot.manifests:
                manifest_input_file = FileSystemInputFile.from_location(
                    manifest.manifest_path, table.ops.conf
                )
                manifest_reader = ManifestReader.read(manifest_input_file)
                data_file: DataFile
                for data_file in manifest_reader.iterator():
                    if self.config.include_field_null_count:
                        null_counts = self._aggregate_counts(
                            null_counts, data_file.null_value_counts()
                        )
                    if self.config.include_field_min_value:
                        self._aggregate_bounds(
                            table.schema(),
                            min,
                            min_bounds,
                            data_file.lower_bounds(),
                        )
                    if self.config.include_field_max_value:
                        self._aggregate_bounds(
                            table.schema(),
                            max,
                            max_bounds,
                            data_file.upper_bounds(),
                        )
                    total_count += data_file.record_count()
        # TODO Work on error handling to provide better feedback.  Iceberg exceptions are weak...
        except FileSystemNotFound as e:
            raise Exception("Error loading table manifests") from e
        if row_count:
            # Iterating through fieldPaths introduces unwanted stats for list element fields...
            for field_id, field_path in field_paths.items():
                field: NestedField = table.schema().find_field(field_id)
                column_profile = DatasetFieldProfileClass(fieldPath=field_path)
                if self.config.include_field_null_count:
                    column_profile.nullCount = cast(int, null_counts.get(field_id, 0))
                    column_profile.nullProportion = float(
                        column_profile.nullCount / row_count
                    )

                if self.config.include_field_min_value:
                    column_profile.min = (
                        self._renderValue(
                            dataset_name, field.type, min_bounds.get(field_id)
                        )
                        if field_id in min_bounds
                        else None
                    )
                if self.config.include_field_max_value:
                    column_profile.max = (
                        self._renderValue(
                            dataset_name, field.type, max_bounds.get(field_id)
                        )
                        if field_id in max_bounds
                        else None
                    )
                dataset_profile.fieldProfiles.append(column_profile)

        # https://github.com/linkedin/datahub/blob/599edd22aeb6b17c71e863587f606c73b87e3b58/metadata-ingestion/src/datahub/ingestion/source/sql/sql_common.py#L829
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="datasetProfile",
            aspect=dataset_profile,
        )
        wu = MetadataWorkUnit(id=f"profile-{dataset_name}", mcp=mcp)
        self.report.report_workunit(wu)
        self.report.report_entity_profiled(dataset_name)
        yield wu

    # The following will eventually be done by the Iceberg API (in the new Python refactored API).
    def _renderValue(
        self, dataset_name: str, value_type: Type, value: Any
    ) -> Union[str, None]:
        try:
            if value_type.type_id == TypeID.TIMESTAMP:
                if value_type.adjust_to_utc:
                    # TODO Deal with utc when required
                    microsecond_unix_ts = value
                else:
                    microsecond_unix_ts = value
                return datetime.fromtimestamp(microsecond_unix_ts / 1000000.0).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            elif value_type.type_id == TypeID.DATE:
                return (datetime(1970, 1, 1, 0, 0) + timedelta(value - 1)).strftime(
                    "%Y-%m-%d"
                )
            return str(value)
        except Exception as e:
            self.report.report_warning(
                "profiling",
                f"Error in dataset {dataset_name} when profiling a {value_type} field with value {value}: {e}",
            )
            return None

    @staticmethod
    def _is_numeric_type(type: Type) -> bool:
        return isinstance(
            type,
            (
                IcebergTypes.DateType,
                IcebergTypes.DecimalType,
                IcebergTypes.DoubleType,
                IcebergTypes.FloatType,
                IcebergTypes.IntegerType,
                IcebergTypes.LongType,
                IcebergTypes.TimestampType,
                IcebergTypes.TimeType,
            ),
        )
