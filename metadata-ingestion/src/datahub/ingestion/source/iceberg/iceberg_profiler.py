from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, Union

from iceberg.api.data_file import DataFile
from iceberg.api.manifest_file import ManifestFile
from iceberg.api.schema import Schema
from iceberg.api.snapshot import Snapshot
from iceberg.api.table import Table
from iceberg.api.types import Conversions, NestedField, Type, TypeID
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
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
)


@dataclass
class IcebergProfiler:
    report: IcebergSourceReport
    config: IcebergProfilingConfig

    def __init__(
        self,
        report: IcebergSourceReport,
        config: IcebergProfilingConfig,
    ):
        self.report = report
        self.config = config
        self.platform = "iceberg"

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
        for field_id, value_encoded in manifest_values.items():
            field = schema.find_field(field_id)
            if not field:
                raise RuntimeError(f"Cannot find field_id {field_id} in schema")
            value_decoded = Conversions.from_byte_buffer(field.type, value_encoded)
            if value_decoded:
                agg_value = aggregated_values.get(field_id)
                aggregated_values[field_id] = (
                    aggregator(agg_value, value_decoded) if agg_value else value_decoded
                )

    def profile_table(
        self,
        dataset_name: str,
        dataset_urn: str,
        table: Table,
        schema_metadata: SchemaMetadata,
    ) -> Iterable[MetadataWorkUnit]:

        if len(table.snapshots()) == 0:
            # Table has no data, cannot profile.
            return
        row_count = int(table.current_snapshot().summary["total-records"])
        dataset_profile = DatasetProfileClass(
            timestampMillis=get_sys_time(),
            rowCount=row_count,
            columnCount=len(schema_metadata.fields)
            # columnCount=len(table.schema().columns())
        )
        dataset_profile.fieldProfiles = []

        field_paths: Dict[
            int, str
        ] = (
            table.schema()._id_to_name
        )  # dict[int, str] = {v: k for k, v in index_by_name(table.schema()).items()}
        current_snapshot: Snapshot = table.current_snapshot()
        total_count = 0
        null_counts: Dict[int, int] = {}
        # valueCounts = {}
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
                    # self._aggregateCounts(
                    #     valueCounts, fieldPaths, dataFile.value_counts()
                    # )
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
        if row_count is not None and row_count > 0:
            # Iterating through fieldPaths introduces unwanted stats for list element fields...
            for field_id, field_path in field_paths.items():
                field: NestedField = table.schema().find_field(field_id)
                column_profile = DatasetFieldProfileClass(fieldPath=field_path)
                if self.config.include_field_null_count:
                    column_profile.nullCount = null_counts.get(field_id, 0)
                    column_profile.nullProportion = (
                        column_profile.nullCount / row_count
                        if column_profile.nullCount
                        else None
                    )
                # column_profile.uniqueCount = valueCounts.get(fieldPath, 0)
                # non_null_count = rowCount - column_profile.nullCount
                # if non_null_count is not None and non_null_count > 0:
                #     column_profile.uniqueProportion = column_profile.uniqueCount / non_null_count

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
