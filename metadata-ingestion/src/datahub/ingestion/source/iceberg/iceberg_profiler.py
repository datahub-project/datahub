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
        aggregated_count: Dict[str, int],
        field_paths: Dict[int, str],
        manifest_counts: Dict[int, int],
    ) -> None:
        for field_id, count in manifest_counts.items():
            field_path = field_paths.get(field_id)
            if not field_path:
                raise RuntimeError(f"Failed to find fieldPath for field_id {field_id}")
            agg = aggregated_count.get(field_path) or 0
            aggregated_count[field_path] = agg + count

    def _aggregate_bounds(
        self,
        schema: Schema,
        aggregator: Callable,
        aggregated_values: Dict[str, Any],
        field_paths: Dict[int, str],
        manifest_values: Dict[int, Any],
    ) -> None:
        for field_id, value_encoded in manifest_values.items():
            field = schema.find_field(field_id)
            value_decoded = Conversions.from_byte_buffer(field.type, value_encoded)
            if value_decoded:
                field_path = field_paths.get(field_id)
                if not field_path:
                    raise RuntimeError(
                        f"Failed to find fieldPath for field_id {field_id}"
                    )
                agg = aggregated_values.get(field_path)
                aggregated_values[field_path] = (
                    aggregator(agg, value_decoded) if agg else value_decoded
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
        null_counts: Dict[str, int] = {}
        # valueCounts = {}
        min_bounds: Dict[str, Any] = {}
        max_bounds: Dict[str, Any] = {}
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
                        self._aggregate_counts(
                            null_counts, field_paths, data_file.null_value_counts()
                        )
                    # self._aggregateCounts(
                    #     valueCounts, fieldPaths, dataFile.value_counts()
                    # )
                    if self.config.include_field_min_value:
                        self._aggregate_bounds(
                            table.schema(),
                            min,
                            min_bounds,
                            field_paths,
                            data_file.lower_bounds(),
                        )
                    if self.config.include_field_max_value:
                        self._aggregate_bounds(
                            table.schema(),
                            max,
                            max_bounds,
                            field_paths,
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
                    column_profile.nullCount = null_counts.get(field_path, 0)
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
                        self._renderValue(field.type, min_bounds.get(field_path))
                        if field_path in min_bounds
                        else None
                    )
                if self.config.include_field_max_value:
                    column_profile.max = (
                        self._renderValue(field.type, max_bounds.get(field_path))
                        if field_path in max_bounds
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
    def _renderValue(self, value_type: Type, value: Any) -> Union[str, None]:
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
                f"Error when profiling a {value_type} field with value {value}: {e}",
            )
            return None
