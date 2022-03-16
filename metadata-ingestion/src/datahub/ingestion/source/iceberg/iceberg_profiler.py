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

    def _aggregateCounts(
        self,
        aggregatedCount: Dict[str, int],
        fieldPaths: Dict[int, str],
        manifestCounts: Dict[int, int],
    ) -> None:
        for field_id, count in manifestCounts.items():
            fieldPath = fieldPaths.get(field_id)
            if not fieldPath:
                raise RuntimeError(f"Failed to find fieldPath for field_id {field_id}")
            agg = aggregatedCount.get(fieldPath) or 0
            aggregatedCount[fieldPath] = agg + count

    def _aggregateBounds(
        self,
        schema: Schema,
        aggregator: Callable,
        aggregatedValues: Dict[str, Any],
        fieldPaths: Dict[int, str],
        manifestValues: Dict[int, Any],
    ) -> None:
        for field_id, valueEncoded in manifestValues.items():
            field = schema.find_field(field_id)
            valueDecoded = Conversions.from_byte_buffer(field.type, valueEncoded)
            if valueDecoded:
                fieldPath = fieldPaths.get(field_id)
                if not fieldPath:
                    raise RuntimeError(
                        f"Failed to find fieldPath for field_id {field_id}"
                    )
                agg = aggregatedValues.get(fieldPath)
                aggregatedValues[fieldPath] = (
                    aggregator(agg, valueDecoded) if agg else valueDecoded
                )

    def profileTable(
        self,
        env: str,
        dataset_name: str,
        dataset_urn: str,
        table: Table,
        schemaMetadata: SchemaMetadata,
    ) -> Iterable[MetadataWorkUnit]:

        if len(table.snapshots()) == 0:
            # Table has no data, cannot profile.
            return
        rowCount = int(table.current_snapshot().summary["total-records"])
        dataset_profile = DatasetProfileClass(
            timestampMillis=get_sys_time(),
            rowCount=rowCount,
            columnCount=len(schemaMetadata.fields)
            # columnCount=len(table.schema().columns())
        )
        dataset_profile.fieldProfiles = []

        fieldPaths: Dict[
            int, str
        ] = (
            table.schema()._id_to_name
        )  # dict[int, str] = {v: k for k, v in index_by_name(table.schema()).items()}
        current_snapshot: Snapshot = table.current_snapshot()
        totalCount = 0
        nullCounts: Dict[str, int] = {}
        # valueCounts = {}
        minBounds: Dict[str, Any] = {}
        maxBounds: Dict[str, Any] = {}
        manifest: ManifestFile
        try:
            for manifest in current_snapshot.manifests:
                manifest_input_file = FileSystemInputFile.from_location(
                    manifest.manifest_path, table.ops.conf
                )
                manifest_reader = ManifestReader.read(manifest_input_file)
                dataFile: DataFile
                for dataFile in manifest_reader.iterator():
                    if self.config.include_field_null_count:
                        self._aggregateCounts(
                            nullCounts, fieldPaths, dataFile.null_value_counts()
                        )
                    # self._aggregateCounts(
                    #     valueCounts, fieldPaths, dataFile.value_counts()
                    # )
                    if self.config.include_field_min_value:
                        self._aggregateBounds(
                            table.schema(),
                            min,
                            minBounds,
                            fieldPaths,
                            dataFile.lower_bounds(),
                        )
                    if self.config.include_field_max_value:
                        self._aggregateBounds(
                            table.schema(),
                            max,
                            maxBounds,
                            fieldPaths,
                            dataFile.upper_bounds(),
                        )
                    totalCount += dataFile.record_count()
        # TODO Work on error handling to provide better feedback.  Iceberg exceptions are weak...
        except FileSystemNotFound as e:
            raise Exception("Error loading table manifests") from e
        if rowCount is not None and rowCount > 0:
            # Iterating through fieldPaths introduces unwanted stats for list element fields...
            for fieldId, fieldPath in fieldPaths.items():
                field: NestedField = table.schema().find_field(fieldId)
                column_profile = DatasetFieldProfileClass(fieldPath=fieldPath)
                if self.config.include_field_null_count:
                    column_profile.nullCount = nullCounts.get(fieldPath, 0)
                    column_profile.nullProportion = (
                        column_profile.nullCount / rowCount
                        if column_profile.nullCount
                        else None
                    )
                # column_profile.uniqueCount = valueCounts.get(fieldPath, 0)
                # non_null_count = rowCount - column_profile.nullCount
                # if non_null_count is not None and non_null_count > 0:
                #     column_profile.uniqueProportion = column_profile.uniqueCount / non_null_count

                if self.config.include_field_min_value:
                    column_profile.min = (
                        self._renderValue(field.type, minBounds.get(fieldPath))
                        if fieldPath in minBounds
                        else None
                    )
                if self.config.include_field_max_value:
                    column_profile.max = (
                        self._renderValue(field.type, maxBounds.get(fieldPath))
                        if fieldPath in maxBounds
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
    def _renderValue(self, valueType: Type, value: Any) -> Union[str, None]:
        try:
            if valueType.type_id == TypeID.TIMESTAMP:
                if valueType.adjust_to_utc:
                    # TODO Deal with utc when required
                    microsecond_unix_ts = value
                else:
                    microsecond_unix_ts = value
                return datetime.fromtimestamp(microsecond_unix_ts / 1000000.0).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            elif valueType.type_id == TypeID.DATE:
                return (datetime(1970, 1, 1, 0, 0) + timedelta(value - 1)).strftime(
                    "%Y-%m-%d"
                )
            return str(value)
        except Exception as e:
            self.report.report_warning(
                "profiling",
                f"Error when profiling a {valueType} field with value {value}: {e}",
            )
            return None
