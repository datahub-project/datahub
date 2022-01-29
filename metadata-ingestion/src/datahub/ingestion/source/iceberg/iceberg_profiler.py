from dataclasses import dataclass
from typing import Any, Callable, Dict

from iceberg.api.data_file import DataFile
from iceberg.api.manifest_file import ManifestFile
from iceberg.api.schema import Schema
from iceberg.api.snapshot import Snapshot
from iceberg.api.table import Table
from iceberg.api.types import Conversions
from iceberg.core.filesystem import FileSystemInputFile
from iceberg.core.manifest_reader import ManifestReader
from iceberg.exceptions.exceptions import FileSystemNotFound

from datahub.emitter.mce_builder import get_sys_time, make_dataset_urn
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
    ):
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
    ):
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
        self, env: str, dataset_name: str, table: Table, schemaMetadata: SchemaMetadata
    ) -> MetadataWorkUnit:
        rowCount = int(table.current_snapshot().summary["total-records"])
        dataset_profile = DatasetProfileClass(
            timestampMillis=get_sys_time(),
            rowCount=rowCount,
            columnCount=len(schemaMetadata.fields),
            fieldProfiles=[]
            # columnCount=len(table.schema().columns())
        )

        fieldPaths: dict[
            int, str
        ] = (
            table.schema()._id_to_name
        )  # dict[int, str] = {v: k for k, v in index_by_name(table.schema()).items()}
        current_snapshot: Snapshot = table.current_snapshot()
        totalCount = 0
        nullCounts = {}
        valueCounts = {}
        minBounds = {}
        maxBounds = {}
        manifest: ManifestFile
        try:
            for manifest in current_snapshot.manifests:
                manifest_input_file = FileSystemInputFile.from_location(
                    manifest.manifest_path, table.ops.conf
                )
                manifest_reader = ManifestReader.read(manifest_input_file)
                dataFile: DataFile
                for dataFile in manifest_reader.iterator():
                    self._aggregateCounts(
                        nullCounts, fieldPaths, dataFile.null_value_counts()
                    )
                    self._aggregateCounts(
                        valueCounts, fieldPaths, dataFile.value_counts()
                    )
                    self._aggregateBounds(
                        table.schema(),
                        min,
                        minBounds,
                        fieldPaths,
                        dataFile.lower_bounds(),
                    )
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
            for fieldPath in fieldPaths.values():
                column_profile = DatasetFieldProfileClass(fieldPath=fieldPath)
                column_profile.nullCount = nullCounts.get(fieldPath, 0)
                column_profile.nullProportion = column_profile.nullCount / rowCount
                # column_profile.uniqueCount = valueCounts.get(fieldPath, 0)
                # non_null_count = rowCount - column_profile.nullCount
                # if non_null_count is not None and non_null_count > 0:
                #     column_profile.uniqueProportion = column_profile.uniqueCount / non_null_count

                # TODO: transform value into domain? Example: transform number to timestamp if type is TimestampType
                column_profile.min = (
                    str(minBounds.get(fieldPath)) if fieldPath in minBounds else None
                )
                column_profile.max = (
                    str(maxBounds.get(fieldPath)) if fieldPath in maxBounds else None
                )
                dataset_profile.fieldProfiles.append(column_profile)

        datasetUrn = make_dataset_urn(self.platform, dataset_name, env)
        # https://github.com/linkedin/datahub/blob/599edd22aeb6b17c71e863587f606c73b87e3b58/metadata-ingestion/src/datahub/ingestion/source/sql/sql_common.py#L829
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=datasetUrn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="datasetProfile",
            aspect=dataset_profile,
        )
        wu = MetadataWorkUnit(id=f"profile-{dataset_name}", mcp=mcp)
        self.report.report_workunit(wu)
        self.report.report_entity_profiled(dataset_name)
        yield wu
