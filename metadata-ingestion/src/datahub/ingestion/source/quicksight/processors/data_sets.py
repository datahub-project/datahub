import logging
from typing import Any, Dict, Iterable, List, Optional

from botocore.exceptions import ClientError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.containers import ParentResolver
from datahub.ingestion.source.quicksight.processors.data_sources import (
    ResolvedDataSource,
)
from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_constants import (
    DEFAULT_COLUMN_FIELD_TYPE,
    QUICKSIGHT_COLUMN_TYPE_TO_FIELD_TYPE,
    SUBTYPE_DATASET,
)
from datahub.ingestion.source.quicksight.quicksight_lineage import (
    QuickSightLineageExtractor,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.ingestion.source.quicksight.quicksight_urn import PLATFORM
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
)
from datahub.sdk.dataset import Dataset

logger = logging.getLogger(__name__)

# describe_data_set raises this for FILE-type (CSV upload) datasets, which have
# no PhysicalTableMap to describe.
_FILE_DATASET_ERROR_CODE = "InvalidParameterValueException"


class DataSetsProcessor:
    """Emits a Dataset per QuickSight dataset (``QuickSight Dataset`` subtype)
    with schema from ``OutputColumns`` and upstream/column lineage derived from
    the dataset's ``PhysicalTableMap``.

    FILE-type datasets (CSV uploads) cannot be described and degrade to
    summary-only metadata.
    """

    def __init__(
        self,
        config: QuickSightSourceConfig,
        report: QuickSightSourceReport,
        api: QuickSightAPI,
        ctx: PipelineContext,
        parent_resolver: ParentResolver,
        data_source_map: Dict[str, ResolvedDataSource],
        enricher: AssetEnricher,
    ) -> None:
        self.config = config
        self.report = report
        self.api = api
        self.parent_resolver = parent_resolver
        self.enricher = enricher
        self.lineage_extractor = QuickSightLineageExtractor(
            config, report, ctx, data_source_map
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        for summary in self.api.list_data_sets():
            data_set_id = summary.get("DataSetId")
            name = summary.get("Name") or data_set_id or ""
            if not data_set_id:
                continue
            if not self.config.dataset_pattern.allowed(name):
                self.report.datasets.dropped(f"{data_set_id} ({name})")
                continue
            yield from self._emit_data_set(data_set_id, name, summary)

    def _emit_data_set(
        self, data_set_id: str, name: str, summary: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        data_set = self._describe(data_set_id)

        custom_properties = {"dataSetId": data_set_id}
        if summary.get("Arn"):
            custom_properties["arn"] = summary["Arn"]
        import_mode = (data_set or summary).get("ImportMode")
        if import_mode:
            custom_properties["importMode"] = str(import_mode)
        if data_set is None:
            # FILE-type fallback: summary-only, no schema or lineage.
            custom_properties["summaryOnly"] = "true"

        schema_fields = (
            self._build_schema(data_set.get("OutputColumns") or [])
            if data_set is not None
            else None
        )

        # Resolves to the dataset's QuickSight folder when foldered, else the
        # namespace container (if enabled) or the platform root.
        parent = self.parent_resolver(data_set_id)
        dataset = Dataset(
            platform=PLATFORM,
            name=f"{self.api.aws_account_id}.{data_set_id}",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=name,
            subtype=SUBTYPE_DATASET,
            custom_properties=custom_properties,
            parent_container=parent if parent is not None else [],
            schema=schema_fields,
            owners=self.enricher.owners("data_set", data_set_id),
            tags=self.enricher.tags(summary.get("Arn") or ""),
        )

        if data_set is not None:
            upstream_lineage = self.lineage_extractor.get_upstream_lineage(
                dataset.urn.urn(), data_set.get("PhysicalTableMap") or {}
            )
            if upstream_lineage is not None:
                dataset.set_upstreams(upstream_lineage)

        self.report.datasets.processed(f"{data_set_id} ({name})")
        yield from dataset.as_workunits()

    def _describe(self, data_set_id: str) -> Optional[Dict[str, Any]]:
        """Describe a dataset; return ``None`` for FILE-type datasets.

        FILE datasets raise ``InvalidParameterValueException`` (they have no
        physical table to describe), which we translate into a summary-only
        emission rather than dropping the entity.
        """
        try:
            return self.api.describe_data_set(data_set_id)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == _FILE_DATASET_ERROR_CODE:
                self.report.num_file_datasets_summary_only += 1
                logger.info(
                    f"DataSet {data_set_id} is FILE-type (cannot be described); "
                    "emitting summary-only metadata."
                )
                return None
            raise

    @staticmethod
    def _build_schema(output_columns: List[Dict[str, Any]]) -> List[SchemaFieldClass]:
        fields: List[SchemaFieldClass] = []
        for column in output_columns:
            column_name = column.get("Name")
            if not column_name:
                continue
            quicksight_type = column.get("Type") or ""
            field_type = QUICKSIGHT_COLUMN_TYPE_TO_FIELD_TYPE.get(
                quicksight_type, DEFAULT_COLUMN_FIELD_TYPE
            )
            fields.append(
                SchemaFieldClass(
                    fieldPath=column_name,
                    type=SchemaFieldDataTypeClass(type=field_type()),
                    nativeDataType=quicksight_type or "UNKNOWN",
                    description=column.get("Description"),
                )
            )
        return fields
