import logging
from typing import Callable, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.m_query import native_sql_parser
from datahub.ingestion.source.powerbi.rest_api_wrapper import data_classes
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DialectClass,
    DimensionClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelPropertiesClass,
    StatusClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sdk._semantic_shared import DialectExpressionInput, build_metric_expression
from datahub.sdk.metric import Metric
from datahub.sdk.semantic_model import SemanticModel

logger = logging.getLogger(__name__)

# Power BI measures and calculated columns are authored in DAX. DAX is not one
# of the Dialect enum values, so the expression is carried under OTHER rather
# than mislabeled as SQL/MDX.
_DAX_DIALECT = DialectClass.OTHER

# Column dataType values (case-insensitive) that make a dimension a time
# dimension. Power BI reports the TOM data type name here.
_TIME_DIMENSION_TYPES = frozenset({"datetime", "date", "time"})


class PowerBiSemanticModelMapper:
    """Emits a Power BI dataset as `semanticModel` + `metric` + logical datasets.

    This is the opt-in shape behind ``emit_semantic_model_entities``. Each Power
    BI table keeps its existing dataset URN (so the M-Query warehouse lineage and
    any downstream references are preserved) but gains the ``Semantic Model
    Dataset`` subtype and a ``semanticModelProperties`` back-reference. Each
    measure becomes a ``metric`` whose ``metricUpstreams`` points at that logical
    dataset, giving the chain ``Metric -> Semantic Model Dataset -> physical
    table``.
    """

    def __init__(
        self,
        *,
        config: PowerBiDashboardSourceConfig,
        reporter: PowerBiDashboardSourceReport,
        table_dataset_urn: Callable[[data_classes.Table], str],
        extract_lineage: Callable[
            [data_classes.Table, str, data_classes.Workspace],
            List[MetadataChangeProposalWrapper],
        ],
        extract_dataset_schema: Callable[
            [data_classes.Table, str], List[MetadataChangeProposalWrapper]
        ],
        data_platform_instance_aspect: Callable[[], DataPlatformInstanceClass],
        workspace_container_urn: Callable[[data_classes.Workspace], str],
        append_tag_mcp: Callable[
            [List[MetadataChangeProposalWrapper], str, str, List[str]], None
        ],
        owner_urn: Callable[[str], str],
    ) -> None:
        self._config = config
        self._report = reporter
        self._table_dataset_urn = table_dataset_urn
        self._extract_lineage = extract_lineage
        self._extract_dataset_schema = extract_dataset_schema
        self._data_platform_instance_aspect = data_platform_instance_aspect
        self._workspace_container_urn = workspace_container_urn
        self._append_tag_mcp = append_tag_mcp
        self._owner_urn = owner_urn

    def emit(
        self,
        dataset: data_classes.PowerBIDataset,
        workspace: data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        workspace_part = (
            workspace.id
            if self._config.workspace_id_as_urn_part
            else workspace.name.replace(" ", "_").lower()
        )
        model = self._semantic_model(dataset, workspace, workspace_part)
        model_urn = str(model.urn)

        mcps: List[MetadataChangeProposalWrapper] = list(model.as_mcps())
        self._report.semantic_models_emitted += 1

        for table in dataset.tables:
            ds_urn = self._table_dataset_urn(table)
            mcps.extend(
                self._logical_dataset_mcps(dataset, table, ds_urn, model_urn, workspace)
            )
            self._report.semantic_model_datasets_emitted += 1
            for measure in table.measures or []:
                metric = self._metric(
                    dataset, table, measure, ds_urn, model_urn, workspace_part
                )
                mcps.extend(metric.as_mcps())
                self._report.metrics_emitted += 1

        return mcps

    def _semantic_model(
        self,
        dataset: data_classes.PowerBIDataset,
        workspace: data_classes.Workspace,
        workspace_part: str,
    ) -> SemanticModel:
        browse_path = BrowsePathsV2Class(
            path=[
                BrowsePathEntryClass(
                    id=workspace.name,
                    urn=self._workspace_container_urn(workspace),
                )
            ]
        )
        return SemanticModel(
            platform=Constant.PLATFORM_NAME,
            path=workspace_part,
            id=dataset.id,
            platform_instance=self._config.platform_instance,
            name=dataset.name or dataset.id,
            description=dataset.description,
            extra_aspects=[browse_path],
        )

    def _logical_dataset_mcps(
        self,
        dataset: data_classes.PowerBIDataset,
        table: data_classes.Table,
        ds_urn: str,
        model_urn: str,
        workspace: data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        alias = table.name
        mcps: List[MetadataChangeProposalWrapper] = [
            MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=DatasetPropertiesClass(
                    name=table.name,
                    description=dataset.description,
                    externalUrl=dataset.webUrl,
                    customProperties={Constant.DATASET_ID: dataset.id},
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=StatusClass(removed=False),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=SubTypesClass(
                    typeNames=[DatasetSubTypes.SEMANTIC_MODEL_DATASET]
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=SemanticModelPropertiesClass(
                    alias=alias, semanticModel=model_urn
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=self._data_platform_instance_aspect(),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(
                            id=workspace.name,
                            urn=self._workspace_container_urn(workspace),
                        ),
                        BrowsePathEntryClass(
                            id=dataset.name or dataset.id, urn=model_urn
                        ),
                    ]
                ),
            ),
        ]

        if table.expression:
            converted = native_sql_parser.remove_special_characters(table.expression)
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=ds_urn,
                    aspect=ViewPropertiesClass(
                        materialized=False,
                        viewLogic=converted,
                        viewLanguage="m_query",
                    ),
                )
            )

        # Schema first (the annotation MCPs are anchored on the schemaField URNs
        # these fields create), then the per-field semantic annotations.
        mcps.extend(self._extract_dataset_schema(table, ds_urn))
        mcps.extend(self._field_annotation_mcps(table, ds_urn, alias))

        if (
            self._config.extract_ownership
            and self._config.ownership.dataset_configured_by_as_owner
            and dataset.configuredBy
        ):
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=ds_urn,
                    aspect=OwnershipClass(
                        owners=[
                            OwnerClass(
                                owner=self._owner_urn(dataset.configuredBy),
                                type=OwnershipTypeClass.NONE,
                            )
                        ]
                    ),
                )
            )

        if self._config.extract_lineage:
            mcps.extend(self._extract_lineage(table, ds_urn, workspace))

        self._append_tag_mcp(mcps, ds_urn, Constant.DATASET, dataset.tags)

        return mcps

    def _field_annotation_mcps(
        self, table: data_classes.Table, ds_urn: str, alias: str
    ) -> List[MetadataChangeProposalWrapper]:
        mcps: List[MetadataChangeProposalWrapper] = []
        for column in table.columns or []:
            mcps.append(
                self._annotation_mcp(
                    ds_urn,
                    alias,
                    field_path=column.name,
                    semantic_type=SemanticFieldTypeClass.DIMENSION,
                    is_time=(column.dataType or "").lower() in _TIME_DIMENSION_TYPES,
                )
            )
        for measure in table.measures or []:
            mcps.append(
                self._annotation_mcp(
                    ds_urn,
                    alias,
                    field_path=measure.name,
                    semantic_type=SemanticFieldTypeClass.MEASURE,
                    is_time=False,
                )
            )
        return mcps

    @staticmethod
    def _annotation_mcp(
        ds_urn: str,
        alias: str,
        *,
        field_path: str,
        semantic_type: str,
        is_time: bool,
    ) -> MetadataChangeProposalWrapper:
        dimension = (
            DimensionClass(isTime=is_time)
            if semantic_type == SemanticFieldTypeClass.DIMENSION
            else None
        )
        return MetadataChangeProposalWrapper(
            entityUrn=SchemaFieldUrn(ds_urn, field_path).urn(),
            aspect=SemanticFieldAnnotationClass(
                type=semantic_type,
                # The field references itself by qualified name; the real DAX for
                # a measure lives on its metric entity.
                expression=build_metric_expression(f"{alias}.{field_path}"),
                dimension=dimension,
            ),
        )

    def _metric(
        self,
        dataset: data_classes.PowerBIDataset,
        table: data_classes.Table,
        measure: data_classes.Measure,
        ds_urn: str,
        model_urn: str,
        workspace_part: str,
    ) -> Metric:
        expression = None
        if measure.expression and measure.expression.strip():
            expression = DialectExpressionInput(
                expression=measure.expression, dialect=_DAX_DIALECT
            )
        return Metric(
            platform=Constant.PLATFORM_NAME,
            path=f"{workspace_part}.{dataset.id}.{table.name}",
            id=measure.name,
            semantic_model=model_urn,
            platform_instance=self._config.platform_instance,
            name=measure.name,
            description=measure.description,
            expression=expression,
            upstream_datasets=[ds_urn],
        )
