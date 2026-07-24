import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from datahub.emitter import mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.models import (
    AasCalcDependency,
    AasTable,
    AasTabularModel,
)
from datahub.ingestion.source.azure_analysis_services.report import (
    AzureAnalysisServicesReport,
)
from datahub.ingestion.source.common.m_query import parser
from datahub.ingestion.source.common.m_query.instance_resolver import (
    ServerToPlatformInstanceResolver,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
)
from datahub.sql_parsing.sqlglot_lineage import ColumnLineageInfo

logger = logging.getLogger(__name__)

# DISCOVER_CALC_DEPENDENCY OBJECT_TYPE / REFERENCED_OBJECT_TYPE values that map
# to a DataHub schema field (and therefore to an intra-model column edge).
_FIELD_DEPENDENCY_TYPES = frozenset(
    {"MEASURE", "COLUMN", "CALC_COLUMN", "CALCULATED_COLUMN", "CALC_TABLE"}
)


@dataclass
class UpstreamLineageResult:
    upstreams: List[UpstreamClass] = field(default_factory=list)
    fine_grained: List[FineGrainedLineageClass] = field(default_factory=list)


@dataclass
class _DependencyEdge:
    downstream_urn: str
    upstream_urn: str


class AasLineageExtractor:
    def __init__(
        self,
        config: AzureAnalysisServicesConfig,
        report: AzureAnalysisServicesReport,
        ctx: PipelineContext,
    ) -> None:
        self.config = config
        self.report = report
        self.ctx = ctx
        self.platform_instance_resolver = ServerToPlatformInstanceResolver(config)

    def _lineage_urn_to_lowercase(self, value: str) -> str:
        if self.config.convert_lineage_urns_to_lowercase:
            return value.lower()
        return value

    def extract_upstream_for_table(
        self, table: AasTable, dataset_urn: str
    ) -> UpstreamLineageResult:
        result = UpstreamLineageResult()
        if not self.config.extract_lineage or not table.expression:
            return result

        try:
            lineages = parser.get_upstream_tables(
                table=table,
                reporter=self.report,
                platform_instance_resolver=self.platform_instance_resolver,
                ctx=self.ctx,
                config=self.config,
                parameters={},
            )
        except Exception as e:
            # The engine is defensive internally, but a table's lineage must
            # never abort the whole model — degrade to a warning and continue.
            self.report.warning(
                title="Upstream lineage extraction failed",
                message="Could not parse a table's M/Power Query for lineage.",
                context=f"table={table.full_name}",
                exc=e,
            )
            return result

        for lineage in lineages:
            for upstream_table in lineage.upstreams:
                result.upstreams.append(
                    UpstreamClass(
                        dataset=self._lineage_urn_to_lowercase(upstream_table.urn),
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                )
            if self.config.extract_column_level_lineage:
                result.fine_grained.extend(
                    self._column_lineage(lineage.column_lineage, dataset_urn)
                )

        if result.upstreams:
            self.report.tables_with_upstream_lineage += 1
        else:
            self.report.tables_without_upstream_lineage += 1
        self.report.column_lineage_edges += len(result.fine_grained)
        return result

    def _column_lineage(
        self, column_lineage: List[ColumnLineageInfo], dataset_urn: str
    ) -> List[FineGrainedLineageClass]:
        fine_grained: List[FineGrainedLineageClass] = []
        for cll in column_lineage:
            downstream = (
                [builder.make_schema_field_urn(dataset_urn, cll.downstream.column)]
                if cll.downstream and cll.downstream.column
                else []
            )
            if not downstream:
                continue
            upstreams = [
                builder.make_schema_field_urn(
                    self._lineage_urn_to_lowercase(column_ref.table),
                    column_ref.column,
                )
                for column_ref in cll.upstreams
                if column_ref.column
            ]
            if not upstreams:
                continue
            fine_grained.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstreams,
                )
            )
        return fine_grained

    def extract_intra_model_lineage(
        self, model: AasTabularModel, dataset_urn_by_table: Dict[str, str]
    ) -> List[FineGrainedLineageClass]:
        # DAX measures / calculated columns reference other columns in the same
        # model. We collapse the DISCOVER_CALC_DEPENDENCY edges into one
        # FineGrainedLineage per downstream field, aggregating its upstreams.
        if not self.config.extract_column_level_lineage:
            return []

        upstreams_by_downstream: Dict[str, List[str]] = {}
        for dependency in model.calc_dependencies:
            edge = self._resolve_dependency_edge(dependency, dataset_urn_by_table)
            if edge is None:
                continue
            bucket = upstreams_by_downstream.setdefault(edge.downstream_urn, [])
            if edge.upstream_urn not in bucket:
                bucket.append(edge.upstream_urn)

        fine_grained = [
            FineGrainedLineageClass(
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[downstream_urn],
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=upstream_urns,
            )
            for downstream_urn, upstream_urns in upstreams_by_downstream.items()
            if upstream_urns
        ]
        self.report.intra_model_dax_edges += len(fine_grained)
        return fine_grained

    def _resolve_dependency_edge(
        self,
        dependency: AasCalcDependency,
        dataset_urn_by_table: Dict[str, str],
    ) -> Optional[_DependencyEdge]:
        object_type = dependency.object_type.upper()
        referenced_type = dependency.referenced_object_type.upper()
        if (
            object_type not in _FIELD_DEPENDENCY_TYPES
            or referenced_type not in _FIELD_DEPENDENCY_TYPES
        ):
            return None
        # Self-reference (a column depending on itself) carries no useful edge.
        if (
            dependency.table == dependency.referenced_table
            and dependency.object_name == dependency.referenced_object
        ):
            return None

        downstream_dataset = dataset_urn_by_table.get(dependency.table.lower())
        upstream_dataset = dataset_urn_by_table.get(dependency.referenced_table.lower())
        if not downstream_dataset or not upstream_dataset:
            return None

        return _DependencyEdge(
            downstream_urn=builder.make_schema_field_urn(
                downstream_dataset, dependency.object_name
            ),
            upstream_urn=builder.make_schema_field_urn(
                upstream_dataset, dependency.referenced_object
            ),
        )
