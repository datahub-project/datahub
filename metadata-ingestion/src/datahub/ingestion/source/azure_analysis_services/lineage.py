import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from datahub.emitter import mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure_analysis_services import constants
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
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn

logger = logging.getLogger(__name__)

# DISCOVER_CALC_DEPENDENCY OBJECT_TYPE / REFERENCED_OBJECT_TYPE values that map
# to a DataHub schema field (and therefore to an intra-model column edge).
# CALC_TABLE is intentionally excluded: it is a table object, not a field, so the
# mapper never emits a schema field for it and an edge to it would dangle.
_FIELD_DEPENDENCY_TYPES = frozenset(
    {"MEASURE", "COLUMN", "CALC_COLUMN", "CALCULATED_COLUMN"}
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
        # Only the dataset name should be lowercased; lowercasing the whole URN
        # would fold the platform and env (e.g. PROD) and point lineage at a URN
        # that does not match the emitted entity.
        if self.config.convert_lineage_urns_to_lowercase:
            return lowercase_dataset_urn(value)
        return value

    def extract_upstream_for_table(
        self, table: AasTable, dataset_urn: str
    ) -> UpstreamLineageResult:
        result = UpstreamLineageResult()
        if not self.config.extract_lineage:
            return result

        # Calculated tables are defined by in-model DAX, not an external M/Power
        # Query source, so they have no upstream source lineage (their in-model
        # references are captured separately as intra-model column lineage).
        # Feeding DAX to the M-Query engine only yields parse warnings/timeouts
        # and would mislabel them as import tables missing lineage.
        if table.is_calculated:
            return result

        # A table can have several query partitions (e.g. incremental refresh),
        # each its own M expression against a different source slice. Parse every
        # partition so lineage captures all sources, not just the first. The
        # engine reads a single ``expression`` (the first partition's), so parse a
        # one-partition copy per partition and merge the results. DAX (calculated)
        # partitions are excluded — only M/Power Query partitions are import sources.
        query_partitions = [
            p
            for p in table.partitions
            if p.query_definition
            and p.partition_type != constants.PartitionType.CALCULATED
        ]
        if not query_partitions:
            return result

        seen_upstreams: Set[str] = set()
        seen_fine_grained: Set[Tuple[object, ...]] = set()
        for partition in query_partitions:
            partition_table = table.model_copy(update={"partitions": [partition]})
            for lineage in self._parse_table(partition_table):
                for upstream_table in lineage.upstreams:
                    urn = self._lineage_urn_to_lowercase(upstream_table.urn)
                    if urn in seen_upstreams:
                        continue
                    seen_upstreams.add(urn)
                    result.upstreams.append(
                        UpstreamClass(
                            dataset=urn, type=DatasetLineageTypeClass.TRANSFORMED
                        )
                    )
                if self.config.extract_column_level_lineage:
                    for fgl in self._column_lineage(
                        lineage.column_lineage, dataset_urn
                    ):
                        # Different partitions of the same table often resolve the
                        # same column edge; dedupe so identical FineGrained edges
                        # are not emitted repeatedly (mirrors seen_upstreams).
                        key = (
                            fgl.downstreamType,
                            tuple(fgl.downstreams or []),
                            fgl.upstreamType,
                            tuple(fgl.upstreams or []),
                        )
                        if key in seen_fine_grained:
                            continue
                        seen_fine_grained.add(key)
                        result.fine_grained.append(fgl)

        if result.upstreams:
            self.report.tables_with_upstream_lineage += 1
        else:
            self.report.tables_without_upstream_lineage += 1
        self.report.column_lineage_edges += len(result.fine_grained)
        return result

    def _parse_table(self, table: AasTable) -> List[parser.Lineage]:
        try:
            return parser.get_upstream_tables(
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
            return []

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

        # Only fields the mapper actually emits (columns and measures) are valid
        # lineage endpoints. Building this index lets us drop edges to internal
        # objects that never become fields — AS system RowNumber-* columns and
        # calc-table self-references (where the "column" is the table name) — which
        # would otherwise dangle as unresolvable schema-field URNs in the golden.
        fields_by_table: Dict[str, frozenset] = {
            table.name.lower(): frozenset(
                [col.name.lower() for col in table.columns]
                + [measure.name.lower() for measure in table.measures]
            )
            for table in model.tables
        }

        upstreams_by_downstream: Dict[str, List[str]] = {}
        for dependency in model.calc_dependencies:
            edge = self._resolve_dependency_edge(
                dependency, dataset_urn_by_table, fields_by_table
            )
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
        fields_by_table: Dict[str, frozenset],
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

        # Both endpoints must be fields the mapper emits as schema fields.
        # This drops edges to AS system RowNumber-* columns and to a calc table's
        # own name (a table object, not a field), which would otherwise dangle.
        downstream_fields = fields_by_table.get(dependency.table.lower(), frozenset())
        upstream_fields = fields_by_table.get(
            dependency.referenced_table.lower(), frozenset()
        )
        if (
            dependency.object_name.lower() not in downstream_fields
            or dependency.referenced_object.lower() not in upstream_fields
        ):
            return None

        return _DependencyEdge(
            downstream_urn=builder.make_schema_field_urn(
                downstream_dataset, dependency.object_name
            ),
            upstream_urn=builder.make_schema_field_urn(
                upstream_dataset, dependency.referenced_object
            ),
        )
