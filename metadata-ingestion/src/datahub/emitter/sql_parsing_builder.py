import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Collection, Dict, Iterable, List, Optional, Set

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig, UsageAggregator
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OperationClass,
    OperationTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sqlglot_lineage import ColumnLineageInfo, SqlParsingResult
from datahub.utilities.file_backed_collections import FileBackedDict

logger = logging.getLogger(__name__)

# TODO: Use this over other sources' equivalent code, if possible

DatasetUrn = str
FieldUrn = str
UserUrn = str


@dataclass
class LineageEdge:
    """Stores information about a single lineage edge, from an upstream table to a downstream table."""

    downstream_urn: DatasetUrn
    upstream_urn: DatasetUrn
    audit_stamp: Optional[datetime]
    actor: Optional[UserUrn]
    type: str = DatasetLineageTypeClass.TRANSFORMED

    # Maps downstream_col -> {upstream_col}
    column_map: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))

    def gen_upstream_aspect(self) -> UpstreamClass:
        return UpstreamClass(
            auditStamp=(
                AuditStampClass(
                    time=int(self.audit_stamp.timestamp() * 1000),
                    actor=self.actor or "",
                )
                if self.audit_stamp
                else None
            ),
            dataset=self.upstream_urn,
            type=self.type,
        )

    def gen_fine_grained_lineage_aspects(self) -> Iterable[FineGrainedLineageClass]:
        for downstream_col, upstream_cols in self.column_map.items():
            yield FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                # Sort to avoid creating multiple aspects in backend with same lineage but different order
                upstreams=sorted(
                    make_schema_field_urn(self.upstream_urn, col)
                    for col in upstream_cols
                ),
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[
                    make_schema_field_urn(self.downstream_urn, downstream_col)
                ],
            )


@dataclass
class SqlParsingBuilder:
    # Open question: does it make sense to iterate over out_tables? When will we have multiple?

    generate_lineage: bool = True
    generate_usage_statistics: bool = True
    generate_operations: bool = True
    usage_config: Optional[BaseUsageConfig] = None

    # Maps downstream urn -> upstream urn -> LineageEdge
    # Builds up a single LineageEdge for each upstream -> downstream pair
    _lineage_map: FileBackedDict[Dict[DatasetUrn, LineageEdge]] = field(
        default_factory=FileBackedDict, init=False
    )

    # TODO: Replace with FileBackedDict approach like in BigQuery usage
    _usage_aggregator: UsageAggregator[DatasetUrn] = field(init=False)

    def __post_init__(self) -> None:
        if self.usage_config:
            self._usage_aggregator = UsageAggregator(self.usage_config)
        elif self.generate_usage_statistics:
            logger.info("No usage config provided, not generating usage statistics")
            self.generate_usage_statistics = False

    def process_sql_parsing_result(
        self,
        result: SqlParsingResult,
        *,
        query: str,
        query_timestamp: Optional[datetime] = None,
        is_view_ddl: bool = False,
        user: Optional[UserUrn] = None,
        custom_operation_type: Optional[str] = None,
        include_urns: Optional[Set[DatasetUrn]] = None,
        include_column_lineage: bool = True,
    ) -> Iterable[MetadataWorkUnit]:
        """Process a single query and yield any generated workunits.

        Args:
            result: The result of parsing the query, or a mock result if parsing failed.
            query: The SQL query to parse and process.
            query_timestamp: When the query was run.
            is_view_ddl: Whether the query is a DDL statement that creates a view.
            user: The urn of the user who ran the query.
            custom_operation_type: Platform-specific operation type, used if the operation type can't be parsed.
            include_urns: If provided, only generate workunits for these urns.
        """
        downstreams_to_ingest = result.out_tables
        upstreams_to_ingest = result.in_tables
        if include_urns:
            logger.debug(f"Skipping urns {set(downstreams_to_ingest) - include_urns}")
            downstreams_to_ingest = list(set(downstreams_to_ingest) & include_urns)
            upstreams_to_ingest = list(set(upstreams_to_ingest) & include_urns)

        if self.generate_lineage:
            for downstream_urn in downstreams_to_ingest:
                # Set explicitly so that FileBackedDict registers any mutations
                self._lineage_map[downstream_urn] = _merge_lineage_data(
                    downstream_urn=downstream_urn,
                    upstream_urns=result.in_tables,
                    column_lineage=(
                        result.column_lineage if include_column_lineage else None
                    ),
                    upstream_edges=self._lineage_map.get(downstream_urn, {}),
                    query_timestamp=query_timestamp,
                    is_view_ddl=is_view_ddl,
                    user=user,
                )

        if self.generate_usage_statistics and query_timestamp is not None:
            upstream_fields = compute_upstream_fields(result)
            for upstream_urn in upstreams_to_ingest:
                self._usage_aggregator.aggregate_event(
                    resource=upstream_urn,
                    start_time=query_timestamp,
                    query=query,
                    user=user,
                    fields=sorted(upstream_fields.get(upstream_urn, [])),
                )

        if self.generate_operations and query_timestamp is not None:
            for downstream_urn in downstreams_to_ingest:
                yield from _gen_operation_workunit(
                    result,
                    downstream_urn=downstream_urn,
                    query_timestamp=query_timestamp,
                    user=user,
                    custom_operation_type=custom_operation_type,
                )

    def add_lineage(
        self,
        downstream_urn: DatasetUrn,
        upstream_urns: Collection[DatasetUrn],
        timestamp: Optional[datetime] = None,
        is_view_ddl: bool = False,
        user: Optional[UserUrn] = None,
    ) -> None:
        """Manually add a single upstream -> downstream lineage edge, e.g. if sql parsing fails."""
        # Set explicitly so that FileBackedDict registers any mutations
        self._lineage_map[downstream_urn] = _merge_lineage_data(
            downstream_urn=downstream_urn,
            upstream_urns=upstream_urns,
            column_lineage=None,
            upstream_edges=self._lineage_map.get(downstream_urn, {}),
            query_timestamp=timestamp,
            is_view_ddl=is_view_ddl,
            user=user,
        )

    def gen_workunits(self) -> Iterable[MetadataWorkUnit]:
        if self.generate_lineage:
            for mcp in self._gen_lineage_mcps():
                yield mcp.as_workunit()
        if self.generate_usage_statistics:
            yield from self._gen_usage_statistics_workunits()

    def _gen_lineage_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        for downstream_urn in self._lineage_map:
            upstreams: List[UpstreamClass] = []
            fine_upstreams: List[FineGrainedLineageClass] = []
            for edge in self._lineage_map[downstream_urn].values():
                upstreams.append(edge.gen_upstream_aspect())
                fine_upstreams.extend(edge.gen_fine_grained_lineage_aspects())

            if not upstreams:
                continue

            upstream_lineage = UpstreamLineageClass(
                upstreams=sorted(upstreams, key=lambda x: x.dataset),
                fineGrainedLineages=sorted(
                    fine_upstreams,
                    key=lambda x: (x.downstreams, x.upstreams),
                )
                or None,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=downstream_urn, aspect=upstream_lineage
            )

    def _gen_usage_statistics_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self._usage_aggregator.generate_workunits(
            resource_urn_builder=lambda urn: urn, user_urn_builder=lambda urn: urn
        )


def _merge_lineage_data(
    downstream_urn: DatasetUrn,
    *,
    upstream_urns: Collection[DatasetUrn],
    column_lineage: Optional[List[ColumnLineageInfo]],
    upstream_edges: Dict[DatasetUrn, LineageEdge],
    query_timestamp: Optional[datetime],
    is_view_ddl: bool,
    user: Optional[UserUrn],
) -> Dict[str, LineageEdge]:
    for upstream_urn in upstream_urns:
        edge = upstream_edges.setdefault(
            upstream_urn,
            LineageEdge(
                downstream_urn=downstream_urn,
                upstream_urn=upstream_urn,
                audit_stamp=query_timestamp,
                actor=user,
                type=(
                    DatasetLineageTypeClass.VIEW
                    if is_view_ddl
                    else DatasetLineageTypeClass.TRANSFORMED
                ),
            ),
        )
        if query_timestamp and (  # Use the most recent query
            edge.audit_stamp is None or query_timestamp > edge.audit_stamp
        ):
            edge.audit_stamp = query_timestamp
            if user:
                edge.actor = user

    # Note: Inefficient as we loop through all column_lineage entries for each downstream table
    for cl in column_lineage or []:
        if cl.downstream.table == downstream_urn:
            for upstream_column_info in cl.upstreams:
                if upstream_column_info.table not in upstream_urns:
                    continue
                column_map = upstream_edges[upstream_column_info.table].column_map
                column_map[cl.downstream.column].add(upstream_column_info.column)

    return upstream_edges


def compute_upstream_fields(
    result: SqlParsingResult,
) -> Dict[DatasetUrn, Set[DatasetUrn]]:
    upstream_fields: Dict[DatasetUrn, Set[DatasetUrn]] = defaultdict(set)
    for cl in result.column_lineage or []:
        for upstream in cl.upstreams:
            upstream_fields[upstream.table].add(upstream.column)
    return upstream_fields


def _gen_operation_workunit(
    result: SqlParsingResult,
    *,
    downstream_urn: DatasetUrn,
    query_timestamp: datetime,
    user: Optional[UserUrn],
    custom_operation_type: Optional[str],
) -> Iterable[MetadataWorkUnit]:
    operation_type = result.query_type.to_operation_type()
    # Filter out SELECT and other undesired statements
    if operation_type is None:
        return
    elif operation_type == OperationTypeClass.UNKNOWN:
        if custom_operation_type is None:
            return
        else:
            operation_type = OperationTypeClass.CUSTOM

    aspect = OperationClass(
        timestampMillis=int(time.time() * 1000),
        operationType=operation_type,
        lastUpdatedTimestamp=int(query_timestamp.timestamp() * 1000),
        actor=user,
        customOperationType=custom_operation_type,
    )
    yield MetadataChangeProposalWrapper(
        entityUrn=downstream_urn, aspect=aspect
    ).as_workunit()
