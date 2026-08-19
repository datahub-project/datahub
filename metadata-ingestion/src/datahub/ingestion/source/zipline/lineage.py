import re
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.zipline.config import ZiplineConfig, ZiplinePlatformDetail
from datahub.ingestion.source.zipline.models import GroupBy, Join, Source
from datahub.ingestion.source.zipline.report import ZiplineSourceReport
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    create_lineage_from_sql_statements,
)

# Chronon SQL embeds Jinja macros (`{{ start_date }}`) that aren't valid SQL.
# Replace them with a bare, unquoted token: templates are often already quoted
# (`'{{ ds }}'`), and adding quotes here would produce an invalid `''...''`.
_TEMPLATE_RE = re.compile(r"\{\{.*?\}\}")
_TEMPLATE_REPLACEMENT = "__zipline_template__"


def strip_sql_templates(query: str) -> str:
    return _TEMPLATE_RE.sub(_TEMPLATE_REPLACEMENT, query)


class SqlLineage(BaseModel):
    """Table- and column-level lineage parsed from a StagingQuery's SQL."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    input_urns: List[str] = []
    fine_grained_lineages: List[FineGrainedLineageClass] = []


class SourceResolver:
    """Resolves Chronon `Source` structs to backing Dataset (and column) URNs.

    The compiled config never records a table's platform, so the user supplies
    `source_platform_map` (per-namespace) plus a default. Two- vs three-tier URN
    shaping mirrors the SQLAlchemy sources and the Airbyte connector.
    """

    def __init__(self, config: ZiplineConfig, report: ZiplineSourceReport) -> None:
        self.config = config
        self.report = report
        # Chronon's namespace and a native connector's database may differ only
        # in case, so match the platform map case-insensitively.
        self._details_lower: Dict[str, ZiplinePlatformDetail] = {
            key.lower(): value for key, value in config.source_platform_map.items()
        }

    def resolve_source_urns(self, source: Source) -> List[str]:
        urns: List[str] = []

        batch_table = source.batch_table
        if batch_table:
            urns.append(self.resolve_table_urn(batch_table))

        topic = source.topic
        if topic:
            urns.append(self._resolve_topic_urn(topic))

        return urns

    def resolve_group_by_sources(self, group_by: GroupBy) -> List[str]:
        urns: List[str] = []
        for source in group_by.sources:
            if source.join_source is not None:
                # A JoinSource nests a parent join whose output we cannot resolve
                # without the join context, so skip it (and count it) here.
                self.report.join_sources_skipped += 1
                continue
            urns.extend(self.resolve_source_urns(source))
        return list(dict.fromkeys(urns))

    def resolve_table_urn(self, table: str) -> str:
        detail = self._detail_for(table)
        platform = (
            detail.platform
            if detail and detail.platform
            else self.config.default_source_platform
        )
        platform_instance = (
            detail.platform_instance
            if detail and detail.platform_instance is not None
            else self.config.source_platform_instance
        )
        env = detail.env if detail and detail.env else self.config.env

        name = self._apply_tiering(table, detail)
        if self._lowercase(detail):
            name = name.lower()
        return make_dataset_urn_with_platform_instance(
            platform=platform,
            name=name,
            platform_instance=platform_instance,
            env=env,
        )

    def resolve_field_urn(self, table: str, column: str) -> str:
        table_urn = self.resolve_table_urn(table)
        detail = self._detail_for(table)
        field = column.lower() if self._lowercase(detail) else column
        return make_schema_field_urn(table_urn, field)

    def _detail_for(self, table: str) -> Optional[ZiplinePlatformDetail]:
        namespace = table.split(".", 1)[0] if "." in table else None
        if namespace is None:
            return None
        detail = self._details_lower.get(namespace.lower())
        if detail is None:
            self.report.report_unmapped_namespace(namespace)
        return detail

    def _lowercase(self, detail: Optional[ZiplinePlatformDetail]) -> bool:
        if detail is not None and detail.convert_urns_to_lowercase is not None:
            return detail.convert_urns_to_lowercase
        return self.config.convert_urns_to_lowercase

    @staticmethod
    def _apply_tiering(table: str, detail: Optional[ZiplinePlatformDetail]) -> str:
        parts = table.split(".")
        default_db = detail.default_db if detail else None

        if len(parts) >= 3:
            # Already fully qualified — trust the compiled name verbatim.
            return table
        if len(parts) == 2:
            namespace, name = parts
            if default_db and SourceResolver._three_tier(detail, namespace):
                return f"{default_db}.{namespace}.{name}"
            return f"{namespace}.{name}"
        # Single, unqualified name: prepend the database if one is configured.
        return f"{default_db}.{parts[0]}" if default_db else parts[0]

    @staticmethod
    def _three_tier(detail: Optional[ZiplinePlatformDetail], namespace: str) -> bool:
        if detail is not None and detail.include_schema_in_urn is not None:
            return detail.include_schema_in_urn
        default_db = detail.default_db if detail else None
        # Auto-detect: a distinct database means the namespace is a schema tier.
        return default_db is not None and default_db != namespace

    def _resolve_topic_urn(self, topic: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.config.stream_platform,
            name=topic,
            platform_instance=self.config.source_platform_instance,
            env=self.config.env,
        )


def build_group_by_column_lineage(
    group_by: GroupBy, resolver: SourceResolver
) -> List[FineGrainedLineageClass]:
    """Best-effort source-column -> feature-column lineage for a GroupBy.

    Aggregations name their input column explicitly, so each output feature maps
    back to that column on every backing source table. Derivation-only GroupBys
    are skipped because the derived expressions aren't column-addressable.
    """
    output_table = group_by.meta_data.output_table_name()
    if output_table is None or group_by.derivations:
        return []

    source_tables = [
        source.batch_table
        for source in group_by.sources
        if source.join_source is None and source.batch_table
    ]
    if not source_tables:
        return []

    fine_grained: List[FineGrainedLineageClass] = []
    for aggregation in group_by.aggregations:
        input_column = aggregation.input_column
        if input_column is None:
            continue
        upstreams = [
            resolver.resolve_field_urn(table, input_column) for table in source_tables
        ]
        for feature in aggregation.output_column_names():
            fine_grained.append(
                _fine_grained(
                    upstreams=upstreams,
                    downstream=resolver.resolve_field_urn(output_table, feature),
                )
            )
    return fine_grained


def build_join_column_lineage(
    join: Join,
    resolver: SourceResolver,
    group_by_output_tables: Dict[str, str],
) -> List[FineGrainedLineageClass]:
    """Best-effort join-part feature-column -> join-output-column lineage.

    Each join part contributes its GroupBy's feature columns to the join output,
    optionally under a prefix. The exact Chronon output naming isn't recorded in
    the compiled config, so the prefixed feature name is a best-effort match.
    """
    output_table = join.meta_data.output_table_name()
    if output_table is None or join.derivations:
        return []

    fine_grained: List[FineGrainedLineageClass] = []
    for join_part in join.join_parts:
        part_name = join_part.group_by.meta_data.name
        group_by_table = (
            group_by_output_tables.get(part_name or "")
            or join_part.group_by.meta_data.output_table_name()
        )
        if not group_by_table:
            continue
        for feature in join_part.group_by.feature_names():
            output_column = (
                f"{join_part.prefix}_{feature}" if join_part.prefix else feature
            )
            fine_grained.append(
                _fine_grained(
                    upstreams=[resolver.resolve_field_urn(group_by_table, feature)],
                    downstream=resolver.resolve_field_urn(output_table, output_column),
                )
            )
    return fine_grained


def _fine_grained(upstreams: List[str], downstream: str) -> FineGrainedLineageClass:
    return FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=upstreams,
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[downstream],
    )


class StagingQueryLineageExtractor:
    """Table- and column-level lineage from a StagingQuery's Spark SQL."""

    def __init__(
        self,
        config: ZiplineConfig,
        report: ZiplineSourceReport,
        graph: Optional[DataHubGraph],
        source_resolver: SourceResolver,
    ) -> None:
        self.config = config
        self.report = report
        self.graph = graph
        self.source_resolver = source_resolver

    def extract(
        self,
        query: str,
        output_table: Optional[str],
        default_namespace: Optional[str],
        name: Optional[str] = None,
    ) -> SqlLineage:
        cleaned = strip_sql_templates(query)
        # A StagingQuery is a bare SELECT with no target, and the statement-level
        # parser only records column lineage flowing INTO a table. Wrap it as a
        # CTAS against the output table so column lineage is produced; the synthetic
        # target is dropped below (only in_tables + column_lineage are consumed).
        if output_table:
            cleaned = f"CREATE TABLE {output_table} AS\n{cleaned.rstrip().rstrip(';')}"
        try:
            result = create_lineage_from_sql_statements(
                queries=cleaned,
                default_db=None,
                # Complete bare table names to the staging query's own namespace
                # so a two-tier `<namespace>.<table>` re-resolves correctly below.
                default_schema=default_namespace,
                platform=self.config.staging_query_dialect,
                platform_instance=self.config.source_platform_instance,
                env=self.config.env,
                graph=self.graph,
            )
        except Exception as exc:
            self.report.sql_lineage_failures += 1
            self.report.warning(
                title="StagingQuery SQL parse failure",
                message="Could not parse StagingQuery SQL for lineage",
                context=name,
                exc=exc,
            )
            return SqlLineage()

        # sqlglot's common failure mode is a result object carrying an error
        # rather than a raised exception; surface it or the DataJob silently
        # loses all input lineage.
        if result.debug_info and result.debug_info.error:
            self.report.sql_lineage_failures += 1
            self.report.warning(
                title="StagingQuery SQL parse failure",
                message="sqlglot could not derive lineage from StagingQuery SQL",
                context=(
                    f"{name}: {result.debug_info.error}"
                    if name
                    else str(result.debug_info.error)
                ),
            )
            return SqlLineage()

        self.report.sql_lineage_parsed += 1
        # sqlglot attributes every derived table to the parse dialect. Re-map each
        # through the resolver so source_platform_map, tiering, lowercasing and the
        # unmapped-namespace warning apply — otherwise a table in a mapped platform
        # (e.g. Snowflake) is mis-attributed and its lineage never stitches.
        input_urns = sorted(
            {
                self.source_resolver.resolve_table_urn(DatasetUrn.from_string(urn).name)
                for urn in result.in_tables
            }
        )
        fine_grained = self._column_lineage(result.column_lineage, output_table)
        return SqlLineage(input_urns=input_urns, fine_grained_lineages=fine_grained)

    def _column_lineage(
        self,
        column_lineage: Optional[List[ColumnLineageInfo]],
        output_table: Optional[str],
    ) -> List[FineGrainedLineageClass]:
        if not column_lineage or output_table is None:
            return []
        fine_grained: List[FineGrainedLineageClass] = []
        for entry in column_lineage:
            if not entry.downstream.column:
                continue
            upstreams = [
                self.source_resolver.resolve_field_urn(
                    DatasetUrn.from_string(ref.table).name, ref.column
                )
                for ref in entry.upstreams
                if ref.column
            ]
            if not upstreams:
                continue
            fine_grained.append(
                _fine_grained(
                    upstreams=upstreams,
                    downstream=self.source_resolver.resolve_field_urn(
                        output_table, entry.downstream.column
                    ),
                )
            )
        return fine_grained
