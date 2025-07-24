import logging
from typing import Dict, List, Optional, Tuple

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.grafana.grafana_config import PlatformConnectionConfig
from datahub.ingestion.source.grafana.models import (
    DatasourceRef,
    GrafanaQueryTarget,
    Panel,
)
from datahub.ingestion.source.grafana.report import GrafanaSourceReport
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)


class LineageExtractor:
    """Handles extraction of lineage information from Grafana panels"""

    def __init__(
        self,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        connection_to_platform_map: Dict[str, PlatformConnectionConfig],
        report: GrafanaSourceReport,
        graph: Optional[DataHubGraph] = None,
        include_column_lineage: bool = True,
    ):
        self.platform = platform
        self.platform_instance = platform_instance
        self.env = env
        self.connection_map = connection_to_platform_map
        self.graph = graph
        self.report = report
        self.include_column_lineage = include_column_lineage

    def extract_panel_lineage(
        self, panel: Panel
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Extract lineage information from a panel."""
        if not panel.datasource_ref:
            return None

        ds_type, ds_uid = self._extract_datasource_info(panel.datasource_ref)
        raw_sql = self._extract_raw_sql(panel.query_targets)
        ds_urn = self._build_dataset_urn(ds_type, ds_uid, panel.id)

        # Handle platform-specific lineage
        if ds_uid in self.connection_map:
            if raw_sql:
                parsed_sql = self._parse_sql(raw_sql, self.connection_map[ds_uid])
                if parsed_sql:
                    lineage = self._create_column_lineage(ds_urn, parsed_sql)
                    if lineage:
                        return lineage

            # Fall back to basic lineage if SQL parsing fails or no column lineage created
            return self._create_basic_lineage(
                ds_uid, self.connection_map[ds_uid], ds_urn
            )

        return None

    def _extract_datasource_info(
        self, datasource_ref: "DatasourceRef"
    ) -> Tuple[str, str]:
        """Extract datasource type and UID."""
        return datasource_ref.type or "unknown", datasource_ref.uid or "unknown"

    def _extract_raw_sql(
        self, query_targets: List["GrafanaQueryTarget"]
    ) -> Optional[str]:
        """Extract raw SQL from panel query targets."""
        for target in query_targets:
            if target.get("rawSql"):
                return target["rawSql"]
        return None

    def _build_dataset_urn(self, ds_type: str, ds_uid: str, panel_id: str) -> str:
        """Build dataset URN."""
        dataset_name = f"{ds_type}.{ds_uid}.{panel_id}"
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.platform_instance,
            env=self.env,
        )

    def _create_basic_lineage(
        self, ds_uid: str, platform_config: PlatformConnectionConfig, ds_urn: str
    ) -> MetadataChangeProposalWrapper:
        """Create basic upstream lineage."""
        name = (
            f"{platform_config.database}.{ds_uid}"
            if platform_config.database
            else ds_uid
        )

        upstream_urn = make_dataset_urn_with_platform_instance(
            platform=platform_config.platform,
            name=name,
            platform_instance=platform_config.platform_instance,
            env=platform_config.env,
        )

        logger.info(f"Generated upstream URN: {upstream_urn}")

        return MetadataChangeProposalWrapper(
            entityUrn=ds_urn,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=upstream_urn,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                ]
            ),
        )

    def _parse_sql(
        self, sql: str, platform_config: PlatformConnectionConfig
    ) -> Optional[SqlParsingResult]:
        """Parse SQL query for lineage information."""
        if not self.graph:
            logger.warning("No DataHub graph specified for SQL parsing.")
            return None

        try:
            return create_lineage_sql_parsed_result(
                query=sql,
                platform=platform_config.platform,
                platform_instance=platform_config.platform_instance,
                env=platform_config.env,
                default_db=platform_config.database,
                default_schema=platform_config.database_schema,
                graph=self.graph,
            )
        except ValueError as e:
            logger.error(f"SQL parsing error for query: {sql}", exc_info=e)
        except Exception as e:
            logger.exception(f"Unexpected error during SQL parsing: {sql}", exc_info=e)

        return None

    def _create_column_lineage(
        self,
        dataset_urn: str,
        parsed_sql: SqlParsingResult,
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create column-level lineage"""
        if not parsed_sql.column_lineage or not self.include_column_lineage:
            return None

        upstream_lineages = []
        for col_lineage in parsed_sql.column_lineage:
            upstream_lineages.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        make_schema_field_urn(
                            dataset_urn, col_lineage.downstream.column
                        )
                    ],
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[
                        make_schema_field_urn(upstream_dataset, col.column)
                        for col in col_lineage.upstreams
                        for upstream_dataset in parsed_sql.in_tables
                    ],
                )
            )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=table,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    for table in parsed_sql.in_tables
                ],
                fineGrainedLineages=upstream_lineages,
            ),
        )
