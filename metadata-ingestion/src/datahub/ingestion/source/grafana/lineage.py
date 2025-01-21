import logging
from typing import Dict, Optional

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.grafana.grafana_config import PlatformConnectionConfig
from datahub.ingestion.source.grafana.models import Panel
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
    ):
        self.platform = platform
        self.platform_instance = platform_instance
        self.env = env
        self.connection_map = connection_to_platform_map
        self.graph = graph
        self.report = report

    def extract_panel_lineage(
        self, panel: Panel
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Extract lineage information from a panel"""
        if not panel.datasource or not isinstance(panel.datasource, dict):
            return None

        ds_type = panel.datasource.get("type", "unknown")
        ds_uid = panel.datasource.get("uid", "unknown")

        # Get raw SQL
        raw_sql = None
        for target in panel.targets:
            if target.get("rawSql"):
                raw_sql = target["rawSql"]
                break

        # Build dataset name
        dataset_name = f"{ds_type}.{ds_uid}.{panel.id}"
        ds_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.platform_instance,
            env=self.env,
        )

        # Process platform mapping
        if ds_uid in self.connection_map:
            platform_config = self.connection_map[ds_uid]

            if raw_sql:
                parsed_sql = self._parse_sql(
                    sql=raw_sql,
                    platform_config=platform_config,
                )
                if parsed_sql:
                    return self._create_column_lineage(ds_urn, parsed_sql)

            # Basic lineage if no SQL or parsing failed
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

            logger.info(upstream_urn)

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

        return None

    def _parse_sql(
        self,
        sql: str,
        platform_config: PlatformConnectionConfig,
    ) -> Optional[SqlParsingResult]:
        """Parse SQL query for lineage information"""
        try:
            if self.graph:
                return create_lineage_sql_parsed_result(
                    query=sql,
                    platform=platform_config.platform,
                    platform_instance=platform_config.platform_instance,
                    env=platform_config.env,
                    default_db=platform_config.database,
                    default_schema=platform_config.database_schema,
                    graph=self.graph,  # Pass graph object
                )
            else:
                logger.warning("No DataHub graph specified")
                return None
        except Exception as e:
            logger.warning(f"Failed to parse SQL query: {sql}", exc_info=e)
            return None

    def _create_column_lineage(
        self,
        dataset_urn: str,
        parsed_sql: SqlParsingResult,
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create column-level lineage"""
        if not parsed_sql.column_lineage:
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
