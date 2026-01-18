import logging
from typing import Dict, List, Optional, Set

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.matillion.config import (
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion.models import (
    MatillionLineageGraph,
    MatillionLineageNode,
    MatillionPipeline,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.schema_resolver import (
    SchemaInfo,
    SchemaResolver,
    match_columns_to_schema,
)
from datahub.sql_parsing.sqlglot_lineage import create_and_cache_schema_resolver
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)


class MatillionLineageHandler:
    def __init__(
        self,
        config: MatillionSourceConfig,
        report: MatillionSourceReport,
        graph: Optional[DataHubGraph] = None,
    ):
        self.config = config
        self.report = report
        self.graph = graph
        self._platform_mapping = self._build_platform_mapping()
        self._schema_resolvers: Dict[str, SchemaResolver] = {}

    def _build_platform_mapping(self) -> Dict[str, str]:
        return {
            "snowflake": "snowflake",
            "bigquery": "bigquery",
            "redshift": "redshift",
            "postgres": "postgres",
            "postgresql": "postgres",
            "mysql": "mysql",
            "sqlserver": "mssql",
            "mssql": "mssql",
            "oracle": "oracle",
            "databricks": "databricks",
            "s3": "s3",
            "abs": "abs",
            "azure": "abs",
            "gcs": "gcs",
            "db2": "db2",
            "teradata": "teradata",
            "sap-hana": "sap-hana",
            "saphana": "sap-hana",
            "mongodb": "mongodb",
            "mongo": "mongodb",
            "cassandra": "cassandra",
            "elasticsearch": "elasticsearch",
            "elastic": "elasticsearch",
            "kafka": "kafka",
            "delta-lake": "delta-lake",
            "delta": "delta-lake",
            "deltalake": "delta-lake",
            "dremio": "dremio",
            "firebolt": "firebolt",
        }

    def _map_platform(self, platform: Optional[str]) -> str:
        if not platform:
            return "external_db"
        platform_lower = platform.lower()
        return self._platform_mapping.get(platform_lower, "external_db")

    def _create_dataset_urn_from_node(
        self, node: MatillionLineageNode
    ) -> Optional[DatasetUrn]:
        if not node.table_name:
            logger.debug(f"Skipping lineage node {node.id} - missing table name")
            return None

        platform = self._map_platform(node.platform)

        if node.schema_name:
            dataset_name = f"{node.schema_name}.{node.table_name}"
        else:
            dataset_name = node.table_name

        try:
            return DatasetUrn.create_from_ids(
                platform_id=platform,
                table_name=dataset_name.lower(),
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
        except Exception as e:
            logger.warning(
                f"Failed to create dataset URN for node {node.id} "
                f"(platform={platform}, dataset={dataset_name}): {e}"
            )
            return None

    def _get_upstream_datasets(
        self,
        lineage_graph: MatillionLineageGraph,
        node_id: str,
        visited: Optional[Set[str]] = None,
    ) -> Set[str]:
        if visited is None:
            visited = set()

        if node_id in visited:
            return set()

        visited.add(node_id)
        upstreams = set()

        for edge in lineage_graph.edges:
            if edge.target_id == node_id:
                source_node = next(
                    (n for n in lineage_graph.nodes if n.id == edge.source_id), None
                )
                if source_node:
                    if source_node.table_name:
                        upstreams.add(edge.source_id)
                    else:
                        upstream_upstreams = self._get_upstream_datasets(
                            lineage_graph, edge.source_id, visited
                        )
                        upstreams.update(upstream_upstreams)

        return upstreams

    def _get_downstream_datasets(
        self,
        lineage_graph: MatillionLineageGraph,
        node_id: str,
        visited: Optional[Set[str]] = None,
    ) -> Set[str]:
        if visited is None:
            visited = set()

        if node_id in visited:
            return set()

        visited.add(node_id)
        downstreams = set()

        for edge in lineage_graph.edges:
            if edge.source_id == node_id:
                target_node = next(
                    (n for n in lineage_graph.nodes if n.id == edge.target_id), None
                )
                if target_node:
                    if target_node.table_name:
                        downstreams.add(edge.target_id)
                    else:
                        downstream_downstreams = self._get_downstream_datasets(
                            lineage_graph, edge.target_id, visited
                        )
                        downstreams.update(downstream_downstreams)

        return downstreams

    def _get_schema_resolver(self, platform: str) -> Optional[SchemaResolver]:
        if not self.graph:
            return None

        if platform not in self._schema_resolvers:
            try:
                resolver = create_and_cache_schema_resolver(
                    platform=platform,
                    env=self.config.env,
                    graph=self.graph,
                    platform_instance=self.config.platform_instance,
                )
                self._schema_resolvers[platform] = resolver
            except Exception as e:
                logger.debug(
                    f"Failed to create schema resolver for platform {platform}: {e}"
                )
                return None

        return self._schema_resolvers.get(platform)

    def _resolve_urn_with_schema(
        self, dataset_urn: DatasetUrn, platform: str
    ) -> tuple[DatasetUrn, Optional[SchemaInfo]]:
        if not self.graph:
            return dataset_urn, None

        schema_resolver = self._get_schema_resolver(platform)
        if not schema_resolver:
            return dataset_urn, None

        try:
            resolved_urn, schema_info = schema_resolver.resolve_urn(str(dataset_urn))
            if resolved_urn:
                dataset_urn = DatasetUrn.create_from_string(resolved_urn)
            return dataset_urn, schema_info
        except Exception as e:
            logger.debug(
                f"Failed to resolve URN {dataset_urn} with schema resolver: {e}"
            )
            return dataset_urn, None

    def _match_column_to_schema(
        self, schema_info: Optional[SchemaInfo], column_name: str
    ) -> str:
        if not schema_info:
            return column_name.lower()

        matched_columns = match_columns_to_schema(schema_info, [column_name])
        return matched_columns[0] if matched_columns else column_name.lower()

    def emit_pipeline_lineage(
        self,
        pipeline: MatillionPipeline,
        pipeline_urn: str,
        lineage_graph: MatillionLineageGraph,
    ) -> List[MetadataWorkUnit]:
        workunits: List[MetadataWorkUnit] = []

        if not lineage_graph.nodes:
            logger.debug(f"No lineage nodes found for pipeline {pipeline.id}")
            return workunits

        node_map: Dict[str, MatillionLineageNode] = {
            node.id: node for node in lineage_graph.nodes
        }

        dataset_nodes = [node for node in lineage_graph.nodes if node.table_name]

        if not dataset_nodes:
            logger.debug(f"No dataset nodes found for pipeline {pipeline.id}")
            return workunits

        for node in dataset_nodes:
            dataset_urn = self._create_dataset_urn_from_node(node)
            if not dataset_urn:
                continue

            platform = self._map_platform(node.platform)
            resolved_urn, downstream_schema = self._resolve_urn_with_schema(
                dataset_urn, platform
            )
            dataset_urn = resolved_urn

            if self.graph and not downstream_schema:
                logger.debug(
                    f"Skipping lineage for {dataset_urn} - dataset not found in DataHub. "
                    "Enable dataset ingestion first to emit lineage."
                )
                continue

            upstream_node_ids = self._get_upstream_datasets(lineage_graph, node.id)

            upstream_urns = []
            for upstream_id in upstream_node_ids:
                upstream_node = node_map.get(upstream_id)
                if upstream_node:
                    upstream_urn = self._create_dataset_urn_from_node(upstream_node)
                    if upstream_urn:
                        upstream_platform = self._map_platform(upstream_node.platform)
                        resolved_upstream_urn, _ = self._resolve_urn_with_schema(
                            upstream_urn, upstream_platform
                        )
                        upstream_urns.append(resolved_upstream_urn)

            fine_grained_lineages_for_dataset: List[FineGrainedLineageClass] = []

            if (
                upstream_urns
                and self.config.include_column_lineage
                and node.column_name
            ):
                downstream_col = self._match_column_to_schema(
                    downstream_schema, node.column_name
                )

                for upstream_id in upstream_node_ids:
                    upstream_node = node_map.get(upstream_id)
                    if upstream_node and upstream_node.column_name:
                        upstream_urn = self._create_dataset_urn_from_node(upstream_node)
                        if upstream_urn:
                            upstream_platform = self._map_platform(
                                upstream_node.platform
                            )
                            resolved_upstream_urn, upstream_schema = (
                                self._resolve_urn_with_schema(
                                    upstream_urn, upstream_platform
                                )
                            )

                            upstream_col = self._match_column_to_schema(
                                upstream_schema, upstream_node.column_name
                            )

                            fine_grained_lineage = FineGrainedLineageClass(
                                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                upstreams=[
                                    make_schema_field_urn(
                                        str(resolved_upstream_urn),
                                        upstream_col,
                                    )
                                ],
                                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                downstreams=[
                                    make_schema_field_urn(
                                        str(dataset_urn),
                                        downstream_col,
                                    )
                                ],
                            )
                            fine_grained_lineages_for_dataset.append(
                                fine_grained_lineage
                            )

            if upstream_urns:
                upstreams = [
                    UpstreamClass(
                        dataset=str(upstream_urn),
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    for upstream_urn in upstream_urns
                ]

                upstream_lineage = UpstreamLineageClass(
                    upstreams=upstreams,
                    fineGrainedLineages=fine_grained_lineages_for_dataset
                    if fine_grained_lineages_for_dataset
                    else None,
                )

                workunits.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=str(dataset_urn),
                        aspect=upstream_lineage,
                    ).as_workunit()
                )

        self.report.report_lineage_emitted(len(workunits))
        return workunits
