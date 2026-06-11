from typing import Dict, List, Optional, Tuple

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.quicksight.processors.data_sources import (
    ResolvedDataSource,
)
from datahub.ingestion.source.quicksight.quicksight_config import (
    ExternalDataSourceConfig,
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
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

S3_PLATFORM = "s3"


class _UpstreamTarget:
    """Resolved upstream-platform coordinates for a single data source.

    Combines the data source's resolved platform/dialect with any per-source
    overrides from ``external_data_sources`` (platform_instance, env, URN
    casing, SQL-parser default db/schema).
    """

    def __init__(
        self,
        resolved: ResolvedDataSource,
        override: Optional[ExternalDataSourceConfig],
        default_env: str,
    ) -> None:
        self.resolved = resolved
        self.platform = resolved.platform
        self.platform_instance = override.platform_instance if override else None
        self.env = override.env if override and override.env else default_env
        self.lowercase = override.convert_urns_to_lowercase if override else False
        self.default_database = override.default_database if override else None
        self.default_schema = override.default_schema if override else None

    def table_urn(self, table_name: str) -> str:
        name = table_name.lower() if self.lowercase else table_name
        return builder.make_dataset_urn_with_platform_instance(
            platform=self.platform,  # type: ignore[arg-type]  # guarded by caller
            name=name,
            platform_instance=self.platform_instance,
            env=self.env,
        )


class QuickSightLineageExtractor:
    """Resolves a QuickSight dataset's ``PhysicalTableMap`` into DataHub
    ``UpstreamLineage`` (+ column-level lineage for CustomSql via sqlglot)."""

    def __init__(
        self,
        config: QuickSightSourceConfig,
        report: QuickSightSourceReport,
        ctx: PipelineContext,
        data_source_map: Dict[str, ResolvedDataSource],
    ) -> None:
        self.config = config
        self.report = report
        self.ctx = ctx
        self.data_source_map = data_source_map

    def get_upstream_lineage(
        self, dataset_urn: str, physical_table_map: Dict[str, dict]
    ) -> Optional[UpstreamLineageClass]:
        if not self.config.extract_lineage:
            return None

        upstream_urns: List[str] = []
        fine_grained: List[FineGrainedLineageClass] = []

        for entry in physical_table_map.values():
            if "RelationalTable" in entry:
                upstream_urns += self._handle_relational_table(entry["RelationalTable"])
            elif "CustomSql" in entry:
                urns, cll = self._handle_custom_sql(dataset_urn, entry["CustomSql"])
                upstream_urns += urns
                fine_grained += cll
            elif "S3Source" in entry:
                upstream_urns += self._handle_s3_source(entry["S3Source"])

        if not upstream_urns:
            return None

        # De-duplicate while preserving order (a dataset can join the same table
        # more than once).
        deduped = list(dict.fromkeys(upstream_urns))
        self.report.num_upstream_lineage_edges += len(deduped)
        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.TRANSFORMED)
                for urn in deduped
            ],
            fineGrainedLineages=fine_grained or None,
        )

    def _target_for(self, data_source_arn: str) -> Optional[_UpstreamTarget]:
        resolved = self.data_source_map.get(data_source_arn)
        if resolved is None:
            self.report.warning(
                title="Data source not resolved for lineage",
                message="A dataset references a data source that was not ingested "
                "(e.g. filtered out or cross-account); skipping its upstream lineage.",
                context=data_source_arn,
            )
            return None
        if resolved.platform is None:
            return None
        external = self.config.external_data_sources
        override = external.get(resolved.data_source_id) or external.get(resolved.name)
        return _UpstreamTarget(resolved, override, self.config.env)

    def _handle_relational_table(self, entry: dict) -> List[str]:
        target = self._target_for(entry.get("DataSourceArn", ""))
        if target is None:
            return []

        catalog = entry.get("Catalog")
        schema = entry.get("Schema")
        name = entry.get("Name")
        if not name:
            return []

        parts: List[str] = []
        # Athena's default catalog (AwsDataCatalog) is omitted to match the
        # Athena connector's two-part (schema.table) URN scheme.
        if catalog and not (
            target.platform == "athena" and catalog == "AwsDataCatalog"
        ):
            parts.append(catalog)
        if schema:
            parts.append(schema)
        parts.append(name)
        return [target.table_urn(".".join(parts))]

    def _handle_s3_source(self, entry: dict) -> List[str]:
        target = self._target_for(entry.get("DataSourceArn", ""))
        if target is None or not target.resolved.s3_manifest_uri:
            return []
        # The manifest URI already encodes bucket/key; S3 datasets carry no
        # platform instance.
        return [
            builder.make_dataset_urn_with_platform_instance(
                platform=S3_PLATFORM,
                name=target.resolved.s3_manifest_uri,
                platform_instance=None,
                env=target.env,
            )
        ]

    def _handle_custom_sql(
        self, dataset_urn: str, entry: dict
    ) -> Tuple[List[str], List[FineGrainedLineageClass]]:
        target = self._target_for(entry.get("DataSourceArn", ""))
        query = entry.get("SqlQuery")
        if target is None or not query:
            return [], []

        result = create_lineage_sql_parsed_result(
            query=query,
            default_db=target.default_database,
            default_schema=target.default_schema,
            platform=target.platform,  # type: ignore[arg-type]  # guarded by _target_for
            platform_instance=target.platform_instance,
            env=target.env,
            graph=self.ctx.graph,
            override_dialect=target.resolved.dialect,
            generate_column_lineage=self.config.extract_column_lineage,
        )

        if result.debug_info.table_error:
            self.report.num_sqlglot_parse_failures += 1
            self.report.warning(
                title="Failed to parse CustomSql",
                message="Could not extract upstream/column lineage from a CustomSql "
                "dataset definition.",
                context=dataset_urn,
                exc=result.debug_info.table_error,
            )
            return [], []

        fine_grained = self._build_column_lineage(dataset_urn, result)
        return list(result.in_tables), fine_grained

    def _build_column_lineage(
        self, dataset_urn: str, result: SqlParsingResult
    ) -> List[FineGrainedLineageClass]:
        if not self.config.extract_column_lineage or not result.column_lineage:
            return []

        fine_grained: List[FineGrainedLineageClass] = []
        for cll in result.column_lineage:
            upstreams = [
                builder.make_schema_field_urn(ref.table, ref.column)
                for ref in cll.upstreams
            ]
            if not upstreams:
                continue
            fine_grained.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        builder.make_schema_field_urn(
                            dataset_urn, cll.downstream.column
                        )
                    ],
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstreams,
                )
            )
            self.report.num_column_lineage_edges += len(upstreams)
        return fine_grained
