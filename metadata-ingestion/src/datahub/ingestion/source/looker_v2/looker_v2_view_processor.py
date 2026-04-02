"""
LookerViewProcessor and LookerViewLineageResolver.

LookerViewProcessor handles view discovery and processing (reachable and unreachable).
LookerViewLineageResolver is an internal helper that resolves upstream lineage for views.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Set, Tuple

from looker_sdk.sdk.api40.models import LookmlModelExplore, WriteQuery

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.looker.looker_common import (
    LookerViewId,
    gen_project_key,
)
from datahub.ingestion.source.looker_v2.looker_v2_config import (
    LookerConnectionDefinition,
)
from datahub.ingestion.source.looker_v2.lookml_parser import LookMLParser, ParsedView
from datahub.ingestion.source.looker_v2.lookml_view_discovery import ViewDiscovery
from datahub.ingestion.source.looker_v2.lookml_view_refinement_handler import (
    merge_additive_parameters,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sdk.dataset import Dataset, UpstreamInputType
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from datahub.utilities.backpressure_aware_executor import BackpressureAwareExecutor

if TYPE_CHECKING:
    from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context
    from datahub.ingestion.source.looker_v2.lookml_view_refinement_handler import (
        RefinementHandler,
    )

logger = logging.getLogger(__name__)

# Must match LookerV2Source.VIEW_PLATFORM for backward-compatible URNs.
_VIEW_PLATFORM = "looker"


def _platform_names_have_2_parts(platform: str) -> bool:
    """Return True for platforms whose fully-qualified names use only 2 parts (db.table)."""
    return platform in {"hive", "mysql", "athena"}


def _generate_fully_qualified_name(
    sql_table_name: str,
    connection_def: LookerConnectionDefinition,
    reporter: Any,
    view_name: str,
) -> str:
    """Expand a sql_table_name to a fully qualified name using the connection defaults."""
    parts = len(sql_table_name.split("."))

    if parts == 3:
        if _platform_names_have_2_parts(connection_def.platform):
            sql_table_name = ".".join(sql_table_name.split(".")[1:])
        return sql_table_name.lower()

    if parts == 1:
        if _platform_names_have_2_parts(connection_def.platform):
            dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        else:
            dataset_name = f"{connection_def.default_db}.{connection_def.default_schema}.{sql_table_name}"
        return dataset_name.lower()

    if parts == 2:
        if _platform_names_have_2_parts(connection_def.platform):
            return sql_table_name.lower()
        dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        return dataset_name.lower()

    reporter.report_warning(
        title="Malformed Table Name",
        message="Table name has more than 3 parts.",
        context=f"view-name: {view_name}, table-name: {sql_table_name}",
    )
    return sql_table_name.lower()


class LookerViewLineageResolver:
    """Internal lineage resolver. Not exposed outside this module."""

    _LIQUID_VIEW_REF_PATTERN = re.compile(r"\$\{(\w+)\.SQL_TABLE_NAME\}", re.IGNORECASE)

    def __init__(self, ctx: "LookerV2Context") -> None:
        self._ctx = ctx

    def _get_view_fields_for_api(
        self, view_name: str, model_name: str, explore_name: str
    ) -> List[str]:
        """Return field names in Looker API format (<view>.<field>) for a given view."""
        cache_key = (model_name, explore_name)
        explore = self._ctx.explore_cache.get(cache_key)
        if not explore or not explore.fields:
            return []

        fields: List[str] = []
        seen_dim_groups: Set[str] = set()

        if explore.fields.dimensions:
            for dim in explore.fields.dimensions:
                if not dim.name or not hasattr(dim, "view") or dim.view != view_name:
                    continue
                if dim.dimension_group:
                    if dim.dimension_group in seen_dim_groups:
                        continue
                    seen_dim_groups.add(dim.dimension_group)
                fields.append(dim.name)

        if explore.fields.measures:
            for measure in explore.fields.measures:
                if (
                    measure.name
                    and hasattr(measure, "view")
                    and measure.view == view_name
                ):
                    fields.append(measure.name)

        return fields

    def _generate_sql_for_chunk(
        self,
        view_name: str,
        model_name: str,
        explore_name: str,
        fields: List[str],
    ) -> Optional[str]:
        """Call generate_sql_query for a subset of fields and return the SQL string."""
        query = WriteQuery(
            model=model_name,
            view=explore_name,
            fields=fields,
            filters={},
            limit="1",
        )
        try:
            return self._ctx.looker_api.generate_sql_query(query)
        except Exception as e:
            logger.warning(
                f"generate_sql_query failed for view '{view_name}' "
                f"(explore={explore_name}, {len(fields)} fields): {e}"
            )
            return None

    def _parse_sql_to_lineage(
        self,
        sql: str,
        conn_def: LookerConnectionDefinition,
        view_name: str,
    ) -> Optional[SqlParsingResult]:
        """Parse rendered SQL into a SqlParsingResult. Returns None on failure."""
        try:
            spr = create_lineage_sql_parsed_result(
                query=sql,
                default_schema=conn_def.default_schema,
                default_db=conn_def.default_db,
                platform=conn_def.platform,
                platform_instance=conn_def.platform_instance,
                env=self._ctx.config.env,
                graph=self._ctx.pipeline_ctx.graph,
            )
            if spr.debug_info.table_error:
                logger.debug(
                    f"SQL parse table error for view '{view_name}': {spr.debug_info.table_error}"
                )
            return spr
        except Exception as e:
            logger.warning(f"SQL parse failed for view '{view_name}': {e}")
            return None

    def _cll_to_fine_grained(
        self, column_lineage: List[ColumnLineageInfo]
    ) -> List[FineGrainedLineageClass]:
        """Convert SqlParsingResult.column_lineage to FineGrainedLineageClass objects."""
        fgl_list: List[FineGrainedLineageClass] = []
        for cll in column_lineage:
            try:
                if not cll.downstream.table or not cll.downstream.column:
                    continue
                downstream_urn = SchemaFieldUrn(
                    cll.downstream.table, cll.downstream.column
                ).urn()
                upstream_urns = [
                    SchemaFieldUrn(u.table, u.column).urn()
                    for u in cll.upstreams
                    if u.table and u.column
                ]
                if upstream_urns:
                    fgl_list.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=upstream_urns,
                            downstreams=[downstream_urn],
                        )
                    )
            except Exception as e:
                logger.debug(f"Failed to convert column lineage entry: {e}")
        return fgl_list

    def build_view_to_explore_map(self) -> None:
        """Build a view_name → (model_name, explore_name) mapping from the explore cache."""
        for (model_name, explore_name), explore in self._ctx.explore_cache.items():
            primary_view = explore.view_name or explore.name
            if primary_view and primary_view not in self._ctx.view_to_explore_map:
                self._ctx.view_to_explore_map[primary_view] = (model_name, explore_name)
            if explore.joins:
                for join in explore.joins:
                    join_view = join.from_ or join.name
                    if join_view and join_view not in self._ctx.view_to_explore_map:
                        self._ctx.view_to_explore_map[join_view] = (
                            model_name,
                            explore_name,
                        )

        logger.debug(
            f"Built view_to_explore_map with {len(self._ctx.view_to_explore_map)} entries"
        )

    def resolve_view_lineage_via_api(
        self,
        view_name: str,
        conn_def: LookerConnectionDefinition,
        view_urn_resolver: Any,
    ) -> Optional[List[UpstreamInputType]]:
        """Resolve lineage for a reachable derived-table view using the Looker API."""
        explore_entry = self._ctx.view_to_explore_map.get(view_name)
        if not explore_entry:
            return None

        model_name, explore_name = explore_entry
        fields = self._get_view_fields_for_api(view_name, model_name, explore_name)
        if not fields:
            logger.debug(
                f"No API fields found for view '{view_name}'; skipping API SQL lineage"
            )
            return None

        chunk_size = self._ctx.config.api_sql_lineage_field_chunk_size
        chunks = [fields[i : i + chunk_size] for i in range(0, len(fields), chunk_size)]

        all_in_tables: Set[str] = set()
        all_cll: List[FineGrainedLineageClass] = []

        for chunk in chunks:
            sql = self._generate_sql_for_chunk(
                view_name, model_name, explore_name, chunk
            )
            if sql is None:
                if not self._ctx.config.api_sql_lineage_individual_field_fallback:
                    continue
                for field in chunk:
                    field_sql = self._generate_sql_for_chunk(
                        view_name, model_name, explore_name, [field]
                    )
                    if field_sql:
                        spr = self._parse_sql_to_lineage(field_sql, conn_def, view_name)
                        if spr:
                            all_in_tables.update(spr.in_tables)
                            if spr.column_lineage:
                                all_cll.extend(
                                    self._cll_to_fine_grained(spr.column_lineage)
                                )
                continue

            spr = self._parse_sql_to_lineage(sql, conn_def, view_name)
            if spr:
                all_in_tables.update(spr.in_tables)
                if spr.column_lineage:
                    all_cll.extend(self._cll_to_fine_grained(spr.column_lineage))

        # Merge PDT graph edges
        pdt_edges = self._ctx.pdt_upstream_map.get(view_name, [])
        if pdt_edges:
            self._ctx.reporter.lineage_via_pdt_graph += 1
        for edge in pdt_edges:
            if edge.is_database_table:
                try:
                    fqn = _generate_fully_qualified_name(
                        sql_table_name=edge.upstream_name,
                        connection_def=conn_def,
                        reporter=self._ctx.reporter,
                        view_name=view_name,
                    )
                    all_in_tables.add(
                        builder.make_dataset_urn_with_platform_instance(
                            platform=conn_def.platform,
                            name=fqn,
                            platform_instance=conn_def.platform_instance,
                            env=self._ctx.config.env,
                        )
                    )
                except (ValueError, KeyError):
                    pass
            else:
                upstream_model = edge.upstream_model or edge.model_name
                view_urn = view_urn_resolver(edge.upstream_name, upstream_model)
                if view_urn:
                    all_in_tables.add(str(view_urn))

        if not all_in_tables and not all_cll:
            return None

        result: List[UpstreamInputType] = list(all_in_tables)
        result.extend(all_cll)
        return result

    def resolve_liquid_view_refs(
        self,
        sql: str,
        conn_def: LookerConnectionDefinition,
        source_view_name: str,
    ) -> List[UpstreamInputType]:
        """Resolve ${view.SQL_TABLE_NAME} references in SQL to upstream URNs."""
        ref_view_names = self._LIQUID_VIEW_REF_PATTERN.findall(sql)
        if not ref_view_names or not self._ctx.view_discovery_result:
            return []

        upstream_urns: List[UpstreamInputType] = []
        for ref_view_name in ref_view_names:
            ref_file = self._ctx.view_discovery_result.view_to_file.get(ref_view_name)
            if not ref_file:
                continue
            ref_project = self._ctx.view_discovery_result.view_to_project.get(
                ref_view_name, self._ctx.config.project_name or "default"
            )
            try:
                cache_key = (ref_file, ref_project)
                ref_views = self._ctx.parsed_view_file_cache.get(cache_key)
                if ref_views is None:
                    parser = LookMLParser(
                        template_variables=self._ctx.config.liquid_variables,
                        constants=self._ctx.config.lookml_constants,
                        environment=self._ctx.config.looker_environment,
                    )
                    ref_views = parser.parse_view_file(ref_file, ref_project)
                    self._ctx.parsed_view_file_cache[cache_key] = ref_views
                for rv in ref_views:
                    if rv.name == ref_view_name and rv.sql_table_name:
                        table_name = rv.sql_table_name.strip().rstrip(";").strip()
                        if table_name and not table_name.startswith("$"):
                            fqn = _generate_fully_qualified_name(
                                sql_table_name=table_name,
                                connection_def=conn_def,
                                reporter=self._ctx.reporter,
                                view_name=ref_view_name,
                            )
                            upstream_urns.append(
                                builder.make_dataset_urn_with_platform_instance(
                                    platform=conn_def.platform,
                                    name=fqn,
                                    platform_instance=conn_def.platform_instance,
                                    env=self._ctx.config.env,
                                )
                            )
                        break
            except (OSError, ValueError, KeyError) as e:
                logger.warning(
                    f"Failed to resolve ${{'{ref_view_name}'.SQL_TABLE_NAME}} "
                    f"in view '{source_view_name}': {e}"
                )
        return upstream_urns

    def get_connection_for_view(
        self, view_name: str
    ) -> Optional[LookerConnectionDefinition]:
        """Find the Looker connection for a view by scanning the explore cache."""
        for (_model_name, _explore_name), explore in self._ctx.explore_cache.items():
            base_view = explore.view_name or explore.name
            matched = base_view == view_name

            if not matched and explore.joins:
                for join in explore.joins:
                    join_view = join.from_ or join.name
                    if join_view == view_name:
                        matched = True
                        break

            if matched and explore.connection_name:
                conn_def = self._ctx.config.connection_to_platform_map.get(
                    explore.connection_name
                )
                if conn_def:
                    return conn_def

        return None


class LookerViewProcessor:
    """Handles LookML view discovery and processing (reachable and unreachable views)."""

    def __init__(
        self,
        ctx: "LookerV2Context",
        refinement_handler: Optional["RefinementHandler"] = None,
    ) -> None:
        self._ctx = ctx
        self._refinement_handler = refinement_handler
        self._lineage_resolver = LookerViewLineageResolver(ctx)
        self._parsed_views: dict = {}

    def process(self) -> Iterable[MetadataWorkUnit]:
        """Discover and process all LookML views."""
        if not self._ctx.config.base_folder:
            return

        self._discover_views()

        if not self._ctx.view_discovery_result:
            return

        yield from self._process_views()

    def _discover_views(self) -> None:
        """Discover and categorize views from LookML files."""
        if not self._ctx.config.base_folder:
            return

        explore_views = self._get_explore_view_names()

        if self._ctx.config.enable_api_sql_lineage:
            self._lineage_resolver.build_view_to_explore_map()

        discovery = ViewDiscovery(
            base_folder=self._ctx.config.base_folder,
            project_name=self._ctx.config.project_name or "default",
            project_dependencies=self._ctx.resolved_project_paths,
        )

        self._ctx.view_discovery_result = discovery.discover(explore_views)

        self._ctx.reporter.views_discovered = len(
            self._ctx.view_discovery_result.reachable_views
        ) + len(self._ctx.view_discovery_result.unreachable_views)
        self._ctx.reporter.views_reachable = len(
            self._ctx.view_discovery_result.reachable_views
        )
        self._ctx.reporter.views_unreachable = len(
            self._ctx.view_discovery_result.unreachable_views
        )

        for file_path in self._ctx.view_discovery_result.orphaned_files:
            self._ctx.reporter.report_orphaned_file(file_path)

        logger.info(
            f"View discovery: reachable={self._ctx.reporter.views_reachable}, "
            f"unreachable={self._ctx.reporter.views_unreachable}, "
            f"orphaned={self._ctx.reporter.orphaned_view_files_count}"
        )

    def _get_explore_view_names(self) -> frozenset:
        """Fetch all explore details in parallel and collect the referenced view names."""
        from looker_sdk.error import SDKError

        explore_pairs: List[Tuple[str, str]] = [
            (model.name, explore_basic.name)
            for model in self._ctx.model_registry.values()
            if model.explores and model.name
            for explore_basic in model.explores
            if explore_basic.name
        ]

        def fetch_explore(
            model_name: str, explore_name: str
        ) -> Optional[Tuple[str, str, LookmlModelExplore]]:
            try:
                explore = self._ctx.looker_api.lookml_model_explore(
                    model_name, explore_name
                )
                return (model_name, explore_name, explore)
            except SDKError as e:
                logger.warning(
                    f"Failed to fetch explore {model_name}.{explore_name}: {e}"
                )
                return None

        view_names: Set[str] = set()
        for future in BackpressureAwareExecutor.map(
            fn=fetch_explore,
            args_list=[(m, e) for m, e in explore_pairs],
            max_workers=self._ctx.config.max_concurrent_requests,
            max_pending=self._ctx.config.max_concurrent_requests * 2,
        ):
            result = future.result()
            if result is None:
                continue
            model_name, explore_name, explore = result
            self._ctx.explore_cache[(model_name, explore_name)] = explore
            if explore.view_name:
                view_names.add(explore.view_name)
            elif explore.name:
                view_names.add(explore.name)
            if explore.joins:
                for join in explore.joins:
                    if join.from_:
                        view_names.add(join.from_)
                    elif join.name:
                        view_names.add(join.name)

        return frozenset(view_names)

    def _process_views(self) -> Iterable[MetadataWorkUnit]:
        """Process LookML views (reachable and unreachable)."""
        logger.info("Processing views...")

        if not self._ctx.view_discovery_result:
            return

        for view_name in self._ctx.view_discovery_result.reachable_views:
            if not self._ctx.config.view_pattern.allowed(view_name):
                self._ctx.reporter.views_filtered.append(view_name)
                continue

            result = self._process_reachable_view(view_name)
            if result:
                for entity in result.entities:
                    for mcp in entity.as_mcps():
                        yield mcp.as_workunit()
                self._ctx.reporter.views_emitted += 1
                if result.lineage_extracted:
                    self._ctx.reporter.lineage_via_api += 1

        if self._ctx.config.emit_unreachable_views:
            for view_name in self._ctx.view_discovery_result.unreachable_views:
                if not self._ctx.config.view_pattern.allowed(view_name):
                    self._ctx.reporter.views_filtered.append(view_name)
                    continue

                result = self._process_unreachable_view(view_name)
                if result:
                    for entity in result.entities:
                        for mcp in entity.as_mcps():
                            yield mcp.as_workunit()
                    self._ctx.reporter.views_emitted += 1
                    if result.lineage_extracted:
                        self._ctx.reporter.lineage_via_file_parse += 1

    def _get_view_urn(self, view_name: str, model_name: str) -> Optional[str]:
        """Resolve a view name to its dataset URN."""
        if self._ctx.view_discovery_result:
            file_path = self._ctx.view_discovery_result.view_to_file.get(view_name)
            project = self._ctx.view_discovery_result.view_to_project.get(
                view_name, self._ctx.config.project_name or "default"
            )
            if file_path:
                dataset_name = self._generate_view_name(view_name, file_path, project)
                return builder.make_dataset_urn_with_platform_instance(
                    platform=_VIEW_PLATFORM,
                    name=dataset_name,
                    platform_instance=self._ctx.config.platform_instance,
                    env=self._ctx.config.env,
                )

        project = self._ctx.config.project_name or "default"
        view_id = LookerViewId(
            project_name=project,
            model_name=model_name,
            view_name=view_name,
            file_path="",
        )
        mapping = view_id.get_mapping(self._ctx.config)
        mapping.file_path = view_id.preprocess_file_path(mapping.file_path)
        dataset_name = self._ctx.config.view_naming_pattern.replace_variables(mapping)
        return builder.make_dataset_urn_with_platform_instance(
            platform=_VIEW_PLATFORM,
            name=dataset_name,
            platform_instance=self._ctx.config.platform_instance,
            env=self._ctx.config.env,
        )

    def _generate_view_name(self, view_name: str, file_path: str, project: str) -> str:
        """Build the dataset name for a view using the configured view_naming_pattern."""
        relative_path = file_path
        if self._ctx.config.base_folder and file_path:
            try:
                relative_path = str(
                    Path(file_path).relative_to(self._ctx.config.base_folder)
                )
            except ValueError:
                if self._ctx.resolved_project_paths:
                    for dep_path in self._ctx.resolved_project_paths.values():
                        try:
                            relative_path = str(Path(file_path).relative_to(dep_path))
                            break
                        except ValueError:
                            continue

        view_id = LookerViewId(
            project_name=project,
            model_name="",
            view_name=view_name,
            file_path=relative_path,
        )
        mapping = view_id.get_mapping(self._ctx.config)
        mapping.file_path = view_id.preprocess_file_path(mapping.file_path)
        return self._ctx.config.view_naming_pattern.replace_variables(mapping)

    def _resolve_view_upstream_lineage(
        self, dataset: Dataset, parsed_view: ParsedView
    ) -> bool:
        """Resolve upstream lineage for a view and attach it to the dataset entity."""
        conn_def = self._lineage_resolver.get_connection_for_view(parsed_view.name)
        if not conn_def:
            logger.debug(
                f"No connection found for view '{parsed_view.name}', skipping lineage"
            )
            return False

        upstream_urns: List[UpstreamInputType] = []

        if parsed_view.sql_table_name:
            table_name = parsed_view.sql_table_name.strip().rstrip(";").strip()
            if table_name and not table_name.startswith("$"):
                try:
                    fqn = _generate_fully_qualified_name(
                        sql_table_name=table_name,
                        connection_def=conn_def,
                        reporter=self._ctx.reporter,
                        view_name=parsed_view.name,
                    )
                    upstream_urns.append(
                        builder.make_dataset_urn_with_platform_instance(
                            platform=conn_def.platform,
                            name=fqn,
                            platform_instance=conn_def.platform_instance,
                            env=self._ctx.config.env,
                        )
                    )
                except (ValueError, KeyError) as e:
                    logger.warning(
                        f"Failed to resolve lineage for view '{parsed_view.name}' "
                        f"table '{table_name}': {e}"
                    )
                    self._ctx.reporter.report_warning(
                        title="Lineage Resolution Failed",
                        message="Could not resolve upstream table for view.",
                        context=f"view={parsed_view.name}, table={table_name}: {e}",
                    )

        elif parsed_view.derived_table_sql:
            api_result: Optional[List[UpstreamInputType]] = None
            if self._ctx.config.enable_api_sql_lineage:
                api_result = self._lineage_resolver.resolve_view_lineage_via_api(
                    parsed_view.name, conn_def, self._get_view_urn
                )

            if api_result is not None:
                upstream_urns.extend(api_result)
                self._ctx.reporter.lineage_via_api += 1
            else:
                table_refs = LookMLParser.extract_table_refs_from_sql(
                    parsed_view.derived_table_sql
                )
                for table_ref in table_refs:
                    try:
                        fqn = _generate_fully_qualified_name(
                            sql_table_name=table_ref,
                            connection_def=conn_def,
                            reporter=self._ctx.reporter,
                            view_name=parsed_view.name,
                        )
                        upstream_urns.append(
                            builder.make_dataset_urn_with_platform_instance(
                                platform=conn_def.platform,
                                name=fqn,
                                platform_instance=conn_def.platform_instance,
                                env=self._ctx.config.env,
                            )
                        )
                    except (ValueError, KeyError) as e:
                        logger.warning(
                            f"Failed to resolve lineage for view '{parsed_view.name}' "
                            f"derived table ref '{table_ref}': {e}"
                        )
                        self._ctx.reporter.report_warning(
                            title="Derived Table Lineage Resolution Failed",
                            message="Could not resolve derived table upstream reference.",
                            context=f"view={parsed_view.name}, ref={table_ref}: {e}",
                        )
                self._ctx.reporter.lineage_via_file_parse += 1

            upstream_urns.extend(
                self._lineage_resolver.resolve_liquid_view_refs(
                    parsed_view.derived_table_sql, conn_def, parsed_view.name
                )
            )

        if upstream_urns:
            dataset.set_upstreams(upstream_urns)
            self._ctx.reporter.lineage_edges_extracted += len(upstream_urns)
            return True

        return False

    def _process_view_with_refinements(
        self,
        view_name: str,
        file_path: str,
        project: str,
        project_key: Any,
    ) -> Tuple[List[Entity], Optional[ParsedView]]:
        """Process a view with optional refinement expansion."""
        if not self._ctx.config.process_refinements or not self._refinement_handler:
            return [], None

        try:
            chain = self._refinement_handler.find_refinements_for_view(view_name)
        except (OSError, ValueError, KeyError) as e:
            logger.warning(f"Refinement processing failed for {view_name}: {e}")
            self._ctx.reporter.report_warning(
                title="Refinement Processing Failed",
                message="Failed to process refinements for view",
                context=f"{view_name}: {e}",
            )
            return [], None

        if not chain or not chain.refinements:
            return [], None

        entities: List[Entity] = []
        merged_view: Optional[ParsedView] = None

        if self._ctx.config.expand_refinement_lineage:
            prev_name: Optional[str] = None
            for node in chain.nodes:
                node_view_name = node.urn_suffix
                dataset_name = self._generate_view_name(
                    node_view_name, node.file_path, node.project_name
                )

                dataset = Dataset(
                    name=dataset_name,
                    display_name=node.display_name,
                    platform=_VIEW_PLATFORM,
                    platform_instance=self._ctx.config.platform_instance,
                    subtype=DatasetSubTypes.VIEW,
                    parent_container=project_key,
                )

                if prev_name is not None:
                    prev_urn = builder.make_dataset_urn_with_platform_instance(
                        platform=_VIEW_PLATFORM,
                        name=prev_name,
                        platform_instance=self._ctx.config.platform_instance,
                    )
                    dataset.set_upstreams([prev_urn])

                entities.append(dataset)
                prev_name = dataset_name

                self._ctx.reporter.report_refinement(
                    view_name=node_view_name,
                    project=node.project_name,
                    fields_added=len(node.added_dimensions) + len(node.added_measures),
                    fields_modified=len(node.modified_fields),
                )
        else:
            base_node = chain.base
            if base_node and base_node.parsed_view:
                merged_view = base_node.parsed_view
                for ref_node in chain.refinements:
                    if ref_node.parsed_view:
                        merged_view = merge_additive_parameters(
                            merged_view, ref_node.parsed_view
                        )
                    self._ctx.reporter.report_refinement(
                        view_name=view_name,
                        project=ref_node.project_name,
                        fields_added=(
                            len(ref_node.added_dimensions)
                            + len(ref_node.added_measures)
                        ),
                        fields_modified=len(ref_node.modified_fields),
                    )

            dataset = Dataset(
                name=self._generate_view_name(view_name, file_path, project),
                display_name=view_name,
                platform=_VIEW_PLATFORM,
                platform_instance=self._ctx.config.platform_instance,
                subtype=DatasetSubTypes.VIEW,
                parent_container=project_key,
            )
            entities.append(dataset)

        return entities, merged_view

    def _process_reachable_view(
        self, view_name: str
    ) -> Optional["_ViewProcessingResult"]:
        """Process a reachable view (referenced by explores)."""
        if not self._ctx.view_discovery_result:
            return None

        file_path = self._ctx.view_discovery_result.view_to_file.get(view_name)
        project = self._ctx.view_discovery_result.view_to_project.get(
            view_name, "default"
        )

        view_id = LookerViewId(
            project_name=project,
            model_name="",
            view_name=view_name,
            file_path=file_path or "",
        )

        project_key = gen_project_key(self._ctx.config, project)

        entities, merged_view = self._process_view_with_refinements(
            view_name, file_path or "", project, project_key
        )
        if not entities:
            dataset = Dataset(
                name=self._generate_view_name(view_name, file_path or "", project),
                display_name=view_name,
                platform=_VIEW_PLATFORM,
                platform_instance=self._ctx.config.platform_instance,
                subtype=DatasetSubTypes.VIEW,
                parent_container=project_key,
            )
            entities = [dataset]

        lineage_extracted = False
        if file_path:
            resolved_view = merged_view
            if not resolved_view:
                parser = LookMLParser(
                    template_variables=self._ctx.config.liquid_variables,
                    constants=self._ctx.config.lookml_constants,
                    environment=self._ctx.config.looker_environment,
                )
                parsed_views = parser.parse_view_file(file_path, project)
                for pv in parsed_views:
                    if pv.name == view_name and not pv.is_refinement:
                        resolved_view = pv
                        break

            if resolved_view:
                last_dataset = entities[-1]
                if isinstance(last_dataset, Dataset):
                    lineage_extracted = self._resolve_view_upstream_lineage(
                        last_dataset, resolved_view
                    )

        return _ViewProcessingResult(
            entities=entities,
            view_id=view_id,
            lineage_extracted=lineage_extracted,
            is_unreachable=False,
        )

    def _process_unreachable_view(
        self, view_name: str
    ) -> Optional["_ViewProcessingResult"]:
        """Process an unreachable view (parse from LookML)."""
        if not self._ctx.view_discovery_result:
            return None

        file_path = self._ctx.view_discovery_result.view_to_file.get(view_name)
        project = self._ctx.view_discovery_result.view_to_project.get(
            view_name, "default"
        )

        if not file_path:
            return None

        parser = LookMLParser(
            template_variables=self._ctx.config.liquid_variables,
            constants=self._ctx.config.lookml_constants,
            environment=self._ctx.config.looker_environment,
        )
        parsed_views = parser.parse_view_file(file_path, project)

        parsed_view = None
        for pv in parsed_views:
            if pv.name == view_name:
                parsed_view = pv
                break

        if not parsed_view:
            return None

        view_id = LookerViewId(
            project_name=project,
            model_name="",
            view_name=view_name,
            file_path=file_path,
        )

        project_key = gen_project_key(self._ctx.config, project)

        entities, _ = self._process_view_with_refinements(
            view_name, file_path, project, project_key
        )
        if not entities:
            dataset = Dataset(
                name=self._generate_view_name(view_name, file_path, project),
                display_name=view_name,
                platform=_VIEW_PLATFORM,
                platform_instance=self._ctx.config.platform_instance,
                subtype=DatasetSubTypes.VIEW,
                parent_container=project_key,
            )
            entities = [dataset]

        lineage_extracted = False
        last_dataset = entities[-1]
        if isinstance(last_dataset, Dataset):
            lineage_extracted = self._resolve_view_upstream_lineage(
                last_dataset, parsed_view
            )

        return _ViewProcessingResult(
            entities=entities,
            view_id=view_id,
            lineage_extracted=lineage_extracted,
            is_unreachable=True,
        )


from dataclasses import dataclass  # noqa: E402


@dataclass
class _ViewProcessingResult:
    """Internal result type for view processing."""

    entities: List[Entity]
    view_id: LookerViewId
    lineage_extracted: bool = False
    is_unreachable: bool = False
