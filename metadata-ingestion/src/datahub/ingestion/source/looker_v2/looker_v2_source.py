"""
Looker V2 Combined Source.

A unified source combining functionality from both looker and lookml sources.
Provides hybrid extraction: API-based for BI assets, file-based for views.
"""

from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path
from typing import Any, FrozenSet, Iterable, List, Optional, Set

from looker_sdk.error import SDKError

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.git.git_import import GitClone
from datahub.ingestion.source.looker.looker_common import (
    LookerExploreRegistry,
    LookerUserRegistry,
    LookerUtil,
    gen_project_key,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker_v2.looker_v2_config import (
    LookerConnectionDefinition,
    LookerV2Config,
    LookerV2GitInfo,
)
from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context
from datahub.ingestion.source.looker_v2.looker_v2_dashboard_processor import (
    LookerDashboardProcessor,
)
from datahub.ingestion.source.looker_v2.looker_v2_explore_processor import (
    LookerExploreProcessor,
)
from datahub.ingestion.source.looker_v2.looker_v2_folder_processor import (
    LookerFolderProcessor,
)
from datahub.ingestion.source.looker_v2.looker_v2_look_processor import (
    LookerLookProcessor,
)
from datahub.ingestion.source.looker_v2.looker_v2_pdt_graph_parser import (
    parse_pdt_graph,
)
from datahub.ingestion.source.looker_v2.looker_v2_report import LookerV2SourceReport
from datahub.ingestion.source.looker_v2.looker_v2_usage_extractor import (
    LookerUsageExtractor,
)
from datahub.ingestion.source.looker_v2.looker_v2_view_processor import (
    LookerViewProcessor,
)
from datahub.ingestion.source.looker_v2.lookml_manifest_parser import ManifestParser
from datahub.ingestion.source.looker_v2.lookml_view_refinement_handler import (
    RefinementHandler,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sdk.container import Container

logger = logging.getLogger(__name__)

_REQUIRED_API_PERMISSIONS: FrozenSet[str] = frozenset(
    {
        "access_data",
        "explore",
        "see_lookml",
        "see_lookml_dashboards",
        "see_looks",
        "see_user_dashboards",
    }
)


@platform_name("Looker")
@support_status(SupportStatus.INCUBATING)
@config_class(LookerV2Config)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Use the `platform_instance` field")
@capability(
    SourceCapability.OWNERSHIP, "Enabled by default, configured using `extract_owners`"
)
@capability(SourceCapability.LINEAGE_COARSE, "Supported by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configured using `extract_column_level_lineage`",
)
@capability(
    SourceCapability.USAGE_STATS,
    "Enabled by default, configured using `extract_usage_history`",
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.LOOKML_MODEL,
        SourceCapabilityModifier.LOOKML_PROJECT,
        SourceCapabilityModifier.LOOKER_FOLDER,
    ],
)
class LookerV2Source(TestableSource, StatefulIngestionSourceBase):
    """
    Looker V2 Combined Source.

    This plugin extracts the following:
    - Looker dashboards, charts, and explores (via Looker API)
    - LookML views including reachable, unreachable, and refined views (via file parsing)
    - Lineage between views and warehouse tables
    - Multi-project dependencies
    - Ownership and usage statistics

    This is a combined source that replaces both the `looker` and `lookml` sources.
    """

    platform = "looker"
    # V1's lookml source uses platform "looker" for views (via LookerCommonConfig.platform_name).
    # We must match this for backward-compatible URNs.
    VIEW_PLATFORM = "looker"

    def __init__(self, config: LookerV2Config, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.reporter = LookerV2SourceReport()
        self.ctx = ctx

        self.looker_api: LookerAPI = LookerAPI(self.config)

        # Build shared context (will be fully populated during _initialize)
        self._ctx = LookerV2Context(
            config=self.config,
            looker_api=self.looker_api,
            reporter=self.reporter,
            pipeline_ctx=self.ctx,
            platform=self.platform,
        )

        self._folder_proc = LookerFolderProcessor(self._ctx)

        # Registries for caching
        self.user_registry: LookerUserRegistry = LookerUserRegistry(
            self.looker_api,
            self.reporter,
        )
        # LookerV2Config has the same bases as LookerDashboardSourceConfig; structurally compatible.
        self.explore_registry: LookerExploreRegistry = LookerExploreRegistry(
            self.looker_api,
            self.reporter,
            self.config,  # type: ignore[arg-type]
        )

        # State tracking
        self.reachable_look_registry: Set[str] = set()
        self.chart_urns: Set[str] = set()

        self._ctx.user_registry = self.user_registry

        self._look_proc = LookerLookProcessor(
            ctx=self._ctx,
            folder_proc=self._folder_proc,
            user_registry=self.user_registry,
            reachable_look_registry=self.reachable_look_registry,
        )

        self._usage_extractor = LookerUsageExtractor(self._ctx, self.user_registry)

        self._dashboard_proc = LookerDashboardProcessor(
            ctx=self._ctx,
            folder_proc=self._folder_proc,
            explore_registry=self.explore_registry,
            reachable_look_registry=self.reachable_look_registry,
            chart_urns=self.chart_urns,
        )

        self._explore_proc = LookerExploreProcessor(self._ctx)

        # Cloned git repos (temp directories)
        self._temp_dirs: List[str] = []

        # Cached refinement handler (created once during initialization)
        self._refinement_handler: Optional[RefinementHandler] = None

        self._view_proc = LookerViewProcessor(
            ctx=self._ctx,
            refinement_handler=None,
        )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to Looker API and LookML repository."""
        test_report = TestConnectionReport()

        try:
            config = LookerV2Config.parse_obj_allow_extras(config_dict)

            # Test Looker API connection
            api = LookerAPI(config)
            permissions = api.get_available_permissions()

            test_report.basic_connectivity = CapabilityReport(capable=True)
            test_report.capability_report = {}

            if _REQUIRED_API_PERMISSIONS.issubset(permissions):
                test_report.capability_report[SourceCapability.DESCRIPTIONS] = (
                    CapabilityReport(capable=True)
                )
            else:
                missing = _REQUIRED_API_PERMISSIONS - permissions
                test_report.capability_report[SourceCapability.DESCRIPTIONS] = (
                    CapabilityReport(
                        capable=False,
                        failure_reason=f"Missing permissions: {', '.join(missing)}",
                    )
                )

            # Test LookML access if configured
            if config.base_folder:
                lookml_path = Path(config.base_folder)
                if lookml_path.exists():
                    test_report.capability_report[SourceCapability.LINEAGE_COARSE] = (
                        CapabilityReport(capable=True)
                    )
                else:
                    test_report.capability_report[SourceCapability.LINEAGE_COARSE] = (
                        CapabilityReport(
                            capable=False,
                            failure_reason=f"LookML path not found: {config.base_folder}",
                        )
                    )

        except Exception as e:
            logger.exception(f"Failed to test connection: {e}")
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )

        return test_report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Main entry point for generating work units."""
        try:
            # Stage 1: Initialize and populate registries
            with self._stage_timer("initialize"):
                yield from self._initialize()

            # Stage 2: Process dashboards and charts
            if self.config.extract_dashboards:
                with self._stage_timer("process_dashboards"):
                    yield from self._dashboard_proc.process()

            # Stage 3: Process standalone looks
            if self.config.extract_looks:
                with self._stage_timer("process_looks"):
                    yield from self._look_proc.process()

            # Stage 4: Process explores
            if self.config.extract_explores:
                with self._stage_timer("process_explores"):
                    yield from self._explore_proc.process()

            # Stage 5: Process views (reachable and unreachable)
            if self.config.extract_views:
                with self._stage_timer("process_views"):
                    yield from self._view_proc.process()

            # Free caches after views are done
            self._ctx.explore_cache.clear()
            self._ctx.parsed_view_file_cache.clear()

            # Stage 6: Emit tag entities (Dimension, Measure, Temporal, group_label)
            if self.config.tag_measures_and_dimensions:
                for tag_urn, tag_props in LookerUtil.tag_definitions.items():
                    yield MetadataChangeProposalWrapper(
                        entityUrn=tag_urn,
                        aspect=tag_props,
                    ).as_workunit()

            # Stage 7: Emit Looker user ID → email platform resource
            with self._stage_timer("user_id_mapping"):
                yield from auto_workunit(
                    self.user_registry.to_platform_resource(
                        self.config.platform_instance
                    )
                )

            # Stage 8: Extract usage statistics
            if self.config.extract_usage_history:
                with self._stage_timer("usage_extraction"):
                    yield from self._usage_extractor.process()
                # Free usage data after extraction
                self._ctx.dashboards_for_usage.clear()

        finally:
            # Cleanup temp directories
            self._cleanup_temp_dirs()

    def _stage_timer(self, stage_name: str) -> Any:
        """Context manager that records wall-clock duration for a named pipeline stage."""
        from contextlib import contextmanager

        @contextmanager
        def timer() -> Any:
            start = time.time()
            try:
                yield
            finally:
                duration = time.time() - start
                self.reporter.stage_timings_seconds[stage_name] = round(duration, 3)
                logger.info(f"Stage '{stage_name}' completed in {duration:.2f}s")

        return timer()

    def _initialize(self) -> Iterable[MetadataWorkUnit]:
        """Initialize source: clone repos, populate registries."""
        # Clone git repos if needed
        if self.config.git_info:
            with self._stage_timer("init.clone_main"):
                self._clone_main_project()

        # Clone dependency repos from config
        for project_name, dep in self.config.project_dependencies.items():
            if isinstance(dep, LookerV2GitInfo):
                with self._stage_timer(f"init.clone_dep.{project_name}"):
                    self._clone_dependency(project_name, dep)
            else:
                self._ctx.resolved_project_paths[project_name] = dep

        # Parse manifest.lkml and resolve additional dependencies
        if self.config.base_folder:
            with self._stage_timer("init.manifest"):
                self._resolve_manifest_dependencies()

        # Populate registries
        with self._stage_timer("init.populate_registries"):
            self._populate_registries()

        # Auto-discover connections from Looker API
        with self._stage_timer("init.discover_connections"):
            self._auto_discover_connections()

        # Fetch PDT lineage graphs
        with self._stage_timer("init.pdt_lineage"):
            self._fetch_pdt_lineage()

        # Initialize refinement handler once for all views
        if self.config.process_refinements and self.config.base_folder:
            with self._stage_timer("init.refinement_handler"):
                self._refinement_handler = RefinementHandler(
                    base_folder=self.config.base_folder,
                    project_name=self.config.project_name or "default",
                    project_dependencies=self._ctx.resolved_project_paths or None,
                )
                self._view_proc._refinement_handler = self._refinement_handler

        # Emit project container
        if self.config.project_name and self.config.base_folder:
            yield from self._emit_project_container()

    def _resolve_manifest_dependencies(self) -> None:
        """Parse manifest.lkml and resolve project dependencies."""
        if not self.config.base_folder:
            return

        manifest_parser = ManifestParser(
            base_folder=self.config.base_folder,
            project_name=self.config.project_name,
            project_dependencies=self._ctx.resolved_project_paths,
            git_info=self.config.git_info,
        )

        try:
            # Parse and resolve all dependencies
            resolved = manifest_parser.parse_and_resolve()

            # Update resolved paths
            for name, path in resolved.items():
                if name not in self._ctx.resolved_project_paths:
                    self._ctx.resolved_project_paths[name] = str(path)

            # Update project name if discovered from manifest
            if manifest_parser.project_name and not self.config.project_name:
                self.config.project_name = manifest_parser.project_name

            # Store constants for later use in LookML parsing
            for name, const in manifest_parser.constants.items():
                if name not in self.config.lookml_constants:
                    self.config.lookml_constants[name] = const.value

            # Track temp dirs for cleanup
            self._temp_dirs.extend(manifest_parser.consume_temp_dirs())

            logger.info(
                f"Manifest resolution complete: "
                f"projects={list(resolved.keys())}, "
                f"constants={len(manifest_parser.constants)}"
            )

        except (OSError, ValueError, KeyError) as e:
            self.reporter.report_warning(
                title="Manifest Resolution Failed",
                message="Failed to parse manifest.lkml",
                context=f"{self.config.base_folder}: {e}",
            )

    def _clone_main_project(self) -> None:
        """Clone the main LookML project from git."""
        if not self.config.git_info:
            return

        git_info = self.config.git_info
        temp_dir = tempfile.mkdtemp(prefix="looker_v2_main_")
        self._temp_dirs.append(temp_dir)

        try:
            git_clone = GitClone(str(temp_dir))
            checkout_dir = git_clone.clone(
                ssh_key=git_info.deploy_key,  # Already SecretStr
                repo_url=git_info.repo,
                branch=git_info.branch,
            )
            self.config.base_folder = str(checkout_dir)
            logger.info(f"Cloned main project to {checkout_dir}")
        except Exception as e:
            self.reporter.report_failure(
                title="Git Clone Failed",
                message="Failed to clone main LookML project from git.",
                context=f"{git_info.repo}: {e}",
            )
            raise

    def _clone_dependency(self, project_name: str, git_info: LookerV2GitInfo) -> None:
        """Clone a dependency project from git."""
        temp_dir = tempfile.mkdtemp(prefix=f"looker_v2_dep_{project_name}_")
        self._temp_dirs.append(temp_dir)

        try:
            git_clone = GitClone(str(temp_dir))
            checkout_dir = git_clone.clone(
                ssh_key=git_info.deploy_key,
                repo_url=git_info.repo,
                branch=git_info.branch,
            )
            self._ctx.resolved_project_paths[project_name] = str(checkout_dir)
            logger.info(f"Cloned dependency '{project_name}' to {checkout_dir}")
        except Exception as e:
            self.reporter.report_warning(
                title="Dependency Clone Failed",
                message="Failed to clone LookML dependency project from git. Views from this dependency will be missing.",
                context=f"project={project_name}, repo={git_info.repo}: {e}",
            )

    def _cleanup_temp_dirs(self) -> None:
        """Cleanup temporary directories."""
        import shutil

        for temp_dir in self._temp_dirs:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp dir {temp_dir}: {e}")

    def _populate_registries(self) -> None:
        """Bulk fetch all reference data upfront."""
        logger.info("Populating registries...")

        self._folder_proc.build_registry()

        # Fetch all models
        try:
            models = self.looker_api.all_lookml_models()
            for model in models:
                if model.name:
                    self._ctx.model_registry[model.name] = model
                    self.reporter.models_discovered += 1
        except SDKError as e:
            self.reporter.report_failure(
                title="Model Fetch Failed",
                message="Could not fetch LookML models from Looker API. No explores, views, or lineage will be extracted.",
                context=str(e),
            )

    def _auto_discover_connections(self) -> None:
        """Auto-discover connection_to_platform_map from Looker API."""
        try:
            connections = self.looker_api.all_connections()
        except SDKError as e:
            logger.warning(f"Failed to fetch connections for auto-discovery: {e}")
            return

        failed_connections: List[str] = []

        for conn in connections:
            if not conn.name:
                continue
            # Manual config entries take precedence
            if conn.name in self.config.connection_to_platform_map:
                continue
            try:
                connection_def = LookerConnectionDefinition.from_looker_connection(conn)
                self.config.connection_to_platform_map[conn.name] = connection_def
                logger.info(
                    f"Auto-discovered connection '{conn.name}' -> "
                    f"platform='{connection_def.platform}', "
                    f"db='{connection_def.default_db}'"
                )
            except ConfigurationError as e:
                failed_connections.append(conn.name)
                logger.warning(f"Could not auto-discover connection '{conn.name}': {e}")

        if failed_connections:
            self.reporter.report_warning(
                title="Connection Auto-Discovery Incomplete",
                message="Could not auto-discover some connections; add them manually to connection_to_platform_map if needed.",
                context=f"{len(failed_connections)} failed: {', '.join(failed_connections)}",
            )

    def _fetch_pdt_lineage(self) -> None:
        """Fetch PDT dependency graphs for all models via API."""
        if not self.config.enable_api_sql_lineage:
            return

        for model_name in self._ctx.model_registry:
            if not self.config.model_pattern.allowed(model_name):
                continue
            try:
                graph = self.looker_api.graph_derived_tables_for_model(model_name)
                if graph and isinstance(graph.graph_text, str) and graph.graph_text:
                    edges = parse_pdt_graph(graph.graph_text)
                    for edge in edges:
                        self._ctx.pdt_upstream_map.setdefault(
                            edge.view_name, []
                        ).append(edge)
                    self.reporter.pdt_graphs_fetched += 1
                    self.reporter.pdt_edges_discovered += len(edges)
            except Exception as e:
                logger.warning(
                    f"Failed to fetch PDT graph for model '{model_name}': {e}"
                )
                self.reporter.report_warning(
                    title="PDT Graph Fetch Failed",
                    message="Could not fetch PDT graph for model",
                    context=f"{model_name}: {e}",
                )

    def _emit_project_container(self) -> Iterable[MetadataWorkUnit]:
        """Emit the LookML project as a container."""
        project_name = self.config.project_name or "default"

        container = Container(
            container_key=gen_project_key(self.config, project_name),
            subtype=BIContainerSubTypes.LOOKML_PROJECT,
            display_name=project_name,
        )

        for mcp in container.as_mcps():
            yield mcp.as_workunit()

        self.reporter.projects_processed += 1

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """Get work unit processors for auto work unit handling."""
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self,
                self.config,
                self.ctx,
            ).workunit_processor,
        ]

    def get_report(self) -> LookerV2SourceReport:
        """Get the source report."""
        return self.reporter

    def close(self) -> None:
        """Cleanup resources."""
        self._cleanup_temp_dirs()
        super().close()
