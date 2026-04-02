"""Explore processor for LookerV2Source."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Optional, Set, Tuple

from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import LookmlModelExplore, LookmlModelExploreField

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.looker.looker_common import (
    LookerUtil,
    LookerViewId,
    ViewField,
    ViewFieldType,
    gen_model_key,
)
from datahub.metadata.schema_classes import (
    EmbedClass,
    OtherSchemaClass,
    SchemaMetadataClass,
)
from datahub.metadata.urns import TagUrn
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset, UpstreamInputType
from datahub.sdk.entity import Entity
from datahub.utilities.backpressure_aware_executor import BackpressureAwareExecutor
from datahub.utilities.url_util import remove_port_from_url

if TYPE_CHECKING:
    from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context

logger = logging.getLogger(__name__)

VIEW_PLATFORM = "looker"


class LookerExploreProcessor:
    """Processes LookML explores into Dataset workunits."""

    def __init__(self, ctx: "LookerV2Context") -> None:
        self._ctx = ctx

    def process(self) -> Iterable[MetadataWorkUnit]:
        """Process LookML explores."""
        logger.info("Processing explores...")

        explores_to_process: List[Tuple[str, str]] = []

        for model_name, model in self._ctx.model_registry.items():
            if not self._ctx.config.model_pattern.allowed(model_name):
                continue

            if model.explores:
                for explore in model.explores:
                    if not explore.name:
                        continue
                    if not self._ctx.config.explore_pattern.allowed(explore.name):
                        continue
                    if (
                        self._ctx.config.emit_used_explores_only
                        and (
                            model_name,
                            explore.name,
                        )
                        not in self._ctx.reachable_explores
                    ):
                        self._ctx.reporter.explores_skipped += 1
                        continue
                    explores_to_process.append((model_name, explore.name))

        self._ctx.reporter.explores_discovered = len(explores_to_process)

        # Emit model containers (matching V1 behavior)
        emitted_models: Set[str] = set()
        for model_name, _ in explores_to_process:
            if model_name not in emitted_models:
                model_key = gen_model_key(self._ctx.config, model_name)
                model_container = Container(
                    container_key=model_key,
                    display_name=model_name,
                    subtype=BIContainerSubTypes.LOOKML_MODEL,
                )
                for mcp in model_container.as_mcps():
                    yield mcp.as_workunit()
                emitted_models.add(model_name)

        # Process explores in parallel
        def process_explore(
            model: str, explore: str
        ) -> Optional[Tuple[Entity, List[MetadataChangeProposalWrapper]]]:
            return self._process_single_explore(model, explore)

        for future in BackpressureAwareExecutor.map(
            fn=process_explore,
            args_list=explores_to_process,
            max_workers=self._ctx.config.max_concurrent_requests,
            max_pending=self._ctx.config.max_concurrent_requests * 2,
        ):
            try:
                result = future.result()
                if result:
                    entity, extra_mcps = result
                    for mcp in entity.as_mcps():
                        yield mcp.as_workunit()
                    for extra_mcp in extra_mcps:
                        yield extra_mcp.as_workunit()
                    self._ctx.reporter.explores_scanned += 1
            except (SDKError, ValueError, KeyError, TypeError) as e:
                self._ctx.reporter.report_warning(
                    title="Explore Processing Failed",
                    message="Error processing explore",
                    context=str(e),
                )

    def _process_single_explore(
        self, model_name: str, explore_name: str
    ) -> Optional[Tuple[Entity, List[MetadataChangeProposalWrapper]]]:
        """Process a single explore with schema fields."""
        # Check cache first (populated during view discovery)
        cache_key = (model_name, explore_name)
        if cache_key in self._ctx.explore_cache:
            explore = self._ctx.explore_cache[cache_key]
            self._ctx.reporter.explore_cache_hits += 1
        else:
            self._ctx.reporter.explore_cache_misses += 1
            try:
                explore = self._ctx.looker_api.lookml_model_explore(
                    model_name, explore_name
                )
            except SDKError as e:
                logger.warning(
                    f"Failed to fetch explore {model_name}.{explore_name}: {e}"
                )
                return None

        if not explore:
            return None

        explore_id = f"{model_name}.explore.{explore_name}"

        # Get model key for container
        model_key = gen_model_key(self._ctx.config, model_name)

        dataset = Dataset(
            name=explore_id,
            display_name=explore.label or explore_name,
            platform=self._ctx.platform,
            platform_instance=self._ctx.config.platform_instance,
            description=explore.description,
            subtype=DatasetSubTypes.LOOKER_EXPLORE,
            parent_container=model_key,
        )

        # Build upstream lineage: explore → views
        upstream_view_urns = self._get_explore_upstream_view_urns(explore, model_name)
        if upstream_view_urns:
            dataset.set_upstreams(upstream_view_urns)

        # Apply explore-level tags (from LookML `tags` property on explore)
        if explore.tags:
            dataset.set_tags([TagUrn(tag) for tag in explore.tags])

        extra_mcps: List[MetadataChangeProposalWrapper] = []

        # Extract schema fields from explore API response
        view_fields = self._extract_explore_fields(explore)
        if view_fields:
            fields, primary_keys = LookerUtil._get_fields_and_primary_keys(
                view_fields=view_fields,
                reporter=self._ctx.reporter,
                tag_measures_and_dimensions=self._ctx.config.tag_measures_and_dimensions,
            )
            if fields:
                schema = SchemaMetadataClass(
                    schemaName=explore_id,
                    platform=f"urn:li:dataPlatform:{self._ctx.platform}",
                    version=0,
                    fields=fields,
                    primaryKeys=primary_keys,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                )
                extra_mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=str(dataset.urn),
                        aspect=schema,
                    )
                )

        # Emit embed URL for explore
        if self._ctx.config.extract_embed_urls and self._ctx.config.external_base_url:
            base_url = remove_port_from_url(self._ctx.config.external_base_url)
            embed_url = f"{base_url}/embed/explore/{model_name}/{explore_name}"
            extra_mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=str(dataset.urn),
                    aspect=EmbedClass(renderUrl=embed_url),
                )
            )

        return (dataset, extra_mcps)

    def _extract_explore_fields(self, explore: LookmlModelExplore) -> List[ViewField]:
        """Extract ViewField objects from explore API response fields."""
        view_fields: List[ViewField] = []

        if not explore.fields:
            return view_fields

        if explore.fields.dimensions:
            for dim in explore.fields.dimensions:
                if not dim.name:
                    continue
                view_fields.append(
                    ViewField(
                        name=dim.name,
                        label=dim.label_short,
                        type=dim.type or "string",
                        description=dim.description or "",
                        field_type=(
                            ViewFieldType.DIMENSION_GROUP
                            if dim.dimension_group
                            else ViewFieldType.DIMENSION
                        ),
                        is_primary_key=dim.primary_key or False,
                        tags=list(dim.tags) if dim.tags else [],
                        group_label=dim.field_group_label,
                    )
                )

        if explore.fields.measures:
            for measure in explore.fields.measures:
                if not measure.name:
                    continue
                view_fields.append(
                    ViewField(
                        name=measure.name,
                        label=measure.label_short,
                        type=measure.type or "string",
                        description=measure.description or "",
                        field_type=ViewFieldType.MEASURE,
                        tags=list(measure.tags) if measure.tags else [],
                        group_label=measure.field_group_label,
                    )
                )

        if explore.fields.parameters:
            for param in explore.fields.parameters:
                if not param.name:
                    continue
                view_fields.append(
                    ViewField(
                        name=param.name,
                        label=param.label_short,
                        type=param.type or "string",
                        description=param.description or "",
                        field_type=ViewFieldType.UNKNOWN,
                    )
                )

        return view_fields

    def _api_field_to_view_field(self, api_field: LookmlModelExploreField) -> ViewField:
        """Convert a LookML API explore field to a ViewField."""
        field_type = ViewFieldType.DIMENSION
        if api_field.category == "measure":
            field_type = ViewFieldType.MEASURE
        elif api_field.dimension_group:
            field_type = ViewFieldType.DIMENSION_GROUP

        return ViewField(
            name=api_field.name or "",
            label=api_field.label_short,
            description=api_field.description or "",
            type=api_field.type or "",
            field_type=field_type,
            is_primary_key=api_field.primary_key or False,
            tags=list(api_field.tags) if api_field.tags else [],
            group_label=api_field.field_group_label,
        )

    def _get_explore_upstream_view_urns(
        self, explore: LookmlModelExplore, model_name: str
    ) -> List[UpstreamInputType]:
        """Get upstream view URNs for an explore (explore→view lineage)."""
        views: Set[str] = set()

        # Base view
        if explore.view_name and explore.view_name != explore.name:
            views.add(explore.view_name)
        elif explore.name:
            views.add(explore.name)

        # Joined views
        if explore.joins:
            for join in explore.joins:
                if join.from_:
                    views.add(join.from_)
                elif join.name:
                    views.add(join.name)

        urns: List[UpstreamInputType] = []
        for view_name in sorted(views):
            urn = self._get_view_urn(view_name, model_name)
            if urn:
                urns.append(urn)
        return urns

    def _get_view_urn(self, view_name: str, model_name: str) -> Optional[str]:
        """Resolve a view name to its dataset URN."""
        view_discovery_result = self._ctx.view_discovery_result
        if view_discovery_result:
            file_path = view_discovery_result.view_to_file.get(view_name)
            project = view_discovery_result.view_to_project.get(
                view_name, self._ctx.config.project_name or "default"
            )
            if file_path:
                dataset_name = self._generate_view_name(view_name, file_path, project)
                return builder.make_dataset_urn_with_platform_instance(
                    platform=VIEW_PLATFORM,
                    name=dataset_name,
                    platform_instance=self._ctx.config.platform_instance,
                    env=self._ctx.config.env,
                )

        # Fallback: generate URN without file path info
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
            platform=VIEW_PLATFORM,
            name=dataset_name,
            platform_instance=self._ctx.config.platform_instance,
            env=self._ctx.config.env,
        )

    def _generate_view_name(self, view_name: str, file_path: str, project: str) -> str:
        """Build the dataset name for a view using the configured view_naming_pattern."""
        # Convert absolute file path to relative path within the project
        relative_path = file_path
        if self._ctx.config.base_folder and file_path:
            try:
                relative_path = str(
                    Path(file_path).relative_to(self._ctx.config.base_folder)
                )
            except ValueError:
                # Check project dependencies
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
