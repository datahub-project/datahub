"""
DataHub source for MicroStrategy.

Extracts metadata including:
- Projects (as containers)
- Folders (as nested containers)
- Dashboards/Dossiers
- Reports (as charts)
- Intelligent Cubes (as datasets)
- Datasets
- Ownership information
- Lineage (optional)
- Usage statistics (optional)
"""

import logging
from typing import Any, Dict, Iterable, List, Optional

from dateutil import parser as date_parser

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
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
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.microstrategy.client import MicroStrategyClient
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChartInfoClass,
    ContainerClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
)
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)


# Custom ContainerKey subclasses for MicroStrategy hierarchy
class ProjectKey(ContainerKey):
    """Container key for MicroStrategy projects."""

    project: str


class FolderKey(ContainerKey):
    """Container key for MicroStrategy folders."""

    project: str
    folder: str


@platform_name("MicroStrategy", id="microstrategy")
@config_class(MicroStrategyConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Enabled by default",
    supported=True,
)
@capability(
    SourceCapability.DOMAINS,
    "Supported via the `domain` config field",
    supported=True,
)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    supported=True,
)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Enabled by default",
    supported=True,
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default via `include_lineage`",
    supported=True,
)
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default via `include_ownership`",
    supported=True,
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled via stateful ingestion",
    supported=True,
)
class MicroStrategySource(StatefulIngestionSourceBase, TestableSource):
    """
    Ingests metadata from MicroStrategy.

    Extracts:
    - Projects as containers
    - Folders as nested containers (hierarchical)
    - Dashboards (Dossiers) with basic metadata
    - Reports as charts
    - Intelligent Cubes as datasets
    - Datasets
    - Ownership information
    - Lineage (optional, not implemented in MVP)
    - Usage statistics (optional, not implemented in MVP)
    """

    platform = "microstrategy"

    def __init__(self, config: MicroStrategyConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = StaleEntityRemovalSourceReport()

        # Initialize API client
        self.client = MicroStrategyClient(self.config.connection)

        # Initialize stale entity removal handler
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

        # Global registries for cross-project resolution
        self.cube_registry: Dict[str, Dict[str, Any]] = {}
        self.dataset_registry: Dict[str, Dict[str, Any]] = {}

        # Domain registry for domain assignment (optional)
        domain_config = getattr(self.config, "domain", {})
        self.domain_registry = DomainRegistry(
            cached_domains=[
                domain_id
                for domain_id in (domain_config.values() if domain_config else [])
            ],
            graph=self.ctx.graph,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MicroStrategySource":
        config = MicroStrategyConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main extraction logic.

        Order:
        1. Emit projects as containers
        2. Emit folders as nested containers (parents before children)
        3. Emit dashboards, reports, cubes, datasets
        4. Emit lineage (if enabled)
        """
        with self.client:
            # Get all projects
            projects = self.client.get_projects()

            # Filter projects by pattern
            filtered_projects = [
                project
                for project in projects
                if self.config.project_pattern.allowed(project.get("name", ""))
            ]

            logger.info(
                f"Processing {len(filtered_projects)} projects (out of {len(projects)})"
            )

            # Phase 1: Build cube/dataset registries for cross-project resolution
            for project in filtered_projects:
                self._build_registries(project)

            # Phase 2: Process projects and emit metadata
            for project in filtered_projects:
                yield from self._process_project(project)

    def _build_registries(self, project: Dict[str, Any]) -> None:
        """
        Build global registries for cubes and datasets.

        This is critical for cross-project lineage resolution.
        """
        project_id = project["id"]
        logger.debug(f"Building registries for project {project.get('name')}")

        try:
            # Register cubes using search API (type=39)
            cubes = self.client.search_objects(project_id, object_type=39)
            for cube in cubes:
                self.cube_registry[cube["id"]] = {**cube, "project_id": project_id}

            logger.debug(
                f"Registered {len(cubes)} cubes from project {project.get('name')}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to build registries for project {project.get('name')}: {e}"
            )

    def _process_project(self, project: Dict[str, Any]) -> Iterable[MetadataWorkUnit]:
        """Process a single project."""
        project_id = project["id"]
        project_name = project.get("name", project_id)

        logger.info(f"Processing project: {project_name}")

        # Emit project container
        yield from self._emit_project_container(project)

        # Get folders in project
        try:
            folders = self.client.get_folders(project_id)
            logger.debug(f"Found {len(folders)} folders in project {project_name}")

            # TODO: Emit folders in topological order (parents before children)
            # For MVP, emit all folders (order doesn't matter for basic extraction)
            for folder in folders:
                if self.config.folder_pattern.allowed(folder.get("name", "")):
                    yield from self._emit_folder_container(folder, project)

        except Exception as e:
            logger.warning(f"Failed to get folders for project {project_name}: {e}")

        # Get dashboards using search API (type=55)
        try:
            dashboards = self.client.search_objects(project_id, object_type=55)
            logger.info(f"Found {len(dashboards)} dashboards in project {project_name}")

            for dashboard in dashboards:
                if self.config.dashboard_pattern.allowed(dashboard.get("name", "")):
                    yield from self._process_dashboard(dashboard, project)

        except Exception as e:
            logger.warning(f"Failed to get dashboards for project {project_name}: {e}")

        # Get reports using search API (type=3)
        try:
            reports = self.client.search_objects(project_id, object_type=3)
            logger.info(f"Found {len(reports)} reports in project {project_name}")

            for report in reports:
                if self.config.report_pattern.allowed(report.get("name", "")):
                    yield from self._process_report(report, project)

        except Exception as e:
            logger.warning(f"Failed to get reports for project {project_name}: {e}")

        # Get cubes using search API (type=39)
        try:
            cubes = self.client.search_objects(project_id, object_type=39)
            logger.info(f"Found {len(cubes)} cubes in project {project_name}")

            for cube in cubes:
                yield from self._process_cube(cube, project)

        except Exception as e:
            logger.warning(f"Failed to get cubes for project {project_name}: {e}")

    def _emit_project_container(
        self, project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Emit project as container."""
        project_key = ProjectKey(
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_containers(
            container_key=project_key,
            name=project.get("name", project["id"]),
            sub_types=[
                BIContainerSubTypes.TABLEAU_PROJECT
            ],  # Using Tableau's PROJECT subtype
            description=project.get("description"),
        )

    def _emit_folder_container(
        self, folder: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Emit folder as container."""
        # TODO: Handle parent-child relationships for nested folders
        # For MVP, emit folders as flat containers under project

        folder_key = FolderKey(
            folder=folder["id"],
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_containers(
            container_key=folder_key,
            name=folder.get("name", folder["id"]),
            sub_types=[BIContainerSubTypes.LOOKER_FOLDER],  # Generic folder subtype
            description=folder.get("description"),
            parent_container_key=ProjectKey(
                project=project["id"],
                platform=self.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
            ),
        )

    def _process_dashboard(
        self, dashboard: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Process dashboard (dossier)."""
        dashboard_urn = make_dashboard_urn(
            platform=self.platform,
            name=dashboard["id"],
            platform_instance=self.config.platform_instance,
        )

        # Get dashboard definition to extract visualizations
        chart_urns: List[str] = []
        if self.config.include_lineage:
            try:
                dashboard_def = self.client.get_dashboard_definition(dashboard["id"])
                visualization_ids = self._extract_visualization_ids(dashboard_def)

                # Create chart URNs for each visualization
                chart_urns = [
                    make_chart_urn(
                        platform=self.platform,
                        name=viz_id,
                        platform_instance=self.config.platform_instance,
                    )
                    for viz_id in visualization_ids
                ]

                logger.debug(
                    f"Found {len(chart_urns)} visualizations in dashboard {dashboard.get('name')}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to get dashboard definition for {dashboard.get('name')}: {e}"
                )

        # Build external URL
        dashboard_url = self._build_dashboard_url(dashboard["id"], project["id"])

        # Extract timestamps
        last_modified = self._build_audit_stamps(
            dashboard.get("dateCreated"),
            dashboard.get("dateModified"),
            dashboard.get("owner"),
        )

        # Dashboard info aspect
        dashboard_info = DashboardInfoClass(
            title=dashboard.get("name", dashboard["id"]),
            description=dashboard.get("description") or "",
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=dashboard_url,
            customProperties={
                "project_id": project["id"],
                "project_name": project.get("name", ""),
                "dashboard_id": dashboard["id"],
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=dashboard_info,
        ).as_workunit()

        # SubTypes aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=SubTypesClass(typeNames=[BIAssetSubTypes.REPORT]),
        ).as_workunit()

        # Status aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # DataPlatformInstance aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=(
                    make_data_platform_urn(self.config.platform_instance)
                    if self.config.platform_instance
                    else None
                ),
            ),
        ).as_workunit()

        # Container aspect (link to project)
        project_container_key = ProjectKey(
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=ContainerClass(container=project_container_key.as_urn()),
        ).as_workunit()

        # Ownership (if enabled and available)
        if self.config.include_ownership and dashboard.get("owner"):
            yield from self._emit_ownership(dashboard_urn, dashboard["owner"])

    def _process_report(
        self, report: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Process report (as chart)."""
        chart_urn = make_chart_urn(
            platform=self.platform,
            name=report["id"],
            platform_instance=self.config.platform_instance,
        )

        # Get upstream datasets (lineage)
        inputs: List[str] = []
        if self.config.include_lineage:
            inputs = self._get_report_inputs(report, project)

        # Build external URL
        chart_url = self._build_report_url(report["id"], project["id"])

        # Extract timestamps
        last_modified = self._build_audit_stamps(
            report.get("dateCreated"),
            report.get("dateModified"),
            report.get("owner"),
        )

        # Chart info aspect
        chart_info = ChartInfoClass(
            title=report.get("name", report["id"]),
            description=report.get("description") or "",
            inputs=inputs,
            lastModified=last_modified,
            externalUrl=chart_url,
            customProperties={
                "project_id": project["id"],
                "project_name": project.get("name", ""),
                "report_id": report["id"],
                "report_type": report.get("type", "report"),
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=chart_info,
        ).as_workunit()

        # SubTypes aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=SubTypesClass(typeNames=[BIAssetSubTypes.REPORT]),
        ).as_workunit()

        # Status aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # DataPlatformInstance aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=(
                    make_data_platform_urn(self.config.platform_instance)
                    if self.config.platform_instance
                    else None
                ),
            ),
        ).as_workunit()

        # Container aspect (link to project)
        project_container_key = ProjectKey(
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ContainerClass(container=project_container_key.as_urn()),
        ).as_workunit()

        # Ownership (if enabled and available)
        if self.config.include_ownership and report.get("owner"):
            yield from self._emit_ownership(chart_urn, report["owner"])

    def _process_cube(
        self, cube: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Process Intelligent Cube (as dataset)."""
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=f"{project['id']}.{cube['id']}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Dataset properties aspect
        dataset_properties = DatasetPropertiesClass(
            name=cube.get("name", cube["id"]),
            description=cube.get("description"),
            customProperties={
                "project_id": project["id"],
                "project_name": project.get("name", ""),
                "cube_id": cube["id"],
                "cube_type": "intelligent_cube",
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        ).as_workunit()

        # SubTypes aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
        ).as_workunit()

        # Status aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # DataPlatformInstance aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=(
                    make_data_platform_urn(self.config.platform_instance)
                    if self.config.platform_instance
                    else None
                ),
            ),
        ).as_workunit()

        # Container aspect (link to project)
        project_container_key = ProjectKey(
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ContainerClass(container=project_container_key.as_urn()),
        ).as_workunit()

        # Schema metadata (if enabled)
        if self.config.include_cube_schema:
            yield from self._emit_cube_schema(dataset_urn, cube)

        # Ownership (if enabled and available)
        if self.config.include_ownership and cube.get("owner"):
            yield from self._emit_ownership(dataset_urn, cube["owner"])

    def _process_dataset(
        self, dataset: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Process dataset."""
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=f"{project['id']}.{dataset['id']}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Dataset properties aspect
        dataset_properties = DatasetPropertiesClass(
            name=dataset.get("name", dataset["id"]),
            description=dataset.get("description"),
            customProperties={
                "project_id": project["id"],
                "project_name": project.get("name", ""),
                "dataset_id": dataset["id"],
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        ).as_workunit()

        # SubTypes aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
        ).as_workunit()

        # Status aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # DataPlatformInstance aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=(
                    make_data_platform_urn(self.config.platform_instance)
                    if self.config.platform_instance
                    else None
                ),
            ),
        ).as_workunit()

        # Container aspect (link to project)
        project_container_key = ProjectKey(
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ContainerClass(container=project_container_key.as_urn()),
        ).as_workunit()

        # Ownership (if enabled and available)
        if self.config.include_ownership and dataset.get("owner"):
            yield from self._emit_ownership(dataset_urn, dataset["owner"])

    def _emit_cube_schema(
        self, dataset_urn: str, cube: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Emit schema metadata for cube (attributes and metrics)."""
        try:
            schema_info = self.client.get_cube_schema(cube["id"])

            fields: List[SchemaFieldClass] = []

            # Add attributes
            for attribute in schema_info.get("attributes", []):
                fields.append(
                    SchemaFieldClass(
                        fieldPath=attribute.get("name", attribute.get("id")),
                        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                        nativeDataType="attribute",
                        description=attribute.get("description"),
                    )
                )

            # Add metrics
            for metric in schema_info.get("metrics", []):
                fields.append(
                    SchemaFieldClass(
                        fieldPath=metric.get("name", metric.get("id")),
                        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                        nativeDataType="metric",
                        description=metric.get("description"),
                    )
                )

            if fields:
                schema_metadata = SchemaMetadataClass(
                    schemaName=cube.get("name", cube["id"]),
                    platform=make_data_platform_urn(self.platform),
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    fields=fields,
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=schema_metadata,
                ).as_workunit()

        except Exception as e:
            logger.warning(f"Failed to get schema for cube {cube.get('name')}: {e}")

    def _emit_ownership(
        self, entity_urn: str, owner_info: Any
    ) -> Iterable[MetadataWorkUnit]:
        """Emit ownership aspect."""
        owners: List[OwnerClass] = []

        # Handle different owner formats
        if isinstance(owner_info, str):
            # Simple username/email string
            owner_urn = make_user_urn(owner_info)
            owners.append(
                OwnerClass(
                    owner=owner_urn,
                    type=OwnershipTypeClass.DATAOWNER,
                )
            )
        elif isinstance(owner_info, dict):
            # Structured owner object
            username = owner_info.get("username") or owner_info.get("email")
            if username:
                owner_urn = make_user_urn(username)
                owners.append(
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                )

        if owners:
            ownership = OwnershipClass(owners=owners)

            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=ownership,
            ).as_workunit()

    def _build_audit_stamps(
        self,
        created_time: Optional[str],
        modified_time: Optional[str],
        owner_info: Optional[Any],
    ) -> ChangeAuditStamps:
        """
        Build ChangeAuditStamps from timestamp strings and owner info.

        MicroStrategy API returns timestamps in ISO format (e.g., "2024-01-15T10:30:00Z")
        """
        actor_urn = "urn:li:corpuser:unknown"

        # Extract actor from owner info
        if owner_info:
            if isinstance(owner_info, str):
                actor_urn = make_user_urn(owner_info)
            elif isinstance(owner_info, dict):
                username = owner_info.get("username") or owner_info.get("email")
                if username:
                    actor_urn = make_user_urn(username)

        audit_stamps = ChangeAuditStamps()

        try:
            # Parse timestamps if available
            if created_time:
                created_dt = date_parser.parse(created_time)
                created_ts = int(
                    created_dt.timestamp() * 1000
                )  # Convert to milliseconds
                audit_stamps.created = AuditStampClass(time=created_ts, actor=actor_urn)

            if modified_time:
                modified_dt = date_parser.parse(modified_time)
                modified_ts = int(modified_dt.timestamp() * 1000)
                audit_stamps.lastModified = AuditStampClass(
                    time=modified_ts, actor=actor_urn
                )
        except Exception as e:
            logger.debug(f"Failed to parse timestamps: {e}")

        return audit_stamps

    def _build_dashboard_url(self, dashboard_id: str, project_id: str) -> Optional[str]:
        """
        Build external URL for dashboard.

        MicroStrategy dashboard URL format:
        {base_url}/app/{project_id}/{dashboard_id}
        """
        if not self.config.connection.base_url:
            return None

        base_url = self.config.connection.base_url.rstrip("/")
        # MicroStrategy Library app URL format
        return f"{base_url}/app/{project_id}/{dashboard_id}"

    def _build_report_url(self, report_id: str, project_id: str) -> Optional[str]:
        """
        Build external URL for report.

        MicroStrategy report URL format is similar to dashboards.
        """
        if not self.config.connection.base_url:
            return None

        base_url = self.config.connection.base_url.rstrip("/")
        return f"{base_url}/app/{project_id}/{report_id}"

    def _extract_visualization_ids(self, dashboard_def: Dict[str, Any]) -> List[str]:
        """
        Extract visualization IDs from dashboard definition.

        Dashboard structure:
        - chapters[] (pages)
          - visualizations[] (charts/widgets)

        Returns list of unique visualization IDs.
        """
        viz_ids: List[str] = []

        try:
            chapters = dashboard_def.get("chapters", [])
            for chapter in chapters:
                visualizations = chapter.get("visualizations", [])
                for viz in visualizations:
                    viz_id = viz.get("key") or viz.get("id")
                    if viz_id:
                        viz_ids.append(viz_id)

            logger.debug(f"Extracted {len(viz_ids)} visualization IDs from definition")
        except Exception as e:
            logger.warning(f"Failed to extract visualization IDs: {e}")

        return viz_ids

    def _get_report_inputs(
        self, report: Dict[str, Any], project: Dict[str, Any]
    ) -> List[str]:
        """
        Get upstream dataset URNs for report lineage.

        Reports can use:
        1. Intelligent Cubes (registered in cube_registry)
        2. Datasets (registered in dataset_registry)
        3. Direct database tables (not tracked in this implementation)

        Returns list of dataset URNs.
        """
        inputs: List[str] = []

        try:
            # Option 1: Get cube/dataset references from report metadata
            # MicroStrategy API may include "dataSource" or "sourceId" fields
            data_source_id = report.get("dataSource", {}).get("id") or report.get(
                "sourceId"
            )

            if data_source_id:
                # Check if it's a cube
                if data_source_id in self.cube_registry:
                    cube_info = self.cube_registry[data_source_id]
                    dataset_urn = make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=f"{cube_info['project_id']}.{data_source_id}",
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    )
                    inputs.append(dataset_urn)
                    logger.debug(f"Found cube lineage: {data_source_id}")

                # Check if it's a dataset
                elif data_source_id in self.dataset_registry:
                    dataset_info = self.dataset_registry[data_source_id]
                    dataset_urn = make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=f"{dataset_info['project_id']}.{data_source_id}",
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    )
                    inputs.append(dataset_urn)
                    logger.debug(f"Found dataset lineage: {data_source_id}")

            # Option 2: Try to get detailed report definition (may require additional API call)
            # This is best-effort - if API doesn't provide this, skip it
            # Future enhancement: parse report definition for data sources

        except Exception as e:
            logger.debug(f"Failed to extract report inputs: {e}")

        return inputs

    def get_report(self) -> SourceReport:
        return self.report

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to MicroStrategy API."""
        test_report = TestConnectionReport()
        test_report.capability_report = {}

        try:
            config = MicroStrategyConfig.parse_obj(config_dict)
            client = MicroStrategyClient(config.connection)

            # Test basic connectivity
            if client.test_connection():
                test_report.basic_connectivity = CapabilityReport(capable=True)
            else:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False, failure_reason="Failed to connect to API"
                )
                return test_report

            # Test metadata extraction capability
            try:
                with client:
                    projects = client.get_projects()
                    if projects:
                        test_report.capability_report[SourceCapability.CONTAINERS] = (
                            CapabilityReport(capable=True)
                        )
                        test_report.capability_report[SourceCapability.DESCRIPTIONS] = (
                            CapabilityReport(capable=True)
                        )
                    else:
                        test_report.capability_report[SourceCapability.CONTAINERS] = (
                            CapabilityReport(
                                capable=False,
                                failure_reason="No projects found. Check permissions.",
                            )
                        )
            except Exception as e:
                test_report.capability_report[SourceCapability.CONTAINERS] = (
                    CapabilityReport(
                        capable=False,
                        failure_reason=f"Failed to get projects: {e}",
                    )
                )

        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )

        return test_report
