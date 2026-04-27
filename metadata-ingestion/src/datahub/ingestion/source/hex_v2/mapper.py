import logging
from datetime import datetime
from typing import Iterable, List, Optional, Tuple, Union

from datahub._codegen.aspect import _Aspect
from datahub.emitter.mce_builder import (
    make_container_urn,
    make_dashboard_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_tag_urn,
    make_ts_millis,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_dashboard_info_to_patch,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.hex_v2.constants import (
    HEX_BASE_URL_DEFAULT,
    HEX_PLATFORM_NAME,
)
from datahub.ingestion.source.hex_v2.model import (
    Analytics,
    Category,
    Collection,
    Component,
    Owner,
    Project,
    RunRecord,
    Status,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStampClass,
    ChangeAuditStampsClass,
    OwnershipType,
)
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    ContainerClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DashboardUsageStatisticsClass,
    DataPlatformInstanceClass,
    EdgeClass,
    GlobalTagsClass,
    OperationClass,
    OperationTypeClass,
    OwnerClass,
    OwnershipClass,
    SubTypesClass,
    TagAssociationClass,
    TimeWindowSizeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import (
    ContainerUrn,
    CorpUserUrn,
    DashboardUrn,
    DatasetUrn,
    Urn,
)

logger = logging.getLogger(__name__)

DEFAULT_INGESTION_USER_URN = CorpUserUrn("_ingestion")
DEFAULT_OWNERSHIP_TYPE = OwnershipType.TECHNICAL_OWNER


class WorkspaceKey(ContainerKey):
    workspace_name: str


class HexV2Mapper:
    def __init__(
        self,
        workspace_name: str,
        base_url: str = HEX_BASE_URL_DEFAULT,
        platform_instance: Optional[str] = None,
        env: Optional[str] = None,
        patch_metadata: bool = False,
        collections_as_tags: bool = True,
        status_as_tag: bool = True,
        categories_as_tags: bool = True,
        set_ownership_from_email: bool = True,
    ):
        self._workspace_name = workspace_name
        self._env = env
        self._platform_instance = platform_instance
        self._base_url = base_url.rstrip("/").replace("/api/v1", "")
        self._patch_metadata = patch_metadata
        self._collections_as_tags = collections_as_tags
        self._status_as_tag = status_as_tag
        self._categories_as_tags = categories_as_tags
        self._set_ownership_from_email = set_ownership_from_email
        self._workspace_urn = self._make_workspace_urn(workspace_name)

    def _make_workspace_urn(self, workspace_name: str) -> ContainerUrn:
        key = WorkspaceKey(
            platform=HEX_PLATFORM_NAME,
            env=self._env,
            platform_instance=self._platform_instance,
            workspace_name=workspace_name,
        )
        urn = Urn.from_string(make_container_urn(guid=key))
        assert isinstance(urn, ContainerUrn)
        return urn

    def _dashboard_urn(self, entity_id: str) -> DashboardUrn:
        urn = Urn.from_string(
            make_dashboard_urn(
                platform=HEX_PLATFORM_NAME,
                name=entity_id,
                platform_instance=self._platform_instance,
            )
        )
        assert isinstance(urn, DashboardUrn)
        return urn

    def _external_url(self, entity: Union[Project, Component]) -> Optional[str]:
        if entity.last_published_at is None:
            return f"{self._base_url}/{self._workspace_name}/hex/{entity.id}"
        return f"{self._base_url}/{self._workspace_name}/app/{entity.id}"

    # ------------------------------------------------------------------
    # Public mapping methods
    # ------------------------------------------------------------------

    def map_workspace(self) -> Iterable[MetadataWorkUnit]:
        yield from self._yield_mcps(
            entity_urn=self._workspace_urn,
            aspects=[
                ContainerPropertiesClass(
                    name=self._workspace_name,
                    env=self._env,
                )
            ],
        )

    def map_project(self, project: Project) -> Iterable[MetadataWorkUnit]:
        urn = self._dashboard_urn(project.id)

        dashboard_info = DashboardInfoClass(
            title=project.title,
            description=project.description or "",
            lastModified=self._change_audit_stamps(
                project.created_at, project.last_edited_at
            ),
            externalUrl=self._external_url(project),
            customProperties={"id": project.id},
            datasetEdges=self._dataset_edges(project.upstream_datasets),
        )

        aspects: List[Optional[_Aspect]] = [
            dashboard_info,
            SubTypesClass(typeNames=[BIAssetSubTypes.HEX_PROJECT]),
            self._platform_instance_aspect(),
            ContainerClass(container=self._workspace_urn.urn()),
            self._global_tags(project.status, project.categories, project.collections),
            self._ownership(project.creator, project.owner),
        ]

        usage_all, usage_7d = self._usage_stats(project.analytics)
        aspects += [usage_all, usage_7d]

        yield from self._yield_mcps(entity_urn=urn, aspects=aspects)

        # Upstream lineage as a separate UpstreamLineage aspect
        if project.upstream_datasets:
            yield from self._yield_upstream_lineage(urn, project.upstream_datasets)

        # Latest run as an Operation aspect
        if project.latest_run:
            yield from self._yield_run_operation(urn, project.latest_run)

    def map_component(self, component: Component) -> Iterable[MetadataWorkUnit]:
        urn = self._dashboard_urn(component.id)

        dashboard_info = DashboardInfoClass(
            title=component.title,
            description=component.description or "",
            lastModified=self._change_audit_stamps(
                component.created_at, component.last_edited_at
            ),
            externalUrl=self._external_url(component),
            customProperties={"id": component.id},
        )

        aspects: List[Optional[_Aspect]] = [
            dashboard_info,
            SubTypesClass(typeNames=[BIAssetSubTypes.HEX_COMPONENT]),
            self._platform_instance_aspect(),
            ContainerClass(container=self._workspace_urn.urn()),
            self._global_tags(
                component.status, component.categories, component.collections
            ),
            self._ownership(component.creator, component.owner),
        ]

        usage_all, usage_7d = self._usage_stats(component.analytics)
        aspects += [usage_all, usage_7d]

        yield from self._yield_mcps(entity_urn=urn, aspects=aspects)

    # ------------------------------------------------------------------
    # Aspect builders
    # ------------------------------------------------------------------

    def _change_audit_stamps(
        self, created_at: Optional[datetime], last_edited_at: Optional[datetime]
    ) -> ChangeAuditStampsClass:
        return ChangeAuditStampsClass(
            created=AuditStampClass(
                time=make_ts_millis(created_at),
                actor=DEFAULT_INGESTION_USER_URN.urn(),
            )
            if created_at
            else None,
            lastModified=AuditStampClass(
                time=make_ts_millis(last_edited_at),
                actor=DEFAULT_INGESTION_USER_URN.urn(),
            )
            if last_edited_at
            else None,
        )

    def _global_tags(
        self,
        status: Optional[Status],
        categories: Optional[List[Category]],
        collections: Optional[List[Collection]],
    ) -> Optional[GlobalTagsClass]:
        tags: List[TagAssociationClass] = []
        if status and self._status_as_tag:
            tags.append(
                TagAssociationClass(tag=make_tag_urn(f"hex:status:{status.name}"))
            )
        if categories and self._categories_as_tags:
            tags += [
                TagAssociationClass(tag=make_tag_urn(f"hex:category:{c.name}"))
                for c in categories
            ]
        if collections and self._collections_as_tags:
            tags += [
                TagAssociationClass(tag=make_tag_urn(f"hex:collection:{c.name}"))
                for c in collections
            ]
        return GlobalTagsClass(tags=tags) if tags else None

    def _ownership(
        self, creator: Optional[Owner], owner: Optional[Owner]
    ) -> Optional[OwnershipClass]:
        if not self._set_ownership_from_email:
            return None
        unique = set(o for o in [creator, owner] if o)
        owners = [
            OwnerClass(owner=make_user_urn(o.email), type=DEFAULT_OWNERSHIP_TYPE)
            for o in unique
        ]
        return OwnershipClass(owners=owners) if owners else None

    def _usage_stats(
        self, analytics: Optional[Analytics]
    ) -> Tuple[
        Optional[DashboardUsageStatisticsClass], Optional[DashboardUsageStatisticsClass]
    ]:
        now_ms = make_ts_millis(datetime.now())
        last_viewed = (
            make_ts_millis(analytics.last_viewed_at)
            if analytics and analytics.last_viewed_at
            else None
        )

        all_time = (
            DashboardUsageStatisticsClass(
                timestampMillis=now_ms,
                viewsCount=analytics.appviews_all_time,
                lastViewedAt=last_viewed,
            )
            if analytics and analytics.appviews_all_time
            else None
        )

        last_7d = (
            DashboardUsageStatisticsClass(
                timestampMillis=now_ms,
                viewsCount=analytics.appviews_last_7_days,
                eventGranularity=TimeWindowSizeClass(
                    unit=CalendarIntervalClass.WEEK, multiple=1
                ),
                lastViewedAt=last_viewed,
            )
            if analytics and analytics.appviews_last_7_days
            else None
        )

        return all_time, last_7d

    def _platform_instance_aspect(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=make_data_platform_urn(HEX_PLATFORM_NAME),
            instance=make_dataplatform_instance_urn(
                HEX_PLATFORM_NAME, self._platform_instance
            )
            if self._platform_instance
            else None,
        )

    def _dataset_edges(self, upstream_urns: List[str]) -> Optional[List[EdgeClass]]:
        if not upstream_urns:
            return None
        edges = []
        for urn_str in upstream_urns:
            try:
                DatasetUrn.from_string(urn_str)
                edges.append(EdgeClass(destinationUrn=urn_str))
            except Exception:
                logger.debug("Skipping non-dataset upstream URN: %s", urn_str)
        return edges or None

    def _yield_upstream_lineage(
        self, dashboard_urn: DashboardUrn, upstream_urns: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        upstreams = []
        for urn_str in upstream_urns:
            try:
                DatasetUrn.from_string(urn_str)
                upstreams.append(
                    UpstreamClass(
                        dataset=urn_str,
                        type="TRANSFORMED",
                    )
                )
            except Exception:
                pass
        if upstreams:
            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn.urn(),
                aspect=UpstreamLineageClass(upstreams=upstreams),
            ).as_workunit()

    def _yield_run_operation(
        self, dashboard_urn: DashboardUrn, run: RunRecord
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn.urn(),
            aspect=OperationClass(
                timestampMillis=make_ts_millis(run.start_time),
                lastUpdatedTimestamp=make_ts_millis(run.start_time),
                operationType=OperationTypeClass.UPDATE,
                actor=DEFAULT_INGESTION_USER_URN.urn(),
                customProperties={
                    "run_id": run.run_id,
                    "status": run.status,
                    "elapsed_seconds": str(run.elapsed_seconds or ""),
                },
            ),
        ).as_workunit()

    # ------------------------------------------------------------------
    # MCP emission
    # ------------------------------------------------------------------

    def _yield_mcps(
        self, entity_urn: Urn, aspects: List[Optional[_Aspect]]
    ) -> Iterable[MetadataWorkUnit]:
        for mcpw in MetadataChangeProposalWrapper.construct_many(
            entityUrn=entity_urn.urn(),
            aspects=aspects,
        ):
            wu = MetadataWorkUnit.from_metadata(metadata=mcpw)
            maybe_wu = self._maybe_patch(wu)
            if maybe_wu:
                yield maybe_wu

    def _maybe_patch(self, wu: MetadataWorkUnit) -> Optional[MetadataWorkUnit]:
        info = wu.get_aspect_of_type(DashboardInfoClass)
        if info and self._patch_metadata:
            return convert_dashboard_info_to_patch(
                wu.get_urn(), info, wu.metadata.systemMetadata
            )
        return wu
