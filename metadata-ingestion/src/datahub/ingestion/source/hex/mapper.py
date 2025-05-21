import logging
from datetime import datetime
from typing import Iterable, List, Optional, Tuple, Union

from datahub._codegen.aspect import (
    _Aspect,  # TODO: is there a better import than this one?
)
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
from datahub.ingestion.source.hex.constants import (
    HEX_API_BASE_URL_DEFAULT,
    HEX_PLATFORM_NAME,
)
from datahub.ingestion.source.hex.model import (
    Analytics,
    Category,
    Collection,
    Component,
    Owner,
    Project,
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
    OwnerClass,
    OwnershipClass,
    SubTypesClass,
    TagAssociationClass,
    TimeWindowSizeClass,
)
from datahub.metadata.urns import (
    ContainerUrn,
    CorpUserUrn,
    DashboardUrn,
    DatasetUrn,
    SchemaFieldUrn,
    Urn,
)

logger = logging.getLogger(__name__)


class WorkspaceKey(ContainerKey):
    workspace_name: str


DEFAULT_INGESTION_USER_URN = CorpUserUrn("_ingestion")
DEFAULT_OWNERSHIP_TYPE = OwnershipType.TECHNICAL_OWNER


class Mapper:
    def __init__(
        self,
        workspace_name: str,
        platform_instance: Optional[str] = None,
        env: Optional[str] = None,
        base_url: str = HEX_API_BASE_URL_DEFAULT,
        patch_metadata: bool = True,
        collections_as_tags: bool = True,
        status_as_tag: bool = True,
        categories_as_tags: bool = True,
        set_ownership_from_email: bool = True,
    ):
        self._workspace_name = workspace_name
        self._env = env
        self._platform_instance = platform_instance
        self._workspace_urn = Mapper._get_workspace_urn(
            workspace_name=workspace_name,
            platform=HEX_PLATFORM_NAME,
            env=env,
            platform_instance=platform_instance,
        )
        self._base_url = base_url.strip("/").replace("/api/v1", "")
        self._patch_metadata = patch_metadata
        self._collections_as_tags = collections_as_tags
        self._status_as_tag = status_as_tag
        self._categories_as_tags = categories_as_tags
        self._set_ownership_from_email = set_ownership_from_email

    def map_workspace(self) -> Iterable[MetadataWorkUnit]:
        container_properties = ContainerPropertiesClass(
            name=self._workspace_name,
            env=self._env,
        )
        yield from self._yield_mcps(
            entity_urn=self._workspace_urn,
            aspects=[container_properties],
        )

    def map_project(self, project: Project) -> Iterable[MetadataWorkUnit]:
        dashboard_urn = self._get_dashboard_urn(name=project.id)

        dashboard_info = DashboardInfoClass(
            title=project.title,
            description=project.description or "",
            lastModified=self._change_audit_stamps(
                created_at=project.created_at, last_edited_at=project.last_edited_at
            ),
            externalUrl=f"{self._base_url}/{self._workspace_name}/hex/{project.id}",
            customProperties=dict(id=project.id),
            datasetEdges=self._dataset_edges(project.upstream_datasets),
            # TODO: support schema field upstream, maybe InputFields?
        )

        subtypes = SubTypesClass(
            typeNames=[BIAssetSubTypes.HEX_PROJECT],
        )

        platform_instance = self._platform_instance_aspect()

        container = ContainerClass(
            container=self._workspace_urn.urn(),
        )

        tags = self._global_tags(
            status=project.status,
            categories=project.categories,
            collections=project.collections,
        )

        ownership = self._ownership(creator=project.creator, owner=project.owner)

        usage_stats_all_time, usage_stats_last_7_days = (
            self._dashboard_usage_statistics(analytics=project.analytics)
        )

        yield from self._yield_mcps(
            entity_urn=dashboard_urn,
            aspects=[
                dashboard_info,
                subtypes,
                platform_instance,
                container,
                tags,
                ownership,
                usage_stats_all_time,
                usage_stats_last_7_days,
            ],
        )

    def map_component(self, component: Component) -> Iterable[MetadataWorkUnit]:
        dashboard_urn = self._get_dashboard_urn(name=component.id)

        dashboard_info = DashboardInfoClass(
            title=component.title,
            description=component.description or "",
            lastModified=self._change_audit_stamps(
                created_at=component.created_at, last_edited_at=component.last_edited_at
            ),
            externalUrl=f"{self._base_url}/{self._workspace_name}/hex/{component.id}",
            customProperties=dict(id=component.id),
        )

        subtypes = SubTypesClass(
            typeNames=[BIAssetSubTypes.HEX_COMPONENT],
        )

        platform_instance = self._platform_instance_aspect()

        container = ContainerClass(
            container=self._workspace_urn.urn(),
        )

        tags = self._global_tags(
            status=component.status,
            categories=component.categories,
            collections=component.collections,
        )

        ownership = self._ownership(creator=component.creator, owner=component.owner)

        usage_stats_all_time, usage_stats_last_7_days = (
            self._dashboard_usage_statistics(analytics=component.analytics)
        )

        yield from self._yield_mcps(
            entity_urn=dashboard_urn,
            aspects=[
                dashboard_info,
                subtypes,
                platform_instance,
                container,
                tags,
                ownership,
                usage_stats_all_time,
                usage_stats_last_7_days,
            ],
        )

    @classmethod
    def _get_workspace_urn(
        cls,
        workspace_name: str,
        platform: str = HEX_PLATFORM_NAME,
        env: Optional[str] = None,
        platform_instance: Optional[str] = None,
    ) -> ContainerUrn:
        workspace_key = WorkspaceKey(
            platform=platform,
            env=env,
            platform_instance=platform_instance,
            workspace_name=workspace_name,
        )
        container_urn_str = make_container_urn(guid=workspace_key)
        container_urn = Urn.from_string(container_urn_str)
        assert isinstance(container_urn, ContainerUrn)
        return container_urn

    def _get_dashboard_urn(self, name: str) -> DashboardUrn:
        dashboard_urn_str = make_dashboard_urn(
            platform=HEX_PLATFORM_NAME,
            name=name,
            platform_instance=self._platform_instance,
        )
        dashboard_urn = Urn.from_string(dashboard_urn_str)
        assert isinstance(dashboard_urn, DashboardUrn)
        return dashboard_urn

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
        tag_associations: List[TagAssociationClass] = []
        if status and self._status_as_tag:
            tag_associations.append(
                TagAssociationClass(tag=make_tag_urn(tag=f"hex:status:{status.name}"))
            )
        if categories and self._categories_as_tags:
            tag_associations.extend(
                [
                    TagAssociationClass(
                        tag=make_tag_urn(tag=f"hex:category:{cat.name}")
                    )
                    for cat in categories
                ]
            )
        if collections and self._collections_as_tags:
            tag_associations.extend(
                [
                    TagAssociationClass(
                        tag=make_tag_urn(tag=f"hex:collection:{col.name}")
                    )
                    for col in collections
                ]
            )

        return GlobalTagsClass(tags=tag_associations) if tag_associations else None

    def _ownership(
        self, creator: Optional[Owner], owner: Optional[Owner]
    ) -> Optional[OwnershipClass]:
        if self._set_ownership_from_email:
            # since we are not making any diff of creator/owner, we usually have duplicates
            # TODO: set or ownership types to properly differentiate them, maybe by config?
            unique_owners = set(o for o in [creator, owner] if o)
            owners: List[OwnerClass] = [
                OwnerClass(owner=make_user_urn(o.email), type=DEFAULT_OWNERSHIP_TYPE)
                for o in unique_owners
            ]

            return OwnershipClass(owners=owners) if owners else None
        return None

    def _dashboard_usage_statistics(
        self, analytics: Optional[Analytics]
    ) -> Tuple[
        Optional[DashboardUsageStatisticsClass], Optional[DashboardUsageStatisticsClass]
    ]:
        tm_millis = make_ts_millis(datetime.now())
        last_viewed_at = (
            make_ts_millis(analytics.last_viewed_at)
            if analytics and analytics.last_viewed_at
            else None
        )

        usage_all_time: Optional[DashboardUsageStatisticsClass] = (
            DashboardUsageStatisticsClass(
                timestampMillis=tm_millis,
                viewsCount=analytics.appviews_all_time,
                lastViewedAt=last_viewed_at,
            )
            if analytics and analytics.appviews_all_time
            else None
        )

        usage_last_7_days: Optional[DashboardUsageStatisticsClass] = (
            DashboardUsageStatisticsClass(
                timestampMillis=tm_millis,
                viewsCount=analytics.appviews_last_7_days,
                eventGranularity=TimeWindowSizeClass(
                    unit=CalendarIntervalClass.WEEK, multiple=1
                ),
                lastViewedAt=last_viewed_at,
            )
            if analytics and analytics.appviews_last_7_days
            else None
        )
        return (usage_all_time, usage_last_7_days)

    def _platform_instance_aspect(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=make_data_platform_urn(HEX_PLATFORM_NAME),
            instance=make_dataplatform_instance_urn(
                platform=HEX_PLATFORM_NAME, instance=self._platform_instance
            )
            if self._platform_instance
            else None,
        )

    def _dataset_edges(
        self, upstream: List[Union[DatasetUrn, SchemaFieldUrn]]
    ) -> Optional[List[EdgeClass]]:
        # TBC: is there support for CLL in Dashboards? for the moment, skip SchemaFieldUrns
        return (
            [
                EdgeClass(
                    destinationUrn=upstream_urn.urn(),
                )
                for upstream_urn in upstream
                if isinstance(upstream_urn, DatasetUrn)
            ]
            if upstream
            else None
        )

    def _yield_mcps(
        self, entity_urn: Urn, aspects: List[Optional[_Aspect]]
    ) -> Iterable[MetadataWorkUnit]:
        for mcpw in MetadataChangeProposalWrapper.construct_many(
            entityUrn=entity_urn.urn(),
            aspects=aspects,
        ):
            wu = MetadataWorkUnit.from_metadata(metadata=mcpw)
            maybe_wu = self._maybe_patch_wu(wu)
            if maybe_wu:
                yield maybe_wu

    def _maybe_patch_wu(self, wu: MetadataWorkUnit) -> Optional[MetadataWorkUnit]:
        # So far we only have support for DashboardInfo aspect

        dashboard_info_aspect: Optional[DashboardInfoClass] = wu.get_aspect_of_type(
            DashboardInfoClass
        )

        if dashboard_info_aspect and self._patch_metadata:
            return convert_dashboard_info_to_patch(
                wu.get_urn(),
                dashboard_info_aspect,
                wu.metadata.systemMetadata,
            )
        else:
            return wu
