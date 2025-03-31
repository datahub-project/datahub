import unittest
from datetime import datetime

from datahub.emitter.mce_builder import make_tag_urn, make_ts_millis, make_user_urn
from datahub.emitter.mcp import (
    MetadataChangeProposalWrapper,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.hex.constants import HEX_PLATFORM_NAME
from datahub.ingestion.source.hex.mapper import Mapper
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
from datahub.metadata.com.linkedin.pegasus2avro.dashboard import (
    DashboardUsageStatistics,
)
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    ContainerClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    GlobalTagsClass,
    MetadataChangeProposalClass,
    OwnershipClass,
    SubTypesClass,
    TimeWindowSizeClass,
)
from datahub.metadata.urns import DashboardUrn


class TestMapper(unittest.TestCase):
    workspace_name = "test-workspace"
    created_at = datetime(2022, 1, 1, 0, 0, 0)
    last_edited_at = datetime(2022, 1, 2, 0, 0, 0)
    last_modified = ChangeAuditStampsClass(
        created=AuditStampClass(
            time=make_ts_millis(datetime(2022, 1, 1)),
            actor="urn:li:corpuser:_ingestion",
        ),
        lastModified=AuditStampClass(
            time=make_ts_millis(datetime(2022, 1, 2)),
            actor="urn:li:corpuser:_ingestion",
        ),
    )

    def test_map_workspace(self):
        mapper = Mapper(
            workspace_name=self.workspace_name,
        )

        work_units = list(mapper.map_workspace())
        assert len(work_units) == 1
        assert isinstance(work_units[0], MetadataWorkUnit) and isinstance(
            work_units[0].metadata, MetadataChangeProposalWrapper
        )
        assert (
            work_units[0].metadata.entityUrn
            == "urn:li:container:635fbdd141a7b358624369c6060847c3"
        )
        aspect = work_units[0].get_aspect_of_type(ContainerPropertiesClass)
        assert aspect and aspect.name == self.workspace_name and not aspect.env

        mapper = Mapper(
            workspace_name=self.workspace_name,
            platform_instance="test-platform",
        )
        work_units = list(mapper.map_workspace())
        assert len(work_units) == 1
        assert isinstance(work_units[0], MetadataWorkUnit) and isinstance(
            work_units[0].metadata, MetadataChangeProposalWrapper
        )
        assert (
            work_units[0].metadata.entityUrn
            == "urn:li:container:635fbdd141a7b358624369c6060847c3"
        )
        aspect = work_units[0].get_aspect_of_type(ContainerPropertiesClass)
        assert aspect and aspect.name == self.workspace_name and not aspect.env

        mapper = Mapper(
            workspace_name=self.workspace_name,
            env="test-env",
            platform_instance="test-platform",
        )
        work_units = list(mapper.map_workspace())
        assert len(work_units) == 1
        assert isinstance(work_units[0], MetadataWorkUnit) and isinstance(
            work_units[0].metadata, MetadataChangeProposalWrapper
        )
        # guid here is the same as before because by default env is ignored in the key
        assert (
            work_units[0].metadata.entityUrn
            == "urn:li:container:635fbdd141a7b358624369c6060847c3"
        )
        aspect = work_units[0].get_aspect_of_type(ContainerPropertiesClass)
        assert (
            aspect and aspect.name == self.workspace_name and aspect.env == "test-env"
        )

    def test_map_project(self):
        mapper = Mapper(
            workspace_name=self.workspace_name,
            patch_metadata=False,
        )

        project = Project(
            id="uuid1",
            title="Test Project",
            description="A test project",
            created_at=self.created_at,
            last_edited_at=self.last_edited_at,
            status=Status(name="Published"),
            categories=[Category(name="Category1"), Category(name="Category2")],
            collections=[Collection(name="Collection1")],
            creator=Owner(email="creator@example.com"),
            owner=Owner(email="owner@example.com"),
            analytics=Analytics(
                appviews_all_time=100,
                appviews_last_7_days=10,
                appviews_last_14_days=20,
                appviews_last_30_days=30,
                last_viewed_at=datetime(2022, 1, 1, 0, 0, 0),
            ),
        )

        # check URNs

        work_units = list(mapper.map_project(project))
        assert len(work_units) == 8
        assert all(
            isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn == "urn:li:dashboard:(hex,uuid1)"
            for wu in work_units
        )

        # check DashboardInfoClass

        dashboard_info_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(DashboardInfoClass)
        ]
        assert len(dashboard_info_wus) == 1
        assert isinstance(
            dashboard_info_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(dashboard_info_wus[0].metadata.aspect, DashboardInfoClass)
        assert dashboard_info_wus[0].metadata.aspect.title == "Test Project"
        assert dashboard_info_wus[0].metadata.aspect.description == "A test project"
        assert (
            dashboard_info_wus[0].metadata.aspect.externalUrl
            == "https://app.hex.tech/test-workspace/hex/uuid1"
        )
        assert dashboard_info_wus[0].metadata.aspect.customProperties == {
            "id": "uuid1",
        }

        # check SubTypesClass

        subtypes_wus = [wu for wu in work_units if wu.get_aspect_of_type(SubTypesClass)]
        assert len(subtypes_wus) == 1
        assert isinstance(
            subtypes_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(subtypes_wus[0].metadata.aspect, SubTypesClass)
        assert subtypes_wus[0].metadata.aspect.typeNames == [
            BIAssetSubTypes.HEX_PROJECT
        ]

        # check DataPlatformInstanceClass

        platform_instance_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(DataPlatformInstanceClass)
        ]
        assert len(platform_instance_wus) == 1
        assert isinstance(
            platform_instance_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(
            platform_instance_wus[0].metadata.aspect, DataPlatformInstanceClass
        )
        assert (
            platform_instance_wus[0].metadata.aspect.platform
            == "urn:li:dataPlatform:hex"
        )
        assert platform_instance_wus[0].metadata.aspect.instance is None

        # check ContainerClass

        container_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(ContainerClass)
        ]
        assert len(container_wus) == 1
        assert isinstance(
            container_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(container_wus[0].metadata.aspect, ContainerClass)
        assert (
            container_wus[0].metadata.aspect.container
            == "urn:li:container:635fbdd141a7b358624369c6060847c3"
        )

        # check GlobalTagsClass

        tags_wus = [wu for wu in work_units if wu.get_aspect_of_type(GlobalTagsClass)]
        assert len(tags_wus) == 1
        assert isinstance(
            tags_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(tags_wus[0].metadata.aspect, GlobalTagsClass)
        assert len(tags_wus[0].metadata.aspect.tags) == 4
        tag_urns = {tag.tag for tag in tags_wus[0].metadata.aspect.tags}
        assert tag_urns == {
            "urn:li:tag:hex:status:Published",
            "urn:li:tag:hex:category:Category1",
            "urn:li:tag:hex:category:Category2",
            "urn:li:tag:hex:collection:Collection1",
        }

        # check OwnershipClass

        ownership_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(OwnershipClass)
        ]
        assert len(ownership_wus) == 1
        assert isinstance(
            ownership_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(ownership_wus[0].metadata.aspect, OwnershipClass)
        assert len(ownership_wus[0].metadata.aspect.owners) == 2
        owner_urns = {owner.owner for owner in ownership_wus[0].metadata.aspect.owners}
        assert owner_urns == {
            "urn:li:corpuser:creator@example.com",
            "urn:li:corpuser:owner@example.com",
        }
        assert all(
            [
                owner.type == OwnershipType.TECHNICAL_OWNER
                for owner in ownership_wus[0].metadata.aspect.owners
            ]
        )

        # check DashboardUsageStatistics

        dashboard_usage_statistics_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(DashboardUsageStatistics)
        ]
        assert len(dashboard_usage_statistics_wus) == 2
        usage_stats_all_time_wu = dashboard_usage_statistics_wus[0]
        usage_stats_last_7_days_wu = dashboard_usage_statistics_wus[1]
        assert (
            isinstance(usage_stats_all_time_wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(
                usage_stats_all_time_wu.metadata.aspect, DashboardUsageStatistics
            )
            and isinstance(
                usage_stats_last_7_days_wu.metadata, MetadataChangeProposalWrapper
            )
            and isinstance(
                usage_stats_last_7_days_wu.metadata.aspect, DashboardUsageStatistics
            )
        )
        assert (
            usage_stats_all_time_wu.metadata.aspect.viewsCount == 100
            and usage_stats_last_7_days_wu.metadata.aspect.viewsCount == 10
        )
        assert (
            not usage_stats_all_time_wu.metadata.aspect.eventGranularity
            and usage_stats_last_7_days_wu.metadata.aspect.eventGranularity
            == TimeWindowSizeClass(unit=CalendarIntervalClass.WEEK, multiple=1)
        )
        assert (
            usage_stats_all_time_wu.metadata.aspect.lastViewedAt
            == usage_stats_last_7_days_wu.metadata.aspect.lastViewedAt
            == make_ts_millis(datetime(2022, 1, 1))
        )

        # what if we set patch_metadata to True

        mapper = Mapper(
            workspace_name=self.workspace_name,
            patch_metadata=True,
        )

        # mostly the same

        work_units = list(mapper.map_project(project))
        assert len(work_units) == 8
        assert all(
            isinstance(
                wu.metadata,
                (MetadataChangeProposalWrapper, MetadataChangeProposalClass),
            )
            and wu.metadata.entityUrn == "urn:li:dashboard:(hex,uuid1)"
            for wu in work_units
        )

        # but DashboardInfo patch

        patche_wus = [
            wu
            for wu in work_units
            if isinstance(wu.metadata, MetadataChangeProposalClass)
            and wu.metadata.changeType == "PATCH"
        ]
        assert len(patche_wus) == 1
        assert isinstance(patche_wus[0].metadata, MetadataChangeProposalClass)
        assert patche_wus[0].metadata.aspectName == "dashboardInfo"

        # what if we set platform_instance

        mapper = Mapper(
            workspace_name=self.workspace_name,
            platform_instance="test-platform",
        )

        # mostly the same but additional instance DataPlatformInstanceClass

        work_units = list(mapper.map_project(project))
        assert len(work_units) == 8
        platform_instance_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(DataPlatformInstanceClass)
        ]
        assert len(platform_instance_wus) == 1
        assert isinstance(
            platform_instance_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(
            platform_instance_wus[0].metadata.aspect, DataPlatformInstanceClass
        )
        assert (
            platform_instance_wus[0].metadata.aspect.platform
            == "urn:li:dataPlatform:hex"
        )
        assert (
            platform_instance_wus[0].metadata.aspect.instance
            == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hex,test-platform)"
        )

    def test_map_component(self):
        mapper = Mapper(
            workspace_name=self.workspace_name,
            patch_metadata=False,
        )

        component = Component(
            id="uuid1",
            title="Test Component",
            description="A test component",
            created_at=self.created_at,
            last_edited_at=self.last_edited_at,
            status=Status(name="Draft"),
            categories=[Category(name="Category3")],
            collections=[Collection(name="Collection2")],
            creator=Owner(email="creator@example.com"),
            owner=Owner(email="owner@example.com"),
            analytics=Analytics(
                appviews_all_time=100,
                appviews_last_7_days=10,
                appviews_last_14_days=20,
                appviews_last_30_days=30,
                last_viewed_at=datetime(2022, 1, 1, 0, 0, 0),
            ),
        )

        # check URNs

        work_units = list(mapper.map_component(component))
        assert len(work_units) == 8
        assert all(
            isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn == "urn:li:dashboard:(hex,uuid1)"
            for wu in work_units
        )

        # check DashboardInfoClass

        dashboard_info_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(DashboardInfoClass)
        ]
        assert len(dashboard_info_wus) == 1
        assert isinstance(
            dashboard_info_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(dashboard_info_wus[0].metadata.aspect, DashboardInfoClass)
        assert dashboard_info_wus[0].metadata.aspect.title == "Test Component"
        assert dashboard_info_wus[0].metadata.aspect.description == "A test component"
        assert (
            dashboard_info_wus[0].metadata.aspect.externalUrl
            == "https://app.hex.tech/test-workspace/hex/uuid1"
        )
        assert dashboard_info_wus[0].metadata.aspect.customProperties == {"id": "uuid1"}

        # check SubTypesClass

        subtypes_wus = [wu for wu in work_units if wu.get_aspect_of_type(SubTypesClass)]
        assert len(subtypes_wus) == 1
        assert isinstance(
            subtypes_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(subtypes_wus[0].metadata.aspect, SubTypesClass)
        assert subtypes_wus[0].metadata.aspect.typeNames == [
            BIAssetSubTypes.HEX_COMPONENT
        ]

        # check DataPlatformInstanceClass

        platform_instance_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(DataPlatformInstanceClass)
        ]
        assert len(platform_instance_wus) == 1
        assert isinstance(
            platform_instance_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(
            platform_instance_wus[0].metadata.aspect, DataPlatformInstanceClass
        )
        assert (
            platform_instance_wus[0].metadata.aspect.platform
            == "urn:li:dataPlatform:hex"
        )
        assert platform_instance_wus[0].metadata.aspect.instance is None

        # check ContainerClass

        container_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(ContainerClass)
        ]
        assert len(container_wus) == 1
        assert isinstance(
            container_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(container_wus[0].metadata.aspect, ContainerClass)
        assert (
            container_wus[0].metadata.aspect.container
            == "urn:li:container:635fbdd141a7b358624369c6060847c3"
        )

        # check GlobalTagsClass

        tags_wus = [wu for wu in work_units if wu.get_aspect_of_type(GlobalTagsClass)]
        assert len(tags_wus) == 1
        assert isinstance(
            tags_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(tags_wus[0].metadata.aspect, GlobalTagsClass)
        assert len(tags_wus[0].metadata.aspect.tags) == 3
        tag_urns = {tag.tag for tag in tags_wus[0].metadata.aspect.tags}
        assert tag_urns == {
            "urn:li:tag:hex:status:Draft",
            "urn:li:tag:hex:category:Category3",
            "urn:li:tag:hex:collection:Collection2",
        }

        # check OwnershipClass

        ownership_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(OwnershipClass)
        ]
        assert len(ownership_wus) == 1
        assert isinstance(
            ownership_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(ownership_wus[0].metadata.aspect, OwnershipClass)
        assert len(ownership_wus[0].metadata.aspect.owners) == 2
        owner_urns = {owner.owner for owner in ownership_wus[0].metadata.aspect.owners}
        assert owner_urns == {
            "urn:li:corpuser:creator@example.com",
            "urn:li:corpuser:owner@example.com",
        }
        assert all(
            [
                owner.type == OwnershipType.TECHNICAL_OWNER
                for owner in ownership_wus[0].metadata.aspect.owners
            ]
        )

        # check DashboardUsageStatistics

        dashboard_usage_statistics_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(DashboardUsageStatistics)
        ]
        assert len(dashboard_usage_statistics_wus) == 2
        usage_stats_all_time_wu = dashboard_usage_statistics_wus[0]
        usage_stats_last_7_days_wu = dashboard_usage_statistics_wus[1]
        assert (
            isinstance(usage_stats_all_time_wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(
                usage_stats_all_time_wu.metadata.aspect, DashboardUsageStatistics
            )
            and isinstance(
                usage_stats_last_7_days_wu.metadata, MetadataChangeProposalWrapper
            )
            and isinstance(
                usage_stats_last_7_days_wu.metadata.aspect, DashboardUsageStatistics
            )
        )
        assert (
            usage_stats_all_time_wu.metadata.aspect.viewsCount == 100
            and usage_stats_last_7_days_wu.metadata.aspect.viewsCount == 10
        )
        assert (
            not usage_stats_all_time_wu.metadata.aspect.eventGranularity
            and usage_stats_last_7_days_wu.metadata.aspect.eventGranularity
            == TimeWindowSizeClass(unit=CalendarIntervalClass.WEEK, multiple=1)
        )
        assert (
            usage_stats_all_time_wu.metadata.aspect.lastViewedAt
            == usage_stats_last_7_days_wu.metadata.aspect.lastViewedAt
            == make_ts_millis(datetime(2022, 1, 1))
        )

        # what if we set patch_metadata to True

        mapper = Mapper(
            workspace_name=self.workspace_name,
            patch_metadata=True,
        )

        # mostly the same

        work_units = list(mapper.map_component(component))
        assert len(work_units) == 8
        assert all(
            isinstance(
                wu.metadata,
                (MetadataChangeProposalWrapper, MetadataChangeProposalClass),
            )
            and wu.metadata.entityUrn == "urn:li:dashboard:(hex,uuid1)"
            for wu in work_units
        )

        # but DashboardInfo patch

        patche_wus = [
            wu
            for wu in work_units
            if isinstance(
                wu.metadata,
                (MetadataChangeProposalWrapper, MetadataChangeProposalClass),
            )
            and wu.metadata.changeType == "PATCH"
        ]
        assert len(patche_wus) == 1
        assert isinstance(patche_wus[0].metadata, MetadataChangeProposalClass)
        assert patche_wus[0].metadata.aspectName == "dashboardInfo"

        # what if we set platform_instance

        mapper = Mapper(
            workspace_name=self.workspace_name,
            platform_instance="test-platform",
        )

        # mostly the same but additional DataPlatformInstanceClass

        work_units = list(mapper.map_component(component))
        assert len(work_units) == 8
        platform_instance_wus = [
            wu for wu in work_units if wu.get_aspect_of_type(DataPlatformInstanceClass)
        ]
        assert len(platform_instance_wus) == 1
        assert isinstance(
            platform_instance_wus[0].metadata, MetadataChangeProposalWrapper
        ) and isinstance(
            platform_instance_wus[0].metadata.aspect, DataPlatformInstanceClass
        )
        assert (
            platform_instance_wus[0].metadata.aspect.platform
            == "urn:li:dataPlatform:hex"
        )
        assert (
            platform_instance_wus[0].metadata.aspect.instance
            == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hex,test-platform)"
        )

    def test_global_tags_status(self):
        status = Status(name="Published")

        mapper = Mapper(
            workspace_name=self.workspace_name,
        )
        tags = mapper._global_tags(status, None, None)
        assert tags is not None
        assert len(tags.tags) == 1
        assert tags.tags[0].tag == make_tag_urn("hex:status:Published")

        mapper = Mapper(
            workspace_name=self.workspace_name,
            status_as_tag=False,
        )
        tags = mapper._global_tags(status, None, None)
        assert tags is None

    def test_global_tags_categories(self):
        categories = [Category(name="Category1"), Category(name="Category2")]

        mapper = Mapper(
            workspace_name=self.workspace_name,
        )
        tags = mapper._global_tags(None, categories, None)
        assert tags is not None
        assert len(tags.tags) == 2
        assert tags.tags[0].tag == make_tag_urn("hex:category:Category1")
        assert tags.tags[1].tag == make_tag_urn("hex:category:Category2")

        mapper = Mapper(
            workspace_name=self.workspace_name,
            categories_as_tags=False,
        )
        tags = mapper._global_tags(None, categories, None)
        assert tags is None

    def test_global_tags_collections(self):
        collections = [Collection(name="Collection1")]

        mapper = Mapper(
            workspace_name=self.workspace_name,
        )
        tags = mapper._global_tags(None, None, collections)
        assert tags is not None
        assert len(tags.tags) == 1
        assert tags.tags[0].tag == make_tag_urn("hex:collection:Collection1")

        mapper = Mapper(
            workspace_name=self.workspace_name,
            collections_as_tags=False,
        )
        tags = mapper._global_tags(None, None, collections)
        assert tags is None

    def test_global_tags_all(self):
        status = Status(name="Published")
        categories = [Category(name="Category1"), Category(name="Category2")]
        collections = [Collection(name="Collection1")]

        mapper = Mapper(
            workspace_name=self.workspace_name,
        )
        tags = mapper._global_tags(status, categories, collections)
        assert tags is not None
        assert len(tags.tags) == 4

    def test_ownership(self):
        mapper = Mapper(
            workspace_name=self.workspace_name,
        )

        creator = Owner(email="creator@example.com")
        owner = Owner(email="owner@example.com")

        ownership = mapper._ownership(creator, owner)
        assert ownership is not None
        assert len(ownership.owners) == 2

        creator_owner = next(
            (
                o
                for o in ownership.owners
                if o.owner == make_user_urn("creator@example.com")
            ),
            None,
        )
        assert creator_owner is not None
        assert creator_owner.type == OwnershipType.TECHNICAL_OWNER

        primary_owner = next(
            (
                o
                for o in ownership.owners
                if o.owner == make_user_urn("owner@example.com")
            ),
            None,
        )
        assert primary_owner is not None
        assert primary_owner.type == OwnershipType.TECHNICAL_OWNER

        ownership = mapper._ownership(creator, creator)
        assert ownership is not None
        assert len(ownership.owners) == 1

        ownership = mapper._ownership(None, None)
        assert ownership is None

        mapper = Mapper(
            workspace_name=self.workspace_name,
            set_ownership_from_email=False,
        )
        ownership = mapper._ownership(creator, owner)
        assert ownership is None

    def test_dashboard_usage_statistics(self):
        mapper = Mapper(
            workspace_name=self.workspace_name,
        )

        analytics = Analytics(
            appviews_all_time=100,
            appviews_last_7_days=10,
            appviews_last_14_days=20,
            appviews_last_30_days=30,
            last_viewed_at=datetime(2022, 1, 1, 0, 0, 0),
        )

        usage_stats_all_time, usage_stats_last_7_days = (
            mapper._dashboard_usage_statistics(analytics)
        )
        assert usage_stats_all_time and usage_stats_last_7_days
        assert (
            usage_stats_all_time.viewsCount == 100
            and usage_stats_last_7_days.viewsCount == 10
        )
        assert (
            not usage_stats_all_time.eventGranularity
            and usage_stats_last_7_days.eventGranularity
            == TimeWindowSizeClass(unit=CalendarIntervalClass.WEEK, multiple=1)
        )
        assert (
            usage_stats_all_time.lastViewedAt
            == usage_stats_last_7_days.lastViewedAt
            == make_ts_millis(datetime(2022, 1, 1))
        )

        analytics = Analytics(
            appviews_all_time=None,
            appviews_last_7_days=None,
            appviews_last_14_days=None,
            appviews_last_30_days=None,
            last_viewed_at=None,
        )

        usage_stats_all_time, usage_stats_last_7_days = (
            mapper._dashboard_usage_statistics(analytics)
        )
        assert not usage_stats_all_time and not usage_stats_last_7_days

        analytics = Analytics(
            appviews_all_time=None,
            appviews_last_7_days=10,
            appviews_last_14_days=None,
            appviews_last_30_days=None,
            last_viewed_at=None,
        )

        usage_stats_all_time, usage_stats_last_7_days = (
            mapper._dashboard_usage_statistics(analytics)
        )
        assert not usage_stats_all_time and usage_stats_last_7_days
        assert usage_stats_last_7_days.viewsCount == 10
        assert usage_stats_last_7_days.eventGranularity == TimeWindowSizeClass(
            unit=CalendarIntervalClass.WEEK, multiple=1
        )
        assert usage_stats_last_7_days.lastViewedAt is None

    def test_platform_instance_aspect(self):
        mapper = Mapper(
            workspace_name=self.workspace_name,
        )
        platform_instance = mapper._platform_instance_aspect()
        assert platform_instance
        assert platform_instance.platform == "urn:li:dataPlatform:hex"
        assert platform_instance.instance is None

        mapper = Mapper(
            workspace_name=self.workspace_name,
            platform_instance="test-platform",
        )
        platform_instance = mapper._platform_instance_aspect()
        assert platform_instance
        assert platform_instance.platform == "urn:li:dataPlatform:hex"
        assert (
            platform_instance.instance
            == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hex,test-platform)"
        )

    def test_get_dashboard_urn(self):
        mapper = Mapper(
            workspace_name=self.workspace_name,
        )
        dashboard_urn = mapper._get_dashboard_urn("dashboard_name")
        assert dashboard_urn == DashboardUrn(
            dashboard_id="dashboard_name", dashboard_tool=HEX_PLATFORM_NAME
        )
        assert dashboard_urn.urn() == "urn:li:dashboard:(hex,dashboard_name)"

        mapper = Mapper(
            workspace_name=self.workspace_name,
            platform_instance="test-platform",
        )
        dashboard_urn = mapper._get_dashboard_urn("dashboard_name")
        assert dashboard_urn == DashboardUrn(
            dashboard_id="test-platform.dashboard_name",
            dashboard_tool=HEX_PLATFORM_NAME,
        )
        assert (
            dashboard_urn.urn() == "urn:li:dashboard:(hex,test-platform.dashboard_name)"
        )
