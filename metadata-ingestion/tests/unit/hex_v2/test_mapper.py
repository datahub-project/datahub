from datetime import datetime, timezone
from typing import List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hex_v2.mapper import HexV2Mapper
from datahub.ingestion.source.hex_v2.model import (
    Analytics,
    Category,
    Component,
    Owner,
    Project,
    RunRecord,
    Status,
)
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DashboardInfoClass,
    DashboardUsageStatisticsClass,
    GlobalTagsClass,
    OperationClass,
    OwnershipClass,
    SubTypesClass,
    UpstreamLineageClass,
)


def _collect_aspects(wus: List[MetadataWorkUnit]) -> dict:
    """Return {aspect_type: aspect_instance} for easy assertions."""
    result = {}
    for wu in wus:
        aspect = wu.get_aspect_of_type(object)  # type: ignore[arg-type]
        if aspect:
            result[type(aspect).__name__] = aspect
    return result


def _mapper(**kwargs: object) -> HexV2Mapper:
    defaults = dict(
        workspace_name="test-workspace",
        base_url="https://app.hex.tech",
        platform_instance=None,
        env="PROD",
    )
    defaults.update(kwargs)  # type: ignore[arg-type]
    return HexV2Mapper(**defaults)  # type: ignore[arg-type]


def _project(**kwargs: object) -> Project:
    defaults = dict(
        id="proj-1",
        title="My Project",
        description="A test project",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        last_edited_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        last_published_at=None,
    )
    defaults.update(kwargs)  # type: ignore[arg-type]
    return Project(**defaults)  # type: ignore[arg-type]


class TestMapWorkspace:
    def test_emits_container_properties(self) -> None:
        mapper = _mapper()
        wus = list(mapper.map_workspace())

        assert any(wu.get_aspect_of_type(ContainerPropertiesClass) for wu in wus)

    def test_workspace_urn_is_stable(self) -> None:
        mapper = _mapper()
        urns = {wu.get_urn() for wu in mapper.map_workspace()}

        assert len(urns) == 1


class TestMapProject:
    def test_emits_required_aspects(self) -> None:
        mapper = _mapper()
        project = _project()
        wus = list(mapper.map_project(project))
        aspect_types = {type(wu.get_aspect_of_type(object)).__name__ for wu in wus}  # type: ignore[arg-type]

        assert "DashboardInfoClass" in aspect_types
        assert "SubTypesClass" in aspect_types
        assert "DataPlatformInstanceClass" in aspect_types
        assert "ContainerClass" in aspect_types

    def test_status_emitted_as_tag(self) -> None:
        mapper = _mapper(status_as_tag=True)
        project = _project(status=Status(name="In Progress"))
        wus = list(mapper.map_project(project))

        tags_wus = [wu for wu in wus if wu.get_aspect_of_type(GlobalTagsClass)]
        assert tags_wus
        tags = tags_wus[0].get_aspect_of_type(GlobalTagsClass)
        assert tags is not None
        tag_names = [t.tag for t in tags.tags]
        assert any("In Progress" in t for t in tag_names)

    def test_status_tag_disabled(self) -> None:
        mapper = _mapper(status_as_tag=False)
        project = _project(status=Status(name="In Progress"))
        wus = list(mapper.map_project(project))

        tags_wus = [wu for wu in wus if wu.get_aspect_of_type(GlobalTagsClass)]
        # No status tag when disabled
        for wu in tags_wus:
            tags = wu.get_aspect_of_type(GlobalTagsClass)
            if tags:
                assert not any("In Progress" in t.tag for t in tags.tags)

    def test_categories_emitted_as_tags(self) -> None:
        mapper = _mapper(categories_as_tags=True)
        project = _project(
            categories=[Category(name="Internal"), Category(name="Analytics")]
        )
        wus = list(mapper.map_project(project))

        all_tags = []
        for wu in wus:
            t = wu.get_aspect_of_type(GlobalTagsClass)
            if t:
                all_tags.extend(t.tags)
        tag_names = [t.tag for t in all_tags]
        assert any("Internal" in t for t in tag_names)
        assert any("Analytics" in t for t in tag_names)

    def test_ownership_from_email(self) -> None:
        mapper = _mapper(set_ownership_from_email=True)
        project = _project(
            owner=Owner(email="owner@example.com"),
            creator=Owner(email="creator@example.com"),
        )
        wus = list(mapper.map_project(project))

        ownership_wus = [wu for wu in wus if wu.get_aspect_of_type(OwnershipClass)]
        assert ownership_wus
        ownership = ownership_wus[0].get_aspect_of_type(OwnershipClass)
        assert ownership is not None
        emails = [o.owner for o in ownership.owners]
        assert any("owner@example.com" in e for e in emails)

    def test_deduplicated_ownership_when_creator_equals_owner(self) -> None:
        mapper = _mapper(set_ownership_from_email=True)
        same = Owner(email="same@example.com")
        project = _project(owner=same, creator=same)
        wus = list(mapper.map_project(project))

        ownership_wus = [wu for wu in wus if wu.get_aspect_of_type(OwnershipClass)]
        ownership = ownership_wus[0].get_aspect_of_type(OwnershipClass)
        assert ownership is not None
        assert len(ownership.owners) == 1

    def test_usage_stats_emitted(self) -> None:
        mapper = _mapper()
        analytics = Analytics(
            appviews_all_time=100,
            appviews_last_7_days=10,
            appviews_last_14_days=20,
            appviews_last_30_days=50,
            last_viewed_at=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
        project = _project(analytics=analytics)
        wus = list(mapper.map_project(project))

        usage_wus = [
            wu for wu in wus if wu.get_aspect_of_type(DashboardUsageStatisticsClass)
        ]
        assert len(usage_wus) == 2  # all-time + 7-day

    def test_upstream_lineage_emitted_when_present(self) -> None:
        mapper = _mapper()
        project = _project(
            upstream_datasets=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.tbl,PROD)"
            ]
        )
        wus = list(mapper.map_project(project))

        lineage_wus = [wu for wu in wus if wu.get_aspect_of_type(UpstreamLineageClass)]
        assert lineage_wus

    def test_no_lineage_aspect_when_no_upstreams(self) -> None:
        mapper = _mapper()
        project = _project()
        wus = list(mapper.map_project(project))

        lineage_wus = [wu for wu in wus if wu.get_aspect_of_type(UpstreamLineageClass)]
        assert not lineage_wus

    def test_run_operation_emitted(self) -> None:
        mapper = _mapper()
        run = RunRecord(
            run_id="run-1",
            status="COMPLETED",
            start_time=datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc),
            elapsed_seconds=63,
        )
        project = _project(latest_run=run)
        wus = list(mapper.map_project(project))

        op_wus = [wu for wu in wus if wu.get_aspect_of_type(OperationClass)]
        assert op_wus

    def test_external_url_draft_when_never_published(self) -> None:
        mapper = _mapper()
        project = _project(last_published_at=None)
        wus = list(mapper.map_project(project))

        for wu in wus:
            info = wu.get_aspect_of_type(DashboardInfoClass)
            if info and info.externalUrl:
                assert "/hex/" in info.externalUrl
                assert "/app/" not in info.externalUrl

    def test_external_url_app_when_published(self) -> None:
        mapper = _mapper()
        project = _project(last_published_at=datetime(2026, 3, 1, tzinfo=timezone.utc))
        wus = list(mapper.map_project(project))

        for wu in wus:
            info = wu.get_aspect_of_type(DashboardInfoClass)
            if info and info.externalUrl:
                assert "/app/" in info.externalUrl


class TestMapComponent:
    def test_component_subtype(self) -> None:
        mapper = _mapper()
        component = Component(
            id="comp-1",
            title="Shared Revenue",
            description="A shared component",
        )
        wus = list(mapper.map_component(component))

        subtype_wus = [wu for wu in wus if wu.get_aspect_of_type(SubTypesClass)]
        assert subtype_wus
        subtypes = subtype_wus[0].get_aspect_of_type(SubTypesClass)
        assert subtypes is not None
        assert any("Component" in t for t in subtypes.typeNames)
