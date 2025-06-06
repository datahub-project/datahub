import pytest

from datahub.ingestion.source.grafana.entity_mcp_builder import (
    _build_custom_properties,
    _build_dashboard_properties,
    _build_ownership,
    build_chart_mcps,
    build_dashboard_mcps,
)
from datahub.ingestion.source.grafana.models import Dashboard, Panel
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    GlobalTagsClass,
    OwnershipClass,
    StatusClass,
)


@pytest.fixture
def mock_panel():
    return Panel(
        id="1",
        title="Test Panel",
        description="Test Description",
        type="graph",
        targets=[{"query": "SELECT * FROM test"}],
        datasource={"type": "mysql", "uid": "test_uid"},
    )


@pytest.fixture
def mock_dashboard():
    return Dashboard(
        uid="dash1",
        title="Test Dashboard",
        description="Test Description",
        version="1",
        panels=[],
        tags=["tag1", "environment:prod"],
        timezone="UTC",
        schemaVersion="1.0",
        meta={"folderId": "123"},
        created_by="test@test.com",
    )


def test_build_custom_properties():
    panel = Panel(
        id="1",
        title="Test Panel",
        type="graph",
        description="Test Description",
        targets=[{"query": "test"}],
        datasource={"type": "mysql", "uid": "test_uid"},
    )

    props = _build_custom_properties(panel)
    assert props["type"] == "graph"
    assert props["datasourceType"] == "mysql"
    assert props["datasourceUid"] == "test_uid"
    assert props["description"] == "Test Description"
    assert props["queryCount"] == "1"


def test_build_dashboard_properties(mock_dashboard):
    props = _build_dashboard_properties(mock_dashboard)
    assert props["version"] == "1"
    assert props["schema_version"] == "1.0"
    assert props["timezone"] == "UTC"


def test_build_ownership(mock_dashboard):
    ownership = _build_ownership(mock_dashboard)
    assert isinstance(ownership, OwnershipClass)
    assert len(ownership.owners) == 2
    assert any(owner.owner.split(":")[-1] == "dash1" for owner in ownership.owners)
    assert any(owner.owner.split(":")[-1] == "test" for owner in ownership.owners)


def test_build_chart_mcps(mock_panel, mock_dashboard):
    dataset_urn, chart_urn, chart_mcps = build_chart_mcps(
        panel=mock_panel,
        dashboard=mock_dashboard,
        platform="grafana",
        platform_instance="test-instance",
        env="PROD",
        base_url="http://grafana.test",
        ingest_tags=True,
    )

    assert dataset_urn is not None
    assert chart_urn.startswith("urn:li:chart")
    assert len(chart_mcps) > 0

    # Test DataPlatformInstance aspect
    platform_instance_mcp = next(
        mcp for mcp in chart_mcps if isinstance(mcp.aspect, DataPlatformInstanceClass)
    )
    assert platform_instance_mcp is not None
    assert isinstance(
        platform_instance_mcp.aspect, DataPlatformInstanceClass
    )  # type safety
    assert platform_instance_mcp.entityUrn == chart_urn
    assert platform_instance_mcp.aspect.instance is not None

    # Test Status aspect
    status_mcp = next(mcp for mcp in chart_mcps if isinstance(mcp.aspect, StatusClass))
    assert status_mcp is not None
    assert isinstance(status_mcp.aspect, StatusClass)  # type safety
    assert status_mcp.entityUrn == chart_urn
    assert status_mcp.aspect.removed is False

    # Test ChartInfo aspect
    chart_info_mcp = next(
        mcp for mcp in chart_mcps if isinstance(mcp.aspect, ChartInfoClass)
    )
    assert chart_info_mcp is not None
    assert isinstance(chart_info_mcp.aspect, ChartInfoClass)  # type safety
    assert chart_info_mcp.entityUrn == chart_urn
    assert chart_info_mcp.aspect.title == "Test Panel"
    assert chart_info_mcp.aspect.description == "Test Description"
    assert chart_info_mcp.aspect.chartUrl is not None
    assert "http://grafana.test" in chart_info_mcp.aspect.chartUrl
    assert chart_info_mcp.aspect.inputs is not None
    assert dataset_urn in chart_info_mcp.aspect.inputs

    # Test Tags aspect
    tags_mcp = next(
        (mcp for mcp in chart_mcps if isinstance(mcp.aspect, GlobalTagsClass)), None
    )
    assert tags_mcp is not None
    assert isinstance(tags_mcp.aspect, GlobalTagsClass)  # type safety
    assert len(tags_mcp.aspect.tags) == 2


def test_build_dashboard_mcps(mock_dashboard):
    chart_urns = ["urn:li:chart:(grafana,chart1)", "urn:li:chart:(grafana,chart2)"]

    dashboard_urn, dashboard_mcps = build_dashboard_mcps(
        dashboard=mock_dashboard,
        platform="grafana",
        platform_instance="test-instance",
        chart_urns=chart_urns,
        base_url="http://grafana.test",
        ingest_owners=True,
        ingest_tags=True,
    )

    assert dashboard_urn.startswith("urn:li:dashboard")
    assert len(dashboard_mcps) > 0

    # Test DataPlatformInstance aspect
    platform_instance_mcp = next(
        mcp
        for mcp in dashboard_mcps
        if isinstance(mcp.aspect, DataPlatformInstanceClass)
    )
    assert platform_instance_mcp is not None
    assert isinstance(
        platform_instance_mcp.aspect, DataPlatformInstanceClass
    )  # type safety
    assert platform_instance_mcp.entityUrn == dashboard_urn
    assert platform_instance_mcp.aspect.instance is not None

    # Test DashboardInfo aspect
    dashboard_info_mcp = next(
        mcp for mcp in dashboard_mcps if isinstance(mcp.aspect, DashboardInfoClass)
    )
    assert dashboard_info_mcp is not None
    assert isinstance(dashboard_info_mcp.aspect, DashboardInfoClass)  # type safety
    assert dashboard_info_mcp.entityUrn == dashboard_urn
    assert dashboard_info_mcp.aspect.title == "Test Dashboard"
    assert dashboard_info_mcp.aspect.description == "Test Description"
    assert set(dashboard_info_mcp.aspect.charts) == set(chart_urns)
    assert dashboard_info_mcp.aspect.dashboardUrl is not None
    assert "http://grafana.test" in dashboard_info_mcp.aspect.dashboardUrl

    # Test Tags aspect
    tags_mcp = next(
        mcp for mcp in dashboard_mcps if isinstance(mcp.aspect, GlobalTagsClass)
    )
    assert tags_mcp is not None
    assert isinstance(tags_mcp.aspect, GlobalTagsClass)  # type safety
    assert len(tags_mcp.aspect.tags) == 2

    # Test Ownership aspect
    ownership_mcp = next(
        mcp for mcp in dashboard_mcps if isinstance(mcp.aspect, OwnershipClass)
    )
    assert ownership_mcp is not None
    assert isinstance(ownership_mcp.aspect, OwnershipClass)  # type safety
    assert len(ownership_mcp.aspect.owners) == 2


def test_build_chart_mcps_no_tags(mock_panel, mock_dashboard):
    mock_dashboard.tags = []  # Ensure it's an empty list rather than None
    dataset_urn, chart_urn, chart_mcps = build_chart_mcps(
        panel=mock_panel,
        dashboard=mock_dashboard,
        platform="grafana",
        platform_instance="test-instance",
        env="PROD",
        base_url="http://grafana.test",
        ingest_tags=True,
    )

    assert not any(isinstance(mcp.aspect, GlobalTagsClass) for mcp in chart_mcps)


def test_build_dashboard_mcps_no_owners(mock_dashboard):
    mock_dashboard.created_by = ""
    mock_dashboard.uid = ""

    dashboard_urn, dashboard_mcps = build_dashboard_mcps(
        dashboard=mock_dashboard,
        platform="grafana",
        platform_instance="test-instance",
        chart_urns=[],
        base_url="http://grafana.test",
        ingest_owners=True,
        ingest_tags=True,
    )

    assert not any(isinstance(mcp.aspect, OwnershipClass) for mcp in dashboard_mcps)
