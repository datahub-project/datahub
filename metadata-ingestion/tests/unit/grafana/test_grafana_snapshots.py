import pytest

from datahub.ingestion.source.grafana.models import Dashboard, Panel
from datahub.ingestion.source.grafana.snapshots import (
    _build_custom_properties,
    _build_dashboard_properties,
    _build_ownership,
    build_chart_mce,
    build_dashboard_mce,
)
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
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


def test_build_chart_mce(mock_panel, mock_dashboard):
    dataset_urn, chart_mce = build_chart_mce(
        panel=mock_panel,
        dashboard=mock_dashboard,
        platform="grafana",
        platform_instance="test-instance",
        env="PROD",
        base_url="http://grafana.test",
        ingest_tags=True,
    )

    assert dataset_urn is not None
    assert chart_mce.urn.startswith("urn:li:chart")

    chart_info = next(
        aspect for aspect in chart_mce.aspects if isinstance(aspect, ChartInfoClass)
    )
    assert chart_info.title == "Test Panel"
    assert chart_info.description == "Test Description"
    assert (
        chart_info.chartUrl is not None and "http://grafana.test" in chart_info.chartUrl
    )
    assert chart_info.inputs is not None and dataset_urn in chart_info.inputs

    status = next(
        aspect for aspect in chart_mce.aspects if isinstance(aspect, StatusClass)
    )
    assert status.removed is False


def test_build_dashboard_mce(mock_dashboard):
    chart_urns = ["urn:li:chart:(grafana,chart1)", "urn:li:chart:(grafana,chart2)"]

    dashboard_mce = build_dashboard_mce(
        dashboard=mock_dashboard,
        platform="grafana",
        platform_instance="test-instance",
        chart_urns=chart_urns,
        base_url="http://grafana.test",
        ingest_owners=True,
        ingest_tags=True,
    )

    assert dashboard_mce.urn.startswith("urn:li:dashboard")

    dashboard_info = next(
        aspect
        for aspect in dashboard_mce.aspects
        if isinstance(aspect, DashboardInfoClass)
    )
    assert dashboard_info.title == "Test Dashboard"
    assert dashboard_info.description == "Test Description"
    assert dashboard_info.charts is not None and set(dashboard_info.charts) == set(
        chart_urns
    )
    assert (
        dashboard_info.dashboardUrl is not None
        and "http://grafana.test" in dashboard_info.dashboardUrl
    )

    tags = next(
        aspect
        for aspect in dashboard_mce.aspects
        if isinstance(aspect, GlobalTagsClass)
    )
    assert len(tags.tags) == 2

    ownership = next(
        aspect for aspect in dashboard_mce.aspects if isinstance(aspect, OwnershipClass)
    )
    assert len(ownership.owners) == 2


def test_build_chart_mce_no_tags(mock_panel, mock_dashboard):
    mock_dashboard.tags = []  # Ensure it's an empty list rather than None
    dataset_urn, chart_mce = build_chart_mce(
        panel=mock_panel,
        dashboard=mock_dashboard,
        platform="grafana",
        platform_instance="test-instance",
        env="PROD",
        base_url="http://grafana.test",
        ingest_tags=True,
    )

    assert not any(isinstance(aspect, GlobalTagsClass) for aspect in chart_mce.aspects)


def test_build_dashboard_mce_no_owners(mock_dashboard):
    mock_dashboard.created_by = ""
    mock_dashboard.uid = ""

    dashboard_mce = build_dashboard_mce(
        dashboard=mock_dashboard,
        platform="grafana",
        platform_instance="test-instance",
        chart_urns=[],
        base_url="http://grafana.test",
        ingest_owners=True,
        ingest_tags=True,
    )

    assert not any(
        isinstance(aspect, OwnershipClass) for aspect in dashboard_mce.aspects
    )
