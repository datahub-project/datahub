from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.grafana.grafana_config import GrafanaSourceConfig
from datahub.ingestion.source.grafana.grafana_source import GrafanaSource
from datahub.ingestion.source.grafana.models import Dashboard, Folder, Panel


@pytest.fixture
def mock_config():
    return GrafanaSourceConfig(
        url="http://grafana.test",
        service_account_token="test-token",
        platform_instance="test-instance",
    )


@pytest.fixture
def mock_context():
    return PipelineContext(run_id="test")


@pytest.fixture
def mock_source(mock_config, mock_context):
    return GrafanaSource(mock_config, mock_context)


@pytest.fixture
def mock_folder():
    return Folder(id="1", title="Test Folder", description="Test Description")


@pytest.fixture
def mock_dashboard():
    return Dashboard(
        uid="dash1",
        title="Test Dashboard",
        description="Test Description",
        version="1",
        panels=[
            Panel(
                id="1",
                title="Test Panel",
                type="graph",
                datasource={"type": "postgres", "uid": "postgres_uid"},
                targets=[{"rawSql": "SELECT * FROM test_table"}],
            )
        ],
        tags=["test"],
        schemaVersion="1.0",
        meta={"folderId": "1"},
    )


def test_source_initialization(mock_source):
    assert mock_source.platform == "grafana"
    assert mock_source.config.url == "http://grafana.test"
    assert mock_source.platform_instance == "test-instance"


@patch("datahub.ingestion.source.grafana.grafana_source.GrafanaAPIClient")
def test_process_folder(mock_api, mock_source, mock_folder):
    workunit_list = list(mock_source._process_folder(mock_folder))

    assert len(workunit_list) > 0
    assert all(isinstance(w, MetadataWorkUnit) for w in workunit_list)


@patch("datahub.ingestion.source.grafana.grafana_source.GrafanaAPIClient")
def test_process_dashboard(mock_api, mock_source, mock_dashboard):
    workunit_list = list(mock_source._process_dashboard(mock_dashboard))

    assert len(workunit_list) > 0
    assert all(isinstance(w, MetadataWorkUnit) for w in workunit_list)
    assert mock_source.report.charts_scanned == 1


@patch("datahub.ingestion.source.grafana.grafana_source.GrafanaAPIClient")
def test_process_panel_dataset(mock_api, mock_source, mock_dashboard):
    panel = mock_dashboard.panels[0]
    workunit_list = list(
        mock_source._process_panel_dataset(
            panel=panel, dashboard_uid=mock_dashboard.uid, ingest_tags=True
        )
    )

    assert len(workunit_list) > 0
    assert all(isinstance(w, MetadataWorkUnit) for w in workunit_list)
    assert mock_source.report.datasets_scanned == 1


@patch("datahub.ingestion.source.grafana.grafana_source.GrafanaAPIClient")
def test_process_dashboard_with_folder(mock_api, mock_source, mock_dashboard):
    workunit_list = list(mock_source._process_dashboard(mock_dashboard))

    assert len(workunit_list) > 0
    # Verify folder container relationship is created
    container_workunits = [wu for wu in workunit_list if "container" in wu.id]
    assert len(container_workunits) > 0


@patch("datahub.ingestion.source.grafana.grafana_source.GrafanaAPIClient")
def test_source_get_workunits_internal(
    mock_api, mock_source, mock_folder, mock_dashboard
):
    # Create a mock API client instance
    mock_api_instance = mock_api.return_value
    mock_api_instance.get_folders.return_value = [mock_folder]
    mock_api_instance.get_dashboards.return_value = [mock_dashboard]

    # Set the mock API client on the source
    mock_source.client = mock_api_instance

    # Run the test
    workunit_list = list(mock_source.get_workunits_internal())

    # Verify results
    assert len(workunit_list) > 0
    assert mock_source.report.folders_scanned == 1
    assert mock_source.report.dashboards_scanned == 1
    assert mock_source.report.charts_scanned == 1

    # Verify API calls
    mock_api_instance.get_folders.assert_called_once()
    mock_api_instance.get_dashboards.assert_called_once()


def test_source_get_report(mock_source):
    report = mock_source.get_report()
    assert report.dashboards_scanned == 0
    assert report.charts_scanned == 0
    assert report.folders_scanned == 0
    assert report.datasets_scanned == 0


@patch("datahub.ingestion.source.grafana.grafana_source.GrafanaAPIClient")
def test_source_close(mock_api, mock_source):
    mock_source.close()
    # Verify any cleanup is performed correctly


def test_source_platform_instance_none():
    config = GrafanaSourceConfig(
        url="http://grafana.test",
        service_account_token="test-token",
    )
    ctx = PipelineContext(run_id="test")
    source = GrafanaSource(config, ctx)
    assert source.platform_instance is None
