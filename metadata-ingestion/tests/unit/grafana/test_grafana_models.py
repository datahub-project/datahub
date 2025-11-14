from typing import Any, Dict

from datahub.ingestion.source.grafana.models import (
    Dashboard,
    Folder,
    Panel,
)


def test_panel_basic():
    panel_data: Dict[str, Any] = {
        "id": "1",
        "title": "Test Panel",
        "description": "Test Description",
        "type": "graph",
        "targets": [],
        "datasource": None,
        "fieldConfig": {},
        "transformations": [],
    }

    panel = Panel.model_validate(panel_data)
    assert panel.id == "1"
    assert panel.title == "Test Panel"
    assert panel.description == "Test Description"
    assert panel.type == "graph"
    assert len(panel.query_targets) == 0


def test_panel_with_datasource():
    panel_data = {
        "id": "1",
        "title": "Test Panel",
        "datasource": {"type": "postgres", "uid": "abc123"},
    }

    panel = Panel.model_validate(panel_data)
    assert panel.datasource_ref is not None
    assert panel.datasource_ref.type == "postgres"
    assert panel.datasource_ref.uid == "abc123"


def test_dashboard_basic():
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "description": "Test Description",
            "version": "1",
            "panels": [],
            "tags": ["test"],
            "timezone": "utc",
            "schemaVersion": "1.0",
            "meta": {"folderId": "123"},
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert dashboard.uid == "dash1"
    assert dashboard.title == "Test Dashboard"
    assert dashboard.version == "1"
    assert dashboard.tags == ["test"]


def test_dashboard_nested_panels():
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "description": "",
            "version": "1",
            "timezone": "utc",
            "schemaVersion": "1.0",
            "panels": [
                {
                    "type": "row",
                    "panels": [{"id": "1", "title": "Nested Panel", "type": "graph"}],
                },
                {"id": "2", "title": "Top Level Panel", "type": "graph"},
            ],
            "tags": [],
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert len(dashboard.panels) == 2
    assert dashboard.panels[0].title == "Nested Panel"
    assert dashboard.panels[1].title == "Top Level Panel"


def test_panel_without_title():
    """Test that text panels without titles are parsed successfully."""
    panel_data: Dict[str, Any] = {
        "id": "1",
        "type": "text",
        "datasource": {"type": "-- Grafana --", "uid": "-- Grafana --"},
    }

    panel = Panel.model_validate(panel_data)
    assert panel.id == "1"
    assert panel.title is None
    assert panel.type == "text"


def test_dashboard_with_text_panel():
    """Test that dashboards with text panels (no title) are parsed successfully."""
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "description": "",
            "version": "1",
            "panels": [
                {"id": "1", "title": "Regular Panel", "type": "graph"},
                {
                    "id": "2",
                    "type": "text",
                    "datasource": {"type": "-- Grafana --", "uid": "-- Grafana --"},
                },
            ],
            "tags": [],
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert len(dashboard.panels) == 2
    assert dashboard.panels[0].title == "Regular Panel"
    assert dashboard.panels[1].title is None
    assert dashboard.panels[1].type == "text"


def test_dashboard_with_invalid_panel_skips():
    """Test that invalid panels are skipped with a warning instead of failing."""
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "description": "",
            "version": "1",
            "panels": [
                {"id": "1", "title": "Valid Panel", "type": "graph"},
                # Missing required 'id' field - should be skipped
                {"title": "Invalid Panel", "type": "graph"},
                {"id": "3", "title": "Another Valid Panel", "type": "table"},
            ],
            "tags": [],
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    # Should have 2 panels (invalid one is skipped)
    assert len(dashboard.panels) == 2
    assert dashboard.panels[0].title == "Valid Panel"
    assert dashboard.panels[1].title == "Another Valid Panel"


def test_dashboard_skip_text_panels():
    """Test that text panels can be skipped when skip_text_panels is True."""
    dashboard_data_with_text = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "description": "",
            "version": "1",
            "panels": [
                {"id": "1", "title": "Regular Panel", "type": "graph"},
                {"id": "2", "type": "text"},
                {"id": "3", "title": "Another Panel", "type": "table"},
            ],
            "tags": [],
        }
    }

    # With skip_text_panels=False (default), all panels should be included
    dashboard_with_text = Dashboard.model_validate(dashboard_data_with_text)
    assert len(dashboard_with_text.panels) == 3

    # With skip_text_panels=True, text panels should be skipped
    dashboard_data_skip_text = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "description": "",
            "version": "1",
            "panels": [
                {"id": "1", "title": "Regular Panel", "type": "graph"},
                {"id": "2", "type": "text"},
                {"id": "3", "title": "Another Panel", "type": "table"},
            ],
            "tags": [],
            "_skip_text_panels": True,
        }
    }
    dashboard_no_text = Dashboard.model_validate(dashboard_data_skip_text)
    assert len(dashboard_no_text.panels) == 2
    assert dashboard_no_text.panels[0].title == "Regular Panel"
    assert dashboard_no_text.panels[0].type == "graph"
    assert dashboard_no_text.panels[1].title == "Another Panel"
    assert dashboard_no_text.panels[1].type == "table"


def test_folder():
    folder_data = {"id": "1", "title": "Test Folder", "description": "Test Description"}

    folder = Folder.model_validate(folder_data)
    assert folder.id == "1"
    assert folder.title == "Test Folder"
    assert folder.description == "Test Description"
