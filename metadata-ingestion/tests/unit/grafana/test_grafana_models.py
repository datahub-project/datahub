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

    panel = Panel.parse_obj(panel_data)
    assert panel.id == "1"
    assert panel.title == "Test Panel"
    assert panel.description == "Test Description"
    assert panel.type == "graph"
    assert len(panel.targets) == 0


def test_panel_with_datasource():
    panel_data = {
        "id": "1",
        "title": "Test Panel",
        "datasource": {"type": "postgres", "uid": "abc123"},
    }

    panel = Panel.parse_obj(panel_data)
    assert panel.datasource == {"type": "postgres", "uid": "abc123"}


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

    dashboard = Dashboard.parse_obj(dashboard_data)
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

    dashboard = Dashboard.parse_obj(dashboard_data)
    assert len(dashboard.panels) == 2
    assert dashboard.panels[0].title == "Nested Panel"
    assert dashboard.panels[1].title == "Top Level Panel"


def test_folder():
    folder_data = {"id": "1", "title": "Test Folder", "description": "Test Description"}

    folder = Folder.parse_obj(folder_data)
    assert folder.id == "1"
    assert folder.title == "Test Folder"
    assert folder.description == "Test Description"
