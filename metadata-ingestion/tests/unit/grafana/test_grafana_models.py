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
        },
        "meta": {"folderId": "123", "createdBy": "admin"},
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert dashboard.uid == "dash1"
    assert dashboard.title == "Test Dashboard"
    assert dashboard.version == "1"
    assert dashboard.tags == ["test"]
    assert dashboard.folder_id == "123"
    assert dashboard.created_by == "admin"


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
    """Test that panels with missing IDs get generated fallback IDs."""
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "description": "",
            "version": "1",
            "panels": [
                {"id": "1", "title": "Valid Panel", "type": "graph"},
                # Missing 'id' field - should get generated ID
                {"title": "Invalid Panel", "type": "graph"},
                {"id": "3", "title": "Another Valid Panel", "type": "table"},
            ],
            "tags": [],
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    # Should have 3 panels (missing ID gets generated)
    assert len(dashboard.panels) == 3
    assert dashboard.panels[0].title == "Valid Panel"
    assert dashboard.panels[0].id == "1"
    assert dashboard.panels[1].title == "Invalid Panel"
    assert dashboard.panels[1].id.startswith("graph_")  # Generated ID
    assert dashboard.panels[2].title == "Another Valid Panel"
    assert dashboard.panels[2].id == "3"


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


# Tests for validation fixes - handling missing fields gracefully


def test_dashboard_missing_tags_field():
    """Test that dashboard without tags field gets default empty list."""
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "panels": [],
            # Missing 'tags' field - should default to []
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert dashboard.uid == "dash1"
    assert dashboard.title == "Test Dashboard"
    assert dashboard.tags == []  # Should default to empty list
    assert dashboard.description == ""  # Should default to empty string


def test_dashboard_missing_optional_fields():
    """Test that dashboard with minimal fields gets appropriate defaults."""
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "panels": [],
            # Missing: tags, description, version, timezone, refresh, created_by
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert dashboard.uid == "dash1"
    assert dashboard.title == "Test Dashboard"
    assert dashboard.tags == []
    assert dashboard.description == ""
    assert dashboard.version is None
    assert dashboard.timezone is None
    assert dashboard.refresh is None
    assert dashboard.folder_id is None
    assert dashboard.created_by is None


def test_dashboard_missing_created_by():
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "description": "Test Description",
            "panels": [],
            "timezone": "utc",
            "schemaVersion": "1.0",
        },
        "meta": {"folderId": "123"},
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert dashboard.uid == "dash1"
    assert dashboard.folder_id == "123"
    assert dashboard.created_by is None


def test_dashboard_missing_folder_id():
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "description": "Test Description",
            "panels": [],
            "timezone": "utc",
            "schemaVersion": "1.0",
        },
        "meta": {"createdBy": "admin"},
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert dashboard.uid == "dash1"
    assert dashboard.folder_id is None
    assert dashboard.created_by == "admin"


def test_panel_missing_optional_fields():
    """Test that panel with minimal fields gets appropriate defaults."""
    panel_data = {
        "id": 123,  # Integer ID - should be converted to string
        "title": "Test Panel",
        # Missing: description, targets, transformations, fieldConfig
    }

    panel = Panel.model_validate(panel_data)
    assert panel.id == "123"  # Should be converted to string
    assert panel.title == "Test Panel"
    assert panel.description == ""  # Should default to empty string
    assert panel.query_targets == []  # Should default to empty list
    assert panel.transformations == []  # Should default to empty list
    assert panel.field_config == {}  # Should default to empty dict


def test_panel_integer_id_conversion():
    """Test that panel IDs are converted from int to string."""
    panel_data = {
        "id": 456,  # Integer ID
        "title": "Test Panel",
        "type": "graph",
    }

    panel = Panel.model_validate(panel_data)
    assert panel.id == "456"
    assert isinstance(panel.id, str)


def test_dashboard_with_panels_missing_fields():
    """Test dashboard with panels that have missing fields."""
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "panels": [
                {
                    "id": 1,  # Integer ID
                    "title": "Panel 1",
                    # Missing description, targets, etc.
                },
                {
                    "id": "2",  # String ID
                    # Missing title (should be None for text panels)
                    "type": "text",
                },
            ],
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert len(dashboard.panels) == 2

    # First panel
    panel1 = dashboard.panels[0]
    assert panel1.id == "1"  # Converted to string
    assert panel1.title == "Panel 1"
    assert panel1.description == ""
    assert panel1.query_targets == []

    # Second panel
    panel2 = dashboard.panels[1]
    assert panel2.id == "2"
    assert panel2.title is None  # No title provided
    assert panel2.type == "text"
    assert panel2.description == ""


def test_dashboard_from_grafana_api_response():
    """Test parsing a realistic Grafana API response with missing fields."""
    # Simulates a real Grafana API response that might be missing some fields
    api_response = {
        "meta": {
            "type": "db",
            "canSave": True,
            "canEdit": True,
            "canAdmin": True,
            "canStar": True,
            "slug": "production-dashboard",
            "url": "/d/abc123/production-dashboard",
            "expires": "0001-01-01T00:00:00Z",
            "created": "2024-01-01T10:00:00Z",
            "updated": "2024-01-02T15:30:00Z",
            "updatedBy": "admin@localhost",
            "createdBy": "admin@localhost",
            "version": 1,
            "hasAcl": False,
            "isFolder": False,
            "folderId": 0,
            "folderUid": "",
            "folderTitle": "General",
            "folderUrl": "",
            "provisioned": False,
            "provisionedExternalId": "",
        },
        "dashboard": {
            "uid": "abc123",
            "title": "Production Dashboard",
            "id": 450,
            "panels": [
                {
                    "id": 1,
                    "title": "CPU Usage",
                    "type": "graph",
                    "targets": [{"expr": "cpu_usage", "refId": "A"}],
                },
                {
                    "id": 2,
                    "type": "text",
                    # No title field for text panel
                    "datasource": {"type": "-- Grafana --", "uid": "-- Grafana --"},
                },
            ],
            "time": {"from": "now-1h", "to": "now"},
            "refresh": "5s",
            "schemaVersion": 30,
            "version": 1,
            # Notice: missing 'tags' field entirely
        },
    }

    dashboard = Dashboard.model_validate(api_response)
    assert dashboard.uid == "abc123"
    assert dashboard.title == "Production Dashboard"
    assert dashboard.tags == []  # Should default to empty list
    assert dashboard.version == "1"  # Should be converted to string
    assert dashboard.refresh == "5s"
    assert dashboard.folder_id == "0"  # Extracted from meta.folderId
    assert dashboard.created_by == "admin@localhost"  # Extracted from meta.createdBy
    assert len(dashboard.panels) == 2

    # First panel should have defaults applied
    panel1 = dashboard.panels[0]
    assert panel1.id == "1"
    assert panel1.title == "CPU Usage"
    assert panel1.description == ""
    assert len(panel1.query_targets) == 1

    # Second panel (text) should handle missing title
    panel2 = dashboard.panels[1]
    assert panel2.id == "2"
    assert panel2.title is None
    assert panel2.type == "text"


def test_dashboard_refresh_boolean_conversion():
    """Test that boolean refresh values are converted to strings."""
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "panels": [],
            "refresh": False,  # Boolean value
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert dashboard.refresh == "False"  # Should be converted to string


def test_nested_panels_with_missing_fields():
    """Test nested panels (in row panels) with missing fields."""
    dashboard_data = {
        "dashboard": {
            "uid": "dash1",
            "title": "Test Dashboard",
            "panels": [
                {
                    "type": "row",
                    "panels": [
                        {
                            "id": 1,
                            "title": "Nested Panel 1",
                            # Missing description, targets, etc.
                        },
                        {
                            "id": 2,
                            # Missing title and other fields
                            "type": "text",
                        },
                    ],
                }
            ],
        }
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert len(dashboard.panels) == 2  # Both nested panels should be extracted

    panel1 = dashboard.panels[0]
    assert panel1.id == "1"
    assert panel1.title == "Nested Panel 1"
    assert panel1.description == ""

    panel2 = dashboard.panels[1]
    assert panel2.id == "2"
    assert panel2.title is None
    assert panel2.type == "text"
