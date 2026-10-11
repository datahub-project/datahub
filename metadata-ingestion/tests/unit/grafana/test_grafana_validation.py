from typing import Any, Dict

from datahub.ingestion.source.grafana.models import Dashboard, Panel


def test_dashboard_missing_tags_validation():
    """Test dashboard validation when tags field is missing."""
    # Dashboard data without tags field
    dashboard_data = {
        "id": 450,
        "panels": [],
        "title": "Some Dashboard",
        "uid": "dashboard-uid",
        "version": 1,
        "folder_id": None,
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert dashboard.uid == "dashboard-uid"
    assert dashboard.title == "Some Dashboard"
    assert dashboard.tags == []
    assert dashboard.version == "1"
    assert dashboard.folder_id is None


def test_panel_validation_with_missing_fields():
    """Test panel validation when optional fields are missing."""
    panel_data_list: list[Dict[str, Any]] = [
        {
            "id": 1,
            "type": "graph",
        },
        {
            "id": 2,
        },
        {
            "id": 3,
            "title": "Chart with missing datasource",
            "type": "graph",
        },
    ]

    for panel_data in panel_data_list:
        panel = Panel.model_validate(panel_data)
        assert panel.id == str(panel_data["id"])
        assert panel.description == ""
        assert panel.query_targets == []
        assert panel.transformations == []
        assert panel.field_config == {}


def test_dashboard_completely_minimal():
    """Test dashboard with only absolutely required fields."""
    minimal_dashboard = {
        "uid": "minimal-dash",
        "title": "Minimal Dashboard",
    }

    dashboard = Dashboard.model_validate(minimal_dashboard)
    assert dashboard.uid == "minimal-dash"
    assert dashboard.title == "Minimal Dashboard"
    assert dashboard.tags == []
    assert dashboard.description == ""
    assert dashboard.panels == []
    assert dashboard.version is None
    assert dashboard.timezone is None
    assert dashboard.refresh is None
    assert dashboard.created_by is None
    assert dashboard.folder_id is None


def test_panel_completely_minimal():
    """Test panel with only absolutely required fields."""
    minimal_panel = {"id": 999}

    panel = Panel.model_validate(minimal_panel)
    assert panel.id == "999"
    assert panel.title is None
    assert panel.description == ""
    assert panel.type is None
    assert panel.query_targets == []
    assert panel.datasource_ref is None
    assert panel.field_config == {}
    assert panel.transformations == []


def test_panel_with_null_field_config():
    """Test panel validation when fieldConfig is explicitly null."""
    panel_data = {
        "id": 123,
        "fieldConfig": None,
    }

    panel = Panel.model_validate(panel_data)
    assert panel.id == "123"
    assert panel.field_config == {}  # Should be converted to empty dict


def test_panel_with_null_transformations():
    """Test panel validation when transformations is explicitly null."""
    panel_data = {
        "id": 456,
        "transformations": None,
    }

    panel = Panel.model_validate(panel_data)
    assert panel.id == "456"
    assert panel.transformations == []  # Should be converted to empty list


def test_panel_with_null_targets():
    """Test panel validation when targets is explicitly null."""
    panel_data = {
        "id": 789,
        "targets": None,
    }

    panel = Panel.model_validate(panel_data)
    assert panel.id == "789"
    assert panel.query_targets == []  # Should be converted to empty list


def test_panel_with_all_null_optional_fields():
    """Test panel validation when all optional fields are null."""
    panel_data = {
        "id": 999,
        "fieldConfig": None,
        "transformations": None,
        "targets": None,
        "datasource": None,
        "title": None,
        "type": None,
    }

    panel = Panel.model_validate(panel_data)
    assert panel.id == "999"
    assert panel.field_config == {}
    assert panel.transformations == []
    assert panel.query_targets == []
    assert panel.datasource_ref is None
    assert panel.title is None
    assert panel.type is None


def test_dashboard_with_incomplete_panels():
    """Test dashboard validation containing panels with missing optional fields."""
    dashboard_data = {
        "uid": "incomplete-dash",
        "title": "Dashboard with Incomplete Panels",
        "panels": [
            {
                "id": 1,
                "type": "graph",
            },
            {
                "id": 2,
            },
            {
                "id": 3,
                "title": "Panel with Some Fields",
                "type": "table",
            },
        ],
    }

    dashboard = Dashboard.model_validate(dashboard_data)
    assert dashboard.uid == "incomplete-dash"
    assert dashboard.title == "Dashboard with Incomplete Panels"
    assert dashboard.tags == []
    assert dashboard.description == ""
    assert len(dashboard.panels) == 3
    panel1 = dashboard.panels[0]
    assert panel1.id == "1"
    assert panel1.type == "graph"
    assert panel1.title is None
    assert panel1.description == ""
    assert panel1.query_targets == []

    panel2 = dashboard.panels[1]
    assert panel2.id == "2"
    assert panel2.title is None
    assert panel2.type is None  # Allowed to be None
    assert panel2.description == ""
    assert panel2.query_targets == []

    panel3 = dashboard.panels[2]
    assert panel3.id == "3"
    assert panel3.title == "Panel with Some Fields"
    assert panel3.type == "table"
    assert panel3.description == ""
    assert panel3.query_targets == []


def test_text_panel_like_real_grafana():
    """Test a text panel structure like those found in real Grafana dashboards."""
    # This mimics the structure from our integration test dashboard
    text_panel_data = {
        "id": 1,
        "type": "text",
        "title": "Dashboard Information",
        "gridPos": {"x": 0, "y": 0, "w": 24, "h": 3},
        "options": {
            "content": "# Test Integration Dashboard\nThis dashboard contains test panels.",
            "mode": "markdown",
        },
        # Note: No fieldConfig, targets, transformations, datasource, or description
    }

    # Should validate successfully with defaults applied
    panel = Panel.model_validate(text_panel_data)
    assert panel.id == "1"
    assert panel.type == "text"
    assert panel.title == "Dashboard Information"
    assert panel.description == ""  # Default applied
    assert panel.field_config == {}  # Default applied
    assert panel.query_targets == []  # Default applied
    assert panel.transformations == []  # Default applied
    assert panel.datasource_ref is None


def test_panel_with_null_description():
    """Test that panels with explicit None description are handled correctly."""
    panel_data = {
        "id": 18,
        "type": "timeseries",
        "title": "Test Panel",
        "description": None,  # Explicit None value from Grafana API
        "datasource": {"type": "prometheus", "uid": "test-uid"},
        "fieldConfig": {"defaults": {"unit": "short"}},
        "targets": [{"expr": "up"}],
    }

    # Should validate successfully with None converted to empty string
    panel = Panel.model_validate(panel_data)
    assert panel.description == ""  # None converted to empty string
    assert panel.id == "18"
    assert panel.type == "timeseries"
    assert panel.title == "Test Panel"


def test_panel_with_string_datasource():
    """Test that panels with string datasource (template variables) are handled correctly."""
    panel_data = {
        "id": 35,
        "type": "timeseries",
        "title": "Sent records",
        "datasource": "$datasource",  # String template variable from Grafana
        "fieldConfig": {"defaults": {"unit": "short"}},
        "targets": [{"datasource": "$datasource", "expr": "up"}],
    }

    # Should validate successfully with string datasource converted to None
    panel = Panel.model_validate(panel_data)
    assert panel.datasource_ref is None  # String converted to None
    assert panel.id == "35"
    assert panel.type == "timeseries"
    assert panel.title == "Sent records"


def test_panel_with_missing_id():
    """Test that panels with missing id field get a generated fallback ID."""
    panel_data = {
        # No id field - common in some text panels
        "type": "text",
        "title": "Performance Testing (k6)",
        "options": {"content": "# Performance Testing", "mode": "markdown"},
        "gridPos": {"h": 4, "w": 24, "x": 0, "y": 0},
    }

    # Should validate successfully with generated ID
    panel = Panel.model_validate(panel_data)
    assert panel.id is not None  # ID was generated
    assert panel.id.startswith("text_")  # Generated based on type
    assert panel.type == "text"
    assert panel.title == "Performance Testing (k6)"


def test_panel_with_null_id():
    """Test that panels with explicit None id field get a generated fallback ID."""
    panel_data = {
        "id": None,  # Explicit None
        "type": "text",
        "title": "Test Panel",
        "options": {"content": "Test content"},
    }

    # Should validate successfully with generated ID
    panel = Panel.model_validate(panel_data)
    assert panel.id is not None  # ID was generated
    assert panel.id.startswith("text_")  # Generated based on type
    assert panel.type == "text"
    assert panel.title == "Test Panel"


def test_multiple_panels_without_id_or_title():
    """Test that multiple panels without ID or title get unique, deterministic IDs."""
    # Two panels with no ID, no title, but different grid positions
    panel1_data = {
        "type": "text",
        "gridPos": {"x": 0, "y": 0, "w": 12, "h": 4},
        "options": {"content": "Panel 1"},
    }

    panel2_data = {
        "type": "text",
        "gridPos": {"x": 12, "y": 0, "w": 12, "h": 4},
        "options": {"content": "Panel 2"},
    }

    # Validate both panels
    panel1 = Panel.model_validate(panel1_data)
    panel2 = Panel.model_validate(panel2_data)

    # Both should have generated IDs
    assert panel1.id is not None
    assert panel2.id is not None
    assert panel1.id.startswith("text_")
    assert panel2.id.startswith("text_")

    # IDs should be different due to different grid positions
    assert panel1.id != panel2.id

    # IDs should be deterministic - same input produces same ID
    panel1_duplicate = Panel.model_validate(panel1_data)
    panel2_duplicate = Panel.model_validate(panel2_data)
    assert panel1.id == panel1_duplicate.id
    assert panel2.id == panel2_duplicate.id


def test_panels_identical_except_position():
    """Test that identical panels in different positions get different IDs."""
    base_panel_data = {
        "type": "text",
        "title": "Same Title",
        "options": {"content": "Same content"},
    }

    # Same panel in different positions
    panel1_data = {**base_panel_data, "gridPos": {"x": 0, "y": 0, "w": 12, "h": 4}}
    panel2_data = {**base_panel_data, "gridPos": {"x": 0, "y": 4, "w": 12, "h": 4}}

    panel1 = Panel.model_validate(panel1_data)
    panel2 = Panel.model_validate(panel2_data)

    # Should have different IDs due to different positions
    assert panel1.id != panel2.id
    assert panel1.id.startswith("text_")
    assert panel2.id.startswith("text_")


def test_realistic_grafana_api_response():
    """Test validation with a realistic Grafana API response format."""
    # Simulates a typical Grafana API response with some optional fields missing
    real_response = {
        "meta": {
            "type": "db",
            "canSave": True,
            "canEdit": True,
            "slug": "production-metrics",
            "url": "/d/real-dashboard/production-metrics",
            "expires": "0001-01-01T00:00:00Z",
            "created": "2024-01-01T10:00:00Z",
            "updated": "2024-01-15T14:30:00Z",
            "updatedBy": "admin@localhost",
            "createdBy": "admin@localhost",
            "version": 5,
            "hasAcl": False,
            "isFolder": False,
            "folderId": 1,
            "folderUid": "general",
            "folderTitle": "General",
            "folderUrl": "",
            "provisioned": False,
            "provisionedExternalId": "",
        },
        "dashboard": {
            "id": 450,
            "uid": "real-dashboard",
            "title": "Production Metrics",
            "url": "/d/real-dashboard/production-metrics",
            "slug": "production-metrics",
            "type": "db",
            "panels": [
                {
                    "id": 1,
                    "title": "CPU Usage",
                    "type": "graph",
                    "targets": [{"expr": "cpu_usage_percent", "refId": "A"}],
                    "datasource": {"type": "prometheus", "uid": "prometheus-uid"},
                },
                {
                    "id": 2,
                    "type": "text",
                    # Text panel with no title - common in Grafana
                    "datasource": {"type": "-- Grafana --", "uid": "-- Grafana --"},
                },
            ],
            "time": {"from": "now-1h", "to": "now"},
            "refresh": "30s",
            "schemaVersion": 30,
            "version": 5,
            # Note: 'tags' and 'description' fields are not included
        },
    }

    dashboard = Dashboard.model_validate(real_response)
    assert dashboard.uid == "real-dashboard"
    assert dashboard.title == "Production Metrics"
    assert dashboard.tags == []  # Should default to empty list
    assert dashboard.description == ""  # Should default to empty string
    assert dashboard.version == "5"  # Should be converted to string
    assert dashboard.refresh == "30s"
    assert dashboard.folder_id == "1"  # Extracted from meta
    assert dashboard.created_by == "admin@localhost"  # Extracted from meta
    assert len(dashboard.panels) == 2

    # Verify panels are processed correctly
    cpu_panel = dashboard.panels[0]
    assert cpu_panel.id == "1"
    assert cpu_panel.title == "CPU Usage"
    assert cpu_panel.type == "graph"
    assert cpu_panel.description == ""  # Default
    assert len(cpu_panel.query_targets) == 1

    text_panel = dashboard.panels[1]
    assert text_panel.id == "2"
    assert text_panel.title is None  # Text panels often have no title
    assert text_panel.type == "text"
    assert text_panel.description == ""  # Default
