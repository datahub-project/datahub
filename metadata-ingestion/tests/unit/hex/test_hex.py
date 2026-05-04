import unittest
import warnings
from unittest.mock import MagicMock, patch

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.hex.hex import HexSource, HexSourceConfig
from tests.unit.hex.conftest import load_json_data


class TestHexSourceConfig(unittest.TestCase):
    def setUp(self):
        self.minimum_input_config = {
            "workspace_name": "test-workspace",
            "token": "test-token",
        }

    def test_required_fields(self):
        with self.assertRaises(ValueError):
            input_config = {**self.minimum_input_config}
            del input_config["workspace_name"]
            HexSourceConfig.model_validate(input_config)

        with self.assertRaises(ValueError):
            input_config = {**self.minimum_input_config}
            del input_config["token"]
            HexSourceConfig.model_validate(input_config)

    def test_minimum_config(self):
        config = HexSourceConfig.model_validate(self.minimum_input_config)

        assert config
        assert config.workspace_name == "test-workspace"
        assert config.token.get_secret_value() == "test-token"

    def test_lineage_config(self):
        config = HexSourceConfig.model_validate(self.minimum_input_config)
        assert config and config.include_lineage

        input_config = {**self.minimum_input_config, "include_lineage": False}
        config = HexSourceConfig.model_validate(input_config)
        assert config and not config.include_lineage

    def test_deprecated_lineage_fields_emit_warnings(self):
        """Removed fields emit ConfigurationWarning instead of silently being ignored."""
        deprecated_fields = [
            "lineage_start_time",
            "lineage_end_time",
            "datahub_page_size",
        ]
        for field in deprecated_fields:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                HexSourceConfig.model_validate(
                    {**self.minimum_input_config, field: "some-value"}
                )
                assert any(
                    issubclass(warning.category, ConfigurationWarning) for warning in w
                ), f"Expected ConfigurationWarning for deprecated field '{field}'"

    def test_category_pattern_filtering(self):
        """Test that category_pattern filters projects/components correctly using page3_data"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure to deny exact "Scratchpad" match but not "Keep_Scratchpad"
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "deny": ["^Scratchpad$"],
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-category-filtering")
        source = HexSource.create(config, ctx)

        # Mock the API to return page3_data
        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: project with "Scratchpad" category should be filtered out
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" not in source.project_registry

        # Verify: project with both "Scratchpad" and "Keep_Scratchpad" should be filtered out
        # (deny takes precedence)
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" not in source.project_registry

        # Verify: component with "Keep_Scratchpad" category should be kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry

    def test_category_pattern_allow(self):
        """Test that category_pattern allow list works correctly using page3_data"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure to only allow "Keep_Scratchpad" category
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "allow": ["^Keep_Scratchpad$"],
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-category-allow")
        source = HexSource.create(config, ctx)

        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: project with "Scratchpad" should be filtered out (not in allow list)
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" not in source.project_registry

        # Verify: project with both "Scratchpad" and "Keep_Scratchpad" should be kept
        # (has at least one allowed category)
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" in source.project_registry

        # Verify: only component with "Keep_Scratchpad" should be kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry

    def test_category_pattern_mixed_categories_deny_precedence(self):
        """Test that deny patterns take precedence over having allowed categories"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure with both allow and deny patterns
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "allow": ["^Keep_Scratchpad$"],
                "deny": ["^Scratchpad$"],
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-mixed-categories-deny")
        source = HexSource.create(config, ctx)

        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: project with only "Scratchpad" is filtered out
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" not in source.project_registry

        # Verify: project with both categories is filtered out (deny takes precedence)
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" not in source.project_registry

        # Verify: component with only "Keep_Scratchpad" is kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry

    def test_category_pattern_mixed_categories_no_deny(self):
        """Test that items with multiple categories are kept if any category matches allow"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure with only allow pattern (no deny)
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "allow": ["^Keep_Scratchpad$"],
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-mixed-categories-allow-only")
        source = HexSource.create(config, ctx)

        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: project with only "Scratchpad" is filtered out (not in allow list)
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" not in source.project_registry

        # Verify: project with both categories is kept (has allowed category)
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" in source.project_registry

        # Verify: component with "Keep_Scratchpad" is kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry

    def test_no_category_pattern_filtering(self):
        """Test that all items are kept when no category_pattern is configured"""
        page3_data = load_json_data("hex_projects_page3.json")

        mock_response = MagicMock()
        mock_response.json.return_value = page3_data

        # Configure without any category_pattern
        config = {
            **self.minimum_input_config,
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-no-category-filter")
        source = HexSource.create(config, ctx)

        with patch.object(source.hex_api.session, "get", return_value=mock_response):
            list(source.get_workunits_internal())

        # Verify: all projects are kept regardless of categories
        assert "d73da67d-c87b-4dd8-9e7f-b79cb7f822cg" in source.project_registry
        assert "e8d7c5a3-2b4f-4e21-9823-1a3b5c7d9e0f" in source.project_registry

        # Verify: all components are kept
        assert "4759f33c-1ab9-403d-92e8-9bef48de00cg" in source.component_registry


class TestHexTestConnection(unittest.TestCase):
    """Tests for test_connection() — especially the cells access probe."""

    def _make_response(self, status_code: int, json_data: dict) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.ok = status_code < 400
        resp.json.return_value = json_data
        resp.raise_for_status = MagicMock(
            side_effect=None if status_code < 400 else Exception(f"HTTP {status_code}")
        )
        return resp

    def _run_test_connection(self, cells_status: int) -> "TestConnectionReport":
        """Run test_connection with a controlled cells endpoint status code."""
        config = {
            "workspace_name": "test-ws",
            "token": "test-token",
            "base_url": "https://app.hex.tech/api/v1",
        }

        def mock_get(url, **kwargs):
            if "users/me" in url:
                return self._make_response(200, {"email": "test@test.com"})
            if "projects" in url and "queriedTables" in url:
                return self._make_response(403, {})
            if "projects" in url:
                return self._make_response(
                    200, {"values": [{"id": "proj-1"}], "pagination": {}}
                )
            if "data-connections" in url:
                return self._make_response(200, {"values": []})
            if "cells" in url:
                return self._make_response(cells_status, {"values": []})
            return self._make_response(404, {})

        def mock_post(url, **kwargs):
            # Export API — return a valid but empty YAML so sampling completes quickly
            if "export" in url:
                return self._make_response(
                    200,
                    {
                        "filename": "test.yaml",
                        "content": "meta:\n  title: Test\ncells: []\n",
                    },
                )
            return self._make_response(404, {})

        with patch(
            "datahub.ingestion.source.hex.hex.HexApi._create_retry_session"
        ) as mock_session_factory:
            mock_session = MagicMock()
            mock_session.get.side_effect = mock_get
            mock_session.post.side_effect = mock_post
            mock_session.request.side_effect = lambda method, url, **kw: (
                mock_post(url, **kw)
                if method.upper() == "POST"
                else mock_get(url, **kw)
            )
            mock_session_factory.return_value = mock_session

            return HexSource.test_connection(config)

    def test_cells_accessible_reports_lineage_capable(self):
        report = self._run_test_connection(cells_status=200)
        assert report.capability_report is not None
        lineage_cap = report.capability_report.get(
            "Lineage via SQL parsing (all tiers)"
        )
        assert lineage_cap is not None
        assert lineage_cap.capable is True

    def test_cells_403_reports_lineage_not_capable(self):
        """Metadata-only token (can list projects, cannot read cells) → lineage not capable."""
        report = self._run_test_connection(cells_status=403)
        assert report.capability_report is not None
        lineage_cap = report.capability_report.get(
            "Lineage via SQL parsing (all tiers)"
        )
        assert lineage_cap is not None
        assert lineage_cap.capable is False
        assert lineage_cap.failure_reason is not None
        assert "403" in lineage_cap.failure_reason
        assert "Read projects" in lineage_cap.failure_reason
