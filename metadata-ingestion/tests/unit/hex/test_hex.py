import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.hex.hex import HexSource, HexSourceConfig
from tests.unit.hex.conftest import load_json_data


def datetime_approx_equal(
    dt1: datetime, dt2: datetime, tolerance_seconds: int = 5
) -> bool:
    if dt1.tzinfo is None:
        dt1 = dt1.replace(tzinfo=timezone.utc)
    if dt2.tzinfo is None:
        dt2 = dt2.replace(tzinfo=timezone.utc)

    diff = abs((dt1 - dt2).total_seconds())
    return diff <= tolerance_seconds


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

        # default values for lineage_start_time and lineage_end_time
        config = HexSourceConfig.model_validate(self.minimum_input_config)
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime.now(tz=timezone.utc) - timedelta(days=1),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time, datetime.now(tz=timezone.utc)
            )
        )
        # set values for lineage_start_time and lineage_end_time
        input_config = {
            **self.minimum_input_config,
            "lineage_start_time": "2025-03-24 12:00:00",
            "lineage_end_time": "2025-03-25 12:00:00",
        }
        config = HexSourceConfig.model_validate(input_config)
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime(2025, 3, 24, 12, 0, 0, tzinfo=timezone.utc),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time,
                datetime(2025, 3, 25, 12, 0, 0, tzinfo=timezone.utc),
            )
        )
        # set lineage_end_time only
        input_config = {
            **self.minimum_input_config,
            "lineage_end_time": "2025-03-25 12:00:00",
        }
        config = HexSourceConfig.model_validate(input_config)
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime(2025, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
                - timedelta(days=1),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time,
                datetime(2025, 3, 25, 12, 0, 0, tzinfo=timezone.utc),
            )
        )
        # set lineage_start_time only
        input_config = {
            **self.minimum_input_config,
            "lineage_start_time": "2025-03-25 12:00:00",
        }
        config = HexSourceConfig.model_validate(input_config)
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime(2025, 3, 25, 12, 0, 0, tzinfo=timezone.utc),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time, datetime.now(tz=timezone.utc)
            )
        )
        # set relative times for lineage_start_time and lineage_end_time
        input_config = {
            **self.minimum_input_config,
            "lineage_start_time": "-3day",
            "lineage_end_time": "now",
        }
        config = HexSourceConfig.model_validate(input_config)
        assert (
            config.lineage_start_time
            and isinstance(config.lineage_start_time, datetime)
            and datetime_approx_equal(
                config.lineage_start_time,
                datetime.now(tz=timezone.utc) - timedelta(days=3),
            )
        )
        assert (
            config.lineage_end_time
            and isinstance(config.lineage_end_time, datetime)
            and datetime_approx_equal(
                config.lineage_end_time, datetime.now(tz=timezone.utc)
            )
        )

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
