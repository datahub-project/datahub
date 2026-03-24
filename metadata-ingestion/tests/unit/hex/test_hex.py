import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.hex.hex import HexSource, HexSourceConfig
from datahub.ingestion.source.hex.model import Category, Component, Project


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
        """Test that category_pattern filters projects/components correctly"""
        # Create test projects with different categories
        project_with_scratchpad = Project(
            id="project-1",
            title="Project with Scratchpad",
            categories=[Category(name="Scratchpad", description="Test scratchpad")],
            created_at=datetime.now(tz=timezone.utc),
            last_edited_at=datetime.now(tz=timezone.utc),
        )

        project_with_keep_scratchpad = Project(
            id="project-2",
            title="Project with Keep_Scratchpad",
            categories=[
                Category(name="Keep_Scratchpad", description="Should be kept")
            ],
            created_at=datetime.now(tz=timezone.utc),
            last_edited_at=datetime.now(tz=timezone.utc),
        )

        project_no_category = Project(
            id="project-3",
            title="Project without category",
            categories=[],
            created_at=datetime.now(tz=timezone.utc),
            last_edited_at=datetime.now(tz=timezone.utc),
        )

        component_with_keep_scratchpad = Component(
            id="component-1",
            title="Component with Keep_Scratchpad",
            categories=[
                Category(name="Keep_Scratchpad", description="Should be kept")
            ],
            created_at=datetime.now(tz=timezone.utc),
            last_edited_at=datetime.now(tz=timezone.utc),
        )

        # Configure to deny categories ending with "Scratchpad" but not "Keep_Scratchpad"
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "deny": ["^Scratchpad$"],  # Only deny exact "Scratchpad" match
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-category-filtering")
        source = HexSource.create(config, ctx)

        # Mock the API to return our test projects
        mock_projects = [
            project_with_scratchpad,
            project_with_keep_scratchpad,
            project_no_category,
            component_with_keep_scratchpad,
        ]

        with patch.object(
            source.hex_api, "fetch_projects", return_value=mock_projects
        ):
            # Run the pipeline to populate the registries
            list(source.get_workunits_internal())

        # Verify: project with "Scratchpad" should be filtered out
        assert "project-1" not in source.project_registry

        # Verify: project with "Keep_Scratchpad" should be kept
        assert "project-2" in source.project_registry

        # Verify: project with no category should be kept
        assert "project-3" in source.project_registry

        # Verify: component with "Keep_Scratchpad" should be kept
        assert "component-1" in source.component_registry

    def test_category_pattern_allow(self):
        """Test that category_pattern allow list works correctly"""
        project_with_production = Project(
            id="project-1",
            title="Production Project",
            categories=[Category(name="Production", description="Prod category")],
            created_at=datetime.now(tz=timezone.utc),
            last_edited_at=datetime.now(tz=timezone.utc),
        )

        project_with_scratchpad = Project(
            id="project-2",
            title="Scratchpad Project",
            categories=[Category(name="Scratchpad", description="Test category")],
            created_at=datetime.now(tz=timezone.utc),
            last_edited_at=datetime.now(tz=timezone.utc),
        )

        # Configure to only allow "Production" category
        config = {
            **self.minimum_input_config,
            "category_pattern": {
                "allow": ["^Production$"],
            },
            "include_lineage": False,
        }

        ctx = PipelineContext(run_id="test-category-allow")
        source = HexSource.create(config, ctx)

        mock_projects = [project_with_production, project_with_scratchpad]

        with patch.object(
            source.hex_api, "fetch_projects", return_value=mock_projects
        ):
            list(source.get_workunits_internal())

        # Verify: only project with "Production" should be kept
        assert "project-1" in source.project_registry
        assert "project-2" not in source.project_registry
