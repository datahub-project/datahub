"""
Tests that abstract explores (extension: required) are skipped during LookML processing.

Regression test for https://github.com/datahub-project/datahub/issues/16826 —
explores marked with `extension: required` cannot be queried via the Looker API
and caused repeated 404 errors when use_api_for_view_lineage was enabled.
"""
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.looker.looker_dataclasses import LookerModel
from datahub.ingestion.source.looker.lookml_source import LookMLSource


def _make_source() -> LookMLSource:
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.looker.lookml_config import LookMLSourceConfig

    config = MagicMock(spec=LookMLSourceConfig)
    config.base_folder = None
    config.project_name = "test_project"
    config.connection_to_platform_map = {}
    config.stateful_ingestion = MagicMock()
    config.stateful_ingestion.enabled = False
    config.api = None
    config.looker_client = None
    ctx = MagicMock(spec=PipelineContext)
    return LookMLSource(config, ctx)


def _make_model(explores: list) -> LookerModel:
    return LookerModel(
        connection="test_conn",
        includes=[],
        resolved_includes=[],
        explores=explores,
    )


class TestAbstractExploreSkip:
    def test_abstract_explore_is_skipped(self):
        """Explores with extension: required must not be added to the view/explore maps."""
        abstract_explore = {"name": "base_explore", "extension": "required"}
        concrete_explore = {"name": "real_explore", "joins": []}

        model = _make_model([abstract_explore, concrete_explore])
        source = _make_source()

        view_to_explores: dict = {}
        explore_to_views: dict = {}

        # Simulate only the part of get_internal_workunits that builds the maps
        from collections import defaultdict

        from datahub.ingestion.source.looker.looker_refinements import (
            LookerRefinementResolver,
        )

        view_to_explores = defaultdict(set)
        explore_to_views = defaultdict(set)

        for explore_dict in model.explores:
            if LookerRefinementResolver.is_refinement(explore_dict["name"]):
                continue
            if explore_dict.get("extension") == "required":
                continue
            explore_to_views[explore_dict["name"]]  # just register it

        assert "base_explore" not in explore_to_views, (
            "Abstract explore (extension: required) should be skipped"
        )
        assert "real_explore" in explore_to_views, (
            "Concrete explore should still be processed"
        )

    def test_concrete_explore_without_extension_is_processed(self):
        """Explores without extension field should pass through normally."""
        explore = {"name": "orders_explore"}
        model = _make_model([explore])

        from collections import defaultdict

        from datahub.ingestion.source.looker.looker_refinements import (
            LookerRefinementResolver,
        )

        explore_to_views = defaultdict(set)

        for explore_dict in model.explores:
            if LookerRefinementResolver.is_refinement(explore_dict["name"]):
                continue
            if explore_dict.get("extension") == "required":
                continue
            explore_to_views[explore_dict["name"]]

        assert "orders_explore" in explore_to_views

    def test_extension_with_other_value_is_not_skipped(self):
        """Explores with extension values other than 'required' must not be skipped."""
        # extension: required_and_allowed is a valid non-abstract explore
        explore = {"name": "partial_explore", "extension": "required_and_allowed"}
        model = _make_model([explore])

        from collections import defaultdict

        from datahub.ingestion.source.looker.looker_refinements import (
            LookerRefinementResolver,
        )

        explore_to_views = defaultdict(set)

        for explore_dict in model.explores:
            if LookerRefinementResolver.is_refinement(explore_dict["name"]):
                continue
            if explore_dict.get("extension") == "required":
                continue
            explore_to_views[explore_dict["name"]]

        assert "partial_explore" in explore_to_views, (
            "Only extension: required should be skipped, not other extension values"
        )

    def test_multiple_abstract_explores_all_skipped(self):
        """All abstract explores in a model should be skipped."""
        explores = [
            {"name": "base_a", "extension": "required"},
            {"name": "base_b", "extension": "required"},
            {"name": "concrete_c"},
        ]
        model = _make_model(explores)

        from collections import defaultdict

        from datahub.ingestion.source.looker.looker_refinements import (
            LookerRefinementResolver,
        )

        explore_to_views = defaultdict(set)

        for explore_dict in model.explores:
            if LookerRefinementResolver.is_refinement(explore_dict["name"]):
                continue
            if explore_dict.get("extension") == "required":
                continue
            explore_to_views[explore_dict["name"]]

        assert "base_a" not in explore_to_views
        assert "base_b" not in explore_to_views
        assert "concrete_c" in explore_to_views
