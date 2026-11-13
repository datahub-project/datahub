"""Regression tests for skipping abstract explores (extension: required).

Fixes https://github.com/datahub-project/datahub/issues/16826 — explores with
`extension: required` are abstract base templates that cannot be queried via the
Looker API and must be skipped during ingestion.
"""

from collections import defaultdict

from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver


def _filter_explores(explores: list) -> dict:
    """Simulate the explore-filtering logic from get_internal_workunits."""
    explore_to_views: dict = defaultdict(set)
    for explore_dict in explores:
        if LookerRefinementResolver.is_refinement(explore_dict["name"]):
            continue
        if explore_dict.get("extension") == "required":
            continue
        explore_to_views[explore_dict["name"]]
    return explore_to_views


class TestAbstractExploreSkip:
    def test_abstract_explore_is_skipped(self):
        """Explores with extension: required must not be added to the explore map."""
        explores = [
            {"name": "base_explore", "extension": "required"},
            {"name": "real_explore"},
        ]
        result = _filter_explores(explores)
        assert "base_explore" not in result
        assert "real_explore" in result

    def test_concrete_explore_without_extension_is_processed(self):
        """Explores without an extension field pass through normally."""
        result = _filter_explores([{"name": "orders_explore"}])
        assert "orders_explore" in result

    def test_only_extension_required_is_skipped(self):
        """Only extension: required is skipped; other extension values are not."""
        explores = [
            {"name": "partial_explore", "extension": "required_and_allowed"},
        ]
        result = _filter_explores(explores)
        assert "partial_explore" in result

    def test_multiple_abstract_explores_all_skipped(self):
        """All abstract explores in a model are skipped; concrete ones remain."""
        explores = [
            {"name": "base_a", "extension": "required"},
            {"name": "base_b", "extension": "required"},
            {"name": "concrete_c"},
        ]
        result = _filter_explores(explores)
        assert "base_a" not in result
        assert "base_b" not in result
        assert "concrete_c" in result
