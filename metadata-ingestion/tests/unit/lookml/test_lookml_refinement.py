"""LookML refinements (``view: +name``) merged into views and explores."""

from typing import Callable, Dict

import pytest

from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver
from tests.unit.lookml.conftest import LookMLProject

BOOK_VIEW = """
view: book {
  sql_table_name: library.book ;;
  dimension: id { type: number sql: ${TABLE}.id ;; }
  dimension: title { type: string sql: ${TABLE}.title ;; }
  measure: count { type: count }
}
"""

BOOK_REFINEMENT = """
view: +book {
  dimension: title { type: string sql: UPPER(${TABLE}.title) ;; description: "refined" }
  dimension: author { type: string sql: ${TABLE}.author ;; }
  measure: total_pages { type: sum sql: ${TABLE}.pages ;; }
  dimension_group: published { type: time sql: ${TABLE}.published_at ;; }
}
"""


@pytest.fixture
def project(lookml_project_factory: Callable[..., LookMLProject]) -> LookMLProject:
    """Refinement processing is opt-in (``process_refinements`` defaults to False)."""
    return lookml_project_factory(process_refinements=True)


class TestMergeRefinements:
    def test_refinement_overrides_existing_field_and_adds_new_ones(self) -> None:
        raw_view: Dict = {
            "name": "book",
            "dimensions": [
                {"name": "id", "type": "number"},
                {"name": "title", "type": "string", "sql": "${TABLE}.title"},
            ],
        }
        refinement: Dict = {
            "name": "+book",
            "dimensions": [
                {"name": "title", "sql": "UPPER(${TABLE}.title)"},
                {"name": "author", "type": "string"},
            ],
            "measures": [{"name": "count", "type": "count"}],
        }

        merged = LookerRefinementResolver.merge_refinements(raw_view, [refinement])

        dims = {d["name"]: d for d in merged["dimensions"]}
        assert dims["title"] == {
            "name": "title",
            "type": "string",
            "sql": "UPPER(${TABLE}.title)",
        }
        assert dims["author"]["type"] == "string"
        assert [d["name"] for d in merged["dimensions"]] == ["id", "title", "author"]
        assert merged["measures"] == [{"name": "count", "type": "count"}]

    def test_later_refinement_wins(self) -> None:
        raw_view: Dict = {"name": "book", "dimensions": [{"name": "title"}]}
        first: Dict = {"name": "+book", "dimensions": [{"name": "title", "label": "A"}]}
        second: Dict = {
            "name": "+book",
            "dimensions": [{"name": "title", "label": "B"}],
        }

        merged = LookerRefinementResolver.merge_refinements(raw_view, [first, second])

        assert merged["dimensions"][0]["label"] == "B"

    def test_original_view_is_not_mutated(self) -> None:
        raw_view: Dict = {"name": "book", "dimensions": [{"name": "title"}]}
        refinement: Dict = {
            "name": "+book",
            "dimensions": [{"name": "title", "label": "B"}],
        }

        LookerRefinementResolver.merge_refinements(raw_view, [refinement])

        assert raw_view == {"name": "book", "dimensions": [{"name": "title"}]}


class TestApplyViewRefinement:
    def test_refinement_from_included_file_is_applied(
        self, project: LookMLProject
    ) -> None:
        project.write("book.view.lkml", BOOK_VIEW)
        project.write("book_refinement.view.lkml", BOOK_REFINEMENT)
        raw_view: Dict = {
            "name": "book",
            "dimensions": [{"name": "id"}, {"name": "title", "type": "string"}],
            "measures": [{"name": "count"}],
        }

        refined = project.refinement_resolver.apply_view_refinement(raw_view)

        dims = {d["name"]: d for d in refined["dimensions"]}
        assert dims["title"]["description"] == "refined"
        assert "author" in dims
        assert {m["name"] for m in refined["measures"]} == {"count", "total_pages"}
        assert [g["name"] for g in refined["dimension_groups"]] == ["published"]

    def test_refinement_views_themselves_are_not_refined(
        self, project: LookMLProject
    ) -> None:
        raw_view: Dict = {"name": "+book", "dimensions": [{"name": "title"}]}

        assert project.refinement_resolver.apply_view_refinement(raw_view) is raw_view

    def test_disabled_refinement_processing_returns_raw_view(
        self, lookml_project_factory: Callable[..., LookMLProject]
    ) -> None:
        project = lookml_project_factory(process_refinements=False)
        project.write("book_refinement.view.lkml", BOOK_REFINEMENT)
        raw_view: Dict = {"name": "book", "dimensions": [{"name": "title"}]}

        refined = project.refinement_resolver.apply_view_refinement(raw_view)

        assert refined is raw_view
        assert [d["name"] for d in refined["dimensions"]] == ["title"]

    def test_view_without_matching_refinement_is_unchanged(
        self, project: LookMLProject
    ) -> None:
        project.write("book_refinement.view.lkml", BOOK_REFINEMENT)
        raw_view: Dict = {"name": "order", "dimensions": [{"name": "id"}]}

        refined = project.refinement_resolver.apply_view_refinement(raw_view)

        assert refined == raw_view


class TestApplyExploreRefinement:
    def test_extends_from_refinements_are_added_to_explore(
        self, project: LookMLProject
    ) -> None:
        project.model.explores = [
            {"name": "+orders", "extends": [["customers"]]},
            {"name": "+orders", "extends__all": [["products"]]},
            {"name": "+other_explore", "extends": [["ignored"]]},
        ]
        raw_explore: Dict = {"name": "orders"}

        refined = project.refinement_resolver.apply_explore_refinement(raw_explore)

        assert set(refined["extends"]) == {"customers", "products"}

    def test_explore_without_refinements_gets_no_extends(
        self, project: LookMLProject
    ) -> None:
        project.model.explores = [{"name": "orders"}]

        refined = project.refinement_resolver.apply_explore_refinement(
            {"name": "orders"}
        )

        assert "extends" not in refined
