"""LookML refinements (``view: +name``) merged into views and explores."""

import copy
import itertools
import pathlib
from typing import Callable, Dict, List, Optional

import pytest

from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    LookMLSourceConfig,
    LookMLSourceReport,
)
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

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Bug: add_extended_explore replaces the explore's own extends / "
            "extends__all with only the refinements' explores, and writes them as "
            "a flat list while LookerExplore.from_dict flattens one more level, so "
            "the extended explore names are read character by character."
        ),
    )
    def test_refinement_extends_add_to_the_explores_own_extends(
        self, project: LookMLProject
    ) -> None:
        """Refinement `extends` is additive: the explore keeps what it already
        extends, and LookerExplore.from_dict reads the merged names intact."""
        project.model.explores = [
            {"name": "+orders", "extends": [["customers"]]},
            {"name": "+orders", "extends__all": [["products"]]},
        ]
        raw_explore: Dict = {"name": "orders", "extends__all": [["base_orders"]]}

        refined = project.refinement_resolver.apply_explore_refinement(raw_explore)

        # The same lookup LookerExplore.from_dict does.
        resolved = itertools.chain.from_iterable(
            refined.get("extends", refined.get("extends__all", []))
        )
        assert set(resolved) == {"base_orders", "customers", "products"}

    def test_explore_without_refinements_gets_no_extends(
        self, project: LookMLProject
    ) -> None:
        project.model.explores = [{"name": "orders"}]

        refined = project.refinement_resolver.apply_explore_refinement(
            {"name": "orders"}
        )

        assert "extends" not in refined


def test_refinement_overrides_view_parameters() -> None:
    raw_view: dict = {
        "name": "flights",
        "sql_table_name": "flightstats.accidents",
        "drill_fields": ["id"],
        "dimensions": [{"name": "id", "type": "number", "sql": "${TABLE}.id"}],
    }
    refinement_views: List[dict] = [
        {
            "name": "+flights",
            "sql_table_name": "flightstats.flights_v1",
            "drill_fields": ["carrier"],
            "dimensions": [{"name": "id", "type": "string"}],
        },
        {"name": "+flights", "sql_table_name": "flightstats.flights_v2"},
    ]

    merged_view = LookerRefinementResolver.merge_refinements(
        raw_view=raw_view, refinement_views=refinement_views
    )

    assert merged_view == {
        "name": "flights",
        # the last refinement wins
        "sql_table_name": "flightstats.flights_v2",
        "drill_fields": ["carrier"],
        "dimensions": [{"name": "id", "type": "string", "sql": "${TABLE}.id"}],
    }


def test_refinement_merges_derived_table_sub_parameters() -> None:
    raw_view: dict = {
        "name": "order_summary",
        "derived_table": {"sql": "SELECT customer_id FROM orders"},
    }
    refinement_views: List[dict] = [
        {
            "name": "+order_summary",
            "derived_table": {"datagroup_trigger": "daily"},
        }
    ]

    merged_view = LookerRefinementResolver.merge_refinements(
        raw_view=raw_view, refinement_views=refinement_views
    )

    assert merged_view["derived_table"] == {
        "sql": "SELECT customer_id FROM orders",
        "datagroup_trigger": "daily",
    }


def test_refinement_extends_is_not_applied() -> None:
    raw_view: dict = {"name": "orders", "extends__all": [["base_orders"]]}
    refinement_views: List[dict] = [
        {"name": "+orders", "extends__all": [["audit_mixin"]]}
    ]

    merged_view = LookerRefinementResolver.merge_refinements(
        raw_view=raw_view, refinement_views=refinement_views
    )

    assert merged_view == raw_view


def test_merge_refinements_does_not_mutate_inputs() -> None:
    raw_view: dict = {
        "name": "orders",
        "derived_table": {"sql": "SELECT 1"},
        "dimensions": [{"name": "id", "type": "number"}],
    }
    refinement_views: List[dict] = [
        {
            "name": "+orders",
            "derived_table": {"persist_for": "24 hours"},
            "dimensions": [{"name": "status", "type": "string"}],
        },
        {
            "name": "+orders",
            "dimensions": [{"name": "status", "sql": "${TABLE}.status"}],
        },
    ]
    raw_view_before = copy.deepcopy(raw_view)
    refinement_views_before = copy.deepcopy(refinement_views)

    first = LookerRefinementResolver.merge_refinements(raw_view, refinement_views)
    second = LookerRefinementResolver.merge_refinements(raw_view, refinement_views)

    assert raw_view == raw_view_before
    assert refinement_views == refinement_views_before
    assert first == second


def test_viewfile_loader_any_lkml_file(tmp_path: pathlib.Path) -> None:
    for file_name in [
        "flights.view.lkml",
        "flights.layer.lkml",
        "flights.model.lkml",
        "flights.dashboard.lkml",
    ]:
        (tmp_path / file_name).write_text('view: +flights { label: "Flights" }')

    source_config = LookMLSourceConfig.model_validate(
        {
            "base_folder": str(tmp_path),
            "api": {
                "base_url": "fake",
                "client_id": "fake_client_id",
                "client_secret": "fake_client_secret",
            },
        }
    )
    reporter = LookMLSourceReport()
    loader = LookerViewFileLoader(
        root_project_name=None,
        base_projects_folder={BASE_PROJECT_NAME: tmp_path},
        reporter=reporter,
        source_config=source_config,
    )

    def loaded_views(file_name: str, allow_any_lkml_file: bool) -> Optional[List[dict]]:
        viewfile = loader.load_viewfile(
            path=str(tmp_path / file_name),
            project_name=BASE_PROJECT_NAME,
            connection=None,
            reporter=reporter,
            allow_any_lkml_file=allow_any_lkml_file,
        )
        return viewfile.views if viewfile else None

    # Views are only read from view files, unless any .lkml file is allowed
    assert loaded_views("flights.view.lkml", allow_any_lkml_file=False)
    assert loaded_views("flights.layer.lkml", allow_any_lkml_file=False) is None
    assert loaded_views("flights.layer.lkml", allow_any_lkml_file=True)
    # Model and dashboard files hold no views
    assert loaded_views("flights.model.lkml", allow_any_lkml_file=True) is None
    assert loaded_views("flights.dashboard.lkml", allow_any_lkml_file=True) is None
    assert reporter.warnings.total_elements == 0
