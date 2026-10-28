import pathlib
from typing import Dict, List

import pytest

from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import (
    LookerModel,
    ProjectInclude,
)
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver

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


def _write(path: pathlib.Path, content: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return str(path)


@pytest.fixture
def base_folder(tmp_path: pathlib.Path) -> pathlib.Path:
    folder = tmp_path / "project"
    folder.mkdir()
    return folder


@pytest.fixture
def connection() -> LookerConnectionDefinition:
    return LookerConnectionDefinition(
        platform="postgres", default_db="db", default_schema="public"
    )


def _make_resolver(
    base_folder: pathlib.Path,
    connection: LookerConnectionDefinition,
    resolved_includes: List[ProjectInclude],
    process_refinements: bool = True,
) -> LookerRefinementResolver:
    config = LookMLSourceConfig.model_validate(
        {
            "base_folder": str(base_folder),
            "connection_to_platform_map": {"my_connection": "postgres"},
            "project_name": "test_project",
            "process_refinements": process_refinements,
        }
    )
    reporter = LookMLSourceReport()
    loader = LookerViewFileLoader(
        root_project_name="test_project",
        base_projects_folder={BASE_PROJECT_NAME: base_folder},
        reporter=reporter,
        source_config=config,
    )
    model = LookerModel(
        connection="my_connection",
        includes=[],
        explores=[],
        resolved_includes=resolved_includes,
    )
    return LookerRefinementResolver(
        looker_model=model,
        looker_viewfile_loader=loader,
        connection_definition=connection,
        source_config=config,
        reporter=reporter,
    )


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
        self, base_folder: pathlib.Path, connection: LookerConnectionDefinition
    ) -> None:
        book_path = _write(base_folder / "book.view.lkml", BOOK_VIEW)
        refinement_path = _write(
            base_folder / "book_refinement.view.lkml", BOOK_REFINEMENT
        )
        resolver = _make_resolver(
            base_folder,
            connection,
            [
                ProjectInclude(BASE_PROJECT_NAME, book_path),
                ProjectInclude(BASE_PROJECT_NAME, refinement_path),
            ],
        )
        raw_view: Dict = {
            "name": "book",
            "dimensions": [{"name": "id"}, {"name": "title", "type": "string"}],
            "measures": [{"name": "count"}],
        }

        refined = resolver.apply_view_refinement(raw_view)

        dims = {d["name"]: d for d in refined["dimensions"]}
        assert dims["title"]["description"] == "refined"
        assert "author" in dims
        assert {m["name"] for m in refined["measures"]} == {"count", "total_pages"}
        assert [g["name"] for g in refined["dimension_groups"]] == ["published"]

    def test_refinement_views_themselves_are_not_refined(
        self, base_folder: pathlib.Path, connection: LookerConnectionDefinition
    ) -> None:
        resolver = _make_resolver(base_folder, connection, [])
        raw_view: Dict = {"name": "+book", "dimensions": [{"name": "title"}]}

        assert resolver.apply_view_refinement(raw_view) is raw_view

    def test_disabled_refinement_processing_returns_raw_view(
        self, base_folder: pathlib.Path, connection: LookerConnectionDefinition
    ) -> None:
        refinement_path = _write(
            base_folder / "book_refinement.view.lkml", BOOK_REFINEMENT
        )
        resolver = _make_resolver(
            base_folder,
            connection,
            [ProjectInclude(BASE_PROJECT_NAME, refinement_path)],
            process_refinements=False,
        )
        raw_view: Dict = {"name": "book", "dimensions": [{"name": "title"}]}

        refined = resolver.apply_view_refinement(raw_view)

        assert refined is raw_view
        assert [d["name"] for d in refined["dimensions"]] == ["title"]

    def test_view_without_matching_refinement_is_unchanged(
        self, base_folder: pathlib.Path, connection: LookerConnectionDefinition
    ) -> None:
        refinement_path = _write(
            base_folder / "book_refinement.view.lkml", BOOK_REFINEMENT
        )
        resolver = _make_resolver(
            base_folder,
            connection,
            [ProjectInclude(BASE_PROJECT_NAME, refinement_path)],
        )
        raw_view: Dict = {"name": "order", "dimensions": [{"name": "id"}]}

        refined = resolver.apply_view_refinement(raw_view)

        assert refined == raw_view


class TestApplyExploreRefinement:
    def test_extends_from_refinements_are_added_to_explore(
        self, base_folder: pathlib.Path, connection: LookerConnectionDefinition
    ) -> None:
        resolver = _make_resolver(base_folder, connection, [])
        resolver.looker_model.explores = [
            {"name": "+orders", "extends": [["customers"]]},
            {"name": "+orders", "extends__all": [["products"]]},
            {"name": "+other_explore", "extends": [["ignored"]]},
        ]
        raw_explore: Dict = {"name": "orders"}

        refined = resolver.apply_explore_refinement(raw_explore)

        assert set(refined["extends"]) == {"customers", "products"}

    def test_explore_without_refinements_gets_no_extends(
        self, base_folder: pathlib.Path, connection: LookerConnectionDefinition
    ) -> None:
        resolver = _make_resolver(base_folder, connection, [])
        resolver.looker_model.explores = [{"name": "orders"}]

        refined = resolver.apply_explore_refinement({"name": "orders"})

        assert "extends" not in refined
