import copy
import pathlib
from typing import List, Optional

from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver


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
