"""View upstream resolution over real LookML files: derived-view references,
native derived tables, SQL parse failures, and the Looker Query API path with
its regex-parsing fallback."""

import logging
import pathlib
from typing import Any, List
from unittest import mock

import pytest
from looker_sdk.sdk.api40.models import (
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    WriteQuery,
)

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.lookml_concept_context import LookerFieldContext
from datahub.ingestion.source.looker.lookml_config import LookMLSourceConfig
from datahub.ingestion.source.looker.view_upstream import (
    DerivedQueryUpstreamSource,
    LookerQueryAPIBasedViewUpstream,
    NativeDerivedViewUpstream,
    RegularViewUpstream,
    create_view_upstream,
)
from tests.unit.lookml.test_lookml_view_context import PARENT_VIEW, Harness


@pytest.fixture
def harness(tmp_path: pathlib.Path) -> Harness:
    base_folder = tmp_path / "project"
    base_folder.mkdir()
    config = LookMLSourceConfig.model_validate(
        {
            "base_folder": str(base_folder),
            "connection_to_platform_map": {"my_connection": "postgres"},
            "project_name": "test_project",
        }
    )
    return Harness(base_folder, config)


ORDERS_VIEW = """
view: orders {
  sql_table_name: analytics.orders ;;
  dimension: id { type: number sql: ${TABLE}.id ;; }
  dimension: amount { type: number sql: ${TABLE}.amount ;; }
  dimension_group: created {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.created_at ;;
  }
  dimension_group: since_signup {
    type: duration
    intervals: [day, week]
    sql_start: ${TABLE}.signup_at ;;
    sql_end: CURRENT_TIMESTAMP ;;
  }
  dimension_group: untyped {
    sql: ${TABLE}.untyped ;;
  }
  dimension_group: odd {
    type: bogus
    sql: ${TABLE}.odd ;;
  }
  measure: count { type: count }
}
"""

ORDERS_SQL = (
    'SELECT orders."ID" AS "orders.id", orders."AMOUNT" AS "orders.amount", '
    'orders."CREATED_AT" AS "orders.created_date" '
    "FROM public.orders AS orders"
)


def _dataset_urn(name: str, platform: str = "postgres") -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"


def _field(harness: Harness, lkml: str, view: str, field: str) -> LookerFieldContext:
    context = harness.context(lkml, view, file_name=f"{view}_{field}.view.lkml")
    raw = next(
        f
        for f in context.dimensions() + context.measures() + context.dimension_groups()
        if f["name"] == field
    )
    return LookerFieldContext(raw_field=raw)


def _upstream(harness: Harness, lkml: str, view: str, **kwargs: Any) -> Any:
    context = harness.context(lkml, view)
    return create_view_upstream(
        view_context=context,
        looker_view_id_cache=harness.view_id_cache,
        config=harness.config,
        ctx=PipelineContext(run_id="t"),
        reporter=harness.reporter,
        **kwargs,
    )


class TestDerivedViewReferences:
    def test_sql_table_name_reference_resolves_to_other_view_urn(
        self, harness: Harness
    ) -> None:
        harness.write("parent.view.lkml", PARENT_VIEW)
        upstream = _upstream(
            harness,
            """
            view: child {
              derived_table: {
                sql: SELECT id, shared FROM ${parent.SQL_TABLE_NAME} ;;
              }
              dimension: id { type: number sql: ${TABLE}.id ;; }
            }
            """,
            "child",
        )
        assert isinstance(upstream, DerivedQueryUpstreamSource)
        assert upstream.get_upstream_dataset_urn() == [
            _dataset_urn("test_project.view.parent", platform="looker")
        ]
        refs = upstream.get_upstream_column_ref(
            _field(harness, PARENT_VIEW, "parent", "id")
        )
        assert [(r.table, r.column) for r in refs] == [
            (_dataset_urn("test_project.view.parent", platform="looker"), "id")
        ]

    def test_unknown_view_reference_is_dropped(
        self, harness: Harness, caplog: pytest.LogCaptureFixture
    ) -> None:
        upstream = _upstream(
            harness,
            """
            view: child {
              derived_table: {
                sql: SELECT id FROM ${missing.SQL_TABLE_NAME} ;;
              }
              dimension: id { type: number sql: ${TABLE}.id ;; }
            }
            """,
            "child",
        )
        with caplog.at_level(logging.WARNING):
            assert upstream.get_upstream_dataset_urn() == []
            assert (
                upstream.get_upstream_column_ref(
                    _field(harness, ORDERS_VIEW, "orders", "id")
                )
                == []
            )
        assert "Not able to resolve to derived view looker id" in caplog.text


class TestNativeDerivedView:
    NATIVE = """
    view: order_summary {
      derived_table: {
        explore_source: orders {
          column: order_id { field: orders.id }
          column: total { field: orders.amount }
          column: plain {}
        }
      }
      dimension: order_id { type: number sql: ${TABLE}.order_id ;; }
      dimension: total { type: number sql: ${TABLE}.total ;; }
      dimension: plain { type: string sql: ${TABLE}.plain ;; }
      dimension: unmapped { type: string sql: ${TABLE}.unmapped ;; }
    }
    """

    def test_upstream_is_the_source_explore(self, harness: Harness) -> None:
        upstream = _upstream(harness, self.NATIVE, "order_summary")
        assert isinstance(upstream, NativeDerivedViewUpstream)
        explore_urn = _dataset_urn("data.explore.orders", platform="looker")
        assert upstream.get_upstream_dataset_urn() == [explore_urn]

        def refs(field: str) -> List[str]:
            return [
                r.column
                for r in upstream.get_upstream_column_ref(
                    _field(harness, self.NATIVE, "order_summary", field)
                )
            ]

        # `field:` maps the column onto the explore field; a bare column keeps its name.
        assert refs("order_id") == ["orders.id"]
        assert refs("plain") == ["plain"]
        assert refs("unmapped") == []

    def test_view_missing_from_cache_is_an_error(self, harness: Harness) -> None:
        context = harness.context(self.NATIVE, "order_summary")
        harness.model.resolved_includes.clear()
        upstream = NativeDerivedViewUpstream(
            view_context=context,
            looker_view_id_cache=harness.view_id_cache,
            config=harness.config,
            reporter=harness.reporter,
            ctx=PipelineContext(run_id="t"),
        )
        with pytest.raises(ValueError, match="not found"):
            upstream.get_upstream_dataset_urn()


class TestSqlParsingSwitches:
    DERIVED = """
    view: facts {
      derived_table: {
        sql: SELECT id, amount FROM analytics.orders ;;
      }
      dimension: id { type: number sql: ${TABLE}.id ;; }
    }
    """

    def test_disabled_sql_parsing_yields_nothing(self, tmp_path: pathlib.Path) -> None:
        base = tmp_path / "project"
        base.mkdir()
        config = LookMLSourceConfig.model_validate(
            {
                "base_folder": str(base),
                "connection_to_platform_map": {"my_connection": "postgres"},
                "project_name": "test_project",
                "parse_table_names_from_sql": False,
            }
        )
        harness = Harness(base, config)
        upstream = _upstream(harness, self.DERIVED, "facts")
        assert upstream.get_upstream_dataset_urn() == []
        assert upstream.create_fields() == []
        assert (
            upstream.get_upstream_column_ref(
                _field(harness, self.DERIVED, "facts", "id")
            )
            == []
        )

    def test_unparseable_sql_is_reported_without_lineage(
        self, harness: Harness
    ) -> None:
        upstream = _upstream(
            harness,
            """
            view: broken {
              derived_table: {
                sql: SELECT FROM WHERE ((( ;;
              }
              dimension: id { type: number sql: ${TABLE}.id ;; }
            }
            """,
            "broken",
        )
        assert upstream.get_upstream_dataset_urn() == []
        assert upstream.create_fields() == []
        assert (
            upstream.get_upstream_column_ref(
                _field(harness, self.DERIVED, "facts", "id")
            )
            == []
        )
        assert any(
            w.title == "Table Level Lineage Missing" for w in harness.reporter.warnings
        )


def _api_harness(tmp_path: pathlib.Path) -> Harness:
    base = tmp_path / "project"
    base.mkdir()
    config = LookMLSourceConfig.model_validate(
        {
            "base_folder": str(base),
            "connection_to_platform_map": {"my_connection": "postgres"},
            "project_name": "test_project",
            "use_api_for_view_lineage": True,
            "api": {
                "client_id": "id",
                "client_secret": "secret",
                "base_url": "https://looker.example.com",
            },
        }
    )
    return Harness(base, config)


def _api_explore() -> LookmlModelExplore:
    return LookmlModelExplore(
        name="orders_explore",
        fields=LookmlModelExploreFieldset(
            dimensions=[
                LookmlModelExploreField(name="orders.id", view="orders"),
                LookmlModelExploreField(
                    name="orders.created_date",
                    view="orders",
                    dimension_group="orders.created",
                ),
                LookmlModelExploreField(
                    name="orders.created_week",
                    view="orders",
                    dimension_group="orders.created",
                ),
                LookmlModelExploreField(name="customers.id", view="customers"),
                LookmlModelExploreField(name=None, view="orders"),
            ],
            measures=[
                LookmlModelExploreField(name="orders.count", view="orders"),
                LookmlModelExploreField(name="customers.count", view="customers"),
            ],
        ),
    )


class TestLookerQueryApiUpstream:
    def test_sql_from_api_drives_table_and_column_lineage(
        self, tmp_path: pathlib.Path
    ) -> None:
        harness = _api_harness(tmp_path)
        client = mock.MagicMock(spec=LookerAPI)
        client.lookml_model_explore.return_value = _api_explore()
        client.generate_sql_query.return_value = ORDERS_SQL

        upstream = _upstream(
            harness,
            ORDERS_VIEW,
            "orders",
            looker_client=client,
            view_to_explore_map={"orders": "orders_explore"},
        )
        assert isinstance(upstream, LookerQueryAPIBasedViewUpstream)
        assert upstream.get_upstream_dataset_urn() == [_dataset_urn("db.public.orders")]

        query: WriteQuery = client.generate_sql_query.call_args.kwargs["write_query"]
        assert query.view == "orders_explore"
        assert query.model == "data"
        # One field per dimension group, and only this view's fields.
        assert query.fields == ["orders.id", "orders.created_date", "orders.count"]

        id_refs = upstream.get_upstream_column_ref(
            _field(harness, ORDERS_VIEW, "orders", "id")
        )
        assert [(r.table, r.column) for r in id_refs] == [
            (_dataset_urn("db.public.orders"), "ID")
        ]
        created_refs = upstream.get_upstream_column_ref(
            _field(harness, ORDERS_VIEW, "orders", "created")
        )
        assert [r.column for r in created_refs] == ["CREATED_AT"]
        assert (
            upstream.get_upstream_column_ref(
                _field(harness, ORDERS_VIEW, "orders", "since_signup")
            )
            == []
        )
        fields = upstream.create_fields()
        assert sorted(f.name for f in fields) == ["amount", "created_date", "id"]
        assert harness.reporter.looker_query_api_latency_seconds

    def test_fields_fall_back_to_view_context_when_explore_lookup_fails(
        self, tmp_path: pathlib.Path
    ) -> None:
        harness = _api_harness(tmp_path)
        client = mock.MagicMock(spec=LookerAPI)
        client.lookml_model_explore.side_effect = RuntimeError("explore unavailable")
        client.generate_sql_query.return_value = ORDERS_SQL

        upstream = _upstream(
            harness,
            ORDERS_VIEW,
            "orders",
            looker_client=client,
            view_to_explore_map={"orders": "orders_explore"},
        )
        assert isinstance(upstream, LookerQueryAPIBasedViewUpstream)
        query: WriteQuery = client.generate_sql_query.call_args.kwargs["write_query"]
        # Time groups use the first timeframe, duration groups the first interval;
        # untyped or unknown dimension groups are skipped.
        assert query.fields == [
            "orders.id",
            "orders.amount",
            "orders.count",
            "orders.created_date",
            "orders.days_since_signup",
        ]

    def test_api_failure_falls_back_to_regex_parsing(
        self, tmp_path: pathlib.Path
    ) -> None:
        harness = _api_harness(tmp_path)
        client = mock.MagicMock(spec=LookerAPI)
        client.lookml_model_explore.return_value = _api_explore()
        client.generate_sql_query.return_value = ""

        upstream = _upstream(
            harness,
            ORDERS_VIEW,
            "orders",
            looker_client=client,
            view_to_explore_map={"orders": "orders_explore"},
        )
        assert isinstance(upstream, RegularViewUpstream)
        assert upstream.get_upstream_dataset_urn() == [
            _dataset_urn("db.analytics.orders")
        ]
        assert any(
            w.title == "Looker Query API based View Upstream Failed"
            for w in harness.reporter.warnings
        )

    def test_view_not_in_explore_map_skips_api(self, tmp_path: pathlib.Path) -> None:
        harness = _api_harness(tmp_path)
        client = mock.MagicMock(spec=LookerAPI)
        upstream = _upstream(
            harness,
            ORDERS_VIEW,
            "orders",
            looker_client=client,
            view_to_explore_map={"other": "other_explore"},
        )
        assert isinstance(upstream, RegularViewUpstream)
        client.generate_sql_query.assert_not_called()


class TestFieldSplitting:
    def _harness(self, tmp_path: pathlib.Path, threshold: int = 1) -> Harness:
        base = tmp_path / "project"
        base.mkdir()
        config = LookMLSourceConfig.model_validate(
            {
                "base_folder": str(base),
                "connection_to_platform_map": {"my_connection": "postgres"},
                "project_name": "test_project",
                "use_api_for_view_lineage": True,
                "field_threshold_for_splitting": threshold,
                "max_workers_for_parallel_processing": 2,
                "api": {
                    "client_id": "id",
                    "client_secret": "secret",
                    "base_url": "https://looker.example.com",
                },
            }
        )
        return Harness(base, config)

    SMALL_VIEW = """
    view: orders {
      sql_table_name: analytics.orders ;;
      dimension: id { type: number sql: ${TABLE}.id ;; }
      dimension: amount { type: number sql: ${TABLE}.amount ;; }
    }
    """

    @staticmethod
    def _sql_for(query: WriteQuery, broken: str) -> str:
        fields = list(query.fields or [])
        if broken in fields:
            return "SELECT ((( FROM"
        column = fields[0].split(".")[1]
        return f'SELECT orders."{column.upper()}" AS "{fields[0]}" FROM public.orders AS orders'

    def test_chunks_are_combined_and_failures_are_isolated(
        self, tmp_path: pathlib.Path
    ) -> None:
        harness = self._harness(tmp_path)
        client = mock.MagicMock(spec=LookerAPI)
        client.lookml_model_explore.side_effect = RuntimeError("no explore")
        client.generate_sql_query.side_effect = lambda write_query, use_cache: (
            self._sql_for(write_query, "orders.amount")
        )

        upstream = _upstream(
            harness,
            self.SMALL_VIEW,
            "orders",
            looker_client=client,
            view_to_explore_map={"orders": "orders_explore"},
        )
        assert isinstance(upstream, LookerQueryAPIBasedViewUpstream)
        assert upstream.get_upstream_dataset_urn() == [_dataset_urn("db.public.orders")]
        assert [f.name for f in upstream.create_fields()] == ["id"]
        titles = [w.title for w in harness.reporter.warnings]
        assert "Problematic Fields Identified in LookMl View Processing" in titles
        assert "Field Splitting Statistics - Partial Success" in titles

    def test_all_chunks_failing_falls_back_to_regex_parsing(
        self, tmp_path: pathlib.Path
    ) -> None:
        harness = self._harness(tmp_path)
        client = mock.MagicMock(spec=LookerAPI)
        client.lookml_model_explore.side_effect = RuntimeError("no explore")
        client.generate_sql_query.side_effect = RuntimeError("api down")

        upstream = _upstream(
            harness,
            self.SMALL_VIEW,
            "orders",
            looker_client=client,
            view_to_explore_map={"orders": "orders_explore"},
        )
        assert isinstance(upstream, RegularViewUpstream)
        titles = [w.title for w in harness.reporter.warnings]
        assert "Field Chunk Processing Failed" in titles
        assert "Looker Query API based View Upstream Failed" in titles
