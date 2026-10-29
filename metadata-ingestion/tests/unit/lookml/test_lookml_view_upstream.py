"""View upstream resolution over real LookML files: derived-view references,
native derived tables, SQL parse failures, and the Looker Query API path with
field splitting and its regex-parsing fallback.

Only the ``LookerAPI`` wrapper is mocked on the API path; the SQL it returns is
parsed by the real sqlglot-based lineage parser."""

import logging
from typing import Callable, List
from unittest import mock

import pytest
from looker_sdk.sdk.api40.models import (
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    WriteQuery,
)

from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.view_upstream import (
    DerivedQueryUpstreamSource,
    LookerQueryAPIBasedViewUpstream,
    NativeDerivedViewUpstream,
    RegularViewUpstream,
)
from tests.unit.lookml.conftest import LOOKER_API_CONFIG, LookMLProject, dataset_urn

PARENT_VIEW = """
view: parent {
  sql_table_name: analytics.parent_table ;;
  dimension: id { type: number sql: ${TABLE}.id ;; }
  dimension: shared { type: string sql: ${TABLE}.parent_shared ;; }
}
"""

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
  dimension_group: updated {
    type: time
    sql: ${TABLE}.updated_at ;;
  }
  dimension_group: since_signup {
    type: duration
    intervals: [day, week]
    sql_start: ${TABLE}.signup_at ;;
    sql_end: CURRENT_TIMESTAMP ;;
  }
  dimension_group: since_order {
    type: duration
    sql_start: ${TABLE}.ordered_at ;;
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

# The alias "note" carries no "orders." prefix, which is how a column the API
# attributes to another view comes back.
ORDERS_SQL = (
    'SELECT orders."ID" AS "orders.id", orders."AMOUNT" AS "orders.amount", '
    'orders."CREATED_AT" AS "orders.created_date", orders."NOTE" AS "note" '
    "FROM public.orders AS orders"
)

SMALL_VIEW = """
view: orders {
  sql_table_name: analytics.orders ;;
  dimension: id { type: number sql: ${TABLE}.id ;; }
  dimension: amount { type: number sql: ${TABLE}.amount ;; }
}
"""

EXPLORE_MAP = {"orders": "orders_explore"}


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


def _api_client(sql: object = ORDERS_SQL) -> mock.MagicMock:
    """A LookerAPI whose explore lookup fails (so fields come from the view
    context) and whose SQL generation returns ``sql`` (or raises it)."""
    client = mock.MagicMock(spec=LookerAPI)
    client.lookml_model_explore.side_effect = RuntimeError("explore unavailable")
    if isinstance(sql, BaseException) or callable(sql):
        client.generate_sql_query.side_effect = sql
    else:
        client.generate_sql_query.return_value = sql
    return client


@pytest.fixture
def api_project_factory(
    lookml_project_factory: Callable[..., LookMLProject],
) -> Callable[..., LookMLProject]:
    def build(**overrides: object) -> LookMLProject:
        return lookml_project_factory(
            use_api_for_view_lineage=True, api=LOOKER_API_CONFIG, **overrides
        )

    return build


class TestDerivedViewReferences:
    def test_sql_table_name_reference_resolves_to_other_view_urn(
        self, lookml_project: LookMLProject
    ) -> None:
        lookml_project.write("parent.view.lkml", PARENT_VIEW)
        upstream = lookml_project.upstream(
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
        parent_urn = dataset_urn("test_project.view.parent", platform="looker")
        assert upstream.get_upstream_dataset_urn() == [parent_urn]
        refs = upstream.get_upstream_column_ref(
            lookml_project.field(PARENT_VIEW, "parent", "id")
        )
        assert [(r.table, r.column) for r in refs] == [(parent_urn, "id")]

    def test_unknown_view_reference_is_dropped(
        self, lookml_project: LookMLProject, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A ``${view.SQL_TABLE_NAME}`` pointing at a view that is not in any
        included file yields no lineage at all rather than a dangling URN."""
        upstream = lookml_project.upstream(
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
                    lookml_project.field(ORDERS_VIEW, "orders", "id")
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

    def test_upstream_is_the_source_explore(
        self, lookml_project: LookMLProject
    ) -> None:
        upstream = lookml_project.upstream(self.NATIVE, "order_summary")
        assert isinstance(upstream, NativeDerivedViewUpstream)
        explore_urn = dataset_urn("data.explore.orders", platform="looker")
        assert upstream.get_upstream_dataset_urn() == [explore_urn]

        def refs(field: str) -> List[str]:
            return [
                r.column
                for r in upstream.get_upstream_column_ref(
                    lookml_project.field(self.NATIVE, "order_summary", field)
                )
            ]

        # `field:` maps the column onto the explore field; a bare column keeps its name.
        assert refs("order_id") == ["orders.id"]
        assert refs("plain") == ["plain"]
        assert refs("unmapped") == []

    def test_view_missing_from_cache_is_an_error(
        self, lookml_project: LookMLProject
    ) -> None:
        upstream = lookml_project.upstream(self.NATIVE, "order_summary")
        lookml_project.model.resolved_includes.clear()
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

    def test_disabled_sql_parsing_yields_nothing(
        self, lookml_project_factory: Callable[..., LookMLProject]
    ) -> None:
        project = lookml_project_factory(parse_table_names_from_sql=False)
        upstream = project.upstream(self.DERIVED, "facts")
        assert upstream.get_upstream_dataset_urn() == []
        assert upstream.create_fields() == []
        assert (
            upstream.get_upstream_column_ref(project.field(self.DERIVED, "facts", "id"))
            == []
        )

    def test_unparseable_sql_is_reported_without_lineage(
        self, lookml_project: LookMLProject
    ) -> None:
        upstream = lookml_project.upstream(
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
                lookml_project.field(self.DERIVED, "facts", "id")
            )
            == []
        )
        assert "Table Level Lineage Missing" in lookml_project.warning_titles()


class TestLookerQueryApiUpstream:
    def test_sql_from_api_drives_table_and_column_lineage(
        self, api_project_factory: Callable[..., LookMLProject]
    ) -> None:
        project = api_project_factory()
        client = _api_client()
        client.lookml_model_explore.side_effect = None
        client.lookml_model_explore.return_value = _api_explore()

        upstream = project.upstream(
            ORDERS_VIEW, "orders", looker_client=client, view_to_explore_map=EXPLORE_MAP
        )
        assert isinstance(upstream, LookerQueryAPIBasedViewUpstream)
        assert upstream.get_upstream_dataset_urn() == [dataset_urn("db.public.orders")]

        query: WriteQuery = client.generate_sql_query.call_args.kwargs["write_query"]
        assert query.view == "orders_explore"
        assert query.model == "data"
        assert query.limit == "1"
        # One field per dimension group, and only this view's fields.
        assert query.fields == ["orders.id", "orders.created_date", "orders.count"]

        id_refs = upstream.get_upstream_column_ref(
            project.field(ORDERS_VIEW, "orders", "id")
        )
        assert [(r.table, r.column) for r in id_refs] == [
            (dataset_urn("db.public.orders"), "ID")
        ]
        created_refs = upstream.get_upstream_column_ref(
            project.field(ORDERS_VIEW, "orders", "created")
        )
        assert [r.column for r in created_refs] == ["CREATED_AT"]
        assert (
            upstream.get_upstream_column_ref(
                project.field(ORDERS_VIEW, "orders", "since_signup")
            )
            == []
        )
        # Aliases prefixed with the view name are unqualified; others are kept whole.
        assert sorted(f.name for f in upstream.create_fields()) == [
            "amount",
            "created_date",
            "id",
            "note",
        ]
        assert project.reporter.looker_query_api_latency_seconds

    def test_fields_fall_back_to_view_context_when_explore_lookup_fails(
        self, api_project_factory: Callable[..., LookMLProject]
    ) -> None:
        """Without explore metadata the query is built from the view's own
        fields: time groups use their first timeframe (``_raw`` when none is
        declared), duration groups their first interval (days by default), and
        untyped or unknown dimension groups are skipped."""
        project = api_project_factory()
        client = _api_client()

        upstream = project.upstream(
            ORDERS_VIEW, "orders", looker_client=client, view_to_explore_map=EXPLORE_MAP
        )
        assert isinstance(upstream, LookerQueryAPIBasedViewUpstream)
        query: WriteQuery = client.generate_sql_query.call_args.kwargs["write_query"]
        assert query.fields == [
            "orders.id",
            "orders.amount",
            "orders.count",
            "orders.created_date",
            "orders.updated_raw",
            "orders.days_since_signup",
            "orders.days_since_order",
        ]

    def test_empty_sql_response_falls_back_to_regex_parsing(
        self, api_project_factory: Callable[..., LookMLProject]
    ) -> None:
        project = api_project_factory()
        client = _api_client(sql="")

        upstream = project.upstream(
            ORDERS_VIEW, "orders", looker_client=client, view_to_explore_map=EXPLORE_MAP
        )
        assert isinstance(upstream, RegularViewUpstream)
        assert upstream.get_upstream_dataset_urn() == [
            dataset_urn("db.analytics.orders")
        ]
        assert "Looker Query API based View Upstream Failed" in project.warning_titles()

    def test_unparseable_sql_keeps_api_upstream_when_partial_results_allowed(
        self, api_project_factory: Callable[..., LookMLProject]
    ) -> None:
        """``allow_partial_lineage_results`` (the default) turns a SQL parse
        failure into a warning plus empty lineage instead of a fallback."""
        project = api_project_factory()
        client = _api_client(sql="SELECT ((( FROM")

        upstream = project.upstream(
            ORDERS_VIEW, "orders", looker_client=client, view_to_explore_map=EXPLORE_MAP
        )
        assert isinstance(upstream, LookerQueryAPIBasedViewUpstream)
        assert upstream.get_upstream_dataset_urn() == []
        assert upstream.create_fields() == []
        assert "Table Level Lineage Extraction Failed" in project.warning_titles()

    def test_unparseable_sql_falls_back_to_regex_when_partial_results_disallowed(
        self, api_project_factory: Callable[..., LookMLProject]
    ) -> None:
        project = api_project_factory(allow_partial_lineage_results=False)
        client = _api_client(sql="SELECT ((( FROM")

        upstream = project.upstream(
            ORDERS_VIEW, "orders", looker_client=client, view_to_explore_map=EXPLORE_MAP
        )
        assert isinstance(upstream, RegularViewUpstream)
        titles = project.warning_titles()
        assert "Table Level Lineage Extraction Failed" in titles
        assert "Looker Query API based View Upstream Failed" in titles

    def test_view_not_in_explore_map_skips_api(
        self, api_project_factory: Callable[..., LookMLProject]
    ) -> None:
        project = api_project_factory()
        client = _api_client()
        upstream = project.upstream(
            ORDERS_VIEW,
            "orders",
            looker_client=client,
            view_to_explore_map={"other": "other_explore"},
        )
        assert isinstance(upstream, RegularViewUpstream)
        client.generate_sql_query.assert_not_called()

    def test_explore_fields_are_fetched_once_per_explore(self) -> None:
        """Explore metadata is cached process-wide keyed on (client, model,
        explore) because many views share one explore."""
        client = mock.MagicMock(spec=LookerAPI)
        client.lookml_model_explore.side_effect = lambda model, explore, fields=None: (
            LookmlModelExplore(name=explore)
        )
        fetch = LookerQueryAPIBasedViewUpstream.get_explore_fields_from_looker_api

        first = fetch(client, "data", "orders_explore")
        assert fetch(client, "data", "orders_explore") is first
        assert fetch(client, "data", "other_explore").name == "other_explore"
        assert client.lookml_model_explore.call_count == 2


class TestFieldSplitting:
    @pytest.fixture
    def project(
        self, api_project_factory: Callable[..., LookMLProject]
    ) -> LookMLProject:
        return api_project_factory(
            field_threshold_for_splitting=1, max_workers_for_parallel_processing=2
        )

    @staticmethod
    def _sql_for(query: WriteQuery, broken: str = "") -> str:
        fields = list(query.fields or [])
        if broken and broken in fields:
            return "SELECT ((( FROM"
        column = fields[0].split(".")[1]
        return f'SELECT orders."{column.upper()}" AS "{fields[0]}" FROM public.orders AS orders'

    def test_all_chunks_succeeding_reports_complete_success(
        self, project: LookMLProject
    ) -> None:
        client = _api_client(
            sql=lambda write_query, use_cache: self._sql_for(write_query)
        )

        upstream = project.upstream(
            SMALL_VIEW, "orders", looker_client=client, view_to_explore_map=EXPLORE_MAP
        )
        assert isinstance(upstream, LookerQueryAPIBasedViewUpstream)
        assert client.generate_sql_query.call_count == 2
        assert upstream.get_upstream_dataset_urn() == [dataset_urn("db.public.orders")]
        assert sorted(f.name for f in upstream.create_fields()) == ["amount", "id"]
        assert "Field Splitting Statistics - Complete Success" in [
            i.title for i in project.reporter.infos
        ]

    def test_chunks_are_combined_and_failures_are_isolated(
        self, project: LookMLProject
    ) -> None:
        """One chunk's SQL failing to parse costs only that chunk's fields; the
        rest of the view still gets lineage from the chunks that succeeded."""
        client = _api_client(
            sql=lambda write_query, use_cache: self._sql_for(
                write_query, broken="orders.amount"
            )
        )

        upstream = project.upstream(
            SMALL_VIEW, "orders", looker_client=client, view_to_explore_map=EXPLORE_MAP
        )
        assert isinstance(upstream, LookerQueryAPIBasedViewUpstream)
        assert upstream.get_upstream_dataset_urn() == [dataset_urn("db.public.orders")]
        assert [f.name for f in upstream.create_fields()] == ["id"]
        titles = project.warning_titles()
        assert "Problematic Fields Identified in LookMl View Processing" in titles
        assert "Field Splitting Statistics - Partial Success" in titles

    def test_all_chunks_failing_falls_back_to_regex_parsing(
        self, project: LookMLProject
    ) -> None:
        client = _api_client(sql=RuntimeError("api down"))

        upstream = project.upstream(
            SMALL_VIEW, "orders", looker_client=client, view_to_explore_map=EXPLORE_MAP
        )
        assert isinstance(upstream, RegularViewUpstream)
        titles = project.warning_titles()
        assert "Field Chunk Processing Failed" in titles
        assert "Looker Query API based View Upstream Failed" in titles
