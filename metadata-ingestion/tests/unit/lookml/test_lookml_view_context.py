import pathlib
from typing import Dict, List, Optional

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_common import (
    ViewFieldType,
    ViewFieldValue,
)
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import (
    LookerModel,
    LookerViewFile,
    ProjectInclude,
)
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.looker_template_language import (
    load_and_preprocess_file,
)
from datahub.ingestion.source.looker.looker_view_id_cache import LookerViewIdCache
from datahub.ingestion.source.looker.lookml_concept_context import (
    LookerFieldContext,
    LookerViewContext,
)
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver
from datahub.ingestion.source.looker.lookml_source import LookerView
from datahub.ingestion.source.looker.view_upstream import (
    AbstractViewUpstream,
    DerivedQueryUpstreamSource,
    DirectQueryUpstreamSource,
    DotSqlTableNameViewUpstream,
    NativeDerivedViewUpstream,
    RegularViewUpstream,
    create_view_upstream,
)

PARENT_VIEW = """
view: parent {
  sql_table_name: analytics.parent_table ;;
  dimension: id { type: number sql: ${TABLE}.id ;; }
  dimension: shared { type: string sql: ${TABLE}.parent_shared ;; }
  measure: count { type: count }
}
"""


class Harness:
    """Real loader, refinement resolver and view-id cache over a tmp project."""

    def __init__(self, base_folder: pathlib.Path, config: LookMLSourceConfig):
        self.base_folder = base_folder
        self.config = config
        self.reporter = LookMLSourceReport()
        self.connection = LookerConnectionDefinition(
            platform="postgres", default_db="db", default_schema="public"
        )
        self.loader = LookerViewFileLoader(
            root_project_name="test_project",
            base_projects_folder={BASE_PROJECT_NAME: base_folder},
            reporter=self.reporter,
            source_config=config,
        )
        self.model = LookerModel(
            connection="my_connection", includes=[], explores=[], resolved_includes=[]
        )
        self.refinement_resolver = LookerRefinementResolver(
            looker_model=self.model,
            looker_viewfile_loader=self.loader,
            connection_definition=self.connection,
            source_config=config,
            reporter=self.reporter,
        )
        self.view_id_cache = LookerViewIdCache(
            project_name="test_project",
            model_name="data",
            looker_model=self.model,
            looker_viewfile_loader=self.loader,
            reporter=self.reporter,
        )

    def write(self, name: str, content: str) -> str:
        path = self.base_folder / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        self.model.resolved_includes.append(
            ProjectInclude(BASE_PROJECT_NAME, str(path))
        )
        return str(path)

    def context(
        self, lkml: str, view_name: str, file_name: Optional[str] = None
    ) -> LookerViewContext:
        path = self.write(file_name or f"{view_name}.view.lkml", lkml)
        parsed = load_and_preprocess_file(
            path=path, source_config=self.config, reporter=self.reporter
        )
        view_file = LookerViewFile.from_looker_dict(
            absolute_file_path=path,
            looker_view_file_dict=parsed,
            project_name=BASE_PROJECT_NAME,
            root_project_name="test_project",
            base_projects_folder={BASE_PROJECT_NAME: self.base_folder},
            raw_file_content=lkml,
            source_config=self.config,
            reporter=self.reporter,
        )
        raw_view = next(v for v in view_file.views if v["name"] == view_name)
        return LookerViewContext(
            raw_view=raw_view,
            view_file=view_file,
            view_connection=self.connection,
            view_file_loader=self.loader,
            looker_refinement_resolver=self.refinement_resolver,
            base_folder_path=str(self.base_folder),
            reporter=self.reporter,
        )


@pytest.fixture
def harness(tmp_path: pathlib.Path) -> Harness:
    base_folder = tmp_path / "project"
    base_folder.mkdir()
    config = LookMLSourceConfig.model_validate(
        {
            "base_folder": str(base_folder),
            "connection_to_platform_map": {"my_connection": "postgres"},
            "project_name": "test_project",
            "liquid_variables": {"region": "emea"},
        }
    )
    return Harness(base_folder, config)


class TestLookerFieldContext:
    def test_field_without_sql_maps_to_its_own_name(self) -> None:
        assert LookerFieldContext({"name": "id"}).column_name_in_sql_attribute() == [
            "id"
        ]

    def test_extracts_every_table_column_from_expression(self) -> None:
        field = LookerFieldContext(
            {"name": "profit", "sql": "${TABLE}.Revenue - ${TABLE}.cost"}
        )

        assert field.column_name_in_sql_attribute() == ["revenue", "cost"]

    def test_strips_quotes_from_column_names(self) -> None:
        field = LookerFieldContext({"name": "grp", "sql": '${TABLE}."group"'})

        assert field.column_name_in_sql_attribute() == ["group"]

    def test_sql_without_table_reference_yields_no_columns(self) -> None:
        field = LookerFieldContext({"name": "const", "sql": "1"})

        assert field.column_name_in_sql_attribute() == []


class TestViewPatternClassification:
    def test_view_without_sql_table_name_defaults_to_view_name(
        self, harness: Harness
    ) -> None:
        ctx = harness.context("view: orders { dimension: id {} }", "orders")

        assert ctx.is_regular_case()
        assert ctx.sql_table_name() == "orders"
        assert ctx.datahub_transformed_sql_table_name() == "orders"

    def test_quoted_sql_table_name_is_cleaned(self, harness: Harness) -> None:
        ctx = harness.context(
            'view: orders { sql_table_name: "Analytics"."Orders" ;; }', "orders"
        )

        assert ctx.is_regular_case()
        assert ctx.datahub_transformed_sql_table_name() == "analytics.orders"

    def test_liquid_in_sql_table_name_is_resolved(self, harness: Harness) -> None:
        ctx = harness.context(
            "view: orders { sql_table_name: {{ region }}_schema.orders ;; }", "orders"
        )

        assert ctx.datahub_transformed_sql_table_name() == "emea_schema.orders"

    def test_sql_table_name_referring_to_another_view(self, harness: Harness) -> None:
        ctx = harness.context(
            "view: orders { sql_table_name: ${base_orders.SQL_TABLE_NAME} ;; }",
            "orders",
        )

        assert ctx.is_sql_table_name_referring_to_view()
        assert not ctx.is_regular_case()
        assert ctx.datahub_transformed_sql_table_name() == "base_orders.sql_table_name"

    def test_sql_based_derived_table(self, harness: Harness) -> None:
        ctx = harness.context(
            "view: orders { derived_table: { sql: SELECT id FROM base ;; } dimension: id {} }",
            "orders",
        )

        assert ctx.is_sql_based_derived_case()
        assert not ctx.is_regular_case()
        assert not ctx.is_sql_based_derived_view_without_fields_case()
        assert ctx.datahub_transformed_sql() == "SELECT id FROM base"

    def test_sql_based_derived_table_without_fields(self, harness: Harness) -> None:
        ctx = harness.context(
            "view: orders { derived_table: { sql: SELECT id FROM base ;; } }", "orders"
        )

        assert ctx.is_sql_based_derived_view_without_fields_case()

    def test_native_derived_table(self, harness: Harness) -> None:
        ctx = harness.context(
            "view: orders { derived_table: { explore_source: base_explore { column: id {} } } }",
            "orders",
        )

        assert ctx.is_native_derived_case()
        assert not ctx.is_regular_case()
        assert ctx.explore_source()["name"] == "base_explore"

    def test_direct_sql_query_in_sql_table_name(self, harness: Harness) -> None:
        ctx = harness.context(
            "view: orders { sql_table_name: (SELECT id FROM base) ;; dimension: id {} }",
            "orders",
        )

        assert ctx.is_direct_sql_query_case()
        assert not ctx.is_regular_case()
        assert not ctx.is_sql_table_name_referring_to_view()
        # The query text must be left intact for the SQL parser.
        assert ctx.datahub_transformed_sql_table_name() == "(SELECT id FROM base)"

    @pytest.mark.parametrize(
        "derived_table, expected",
        [
            ("sql: SELECT 1 ;; sql_trigger_value: SELECT 1 ;;", True),
            ("sql: SELECT 1 ;; datagroup_trigger: daily", True),
            ("sql: SELECT 1 ;; materialized_view: yes", True),
            ("sql: SELECT 1 ;; materialized_view: no", False),
            ("sql: SELECT 1 ;;", False),
        ],
    )
    def test_materialized_derived_view_detection(
        self, harness: Harness, derived_table: str, expected: bool
    ) -> None:
        ctx = harness.context(
            f"view: orders {{ derived_table: {{ {derived_table} }} }}", "orders"
        )

        assert ctx.is_materialized_derived_view() == expected


class TestExtends:
    def test_fields_and_sql_table_name_are_inherited_from_parent_in_same_file(
        self, harness: Harness
    ) -> None:
        lkml = (
            PARENT_VIEW
            + """
view: child {
  extends: [parent]
  dimension: shared { type: string sql: ${TABLE}.child_shared ;; }
  dimension: extra { type: string sql: ${TABLE}.extra ;; }
}
"""
        )
        ctx = harness.context(lkml, "child")

        assert ctx.sql_table_name() == "analytics.parent_table"
        dims = {d["name"]: d for d in ctx.dimensions()}
        assert set(dims) == {"shared", "extra", "id"}
        # child definition wins over the parent's
        assert dims["shared"]["sql"] == "${TABLE}.child_shared"
        assert [m["name"] for m in ctx.measures()] == ["count"]

    def test_parent_from_included_file_is_resolved(self, harness: Harness) -> None:
        harness.write("views/parent.view.lkml", PARENT_VIEW)
        lkml = 'include: "/views/parent.view.lkml"\nview: child { extends: [parent] }\n'
        ctx = harness.context(lkml, "child")

        assert ctx.sql_table_name() == "analytics.parent_table"
        assert [d["name"] for d in ctx.dimensions()] == ["id", "shared"]

    def test_last_parent_in_extends_list_takes_precedence(
        self, harness: Harness
    ) -> None:
        lkml = """
view: first { sql_table_name: first_table ;; }
view: second { sql_table_name: second_table ;; }
view: child { extends: [first, second] }
"""
        ctx = harness.context(lkml, "child")

        assert ctx.sql_table_name() == "second_table"

    def test_unresolvable_parent_raises(self, harness: Harness) -> None:
        ctx = harness.context("view: child { extends: [ghost] }", "child")

        with pytest.raises(NameError):
            ctx.sql_table_name()


class TestViewFileName:
    def test_relative_to_base_folder(self, harness: Harness) -> None:
        ctx = harness.context(
            "view: orders {}", "orders", file_name="nested/orders.view.lkml"
        )

        assert ctx.view_file_name() == "nested/orders.view.lkml"

    def test_not_available_when_outside_base_folder(self, harness: Harness) -> None:
        ctx = harness.context("view: orders {}", "orders")
        ctx.base_folder_path = "/somewhere/else"

        assert ctx.view_file_name() == ViewFieldValue.NOT_AVAILABLE.value


class TestCreateViewUpstream:
    def _upstream(
        self, harness: Harness, ctx: LookerViewContext
    ) -> AbstractViewUpstream:
        return create_view_upstream(
            view_context=ctx,
            looker_view_id_cache=harness.view_id_cache,
            config=harness.config,
            ctx=PipelineContext(run_id="test"),
            reporter=harness.reporter,
        )

    @pytest.mark.parametrize(
        "lkml, expected_type",
        [
            ("view: v { sql_table_name: t ;; }", RegularViewUpstream),
            (
                "view: v { sql_table_name: ${o.SQL_TABLE_NAME} ;; }",
                DotSqlTableNameViewUpstream,
            ),
            (
                "view: v { derived_table: { sql: SELECT 1 FROM t ;; } }",
                DerivedQueryUpstreamSource,
            ),
            (
                "view: v { sql_table_name: (SELECT 1 FROM t) ;; }",
                DirectQueryUpstreamSource,
            ),
            (
                "view: v { derived_table: { explore_source: e {} } }",
                NativeDerivedViewUpstream,
            ),
        ],
    )
    def test_dispatches_on_view_pattern(
        self, harness: Harness, lkml: str, expected_type: type
    ) -> None:
        upstream = self._upstream(harness, harness.context(lkml, "v"))

        assert isinstance(upstream, expected_type)

    def test_regular_view_upstream_builds_fully_qualified_urn(
        self, harness: Harness
    ) -> None:
        ctx = harness.context(
            "view: orders { sql_table_name: Sales.Orders ;; }", "orders"
        )

        upstream = self._upstream(harness, ctx)

        assert upstream.get_upstream_dataset_urn() == [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sales.orders,PROD)"
        ]

    def test_regular_view_column_refs_fall_back_to_declared_columns(
        self, harness: Harness
    ) -> None:
        ctx = harness.context("view: orders { sql_table_name: orders ;; }", "orders")
        upstream = self._upstream(harness, ctx)

        refs = upstream.get_upstream_column_ref(
            LookerFieldContext(
                {"name": "profit", "sql": "${TABLE}.Rev - ${TABLE}.Cost"}
            )
        )

        assert [(r.table.endswith("public.orders,PROD)"), r.column) for r in refs] == [
            (True, "rev"),
            (True, "cost"),
        ]

    def test_dot_sql_table_name_resolves_to_other_view_urn(
        self, harness: Harness
    ) -> None:
        harness.write(
            "base_orders.view.lkml", "view: base_orders { sql_table_name: t ;; }"
        )
        ctx = harness.context(
            "view: orders { sql_table_name: ${base_orders.SQL_TABLE_NAME} ;; }",
            "orders",
        )

        upstream = self._upstream(harness, ctx)
        urns: List[str] = upstream.get_upstream_dataset_urn()

        assert len(urns) == 1
        assert urns[0].startswith("urn:li:dataset:(urn:li:dataPlatform:looker,")
        assert "base_orders" in urns[0]

    def test_dot_sql_table_name_with_unknown_view_yields_no_upstream(
        self, harness: Harness
    ) -> None:
        ctx = harness.context(
            "view: orders { sql_table_name: ${ghost.SQL_TABLE_NAME} ;; }", "orders"
        )

        upstream = self._upstream(harness, ctx)

        assert upstream.get_upstream_dataset_urn() == []
        assert (
            upstream.get_upstream_column_ref(LookerFieldContext({"name": "id"})) == []
        )

    def test_derived_sql_upstream_resolves_tables_and_view_references(
        self, harness: Harness
    ) -> None:
        harness.write(
            "base_orders.view.lkml", "view: base_orders { sql_table_name: t ;; }"
        )
        ctx = harness.context(
            "view: orders { derived_table: { sql: SELECT o.id, c.name FROM ${base_orders.SQL_TABLE_NAME} o JOIN sales.customers c ON o.cid = c.id ;; } }",
            "orders",
        )

        upstream = self._upstream(harness, ctx)
        urns: List[str] = sorted(upstream.get_upstream_dataset_urn())

        assert len(urns) == 2
        looker_urn: Optional[str] = next(
            (u for u in urns if "dataPlatform:looker" in u), None
        )
        assert looker_urn is not None and "base_orders" in looker_urn
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sales.customers,PROD)"
            in urns
        )

    def test_derived_sql_without_fields_creates_fields_from_parse(
        self, harness: Harness
    ) -> None:
        ctx = harness.context(
            "view: orders { derived_table: { sql: SELECT id, amount AS total FROM sales.orders ;; } }",
            "orders",
        )

        upstream = self._upstream(harness, ctx)
        fields: Dict[str, List[str]] = {
            f.name: [u.column for u in f.upstream_fields]
            for f in upstream.create_fields()
        }

        assert fields == {"id": ["id"], "total": ["amount"]}


class TestLookerViewFromLookerDict:
    def _build(self, harness: Harness, ctx: LookerViewContext) -> LookerView:
        view = LookerView.from_looker_dict(
            project_name="test_project",
            model_name="data",
            view_context=ctx,
            looker_view_id_cache=harness.view_id_cache,
            reporter=harness.reporter,
            max_file_snippet_length=10_000,
            config=harness.config,
            ctx=PipelineContext(run_id="test"),
            extract_col_level_lineage=True,
        )
        assert view is not None
        return view

    def test_regular_view_collects_fields_with_column_lineage(
        self, harness: Harness
    ) -> None:
        lkml = """
view: orders {
  sql_table_name: sales.orders ;;
  dimension: id { type: number sql: ${TABLE}.id ;; }
  dimension_group: created { type: time sql: ${TABLE}.created_at ;; }
  measure: total { type: sum sql: ${TABLE}.amount ;; }
}
"""
        view = self._build(harness, harness.context(lkml, "orders"))

        assert view.id.view_name == "orders"
        assert view.id.file_path == "orders.view.lkml"
        assert view.upstream_dataset_urns == [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sales.orders,PROD)"
        ]
        fields = {f.name: f for f in view.fields}
        assert fields["id"].field_type == ViewFieldType.DIMENSION
        assert fields["created"].field_type == ViewFieldType.DIMENSION_GROUP
        assert fields["total"].field_type == ViewFieldType.MEASURE
        assert [c.column for c in fields["total"].upstream_fields] == ["amount"]
        assert view.view_details is not None
        assert view.view_details.viewLanguage == "lookml"
        assert "sql_table_name: sales.orders" in view.view_details.viewLogic

    def test_derived_sql_view_without_fields_derives_them_from_sql(
        self, harness: Harness
    ) -> None:
        lkml = """
view: order_summary {
  derived_table: {
    sql: SELECT customer_id, SUM(amount) AS spend FROM sales.orders GROUP BY 1 ;;
    sql_trigger_value: SELECT 1 ;;
  }
}
"""
        view = self._build(harness, harness.context(lkml, "order_summary"))

        assert view.upstream_dataset_urns == [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.sales.orders,PROD)"
        ]
        assert sorted(f.name for f in view.fields) == ["customer_id", "spend"]
        assert view.view_details is not None
        assert view.view_details.viewLanguage == "sql"
        assert view.view_details.viewLogic.startswith("SELECT customer_id")
