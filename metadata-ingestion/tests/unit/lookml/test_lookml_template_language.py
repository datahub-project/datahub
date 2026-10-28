import pathlib
from typing import Any, Dict

import pytest

from datahub.ingestion.source.looker.looker_liquid_tag import create_template
from datahub.ingestion.source.looker.looker_template_language import (
    DropDerivedViewPatternTransformer,
    IncompleteSqlTransformer,
    LookMlIfCommentTransformer,
    process_lookml_template_language,
    resolve_liquid_variable,
)
from datahub.ingestion.source.looker.lookml_config import (
    LookMLSourceConfig,
    LookMLSourceReport,
)


@pytest.fixture
def config_dict(tmp_path: pathlib.Path) -> Dict[str, object]:
    base = tmp_path / "base"
    base.mkdir()
    return {
        "base_folder": str(base),
        "connection_to_platform_map": {"my_connection": "postgres"},
        "project_name": "test_project",
    }


@pytest.fixture
def prod_config(config_dict: Dict[str, object]) -> LookMLSourceConfig:
    return LookMLSourceConfig.model_validate(config_dict)


@pytest.fixture
def dev_config(config_dict: Dict[str, object]) -> LookMLSourceConfig:
    return LookMLSourceConfig.model_validate(
        {**config_dict, "looker_environment": "dev"}
    )


class TestLiquidTags:
    def test_condition_tag_renders_as_dummy_equality(self) -> None:
        text = "SELECT * FROM orders WHERE {% condition order_region %} orders.region {% endcondition %}"

        rendered = create_template(text).render({})

        assert rendered == "SELECT * FROM orders WHERE orders.region='dummy_value'"

    def test_increment_condition_renders_as_date_comparison(self) -> None:
        text = "WHERE {% incrementcondition %} events.created_at {% endincrementcondition %}"

        rendered = create_template(text).render({})

        assert rendered == "WHERE events.created_at > '2023-01-01'"

    def test_condition_tag_inside_if_block_is_not_suppressed(self) -> None:
        # python-liquid v2 discards blocks made only of "blank" nodes; our custom
        # nodes must opt out or the SQL inside an {% if %} vanishes.
        text = "{% if true %}{% condition f %} t.col {% endcondition %}{% endif %}"

        rendered = create_template(text).render({})

        assert rendered == "t.col='dummy_value'"

    def test_sql_quote_filter_wraps_in_single_quotes(self) -> None:
        rendered = create_template("{{ tbl | sql_quote }}").render({"tbl": "orders"})

        assert rendered == "'orders'"


class TestResolveLiquidVariable:
    def test_substitutes_configured_variables(self) -> None:
        resolved = resolve_liquid_variable(
            text="SELECT * FROM {{ schema }}.orders",
            view_name="orders",
            liquid_variable={"schema": "prod_core"},
            report=LookMLSourceReport(),
        )

        assert resolved == "SELECT * FROM prod_core.orders"

    def test_undefined_variable_renders_as_null(self) -> None:
        resolved = resolve_liquid_variable(
            text="WHERE region = {{ missing_variable }}",
            view_name="orders",
            liquid_variable={},
            report=LookMLSourceReport(),
        )

        assert resolved == "WHERE region = NULL"

    def test_nested_variables_drive_conditionals(self) -> None:
        text = (
            "{% if order.region == 'ap-south-1' %}region_a{% else %}region_b{% endif %}"
        )

        resolved = resolve_liquid_variable(
            text=text,
            view_name="orders",
            liquid_variable={"order": {"region": "ap-south-1"}},
            report=LookMLSourceReport(),
        )

        assert resolved == "region_a"

    def test_unparseable_template_returns_original_text(self) -> None:
        text = "SELECT * FROM orders {% unknown_looker_tag %}"

        resolved = resolve_liquid_variable(
            text=text,
            view_name="orders",
            liquid_variable={},
            report=LookMLSourceReport(),
        )

        assert resolved == text


class TestLookMlIfCommentTransformer:
    def test_prod_environment_keeps_prod_line_and_drops_dev_line(
        self, prod_config: LookMLSourceConfig
    ) -> None:
        transformer = LookMlIfCommentTransformer(
            source_config=prod_config, reporter=LookMLSourceReport()
        )
        view = {
            "name": "orders",
            "sql_table_name": "-- if prod -- prod.orders\n-- if dev -- dev.orders",
        }

        result = transformer.transform(view)

        assert result["datahub_transformed_sql_table_name"].strip() == "prod.orders"

    def test_dev_environment_keeps_dev_line_and_drops_prod_line(
        self, dev_config: LookMLSourceConfig
    ) -> None:
        transformer = LookMlIfCommentTransformer(
            source_config=dev_config, reporter=LookMLSourceReport()
        )
        view = {
            "name": "orders",
            "sql_table_name": "-- if prod -- prod.orders\n-- if dev -- dev.orders",
        }

        result = transformer.transform(view)

        assert result["datahub_transformed_sql_table_name"].strip() == "dev.orders"

    def test_if_comment_is_case_insensitive(
        self, prod_config: LookMLSourceConfig
    ) -> None:
        transformer = LookMlIfCommentTransformer(
            source_config=prod_config, reporter=LookMLSourceReport()
        )
        view = {"name": "orders", "sql_table_name": "-- IF PROD -- prod.orders"}

        result = transformer.transform(view)

        assert result["datahub_transformed_sql_table_name"].strip() == "prod.orders"


class TestIncompleteSqlTransformer:
    def test_adds_missing_select_and_from(
        self, prod_config: LookMLSourceConfig
    ) -> None:
        transformer = IncompleteSqlTransformer(
            source_config=prod_config, reporter=LookMLSourceReport()
        )
        view = {"name": "orders", "derived_table": {"sql": "col_a, col_b"}}

        result = transformer.transform(view)

        assert (
            result["derived_table"]["datahub_transformed_sql"]
            == "SELECT col_a, col_b FROM orders"
        )

    def test_complete_sql_is_left_alone(self, prod_config: LookMLSourceConfig) -> None:
        transformer = IncompleteSqlTransformer(
            source_config=prod_config, reporter=LookMLSourceReport()
        )
        sql = "select col_a from base_table"
        view = {"name": "orders", "derived_table": {"sql": sql}}

        result = transformer.transform(view)

        assert result["derived_table"]["datahub_transformed_sql"] == sql

    def test_does_not_touch_sql_table_name(
        self, prod_config: LookMLSourceConfig
    ) -> None:
        # Only derived_table.sql may be a SQL fragment; a bare sql_table_name must
        # not be rewritten into "SELECT my_table FROM orders".
        transformer = IncompleteSqlTransformer(
            source_config=prod_config, reporter=LookMLSourceReport()
        )

        result = transformer.transform({"name": "orders", "sql_table_name": "my_table"})

        assert result == {}


class TestDropDerivedViewPatternTransformer:
    def test_strips_dollar_braces_from_view_reference(
        self, prod_config: LookMLSourceConfig
    ) -> None:
        transformer = DropDerivedViewPatternTransformer(
            source_config=prod_config, reporter=LookMLSourceReport()
        )
        view = {
            "name": "orders",
            "sql_table_name": "${employee_income_source.SQL_TABLE_NAME}",
        }

        result = transformer.transform(view)

        assert (
            result["datahub_transformed_sql_table_name"]
            == "employee_income_source.SQL_TABLE_NAME"
        )


class TestProcessLookmlTemplateLanguage:
    def test_transformers_chain_on_derived_sql(
        self, prod_config: LookMLSourceConfig
    ) -> None:
        # The -- if -- comment, liquid variable, ${} pattern and the missing FROM
        # must all be resolved, and each stage must build on the previous one's
        # output rather than on the raw sql.
        lkml: Dict[str, Any] = {
            "views": [
                {
                    "name": "orders",
                    "derived_table": {
                        "sql": "-- if prod -- id, {{ col }} FROM ${base.SQL_TABLE_NAME}\n-- if dev -- id FROM dev_table"
                    },
                }
            ]
        }
        prod_config.liquid_variables = {"col": "amount"}

        process_lookml_template_language(
            source_config=prod_config,
            view_lkml_file_dict=lkml,
            reporter=LookMLSourceReport(),
        )

        derived = lkml["views"][0]["derived_table"]
        assert (
            " ".join(derived["datahub_transformed_sql"].split())
            == "SELECT id, amount FROM base.SQL_TABLE_NAME"
        )
        # the original is preserved alongside the transformed value
        assert "-- if prod --" in derived["sql"]

    def test_view_without_sql_attributes_is_unchanged(
        self, prod_config: LookMLSourceConfig
    ) -> None:
        lkml = {"views": [{"name": "orders", "dimensions": [{"name": "id"}]}]}

        process_lookml_template_language(
            source_config=prod_config,
            view_lkml_file_dict=lkml,
            reporter=LookMLSourceReport(),
        )

        assert lkml["views"][0] == {"name": "orders", "dimensions": [{"name": "id"}]}

    def test_file_without_views_is_ignored(
        self, prod_config: LookMLSourceConfig
    ) -> None:
        lkml = {"explores": [{"name": "orders"}]}

        process_lookml_template_language(
            source_config=prod_config,
            view_lkml_file_dict=lkml,
            reporter=LookMLSourceReport(),
        )

        assert lkml == {"explores": [{"name": "orders"}]}
