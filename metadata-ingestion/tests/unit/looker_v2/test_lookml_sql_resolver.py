"""Unit tests for LookMLSQLResolver."""

from typing import Any

from datahub.ingestion.source.looker_v2.lookml_sql_resolver import LookMLSQLResolver


def resolver(**kwargs: Any) -> LookMLSQLResolver:
    return LookMLSQLResolver(**kwargs)


class TestConstantResolution:
    def test_resolves_constant(self):
        r = resolver(constants={"schema": "prod_schema"})
        assert (
            r.resolve("SELECT * FROM @{schema}.orders")
            == "SELECT * FROM prod_schema.orders"
        )

    def test_unknown_constant_preserved(self):
        r = resolver(constants={})
        result = r.resolve("SELECT * FROM @{unknown}.orders")
        assert "@{unknown}" in result

    def test_multiple_constants(self):
        r = resolver(constants={"db": "mydb", "schema": "myschema"})
        result = r.resolve("SELECT @{db}.@{schema}.table")
        assert result == "SELECT mydb.myschema.table"


class TestIfComments:
    def test_prod_environment_keeps_prod_block(self):
        r = resolver(environment="prod")
        sql = "SELECT -- if prod -- 1 -- end if --"
        assert r.resolve(sql) == "SELECT  1 "

    def test_prod_environment_drops_dev_block(self):
        r = resolver(environment="prod")
        sql = "SELECT -- if dev -- debug_col -- end if -- 1"
        assert "debug_col" not in r.resolve(sql)

    def test_dev_environment_keeps_dev_block(self):
        r = resolver(environment="dev")
        sql = "SELECT -- if dev -- debug_col -- end if --"
        assert "debug_col" in r.resolve(sql)

    def test_default_environment_is_prod(self):
        r = resolver()
        sql = "SELECT -- if prod -- prod_col -- end if --"
        assert "prod_col" in r.resolve(sql)


class TestLiquidTags:
    def test_unless_tag(self):
        r = resolver(template_variables={"hide": False})
        sql = "{% unless hide %}col{% endunless %}"
        result = r.resolve(sql)
        assert "col" in result

    def test_assign_becomes_set(self):
        r = resolver()
        sql = "{% assign x = 'hello' %}{{ x }}"
        result = r.resolve(sql)
        assert "hello" in result

    def test_liquid_if_variable(self):
        r = resolver(template_variables={"show_column": True})
        sql = "{% if show_column %}revenue{% endif %}"
        assert "revenue" in r.resolve(sql)

    def test_liquid_if_false_variable(self):
        r = resolver(template_variables={"show_column": False})
        sql = "{% if show_column %}revenue{% endif %}"
        assert "revenue" not in r.resolve(sql)


class TestFilters:
    def test_upcase_filter(self):
        r = resolver(template_variables={"name": "hello"})
        assert r.resolve("{{ name | upcase }}") == "HELLO"

    def test_downcase_filter(self):
        r = resolver(template_variables={"name": "HELLO"})
        assert r.resolve("{{ name | downcase }}") == "hello"

    def test_size_filter(self):
        r = resolver(template_variables={"items": [1, 2, 3]})
        assert r.resolve("{{ items | size }}") == "3"


class TestSafePassthrough:
    def test_no_template_syntax_returns_unchanged(self):
        r = resolver()
        sql = "SELECT * FROM orders WHERE id = 1"
        assert r.resolve(sql) == sql

    def test_table_ref_preserved(self):
        r = resolver()
        sql = "SELECT * FROM ${TABLE}"
        assert "${TABLE}" in r.resolve(sql)

    def test_invalid_template_returns_original(self):
        r = resolver()
        sql = "{% invalid_tag %}"
        # Should not raise — returns original sql
        result = r.resolve(sql)
        assert result == sql

    def test_empty_string(self):
        r = resolver()
        assert r.resolve("") == ""


class TestResolveDictValues:
    def test_resolves_named_keys(self):
        r = resolver(constants={"schema": "myschema"})
        d = {"sql_table_name": "@{schema}.orders", "name": "orders"}
        result = r.resolve_dict_values(d, ["sql_table_name"])
        assert result["sql_table_name"] == "myschema.orders"
        assert result["name"] == "orders"

    def test_non_string_values_skipped(self):
        r = resolver()
        d = {"count": 42, "sql": "SELECT 1"}
        result = r.resolve_dict_values(d, ["count", "sql"])
        assert result["count"] == 42
        assert result["sql"] == "SELECT 1"
