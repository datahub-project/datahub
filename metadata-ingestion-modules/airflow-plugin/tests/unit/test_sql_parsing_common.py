"""Unit tests for _sql_parsing_common module."""

from unittest import mock

from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult
from datahub_airflow_plugin._sql_parsing_common import (
    format_sql_for_job_facet,
    parse_sql_with_datahub,
)


class TestFormatSqlForJobFacet:
    def test_string_input(self):
        assert format_sql_for_job_facet("SELECT 1", True) == "SELECT 1"
        assert format_sql_for_job_facet("SELECT 1", False) == "SELECT 1"

    def test_list_multi_statement_enabled(self):
        sql = ["CREATE TEMP TABLE t AS SELECT 1", "SELECT * FROM t"]
        result = format_sql_for_job_facet(sql, enable_multi_statement=True)
        assert result == "CREATE TEMP TABLE t AS SELECT 1;\nSELECT * FROM t"

    def test_list_multi_statement_disabled(self):
        sql = ["SELECT 1", "SELECT 2"]
        result = format_sql_for_job_facet(sql, enable_multi_statement=False)
        assert result == "SELECT 1"

    def test_empty_list(self):
        assert format_sql_for_job_facet([], True) == ""
        assert format_sql_for_job_facet([], False) == ""

    def test_list_filters_empty_strings(self):
        sql = ["SELECT 1", "", "SELECT 2"]
        result = format_sql_for_job_facet(sql, enable_multi_statement=True)
        assert result == "SELECT 1;\nSELECT 2"


class TestParseSqlWithDatahub:
    """Tests that parse_sql_with_datahub dispatches to the correct underlying function."""

    @mock.patch(
        "datahub_airflow_plugin._sql_parsing_common.create_lineage_sql_parsed_result"
    )
    def test_single_statement_mode_with_string(self, mock_single):
        mock_single.return_value = SqlParsingResult(in_tables=[], out_tables=[])

        result = parse_sql_with_datahub(
            sql="SELECT 1",
            default_database="db",
            platform="postgres",
            env="PROD",
            default_schema="public",
            graph=None,
            enable_multi_statement=False,
        )

        mock_single.assert_called_once_with(
            query="SELECT 1",
            default_db="db",
            platform="postgres",
            platform_instance=None,
            env="PROD",
            default_schema="public",
            graph=None,
        )
        assert result.in_tables == []

    @mock.patch(
        "datahub_airflow_plugin._sql_parsing_common.create_lineage_sql_parsed_result"
    )
    def test_single_statement_mode_with_list_takes_first(self, mock_single):
        mock_single.return_value = SqlParsingResult(in_tables=[], out_tables=[])

        parse_sql_with_datahub(
            sql=["SELECT 1", "SELECT 2"],
            default_database=None,
            platform="postgres",
            env="PROD",
            default_schema=None,
            graph=None,
            enable_multi_statement=False,
        )

        mock_single.assert_called_once()
        assert mock_single.call_args.kwargs["query"] == "SELECT 1"

    @mock.patch(
        "datahub_airflow_plugin._sql_parsing_common.create_lineage_sql_parsed_result"
    )
    def test_single_statement_mode_with_empty_list(self, mock_single):
        mock_single.return_value = SqlParsingResult(in_tables=[], out_tables=[])

        parse_sql_with_datahub(
            sql=[],
            default_database=None,
            platform="postgres",
            env="PROD",
            default_schema=None,
            graph=None,
            enable_multi_statement=False,
        )

        mock_single.assert_called_once()
        assert mock_single.call_args.kwargs["query"] == ""

    @mock.patch(
        "datahub_airflow_plugin._sql_parsing_common.create_lineage_from_sql_statements"
    )
    def test_multi_statement_mode_with_string(self, mock_multi):
        mock_multi.return_value = SqlParsingResult(in_tables=[], out_tables=[])

        parse_sql_with_datahub(
            sql="CREATE TEMP TABLE t AS SELECT 1; SELECT * FROM t",
            default_database="db",
            platform="postgres",
            env="PROD",
            default_schema="public",
            graph=None,
            enable_multi_statement=True,
        )

        mock_multi.assert_called_once_with(
            queries="CREATE TEMP TABLE t AS SELECT 1; SELECT * FROM t",
            default_db="db",
            platform="postgres",
            platform_instance=None,
            env="PROD",
            default_schema="public",
            graph=None,
        )

    @mock.patch(
        "datahub_airflow_plugin._sql_parsing_common.create_lineage_from_sql_statements"
    )
    def test_multi_statement_mode_with_list(self, mock_multi):
        mock_multi.return_value = SqlParsingResult(in_tables=[], out_tables=[])
        sql_list = ["CREATE TEMP TABLE t AS SELECT 1", "SELECT * FROM t"]

        parse_sql_with_datahub(
            sql=sql_list,
            default_database=None,
            platform="postgres",
            env="PROD",
            default_schema=None,
            graph=None,
            enable_multi_statement=True,
        )

        mock_multi.assert_called_once()
        assert mock_multi.call_args.kwargs["queries"] == sql_list
