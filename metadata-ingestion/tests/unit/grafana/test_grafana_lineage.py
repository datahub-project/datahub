from typing import Optional
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.grafana.grafana_config import PlatformConnectionConfig
from datahub.ingestion.source.grafana.lineage import (
    LineageExtractor,
    _clean_grafana_macros,
    _probe_grafana_template_variables,
    _select_query_for_lineage,
)
from datahub.ingestion.source.grafana.models import Panel
from datahub.ingestion.source.grafana.report import GrafanaSourceReport
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)
from datahub.sql_parsing.sqlglot_utils import get_dialect

POSTGRES_CONNECTION = PlatformConnectionConfig(
    platform="postgres",
    database="test_db",
    database_schema="public",
)


@pytest.fixture
def mock_graph():
    return MagicMock()


@pytest.fixture
def mock_report():
    return GrafanaSourceReport()


@pytest.fixture
def lineage_extractor(mock_graph, mock_report):
    return LineageExtractor(
        platform="grafana",
        platform_instance="test-instance",
        env="PROD",
        connection_to_platform_map={
            "postgres_uid": PlatformConnectionConfig(
                platform="postgres",
                database="test_db",
                database_schema="public",
            ),
            "mysql_uid": PlatformConnectionConfig(
                platform="mysql",
                database="test_db",
            ),
        },
        report=mock_report,
        graph=mock_graph,
    )


def test_extract_panel_lineage_no_datasource(lineage_extractor):
    panel = Panel(id="1", title="Test Panel", type="graph", datasource=None, targets=[])

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")
    assert lineage is None


def test_extract_panel_lineage_unknown_datasource(lineage_extractor):
    panel = Panel(
        id="1",
        title="Test Panel",
        type="graph",
        datasource={"type": "unknown", "uid": "unknown_uid"},
        targets=[],
    )

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")
    assert lineage is None


def test_extract_panel_lineage_postgres(lineage_extractor):
    panel = Panel(
        id="1",
        title="Test Panel",
        type="graph",
        datasource={"type": "postgres", "uid": "postgres_uid"},
        targets=[
            {
                "rawSql": "SELECT value, timestamp FROM test_table",
                "format": "table",
                "sql": {
                    "columns": [
                        {
                            "type": "number",
                            "parameters": [{"type": "column", "name": "value"}],
                        },
                        {
                            "type": "time",
                            "parameters": [{"type": "column", "name": "timestamp"}],
                        },
                    ]
                },
            }
        ],
    )

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")
    assert lineage is not None, "Lineage should not be None"
    assert isinstance(lineage, MetadataChangeProposalWrapper)
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    assert len(lineage.aspect.upstreams) == 1
    assert lineage.aspect.upstreams[0].type == DatasetLineageTypeClass.TRANSFORMED


def _sql_panel(raw_sql: str, platform: str = "postgres") -> Panel:
    return Panel(
        id="1",
        title="Test Panel",
        type="graph",
        datasource={"type": platform, "uid": f"{platform}_uid"},
        targets=[{"rawSql": raw_sql, "format": "table"}],
    )


def _only_upstream_of(lineage: Optional[MetadataChangeProposalWrapper]) -> str:
    assert lineage is not None
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    assert len(lineage.aspect.upstreams) == 1
    return lineage.aspect.upstreams[0].dataset


@pytest.fixture
def parsing_extractor(lineage_extractor, mock_graph):
    # Without a real resolver a bare MagicMock graph makes every parse fail, so
    # these tests would silently assert against the datasource-UID fallback.
    mock_graph._make_schema_resolver.return_value = SchemaResolver(
        platform="postgres", env="PROD", graph=None
    )
    return lineage_extractor


def test_parse_sql_prefers_the_raw_query(parsing_extractor):
    # A quoted '${var}' is a valid string literal, so the query parses as authored.
    # Cleaning rewrites it to ''grafana_var'', which does not parse - so reaching
    # the real table proves the raw query was tried first.
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel("SELECT value FROM test_table WHERE id = CAST('${id}' AS INTEGER)"),
        "test-dashboard",
    )

    assert "test_db.public.test_table" in _only_upstream_of(lineage)


def test_parse_sql_falls_back_to_cleaning_when_raw_fails(parsing_extractor):
    # Braces are not SQL, so this does not parse as authored at all. Cleaning
    # rewrites the variable to a string literal, which does - and every table name
    # is real, so the lineage survives.
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel("SELECT v FROM test_table WHERE name = ${env}"), "test-dashboard"
    )

    assert "test_db.public.test_table" in _only_upstream_of(lineage)


def test_parse_sql_keeps_identifiers_containing_a_dollar_sign(parsing_extractor):
    # Oracle allows $ inside identifiers. Substituting into one would make the two
    # parses disagree over a name that was never in doubt, and the query would be
    # cleaned - leaving a truncated "v" behind.
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel("SELECT * FROM v$session"), "test-dashboard"
    )

    assert "test_db.public.v$session" in _only_upstream_of(lineage)


@pytest.mark.parametrize("macro", ["$__timeFilter(time)", "time > $__timeFrom()"])
def test_parse_sql_handles_a_macro_next_to_a_quoted_variable(parsing_extractor, macro):
    # Postgres cannot parse a macro as authored, so without replacing macros on
    # both sides of the comparison neither parse succeeds and the fully cleaned
    # form is used - where '${host}' becomes ''grafana_var'', which does not parse
    # either. Reaching the table proves the macro-cleaned form was compared and
    # chosen.
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel(
            f"SELECT time, value FROM metrics WHERE {macro} AND host = '${{host}}'"
        ),
        "test-dashboard",
    )

    assert "test_db.public.metrics" in _only_upstream_of(lineage)


@pytest.mark.parametrize("platform", ["postgres", "clickhouse", "snowflake"])
def test_select_query_for_lineage_compares_with_macros_replaced(platform):
    sql = (
        "SELECT time, value FROM metrics WHERE $__timeFilter(time) AND host = '${host}'"
    )

    assert _select_query_for_lineage(sql, get_dialect(platform)) == (
        _clean_grafana_macros(sql)
    )


def test_parse_sql_drops_a_variable_read_as_a_column(parsing_extractor, mock_graph):
    # MySQL, Trino and T-SQL read an unquoted $metric as a column. The table names
    # agree, so the query is used as authored, but t.$metric is not a real field.
    # The resolver's platform decides how columns are read, so it must be MySQL's.
    mock_graph._make_schema_resolver.return_value = SchemaResolver(
        platform="mysql", env="PROD", graph=None
    )
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel("SELECT $metric AS value, ts FROM test_table", platform="mysql"),
        "test-dashboard",
    )

    assert lineage is not None
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    upstreams_by_field = {
        f.downstreams[0].rsplit(",", 1)[-1].rstrip(")"): f.upstreams or []
        for f in lineage.aspect.fineGrainedLineages or []
        if f.downstreams
    }
    assert upstreams_by_field["value"] == []
    assert any(u.endswith(",ts)") for u in upstreams_by_field["ts"])


def test_parse_sql_keeps_lineage_with_an_unquoted_and_a_quoted_variable(
    parsing_extractor,
):
    # Comparing column renderings instead of dropping variable columns would send
    # this to the cleaned form, which does not parse ('${x}' -> ''grafana_var'').
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel(
            "SELECT v FROM test_table WHERE col = $var AND name = '${x}'",
            platform="mysql",
        ),
        "test-dashboard",
    )

    assert "test_db.test_table" in _only_upstream_of(lineage)


def test_parse_sql_emits_a_variable_appended_to_a_table_name(parsing_extractor):
    # Known limitation: "metrics_$env" cannot be told apart from an identifier
    # that contains a dollar sign, such as Oracle's v$session, so it is emitted as
    # authored - naming a dataset that does not exist.
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel("SELECT v FROM metrics_$env"), "test-dashboard"
    )

    assert "test_db.public.metrics_$env" in _only_upstream_of(lineage)


def test_parse_sql_ignores_from_inside_a_string_literal(parsing_extractor):
    # A text-level check for a variable after FROM/JOIN also fires on prose inside
    # a string literal, which would discard a perfectly good raw parse.
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel("SELECT 'imported from $source' AS note, v FROM test_table"),
        "test-dashboard",
    )

    assert "test_db.public.test_table" in _only_upstream_of(lineage)


def test_parse_sql_survives_a_non_string_raw_sql(parsing_extractor):
    # GrafanaQueryTarget is Dict[str, Any], so rawSql can hold any JSON value
    # despite _extract_raw_sql's Optional[str] annotation.
    assert (
        parsing_extractor._parse_sql({"not": "a string"}, POSTGRES_CONNECTION) is None
    )


def test_select_query_for_lineage_picks_the_form_to_parse():
    postgres = get_dialect("postgres")
    authored = "SELECT v FROM test_table WHERE s = '${env}'"
    # The variable sits inside a string literal, so no table name changes.
    assert _select_query_for_lineage(authored, postgres) == authored

    # Here it stands in for the table name, so the cleaned form has to be used.
    chosen = _select_query_for_lineage("SELECT v FROM ${tbl}", postgres)
    assert chosen is not None and "grafana_var" in chosen

    # Neither form parses.
    assert _select_query_for_lineage("SELECT FROM WHERE ${x}", postgres) is None

    # The lineage parser rejects more than one statement, so the selection must too.
    assert _select_query_for_lineage("SELECT v FROM t; SELECT 1", postgres) is None


def test_parse_sql_reports_an_unknown_configured_platform(parsing_extractor):
    # The platform comes from connection_to_platform_map, so a typo is reachable.
    # Choosing a query form needs the dialect up front, and a misspelling used to be
    # absorbed by the lineage parser - it must not degrade every panel in silence.
    bad_platform = PlatformConnectionConfig(platform="postgress", database="test_db")

    assert parsing_extractor._parse_sql("SELECT * FROM t", bad_platform) is None
    assert parsing_extractor._parse_sql("SELECT * FROM u", bad_platform) is None

    # Once per platform, not once per panel.
    warnings = [
        w for w in parsing_extractor.report.warnings if "known SQL dialect" in w.title
    ]
    assert len(warnings) == 1
    assert len(warnings[0].context) == 1


def test_probe_substitutes_variables_but_not_macros():
    probed = _probe_grafana_template_variables(
        "SELECT $col FROM ${tbl} WHERE $__timeFilter(ts) AND x = [[v]]"
    )

    assert "$__timeFilter(ts)" in probed
    assert "$col" not in probed
    assert "${tbl}" not in probed
    assert "[[v]]" not in probed


def test_extract_panel_lineage_mysql(lineage_extractor):
    panel = Panel(
        id="1",
        title="Test Panel",
        type="graph",
        datasource={"type": "mysql", "uid": "mysql_uid"},
        targets=[
            {
                "rawSql": "SELECT value, timestamp FROM test_table",
                "format": "table",
                "sql": {
                    "columns": [
                        {
                            "type": "number",
                            "parameters": [{"type": "column", "name": "value"}],
                        },
                        {
                            "type": "time",
                            "parameters": [{"type": "column", "name": "timestamp"}],
                        },
                    ]
                },
            }
        ],
    )

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")
    assert lineage is not None, "Lineage should not be None"
    assert isinstance(lineage, MetadataChangeProposalWrapper)
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    assert len(lineage.aspect.upstreams) == 1


def test_extract_panel_lineage_prometheus(lineage_extractor):
    panel = Panel(
        id="1",
        title="Test Panel",
        type="graph",
        datasource={"type": "prometheus", "uid": "prom_uid"},
        targets=[{"expr": "rate(http_requests_total[5m])"}],
    )

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")
    assert lineage is None


def test_create_basic_lineage(lineage_extractor):
    ds_uid = "postgres_uid"
    ds_urn = make_dataset_urn_with_platform_instance(
        platform="grafana",
        name="test_dataset",
        platform_instance="test-instance",
        env="PROD",
    )

    platform_config = PlatformConnectionConfig(
        platform="postgres",
        database="test_db",
        database_schema="public",
    )

    lineage = lineage_extractor._create_basic_lineage(ds_uid, platform_config, ds_urn)

    assert isinstance(lineage, MetadataChangeProposalWrapper)
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    assert len(lineage.aspect.upstreams) == 1


def test_create_column_lineage(lineage_extractor, mock_graph):
    mock_parsed_sql = MagicMock()
    mock_parsed_sql.in_tables = [
        "urn:li:dataset:(postgres,test_db.public.test_table,PROD)"
    ]
    mock_parsed_sql.column_lineage = [
        MagicMock(
            downstream=MagicMock(column="test_col"),
            upstreams=[MagicMock(column="source_col")],
        )
    ]

    ds_urn = make_dataset_urn_with_platform_instance(
        platform="grafana",
        name="test_dataset",
        platform_instance="test-instance",
        env="PROD",
    )

    lineage = lineage_extractor._create_column_lineage(ds_urn, mock_parsed_sql)
    assert isinstance(lineage, MetadataChangeProposalWrapper)
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    assert lineage.aspect.fineGrainedLineages is not None


def test_create_column_lineage_skips_unresolved_columns(lineage_extractor, mock_graph):
    upstream_table_urn = "urn:li:dataset:(postgres,test_db.public.source,PROD)"
    mock_parsed_sql = MagicMock()
    mock_parsed_sql.in_tables = [
        "urn:li:dataset:(postgres,test_db.public.test_table,PROD)"
    ]
    mock_parsed_sql.column_lineage = [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(column="test_col"),
            upstreams=[
                ColumnRef(table=upstream_table_urn, column=""),
                ColumnRef(table=upstream_table_urn, column="source_col"),
            ],
        ),
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(column=""),
            upstreams=[ColumnRef(table=upstream_table_urn, column="other_col")],
        ),
    ]

    ds_urn = make_dataset_urn_with_platform_instance(
        platform="grafana",
        name="test_dataset",
        platform_instance="test-instance",
        env="PROD",
    )

    lineage = lineage_extractor._create_column_lineage(ds_urn, mock_parsed_sql)
    assert lineage.aspect.fineGrainedLineages is not None
    fgl_with_unresolved_upstream, fgl_with_empty_downstream = (
        lineage.aspect.fineGrainedLineages
    )

    assert len(fgl_with_unresolved_upstream.upstreams) == 1
    assert "source_col" in fgl_with_unresolved_upstream.upstreams[0]

    assert fgl_with_empty_downstream.downstreams == []
