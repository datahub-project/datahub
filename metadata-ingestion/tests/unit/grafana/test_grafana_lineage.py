from typing import Optional
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.grafana.grafana_config import PlatformConnectionConfig
from datahub.ingestion.source.grafana.lineage import (
    LineageExtractor,
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


def _sql_panel(raw_sql: str) -> Panel:
    return Panel(
        id="1",
        title="Test Panel",
        type="graph",
        datasource={"type": "postgres", "uid": "postgres_uid"},
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


def test_parse_sql_is_not_confused_by_a_macro(parsing_extractor):
    # The cleaner replaces $__timeFrom() with TRUE even though it produces a value,
    # which can break a query that parses perfectly well as authored. A macro can
    # never be a table name, so the comparison must ignore macros entirely rather
    # than be blinded by the cleaner's handling of them.
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel("SELECT v FROM test_table WHERE ts > $__timeFrom() AND x = 1"),
        "test-dashboard",
    )

    assert "test_db.public.test_table" in _only_upstream_of(lineage)


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


def test_parse_sql_reports_an_unknown_configured_platform(parsing_extractor):
    # The platform comes from connection_to_platform_map, so a typo is reachable.
    # Choosing a query form needs the dialect up front, and a misspelling used to be
    # absorbed by the lineage parser - it must not degrade every panel in silence.
    bad_platform = PlatformConnectionConfig(platform="postgress", database="test_db")

    assert parsing_extractor._parse_sql("SELECT * FROM t", bad_platform) is None
    assert any(
        "known SQL dialect" in w.title for w in parsing_extractor.report.warnings
    )


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
