from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.grafana.grafana_config import PlatformConnectionConfig
from datahub.ingestion.source.grafana.lineage import (
    LineageExtractor,
    _partition_upstreams,
    _probe_grafana_template_variables,
    _restrict_to_upstreams,
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
    SqlParsingResult,
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


def _upstreams_of(lineage: Optional[MetadataChangeProposalWrapper]) -> List[str]:
    assert lineage is not None
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    return [upstream.dataset for upstream in lineage.aspect.upstreams]


def _only_upstream_of(lineage: Optional[MetadataChangeProposalWrapper]) -> str:
    upstreams = _upstreams_of(lineage)
    assert len(upstreams) == 1
    return upstreams[0]


def _urn(name: str, platform: str = "postgres") -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"


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


@pytest.mark.parametrize(
    "raw_sql",
    [
        "SELECT * FROM $tbl",
        "SELECT * FROM a, $tbl",
        "SELECT * FROM ONLY $tbl",
        "SELECT * FROM /* comment */ $tbl",
        "WITH c AS (SELECT 1 AS x) SELECT * FROM c, $tbl",
        "SELECT * FROM (SELECT y FROM $tbl) s",
        "SELECT * FROM $schema.events",
        "SELECT * FROM $cat.public.events",
    ],
    ids=[
        "plain",
        "comma_join",
        "only",
        "after_comment",
        "after_cte",
        "in_subquery",
        "schema_position",
        "catalog_position",
    ],
)
def test_parse_sql_does_not_name_a_table_after_a_variable(parsing_extractor, raw_sql):
    # In dialects that drop the $ sigil a raw parse of "FROM $tbl" yields a table
    # called "tbl" - the variable's name, not the table it stands for - and the
    # resulting URN looks entirely ordinary. The last two shapes are worse still:
    # the variable is swallowed and the configured schema silently takes its place,
    # so "$schema.events" resolves to "test_db.public.events".
    #
    # Asserted against _parse_sql rather than the emitted lineage on purpose:
    # extract_panel_lineage always falls back to a datasource-named upstream, which
    # can never contain these names, so an assertion made there would pass without
    # testing anything.
    assert parsing_extractor._parse_sql(raw_sql, POSTGRES_CONNECTION) is None


def test_parse_sql_keeps_a_real_table_alongside_an_unresolvable_one(parsing_extractor):
    # [[tbl]] is a table name we never learned, but test_table is one we did.
    # Rejecting the whole result over the first would throw away the second.
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel("SELECT a.x FROM test_table a JOIN [[tbl]] b ON a.i = b.i"),
        "test-dashboard",
    )

    upstreams = _upstreams_of(lineage)
    assert any("test_db.public.test_table" in u for u in upstreams)
    assert not any("grafana_identifier" in u for u in upstreams)


def test_parse_sql_keeps_table_functions_with_positional_parameters(parsing_extractor):
    # $1 is an ordinary database positional parameter, not a Grafana variable, and
    # it lives in the same subtree as the table name. Treating that subtree as a
    # variable check would discard the real table sitting next to it.
    lineage = parsing_extractor.extract_panel_lineage(
        _sql_panel("SELECT t.v FROM test_table t, generate_series($1, $2) AS g"),
        "test-dashboard",
    )

    assert "test_db.public.test_table" in _only_upstream_of(lineage)


def test_parse_sql_drops_an_upstream_that_is_only_the_qualifier(parsing_extractor):
    # A table function gives the parser no name to qualify, so this resolves to
    # "test_db.public" on its own, which is not a dataset.
    assert (
        parsing_extractor._parse_sql(
            "SELECT * FROM generate_series($1, $2)", POSTGRES_CONNECTION
        )
        is None
    )


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


def test_parse_sql_reports_when_nothing_resolves(parsing_extractor):
    # The report is the whole point: a panel that falls back to datasource lineage
    # has to say so, or the failure is invisible again.
    assert (
        parsing_extractor._parse_sql("SELECT * FROM ${tbl}", POSTGRES_CONNECTION)
        is None
    )
    assert any(
        "did not resolve to upstream tables" in w.title
        for w in parsing_extractor.report.warnings
    )


def test_parse_sql_reports_the_upstreams_it_dropped(parsing_extractor):
    # Dropping an unresolvable upstream while keeping the rest is only safe if the
    # drop is announced; otherwise lineage quietly becomes incomplete.
    result = parsing_extractor._parse_sql(
        "SELECT a.x FROM test_table a JOIN [[tbl]] b ON a.i = b.i", POSTGRES_CONNECTION
    )

    assert result is not None and len(result.in_tables) == 1
    assert any(
        "could not be resolved" in w.title for w in parsing_extractor.report.warnings
    )


def test_partition_upstreams_drops_an_unreadable_urn():
    # Feeding a whole URN to the name tests would match against the platform and
    # env too, so a URN that cannot be read counts as unresolvable.
    result = SqlParsingResult(in_tables=["not-a-urn-at-all"], out_tables=[])

    kept, dropped = _partition_upstreams(
        result, POSTGRES_CONNECTION, from_cleaned_query=False
    )
    assert not kept
    assert dropped == ["not-a-urn-at-all"]


def test_probe_substitutes_variables_but_not_macros():
    probed = _probe_grafana_template_variables(
        "SELECT $col FROM ${tbl} WHERE $__timeFilter(ts) AND x = [[v]]"
    )

    assert "$__timeFilter(ts)" in probed
    assert "$col" not in probed
    assert "${tbl}" not in probed
    assert "[[v]]" not in probed


def test_partition_upstreams_splits_resolvable_from_invented():
    real = _urn("test_db.public.events")
    result = SqlParsingResult(
        in_tables=[
            real,
            _urn("test_db.${schema}.events"),
            _urn("test_db.public.[[tbl]]"),
            _urn("test_db.public.$tbl"),
            _urn("test_db.public"),  # a table function, qualifier only
        ],
        out_tables=[],
    )

    kept, dropped = _partition_upstreams(
        result, POSTGRES_CONNECTION, from_cleaned_query=False
    )
    assert kept == [real]
    # identities, not a count: two compensating mis-partitions would pass a count
    assert dropped == [
        _urn("test_db.${schema}.events"),
        _urn("test_db.public.[[tbl]]"),
        _urn("test_db.public.$tbl"),
        _urn("test_db.public"),
    ]

    # Oracle allows $ inside identifiers; those are real tables, not variables.
    oracle_table = _urn("db.sys.V$SESSION", platform="oracle")
    kept, dropped = _partition_upstreams(
        SqlParsingResult(in_tables=[oracle_table], out_tables=[]),
        POSTGRES_CONNECTION,
        from_cleaned_query=False,
    )
    assert kept == [oracle_table]
    assert not dropped


def test_partition_upstreams_rejects_placeholders_only_from_the_cleaned_query():
    invented = SqlParsingResult(
        in_tables=[_urn("test_db.public.grafana_identifier")], out_tables=[]
    )
    # The cleaner invents that name, so on that path it stands for a table we never
    # identified...
    kept, _dropped = _partition_upstreams(
        invented, POSTGRES_CONNECTION, from_cleaned_query=True
    )
    assert not kept

    # ...but a table genuinely authored with that name is a real table.
    kept, _dropped = _partition_upstreams(
        invented, POSTGRES_CONNECTION, from_cleaned_query=False
    )
    assert kept == invented.in_tables

    # A name that merely starts with the placeholder text is not the placeholder.
    ok = SqlParsingResult(
        in_tables=[_urn("grafana_var_store.public.events")], out_tables=[]
    )
    kept, _dropped = _partition_upstreams(
        ok, POSTGRES_CONNECTION, from_cleaned_query=True
    )
    assert kept == ok.in_tables


def test_restrict_to_upstreams_narrows_column_lineage_too():
    kept_urn = _urn("test_db.public.events")
    dropped_urn = _urn("test_db.public.grafana_identifier")
    result = SqlParsingResult(
        in_tables=[kept_urn, dropped_urn],
        out_tables=[],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=None, column="x"),
                upstreams=[
                    ColumnRef(table=kept_urn, column="a"),
                    ColumnRef(table=dropped_urn, column="b"),
                ],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=None, column="y"),
                upstreams=[ColumnRef(table=dropped_urn, column="c")],
            ),
            # a constant column has no upstreams to begin with, so narrowing must
            # not mistake it for one that was emptied
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=None, column="literal"),
                upstreams=[],
            ),
        ],
    )

    narrowed = _restrict_to_upstreams(result, [kept_urn])

    assert narrowed.in_tables == [kept_urn]
    # A fine-grained edge pointing at a dropped table would create in DataHub the
    # very dataset the drop avoided, so the whole "y" entry goes.
    assert narrowed.column_lineage is not None
    assert [cl.downstream.column for cl in narrowed.column_lineage] == ["x", "literal"]
    assert [ref.table for ref in narrowed.column_lineage[0].upstreams] == [kept_urn]
    assert narrowed.column_lineage[1].upstreams == []


def test_partition_upstreams_without_a_configured_database_or_schema():
    # Both are Optional, so the qualifier-only test must not fire when there is no
    # qualifier to match against.
    config = PlatformConnectionConfig(platform="postgres")
    result = SqlParsingResult(in_tables=[_urn("events")], out_tables=[])

    kept, dropped = _partition_upstreams(result, config, from_cleaned_query=False)
    assert kept == [_urn("events")]
    assert not dropped


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
