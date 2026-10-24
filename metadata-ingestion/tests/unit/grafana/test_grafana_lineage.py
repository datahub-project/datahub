import logging
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.grafana.grafana_config import PlatformConnectionConfig
from datahub.ingestion.source.grafana.lineage import LineageExtractor
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


@pytest.fixture
def parsing_extractor(lineage_extractor, mock_graph):
    # A bare MagicMock graph makes every parse fail, which would make the success
    # case below indistinguishable from the failure case.
    mock_graph._make_schema_resolver.return_value = SchemaResolver(
        platform="postgres", env="PROD", graph=None
    )
    return lineage_extractor


def test_parse_sql_reports_parse_failures(parsing_extractor, mock_report):
    # Cleaning rewrites the quoted '${id}' to ''grafana_var'', which no dialect
    # parses. create_lineage_sql_parsed_result swallows that into a truthy result.
    parsing_extractor._parse_sql(
        "SELECT value FROM test_table WHERE id = CAST('${id}' AS INTEGER)",
        POSTGRES_CONNECTION,
    )

    assert mock_report.sql_parsing_attempts == 1
    assert mock_report.sql_parsing_failures == 1
    assert mock_report.sql_parsing_successes == 0
    # Assert the title rather than the count: warnings are deduplicated by
    # title+message, so a count cannot say which warning was raised.
    assert [w.title for w in mock_report.warnings] == ["Panel SQL could not be parsed"]


def test_parse_sql_reports_success_without_warning(parsing_extractor, mock_report):
    parsed = parsing_extractor._parse_sql(
        "SELECT value FROM test_table", POSTGRES_CONNECTION
    )

    assert parsed is not None
    assert mock_report.sql_parsing_successes == 1
    assert mock_report.sql_parsing_failures == 0
    assert list(mock_report.warnings) == []


@pytest.mark.parametrize(
    "raw_sql", ["SELECT 1", "SELECT now()"], ids=["constant", "function"]
)
def test_parse_sql_does_not_call_a_table_less_query_a_failure(
    parsing_extractor, mock_report, raw_sql
):
    # A stat panel selecting a constant parses cleanly and simply has no upstream.
    # Counting that as a parse failure would flag ordinary dashboards as broken.
    assert parsing_extractor._parse_sql(raw_sql, POSTGRES_CONNECTION) is None
    assert mock_report.sql_parsing_failures == 0
    assert list(mock_report.warnings) == []
    assert mock_report.panels_without_lineage == 1


def test_parse_sql_reports_a_failure_to_reach_the_graph(parsing_extractor, mock_graph):
    # create_schema_resolver runs outside create_lineage_sql_parsed_result's own
    # try block, so a graph that cannot be reached raises rather than returning a
    # result carrying the error. Without this the failure is counted but invisible,
    # and a graph outage yields datasource-UID lineage for every panel.
    mock_graph._make_schema_resolver.side_effect = ConnectionError("GMS unreachable")

    assert (
        parsing_extractor._parse_sql("SELECT v FROM test_table", POSTGRES_CONNECTION)
        is None
    )
    assert parsing_extractor.report.sql_parsing_failures == 1
    # A systemic outage must not be filed under the per-panel SQL title, or it is
    # buried behind hundreds of entries blaming the dashboards.
    assert [w.title for w in parsing_extractor.report.warnings] == [
        "Unable to reach DataHub graph for SQL parsing"
    ]


def test_parse_sql_reports_a_non_string_query_without_raising(
    parsing_extractor, mock_report
):
    # rawSql comes from user-authored dashboard JSON and is not guaranteed to be a
    # string. Reporting the failure must not fail on the value it is reporting -
    # raising here would cost the panel even its datasource fallback.
    panel = Panel(
        id="1",
        title="Test Panel",
        type="graph",
        datasource={"type": "postgres", "uid": "postgres_uid"},
        targets=[{"rawSql": {"unexpected": "shape"}}],
    )

    lineage = parsing_extractor.extract_panel_lineage(panel, "test-dashboard")

    assert lineage is not None
    assert mock_report.sql_parsing_failures == 1


def test_parse_sql_warns_once_when_no_graph_is_configured(
    lineage_extractor, mock_report, caplog
):
    lineage_extractor.graph = None

    with caplog.at_level(logging.WARNING, logger="datahub.ingestion.api.source"):
        for _ in range(3):
            assert lineage_extractor._parse_sql("SELECT 1", POSTGRES_CONNECTION) is None

    assert [w.title for w in mock_report.warnings] == [
        "No DataHub graph configured for SQL parsing"
    ]
    # The report deduplicates by title+message on its own, so the report alone
    # cannot show whether the once-only latch works. The console log can: without
    # it every panel emits the same line.
    assert len(caplog.records) == 1


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
