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

    lineage = lineage_extractor.extract_panel_lineage(panel)
    assert lineage is None


def test_extract_panel_lineage_unknown_datasource(lineage_extractor):
    panel = Panel(
        id="1",
        title="Test Panel",
        type="graph",
        datasource={"type": "unknown", "uid": "unknown_uid"},
        targets=[],
    )

    lineage = lineage_extractor.extract_panel_lineage(panel)
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

    lineage = lineage_extractor.extract_panel_lineage(panel)
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

    lineage = lineage_extractor.extract_panel_lineage(panel)
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

    lineage = lineage_extractor.extract_panel_lineage(panel)
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
