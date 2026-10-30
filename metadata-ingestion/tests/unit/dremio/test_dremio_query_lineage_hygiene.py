"""Query lineage from Dremio job history must not override the catalog walk:
the catalog's current view definition is authoritative for a view, and a
table that is in scope but no longer in the catalog (dropped, temporary)
must not come back as a live entity."""

from typing import List
from unittest.mock import Mock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioCatalog,
    DremioQuery,
)
from datahub.ingestion.source.dremio.dremio_source import DremioSource
from datahub.metadata.schema_classes import UpstreamLineageClass


def _urn(name: str) -> str:
    return make_dataset_urn("dremio", f"dremio.{name}")


ORDERS_VIEW = _urn("mysys.vdata.orders")
ORDERS_BLUE = _urn("my_s3.materialize.orders_blue")
ORDERS_GREEN = _urn("my_s3.materialize.orders_green")
SRC_ORDERS = _urn("src_db.sales.orders")


@pytest.fixture
def source(monkeypatch):
    mock_session = Mock()
    monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
    mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
    mock_session.post.return_value.status_code = 200

    config = DremioSourceConfig(
        hostname="dummy-host",
        port=9047,
        tls=False,
        include_query_lineage=True,
        schema_pattern=AllowDenyPattern(
            allow=["^mysys($|\\..*)", "^my_s3($|\\..*)", "^src_db($|\\..*)"]
        ),
    )
    ctx = Mock()
    ctx.run_id = "test-run-id"
    ctx.graph = None

    src = DremioSource(config, ctx)
    src.dremio_catalog = Mock(spec=DremioCatalog)
    # What the catalog walk found: orders_blue has been dropped since the
    # jobs below ran, so it is absent.
    src.catalog_dataset_names = {
        "mysys.vdata.orders",
        "my_s3.materialize.orders_green",
        "src_db.sales.orders",
    }
    return src


def _run_query(source: DremioSource, sql: str, ts: str) -> None:
    source.process_query(
        DremioQuery(
            job_id="job",
            username="dbt_user",
            submitted_ts=ts,
            query=sql,
            queried_datasets="[]",
        )
    )


def _mcps(source: DremioSource) -> List[MetadataChangeProposalWrapper]:
    return [
        mcp
        for mcp in source.sql_parsing_aggregator.gen_metadata()
        if isinstance(mcp, MetadataChangeProposalWrapper)
    ]


def _upstreams(mcps: List[MetadataChangeProposalWrapper], urn: str) -> List[str]:
    return [
        u.dataset
        for mcp in mcps
        if mcp.entityUrn == urn and isinstance(mcp.aspect, UpstreamLineageClass)
        for u in mcp.aspect.upstreams
    ]


@pytest.mark.parametrize(
    "view_ddl",
    [
        "CREATE OR REPLACE VIEW MySys.vdata.Orders AS "
        "SELECT * FROM my_s3.materialize.orders_blue",
        "create view MySys.vdata.Orders as select * from my_s3.materialize.orders_blue",
        # dbt prefixes its statements with a comment header.
        '/* {"app": "dbt", "node_id": "model.orders"} */\n'
        "-- deploy\n  create or replace view MySys.vdata.Orders as "
        "select * from my_s3.materialize.orders_blue",
    ],
)
def test_catalog_view_definition_wins_over_observed_view_ddl(source, view_ddl):
    source.sql_parsing_aggregator.add_view_definition(
        view_urn=ORDERS_VIEW,
        view_definition="SELECT * FROM my_s3.materialize.orders_green",
        default_db="dremio",
    )
    _run_query(source, view_ddl, "2024-01-01 12:00:00.000")

    assert _upstreams(_mcps(source), ORDERS_VIEW) == [ORDERS_GREEN]
    assert source.report.num_view_ddl_queries_skipped == 1


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE my_s3.materialize.orders_blue AS SELECT * FROM SRC_DB.sales.orders",
        "INSERT INTO my_s3.materialize.orders_blue SELECT * FROM SRC_DB.sales.orders",
    ],
)
def test_dropped_in_scope_table_is_not_resurrected(source, sql):
    _run_query(source, sql, "2024-01-01 12:00:00.000")

    mcps = _mcps(source)

    assert [m.aspectName for m in mcps if m.entityUrn == ORDERS_BLUE] == []
    assert source.report.lineage_dropped_not_in_catalog > 0


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE my_s3.materialize.orders_green AS SELECT * FROM SRC_DB.sales.orders",
        "INSERT INTO my_s3.materialize.orders_green SELECT * FROM SRC_DB.sales.orders",
    ],
)
def test_catalog_table_keeps_query_lineage(source, sql):
    _run_query(source, sql, "2024-01-01 12:00:00.000")

    assert _upstreams(_mcps(source), ORDERS_GREEN) == [SRC_ORDERS]
    assert source.report.lineage_dropped_not_in_catalog == 0


def test_is_allowed_table_normalises_case_and_prefix(source):
    assert source._is_allowed_table("dremio.MY_S3.Materialize.Orders_Green")
    assert source._is_allowed_table("my_s3.materialize.orders_green")


def test_out_of_scope_name_still_counted_as_filtered(source):
    assert not source._is_allowed_table("dremio.other_source.sales.orders")
    assert source.report.lineage_dropped_filtered == 1
    assert source.report.lineage_dropped_not_in_catalog == 0
