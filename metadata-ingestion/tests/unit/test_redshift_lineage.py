from datetime import datetime
from typing import List
from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.lineage import (
    LineageDataset,
    LineageDatasetPlatform,
    RedshiftLineageExtractor,
    parse_alter_table_rename,
)
from datahub.ingestion.source.redshift.redshift_schema import TempTableRow
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.utilities.sqlglot_lineage import ColumnLineageInfo, DownstreamColumnRef


def test_get_sources_from_query():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    report = RedshiftReport()

    test_query = """
        select * from my_schema.my_table
    """
    lineage_extractor = RedshiftLineageExtractor(
        config, report, PipelineContext(run_id="foo")
    )
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.my_schema.my_table,PROD)"
    )


def test_get_sources_from_query_with_only_table_name():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    report = RedshiftReport()

    test_query = """
        select * from my_table
    """
    lineage_extractor = RedshiftLineageExtractor(
        config, report, PipelineContext(run_id="foo")
    )
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.my_table,PROD)"
    )


def test_get_sources_from_query_with_database():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    report = RedshiftReport()

    test_query = """
        select * from test.my_schema.my_table
    """
    lineage_extractor = RedshiftLineageExtractor(
        config, report, PipelineContext(run_id="foo")
    )
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.my_schema.my_table,PROD)"
    )


def test_get_sources_from_query_with_non_default_database():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    report = RedshiftReport()

    test_query = """
        select * from test2.my_schema.my_table
    """
    lineage_extractor = RedshiftLineageExtractor(
        config, report, PipelineContext(run_id="foo")
    )
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test2.my_schema.my_table,PROD)"
    )


def test_get_sources_from_query_with_only_table():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    report = RedshiftReport()

    test_query = """
        select * from my_table
    """
    lineage_extractor = RedshiftLineageExtractor(
        config, report, PipelineContext(run_id="foo")
    )
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.my_table,PROD)"
    )


def test_parse_alter_table_rename():
    assert parse_alter_table_rename("public", "alter table foo rename to bar") == (
        "public",
        "foo",
        "bar",
    )
    assert parse_alter_table_rename(
        "public", "alter table second_schema.storage_v2_stg rename to storage_v2; "
    ) == (
        "second_schema",
        "storage_v2_stg",
        "storage_v2",
    )


def get_lineage_extractor() -> RedshiftLineageExtractor:
    config = RedshiftConfig(
        host_port="localhost:5439", database="test", resolve_temp_table_in_lineage=True
    )
    report = RedshiftReport()
    lineage_extractor = RedshiftLineageExtractor(
        config, report, PipelineContext(run_id="foo")
    )

    return lineage_extractor


def test_cll():
    test_query = """
        select a,b,c from db.public.customer inner join db.public.order on db.public.customer.id = db.public.order.customer_id
    """

    lineage_extractor = get_lineage_extractor()

    _, cll = lineage_extractor._get_sources_from_query(db_name="db", query=test_query)

    assert cll == [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(table=None, column="a"),
            upstreams=[],
            logic=None,
        ),
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(table=None, column="b"),
            upstreams=[],
            logic=None,
        ),
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(table=None, column="c"),
            upstreams=[],
            logic=None,
        ),
    ]


def test_collapse_temp_lineage():
    lineage_extractor = get_lineage_extractor()

    lineage_extractor.get_temp_tables = lambda connection: [  # type: ignore
        TempTableRow(
            transaction_id=126,
            query_text="CREATE TABLE #player_price distkey(player_id) AS SELECT player_id, SUM(price) AS price_usd "
            "from player_activity group by player_id",
            start_time=datetime.now(),
            session_id="abc",
            create_command="CREATE TABLE #player_price",
        ),
    ]

    datasets: List[LineageDataset] = lineage_extractor._get_upstream_lineages(
        sources=[
            LineageDataset(
                platform=LineageDatasetPlatform.REDSHIFT,
                urn="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
            )
        ],
        target_table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_price_with_hike_v4,PROD)",
        raw_db_name="dev",
        alias_db_name="dev",
        all_tables_set={
            "dev": {
                "public": set(),
            }
        },
        connection=MagicMock(),
    )

    assert len(datasets) == 1
    assert (
        datasets[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)"
    )
