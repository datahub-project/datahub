from datetime import datetime
from functools import partial
from typing import List
from unittest.mock import MagicMock

import datahub.sql_parsing.sqlglot_lineage as sqlglot_l
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.lineage import (
    LineageCollectorType,
    LineageDataset,
    LineageDatasetPlatform,
    LineageItem,
    RedshiftLineageExtractor,
    parse_alter_table_rename,
)
from datahub.ingestion.source.redshift.redshift_schema import TempTableRow
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.metadata.schema_classes import NumberTypeClass, SchemaFieldDataTypeClass
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    DownstreamColumnRef,
    SqlParsingDebugInfo,
    SqlParsingResult,
)
from tests.unit.redshift.redshift_query_mocker import mock_cursor


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
        host_port="localhost:5439",
        database="test",
        resolve_temp_table_in_lineage=True,
        start_time=datetime(2024, 1, 1, 12, 0, 0).isoformat() + "Z",
        end_time=datetime(2024, 1, 10, 12, 0, 0).isoformat() + "Z",
    )
    report = RedshiftReport()

    lineage_extractor = RedshiftLineageExtractor(
        config, report, PipelineContext(run_id="foo", graph=mock_graph())
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


def cursor_execute_side_effect(cursor: MagicMock, query: str) -> None:
    mock_cursor(cursor=cursor, query=query)


def mock_redshift_connection() -> MagicMock:
    connection = MagicMock()

    cursor = MagicMock()

    connection.cursor.return_value = cursor

    cursor.execute.side_effect = partial(cursor_execute_side_effect, cursor)

    return connection


def mock_graph() -> DataHubGraph:
    graph = MagicMock()

    graph._make_schema_resolver.return_value = SchemaResolver(
        platform="redshift",
        env="PROD",
        platform_instance=None,
        graph=None,
    )

    return graph


def test_collapse_temp_lineage():
    lineage_extractor = get_lineage_extractor()

    connection: MagicMock = mock_redshift_connection()

    lineage_extractor._init_temp_table_schema(
        database=lineage_extractor.config.database,
        temp_tables=list(lineage_extractor.get_temp_tables(connection=connection)),
    )

    lineage_extractor._populate_lineage_map(
        query="select * from test_collapse_temp_lineage",
        database=lineage_extractor.config.database,
        all_tables_set={
            lineage_extractor.config.database: {"public": {"player_price_with_hike_v6"}}
        },
        connection=connection,
        lineage_type=LineageCollectorType.QUERY_SQL_PARSER,
    )

    print(lineage_extractor._lineage_map)

    target_urn: str = "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.player_price_with_hike_v6,PROD)"

    assert lineage_extractor._lineage_map.get(target_urn) is not None

    lineage_item: LineageItem = lineage_extractor._lineage_map[target_urn]

    assert list(lineage_item.upstreams)[0].urn == (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.player_activity,PROD)"
    )

    assert lineage_item.cll is not None

    assert lineage_item.cll[0].downstream.table == (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,"
        "test.public.player_price_with_hike_v6,PROD)"
    )

    assert lineage_item.cll[0].downstream.column == "price"

    assert lineage_item.cll[0].upstreams[0].table == (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.player_activity,PROD)"
    )

    assert lineage_item.cll[0].upstreams[0].column == "price"


def test_collapse_temp_recursive_cll_lineage():
    lineage_extractor = get_lineage_extractor()

    temp_table: TempTableRow = TempTableRow(
        transaction_id=126,
        query_text="CREATE TABLE #player_price distkey(player_id) AS SELECT player_id, SUM(price_usd) AS price_usd "
        "from #player_activity_temp group by player_id",
        start_time=datetime.now(),
        session_id="abc",
        create_command="CREATE TABLE #player_price",
        parsed_result=SqlParsingResult(
            query_type=QueryType.CREATE_TABLE_AS_SELECT,
            in_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)"
            ],
            out_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)"
            ],
            debug_info=SqlParsingDebugInfo(),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                        column="player_id",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="INTEGER",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                            column="player_id",
                        )
                    ],
                    logic=None,
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                        column="price_usd",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="BIGINT",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                            column="price_usd",
                        )
                    ],
                    logic=None,
                ),
            ],
        ),
        urn="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
    )

    temp_table_activity: TempTableRow = TempTableRow(
        transaction_id=127,
        query_text="CREATE TABLE #player_activity_temp SELECT player_id, SUM(price) AS price_usd "
        "from player_activity",
        start_time=datetime.now(),
        session_id="abc",
        create_command="CREATE TABLE #player_activity_temp",
        parsed_result=SqlParsingResult(
            query_type=QueryType.CREATE_TABLE_AS_SELECT,
            in_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)"
            ],
            out_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)"
            ],
            debug_info=SqlParsingDebugInfo(),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                        column="player_id",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="INTEGER",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)",
                            column="player_id",
                        )
                    ],
                    logic=None,
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                        column="price_usd",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="BIGINT",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)",
                            column="price",
                        )
                    ],
                    logic=None,
                ),
            ],
        ),
        urn="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
    )

    assert temp_table.urn
    assert temp_table_activity.urn

    lineage_extractor.temp_tables[temp_table.urn] = temp_table
    lineage_extractor.temp_tables[temp_table_activity.urn] = temp_table_activity

    target_dataset_cll: List[sqlglot_l.ColumnLineageInfo] = [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_price_with_hike_v6,PROD)",
                column="price",
                column_type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                native_column_type="DOUBLE PRECISION",
            ),
            upstreams=[
                sqlglot_l.ColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                    column="price_usd",
                )
            ],
            logic=None,
        )
    ]

    datasets = lineage_extractor._get_upstream_lineages(
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
        target_dataset_cll=target_dataset_cll,
    )

    assert len(datasets) == 1

    assert (
        datasets[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)"
    )

    assert target_dataset_cll[0].upstreams[0].table == (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)"
    )
    assert target_dataset_cll[0].upstreams[0].column == "price"


def test_collapse_temp_recursive_with_compex_column_cll_lineage():
    lineage_extractor = get_lineage_extractor()

    temp_table: TempTableRow = TempTableRow(
        transaction_id=126,
        query_text="CREATE TABLE #player_price distkey(player_id) AS SELECT player_id, SUM(price+tax) AS price_usd "
        "from #player_activity_temp group by player_id",
        start_time=datetime.now(),
        session_id="abc",
        create_command="CREATE TABLE #player_price",
        parsed_result=SqlParsingResult(
            query_type=QueryType.CREATE_TABLE_AS_SELECT,
            in_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)"
            ],
            out_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)"
            ],
            debug_info=SqlParsingDebugInfo(),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                        column="player_id",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="INTEGER",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                            column="player_id",
                        )
                    ],
                    logic=None,
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                        column="price_usd",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="BIGINT",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                            column="price",
                        ),
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                            column="tax",
                        ),
                    ],
                    logic=None,
                ),
            ],
        ),
        urn="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
    )

    temp_table_activity: TempTableRow = TempTableRow(
        transaction_id=127,
        query_text="CREATE TABLE #player_activity_temp SELECT player_id, price, tax "
        "from player_activity",
        start_time=datetime.now(),
        session_id="abc",
        create_command="CREATE TABLE #player_activity_temp",
        parsed_result=SqlParsingResult(
            query_type=QueryType.CREATE_TABLE_AS_SELECT,
            in_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)"
            ],
            out_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)"
            ],
            debug_info=SqlParsingDebugInfo(),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                        column="player_id",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="INTEGER",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)",
                            column="player_id",
                        )
                    ],
                    logic=None,
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                        column="price",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="BIGINT",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)",
                            column="price",
                        )
                    ],
                    logic=None,
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                        column="tax",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="BIGINT",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)",
                            column="tax",
                        )
                    ],
                    logic=None,
                ),
            ],
        ),
        urn="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
    )
    assert temp_table.urn
    assert temp_table_activity.urn

    lineage_extractor.temp_tables[temp_table.urn] = temp_table
    lineage_extractor.temp_tables[temp_table_activity.urn] = temp_table_activity

    target_dataset_cll: List[sqlglot_l.ColumnLineageInfo] = [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_price_with_hike_v6,PROD)",
                column="price",
                column_type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                native_column_type="DOUBLE PRECISION",
            ),
            upstreams=[
                sqlglot_l.ColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                    column="price_usd",
                )
            ],
            logic=None,
        ),
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_price_with_hike_v6,PROD)",
                column="player_id",
                column_type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                native_column_type="BIGINT",
            ),
            upstreams=[
                sqlglot_l.ColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                    column="player_id",
                )
            ],
            logic=None,
        ),
    ]

    datasets = lineage_extractor._get_upstream_lineages(
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
        target_dataset_cll=target_dataset_cll,
    )

    assert len(datasets) == 1

    assert (
        datasets[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)"
    )

    assert target_dataset_cll[0].upstreams[0].table == (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)"
    )
    assert target_dataset_cll[0].upstreams[0].column == "price"
    assert target_dataset_cll[0].upstreams[1].column == "tax"
    assert target_dataset_cll[1].upstreams[0].column == "player_id"


def test_collapse_temp_recursive_cll_lineage_with_circular_reference():
    lineage_extractor = get_lineage_extractor()

    temp_table: TempTableRow = TempTableRow(
        transaction_id=126,
        query_text="CREATE TABLE #player_price distkey(player_id) AS SELECT player_id, SUM(price_usd) AS price_usd "
        "from #player_activity_temp group by player_id",
        start_time=datetime.now(),
        session_id="abc",
        create_command="CREATE TABLE #player_price",
        parsed_result=SqlParsingResult(
            query_type=QueryType.CREATE_TABLE_AS_SELECT,
            in_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)"
            ],
            out_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)"
            ],
            debug_info=SqlParsingDebugInfo(),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                        column="player_id",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="INTEGER",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                            column="player_id",
                        )
                    ],
                    logic=None,
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                        column="price_usd",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="BIGINT",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                            column="price_usd",
                        )
                    ],
                    logic=None,
                ),
            ],
        ),
        urn="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
    )

    temp_table_activity: TempTableRow = TempTableRow(
        transaction_id=127,
        query_text="CREATE TABLE #player_activity_temp SELECT player_id, SUM(price) AS price_usd "
        "from #player_price",
        start_time=datetime.now(),
        session_id="abc",
        create_command="CREATE TABLE #player_activity_temp",
        parsed_result=SqlParsingResult(
            query_type=QueryType.CREATE_TABLE_AS_SELECT,
            in_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_activity,PROD)"
            ],
            out_tables=[
                "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)"
            ],
            debug_info=SqlParsingDebugInfo(),
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                        column="player_id",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="INTEGER",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                            column="player_id",
                        )
                    ],
                    logic=None,
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                        column="price_usd",
                        column_type=SchemaFieldDataTypeClass(NumberTypeClass()),
                        native_column_type="BIGINT",
                    ),
                    upstreams=[
                        sqlglot_l.ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
                            column="price_usd",
                        )
                    ],
                    logic=None,
                ),
            ],
        ),
        urn="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_activity_temp,PROD)",
    )

    assert temp_table.urn
    assert temp_table_activity.urn

    lineage_extractor.temp_tables[temp_table.urn] = temp_table
    lineage_extractor.temp_tables[temp_table_activity.urn] = temp_table_activity

    target_dataset_cll: List[sqlglot_l.ColumnLineageInfo] = [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.player_price_with_hike_v6,PROD)",
                column="price",
                column_type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                native_column_type="DOUBLE PRECISION",
            ),
            upstreams=[
                sqlglot_l.ColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.#player_price,PROD)",
                    column="price_usd",
                )
            ],
            logic=None,
        )
    ]

    datasets = lineage_extractor._get_upstream_lineages(
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
        target_dataset_cll=target_dataset_cll,
    )

    assert len(datasets) == 1
    # Here we only interested if it fails or not
