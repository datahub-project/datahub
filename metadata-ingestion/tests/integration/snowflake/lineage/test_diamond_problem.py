import pathlib
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

from datahub.ingestion.sink.file import write_metadata_file
from datahub.metadata._urns.urn_defs import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    QueryMetadata,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)
from datahub.testing import mce_helpers
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedDict
from datahub.utilities.ordered_set import OrderedSet


def connection_wrapper() -> ConnectionWrapper:
    tables: Dict[str, Dict[str, Any]] = {
        'stored_queries': {},
        'query_map': {
            '871e19419420f81274c085321111eaefb5b9b3d80992b4c0fb39c61458e58c43': QueryMetadata(
                query_id='871e19419420f81274c085321111eaefb5b9b3d80992b4c0fb39c61458e58c43',
                formatted_query_string='CREATE TABLE diamond_destination as select * from t4;',
                session_id='14774700499701726',
                query_type=QueryType.CREATE_TABLE_AS_SELECT,
                lineage_type='TRANSFORMED',
                latest_timestamp=datetime(2025, 7, 1, 13, 52, 22, 651000, tzinfo=timezone.utc),
                actor=CorpUserUrn(username='user@test'),
                upstreams=[
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t4,PROD)'
                ],
                column_lineage=[
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_b',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t4,PROD)', column='col_b')
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_a',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t4,PROD)', column='col_a')
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_c',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t4,PROD)', column='col_c')
                        ],
                        logic=None
                    )
                ],
                column_usage={
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t4,PROD)': {'col_a', 'col_c', 'col_b'}
                },
                confidence_score=1.0,
                used_temp_tables=True,
                extra_info={
                    'snowflake_query_id': '01bd6600-0908-c4fc-0034-7d831435616e',
                    'snowflake_root_query_id': None,
                    'snowflake_query_type': 'CREATE_TABLE_AS_SELECT',
                    'snowflake_role_name': 'ACCOUNTADMIN',
                    'query_duration': 496,
                    'rows_inserted': 0,
                    'rows_updated': 0,
                    'rows_deleted': 0
                },
                origin=None
            ),
            '62375ec02d44aa1fbaa8213ddd8d9e33d5ed80d93bd0e51942aa18f5643979d2': QueryMetadata(
                query_id='62375ec02d44aa1fbaa8213ddd8d9e33d5ed80d93bd0e51942aa18f5643979d2',
                formatted_query_string='CREATE TEMPORARY TABLE t1 as select * from diamond_source1;',
                session_id='14774700499701726',
                query_type=QueryType.CREATE_TABLE_AS_SELECT,
                lineage_type='TRANSFORMED',
                latest_timestamp=datetime(2025, 7, 1, 13, 52, 18, 741000, tzinfo=timezone.utc),
                actor=CorpUserUrn(username='user@test'),
                upstreams=[
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.diamond_source1,PROD)'
                ],
                column_lineage=[
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_a',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(
                                table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.diamond_source1,PROD)',
                                column='col_a'
                            )
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_c',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(
                                table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.diamond_source1,PROD)',
                                column='col_c'
                            )
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_b',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(
                                table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.diamond_source1,PROD)',
                                column='col_b'
                            )
                        ],
                        logic=None
                    )
                ],
                column_usage={
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.diamond_source1,PROD)': {'col_a', 'col_c', 'col_b'}
                },
                confidence_score=1.0,
                used_temp_tables=True,
                extra_info={
                    'snowflake_query_id': '01bd6600-0908-c087-0034-7d8314357182',
                    'snowflake_root_query_id': None,
                    'snowflake_query_type': 'CREATE_TABLE_AS_SELECT',
                    'snowflake_role_name': 'ACCOUNTADMIN',
                    'query_duration': 825,
                    'rows_inserted': 0,
                    'rows_updated': 0,
                    'rows_deleted': 0
                },
                origin=None
            ),
            'b61251c5f21ec2d897955a1324744d661763c88d456c956e5f8a329757ea62ac': QueryMetadata(
                query_id='b61251c5f21ec2d897955a1324744d661763c88d456c956e5f8a329757ea62ac',
                formatted_query_string='CREATE TEMPORARY TABLE t2 as select * from t1;',
                session_id='14774700499701726',
                query_type=QueryType.CREATE_TABLE_AS_SELECT,
                lineage_type='TRANSFORMED',
                latest_timestamp=datetime(2025, 7, 1, 13, 52, 19, 940000, tzinfo=timezone.utc),
                actor=CorpUserUrn(username='user@test'),
                upstreams=[
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)'
                ],
                column_lineage=[
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_c',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)', column='col_c')
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_a',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)', column='col_a')
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_b',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)', column='col_b')
                        ],
                        logic=None
                    )
                ],
                column_usage={
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)': {'col_a', 'col_c', 'col_b'}
                },
                confidence_score=1.0,
                used_temp_tables=True,
                extra_info={
                    'snowflake_query_id': '01bd6600-0908-c4fc-0034-7d831435615e',
                    'snowflake_root_query_id': None,
                    'snowflake_query_type': 'CREATE_TABLE_AS_SELECT',
                    'snowflake_role_name': 'ACCOUNTADMIN',
                    'query_duration': 604,
                    'rows_inserted': 0,
                    'rows_updated': 0,
                    'rows_deleted': 0
                },
                origin=None
            ),
            '242eba8de494f2cd8cba8806237e68108202e1b9de39cae1b676289a3c9ece50': QueryMetadata(
                query_id='242eba8de494f2cd8cba8806237e68108202e1b9de39cae1b676289a3c9ece50',
                formatted_query_string='CREATE TEMPORARY TABLE t3 as select * from t1;',
                session_id='14774700499701726',
                query_type=QueryType.CREATE_TABLE_AS_SELECT,
                lineage_type='TRANSFORMED',
                latest_timestamp=datetime(2025, 7, 1, 13, 52, 20, 863000, tzinfo=timezone.utc),
                actor=CorpUserUrn(username='user@test'),
                upstreams=[
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)'
                ],
                column_lineage=[
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_c',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)', column='col_c')
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_a',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)', column='col_a')
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_b',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)', column='col_b')
                        ],
                        logic=None
                    )
                ],
                column_usage={
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)': {'col_a', 'col_c', 'col_b'}
                },
                confidence_score=1.0,
                used_temp_tables=True,
                extra_info={
                    'snowflake_query_id': '01bd6600-0908-bfbc-0034-7d83143533da',
                    'snowflake_root_query_id': None,
                    'snowflake_query_type': 'CREATE_TABLE_AS_SELECT',
                    'snowflake_role_name': 'ACCOUNTADMIN',
                    'query_duration': 442,
                    'rows_inserted': 0,
                    'rows_updated': 0,
                    'rows_deleted': 0
                },
                origin=None
            ),
            'fb6fbee45bd6d3131b1158effbf13031a27d7b1941558fa3597471a12b61409f': QueryMetadata(
                query_id='fb6fbee45bd6d3131b1158effbf13031a27d7b1941558fa3597471a12b61409f',
                formatted_query_string='CREATE TEMPORARY TABLE t4 as select t2.col_a, t3.col_b, t2.col_c from t2 join t3 on t2.col_a = t3.col_a;',
                session_id='14774700499701726',
                query_type=QueryType.CREATE_TABLE_AS_SELECT,
                lineage_type='TRANSFORMED',
                latest_timestamp=datetime(2025, 7, 1, 13, 52, 21, 609000, tzinfo=timezone.utc),
                actor=CorpUserUrn(username='user@test'),
                upstreams=[
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t3,PROD)',
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t2,PROD)'
                ],
                column_lineage=[
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_b',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t3,PROD)', column='col_b')
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_a',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t2,PROD)', column='col_a')
                        ],
                        logic=None
                    ),
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=None,
                            column='col_c',
                            column_type=None,
                            native_column_type=None
                        ),
                        upstreams=[
                            ColumnRef(table='urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t2,PROD)', column='col_c')
                        ],
                        logic=None
                    )
                ],
                column_usage={
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t3,PROD)': {'col_a', 'col_b'},
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t2,PROD)': {'col_a', 'col_c'}
                },
                confidence_score=1.0,
                used_temp_tables=True,
                extra_info={
                    'snowflake_query_id': '01bd6600-0908-c4fc-0034-7d8314356166',
                    'snowflake_root_query_id': None,
                    'snowflake_query_type': 'CREATE_TABLE_AS_SELECT',
                    'snowflake_role_name': 'ACCOUNTADMIN',
                    'query_duration': 714,
                    'rows_inserted': 0,
                    'rows_updated': 0,
                    'rows_deleted': 0
                },
                origin=None
            ),
        },
        'lineage_map': {
            'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.diamond_destination,PROD)': OrderedSet(['871e19419420f81274c085321111eaefb5b9b3d80992b4c0fb39c61458e58c43'])
        },
        'view_definitions': {},
        'temp_lineage_map': {
            '14774700499701726': {
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t1,PROD)': OrderedSet(['62375ec02d44aa1fbaa8213ddd8d9e33d5ed80d93bd0e51942aa18f5643979d2']),
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t2,PROD)': OrderedSet(['b61251c5f21ec2d897955a1324744d661763c88d456c956e5f8a329757ea62ac']),
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t3,PROD)': OrderedSet(['242eba8de494f2cd8cba8806237e68108202e1b9de39cae1b676289a3c9ece50']),
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.t4,PROD)': OrderedSet(['fb6fbee45bd6d3131b1158effbf13031a27d7b1941558fa3597471a12b61409f'])
            }
        },
        'inferred_temp_schemas': {},
        'table_renames': {},
        'table_swaps': {},
        'query_usage_counts': {
            '62375ec02d44aa1fbaa8213ddd8d9e33d5ed80d93bd0e51942aa18f5643979d2': {datetime(2025, 7, 1, 0, 0, 0, 0, tzinfo=timezone.utc): 1},
            'b61251c5f21ec2d897955a1324744d661763c88d456c956e5f8a329757ea62ac': {datetime(2025, 7, 1, 0, 0, 0, 0, tzinfo=timezone.utc): 1},
            '242eba8de494f2cd8cba8806237e68108202e1b9de39cae1b676289a3c9ece50': {datetime(2025, 7, 1, 0, 0, 0, 0, tzinfo=timezone.utc): 1},
            'fb6fbee45bd6d3131b1158effbf13031a27d7b1941558fa3597471a12b61409f': {datetime(2025, 7, 1, 0, 0, 0, 0, tzinfo=timezone.utc): 1},
            '871e19419420f81274c085321111eaefb5b9b3d80992b4c0fb39c61458e58c43': {datetime(2025, 7, 1, 0, 0, 0, 0, tzinfo=timezone.utc): 1}
        }
    }

    conn = ConnectionWrapper(filename=":memory:") # type: ignore[arg-type]

    for table_name, data in tables.items():

        table_data: FileBackedDict[Any] = FileBackedDict(
            shared_connection=conn, tablename=table_name
        )
        for k, v in data.items():
            table_data[k] = v

        table_data.flush()

    return conn


def test_diamond_problem(pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
    conn = connection_wrapper()

    x = SqlParsingAggregator(platform="snowflake", shared_sql_connection=conn)
    mcpws = [mcp for mcp in x._gen_lineage_for_downstream("urn:li:dataset:(urn:li:dataPlatform:snowflake,diamond_problem.dummy_test.diamond_problem.diamond_destination,PROD)", queries_generated=set())]
    lineage_mcpws = [mcpw for mcpw in mcpws if mcpw.aspectName == "upstreamLineage"]
    out_path = tmp_path / "mcpw.json"
    write_metadata_file(out_path, lineage_mcpws)

    mce_helpers.check_golden_file(
        pytestconfig,
        out_path,
        pytestconfig.rootpath / "tests/integration/snowflake/lineage/diamond_problem_golden.json",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['upstreams'\]\[\d+\]\['auditStamp'\]\['time'\]",
        ],
    )
