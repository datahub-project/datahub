import json
from datetime import datetime, timezone

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BucketDuration
from datahub.ingestion.source.snowflake import snowflake_query
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery

NUM_TABLES = 10
NUM_VIEWS = 2
NUM_COLS = 10
NUM_OPS = 10


FROZEN_TIME = "2022-06-07 17:00:00"


def default_query_results(  # noqa: C901
    query,
    num_tables=NUM_TABLES,
    num_views=NUM_VIEWS,
    num_cols=NUM_COLS,
    num_ops=NUM_OPS,
):
    if query == SnowflakeQuery.current_account():
        return [{"CURRENT_ACCOUNT()": "ABC12345"}]
    if query == SnowflakeQuery.current_region():
        return [{"CURRENT_REGION()": "AWS_AP_SOUTH_1"}]
    if query == SnowflakeQuery.show_tags():
        return []
    if query == SnowflakeQuery.current_role():
        return [{"CURRENT_ROLE()": "TEST_ROLE"}]
    elif query == SnowflakeQuery.current_version():
        return [{"CURRENT_VERSION()": "X.Y.Z"}]
    elif query == SnowflakeQuery.current_database():
        return [{"CURRENT_DATABASE()": "TEST_DB"}]
    elif query == SnowflakeQuery.current_schema():
        return [{"CURRENT_SCHEMA()": "TEST_SCHEMA"}]
    elif query == SnowflakeQuery.current_warehouse():
        return [{"CURRENT_WAREHOUSE()": "TEST_WAREHOUSE"}]
    elif query == SnowflakeQuery.show_databases():
        return [
            {
                "name": "TEST_DB",
                "created_on": datetime(2021, 6, 8, 0, 0, 0, 0),
                "comment": "Comment for TEST_DB",
            }
        ]
    elif query == SnowflakeQuery.get_databases("TEST_DB"):
        return [
            {
                "DATABASE_NAME": "TEST_DB",
                "CREATED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "LAST_ALTERED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "COMMENT": "Comment for TEST_DB",
            }
        ]
    elif query == SnowflakeQuery.schemas_for_database("TEST_DB"):
        return [
            {
                "SCHEMA_NAME": "TEST_SCHEMA",
                "CREATED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "LAST_ALTERED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "COMMENT": "comment for TEST_DB.TEST_SCHEMA",
            },
            {
                "SCHEMA_NAME": "TEST2_SCHEMA",
                "CREATED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "LAST_ALTERED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "COMMENT": "comment for TEST_DB.TEST_SCHEMA",
            },
        ]
    elif query == SnowflakeQuery.tables_for_database("TEST_DB"):
        raise Exception("Information schema query returned too much data")
    elif query == SnowflakeQuery.show_views_for_database("TEST_DB"):
        raise Exception("Information schema query returned too much data")
    elif query == SnowflakeQuery.tables_for_schema("TEST_SCHEMA", "TEST_DB"):
        return [
            {
                "TABLE_SCHEMA": "TEST_SCHEMA",
                "TABLE_NAME": "TABLE_{}".format(tbl_idx),
                "CREATED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "LAST_ALTERED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "BYTES": 1024,
                "ROW_COUNT": 10000,
                "COMMENT": "Comment for Table",
                "CLUSTERING_KEY": None,
            }
            for tbl_idx in range(1, num_tables + 1)
        ]
    elif query == SnowflakeQuery.show_views_for_schema("TEST_SCHEMA", "TEST_DB"):
        return [
            {
                "schema_name": "TEST_SCHEMA",
                "name": "VIEW_{}".format(view_idx),
                "created_on": datetime(2021, 6, 8, 0, 0, 0, 0),
                "comment": "Comment for View",
                "text": f"create view view_{view_idx} as select * from table_{view_idx}",
            }
            for view_idx in range(1, num_views + 1)
        ]
    elif query == SnowflakeQuery.columns_for_schema("TEST_SCHEMA", "TEST_DB"):
        raise Exception("Information schema query returned too much data")
    elif query in [
        *[
            SnowflakeQuery.columns_for_table(
                "TABLE_{}".format(tbl_idx), "TEST_SCHEMA", "TEST_DB"
            )
            for tbl_idx in range(1, num_tables + 1)
        ],
        *[
            SnowflakeQuery.columns_for_table(
                "VIEW_{}".format(view_idx), "TEST_SCHEMA", "TEST_DB"
            )
            for view_idx in range(1, num_views + 1)
        ],
    ]:
        return [
            {
                # "TABLE_CATALOG": "TEST_DB",
                # "TABLE_SCHEMA": "TEST_SCHEMA",
                # "TABLE_NAME": "TABLE_{}".format(tbl_idx),
                "COLUMN_NAME": "COL_{}".format(col_idx),
                "ORDINAL_POSITION": col_idx,
                "IS_NULLABLE": "NO",
                "DATA_TYPE": "TEXT" if col_idx > 1 else "NUMBER",
                "COMMENT": "Comment for column",
                "CHARACTER_MAXIMUM_LENGTH": 255 if col_idx > 1 else None,
                "NUMERIC_PRECISION": None if col_idx > 1 else 38,
                "NUMERIC_SCALE": None if col_idx > 1 else 0,
            }
            for col_idx in range(1, num_cols + 1)
        ]
    elif query in (
        SnowflakeQuery.use_database("TEST_DB"),
        SnowflakeQuery.show_primary_keys_for_schema("TEST_SCHEMA", "TEST_DB"),
        SnowflakeQuery.show_foreign_keys_for_schema("TEST_SCHEMA", "TEST_DB"),
    ):
        return []
    elif query == SnowflakeQuery.get_access_history_date_range():
        return [
            {
                "MIN_TIME": datetime(2021, 6, 8, 0, 0, 0, 0),
                "MAX_TIME": datetime(2022, 6, 7, 7, 17, 0, 0),
            }
        ]
    elif query == snowflake_query.SnowflakeQuery.operational_data_for_time_window(
        1654473600000,
        1654586220000,
    ):
        return [
            {
                "QUERY_START_TIME": datetime(2022, 6, 2, 4, 41, 1, 367000).replace(
                    tzinfo=timezone.utc
                ),
                "QUERY_TEXT": "create or replace table TABLE_{}  as select * from TABLE_2 left join TABLE_3 using COL_1 left join TABLE 4 using COL2".format(
                    op_idx
                ),
                "QUERY_TYPE": "CREATE_TABLE_AS_SELECT",
                "ROWS_INSERTED": 0,
                "ROWS_UPDATED": 0,
                "ROWS_DELETED": 0,
                "BASE_OBJECTS_ACCESSED": json.dumps(
                    [
                        {
                            "columns": [
                                {"columnId": 0, "columnName": "COL_{}".format(col_idx)}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_2",
                        },
                        {
                            "columns": [
                                {"columnId": 0, "columnName": "COL_{}".format(col_idx)}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_3",
                        },
                        {
                            "columns": [
                                {"columnId": 0, "columnName": "COL_{}".format(col_idx)}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_4",
                        },
                    ]
                ),
                "DIRECT_OBJECTS_ACCESSED": json.dumps(
                    [
                        {
                            "columns": [
                                {"columnId": 0, "columnName": "COL_{}".format(col_idx)}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_2",
                        },
                        {
                            "columns": [
                                {"columnId": 0, "columnName": "COL_{}".format(col_idx)}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_3",
                        },
                        {
                            "columns": [
                                {"columnId": 0, "columnName": "COL_{}".format(col_idx)}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_4",
                        },
                    ]
                ),
                "OBJECTS_MODIFIED": json.dumps(
                    [
                        {
                            "columns": [
                                {
                                    "columnId": 0,
                                    "columnName": "COL_{}".format(col_idx),
                                    "directSources": [
                                        {
                                            "columnName": "COL_{}".format(col_idx),
                                            "objectDomain": "Table",
                                            "objectId": 0,
                                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_2",
                                        }
                                    ],
                                }
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_{}".format(op_idx),
                        }
                    ]
                ),
                "USER_NAME": "SERVICE_ACCOUNT_TESTS_ADMIN",
                "FIRST_NAME": None,
                "LAST_NAME": None,
                "DISPLAY_NAME": "SERVICE_ACCOUNT_TESTS_ADMIN",
                "EMAIL": "abc@xyz.com",
                "ROLE_NAME": "ACCOUNTADMIN",
            }
            for op_idx in range(1, num_ops + 1)
        ]
    elif (
        query
        == snowflake_query.SnowflakeQuery.usage_per_object_per_time_bucket_for_time_window(
            1654473600000,
            1654586220000,
            use_base_objects=False,
            top_n_queries=10,
            include_top_n_queries=True,
            time_bucket_size=BucketDuration.DAY,
            email_domain=None,
            email_filter=AllowDenyPattern.allow_all(),
        )
    ):
        return []
    elif query in (
        snowflake_query.SnowflakeQuery.table_to_table_lineage_history(
            1654473600000,
            1654586220000,
        ),
        snowflake_query.SnowflakeQuery.table_to_table_lineage_history(
            1654473600000, 1654586220000, False
        ),
    ):
        return [
            {
                "DOWNSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.TABLE_{}".format(op_idx),
                "UPSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.TABLE_2",
                "UPSTREAM_TABLE_COLUMNS": json.dumps(
                    [
                        {"columnId": 0, "columnName": "COL_{}".format(col_idx)}
                        for col_idx in range(1, num_cols + 1)
                    ]
                ),
                "DOWNSTREAM_TABLE_COLUMNS": json.dumps(
                    [
                        {
                            "columnId": 0,
                            "columnName": "COL_{}".format(col_idx),
                            "directSources": [
                                {
                                    "columnName": "COL_{}".format(col_idx),
                                    "objectDomain": "Table",
                                    "objectId": 0,
                                    "objectName": "TEST_DB.TEST_SCHEMA.TABLE_2",
                                }
                            ],
                        }
                        for col_idx in range(1, num_cols + 1)
                    ]
                ),
            }
            for op_idx in range(1, num_ops + 1)
        ] + [
            {
                "DOWNSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.TABLE_1",
                "UPSTREAM_TABLE_NAME": "OTHER_DB.OTHER_SCHEMA.TABLE_1",
                "UPSTREAM_TABLE_COLUMNS": json.dumps(
                    [{"columnId": 0, "columnName": "COL_1"}]
                ),
                "DOWNSTREAM_TABLE_COLUMNS": json.dumps(
                    [
                        {
                            "columnId": 0,
                            "columnName": "COL_1",
                            "directSources": [
                                {
                                    "columnName": "COL_1",
                                    "objectDomain": "Table",
                                    "objectId": 0,
                                    "objectName": "OTHER_DB.OTHER_SCHEMA.TABLE_1",
                                }
                            ],
                        }
                    ]
                ),
            }
        ]
    elif query in (
        snowflake_query.SnowflakeQuery.table_to_table_lineage_history_v2(
            start_time_millis=1654473600000,
            end_time_millis=1654586220000,
            include_view_lineage=True,
            include_column_lineage=True,
        ),
    ):
        return [
            {
                "DOWNSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.TABLE_{}".format(op_idx),
                "DOWNSTREAM_TABLE_DOMAIN": "TABLE",
                "UPSTREAM_TABLES": json.dumps(
                    [
                        {
                            "upstream_object_name": "TEST_DB.TEST_SCHEMA.TABLE_2",
                            "upstream_object_domain": "TABLE",
                        }
                    ]
                    + (  # This additional upstream is only for TABLE_1
                        [
                            {
                                "upstream_object_name": "TEST_DB.TEST_SCHEMA.VIEW_1",
                                "upstream_object_domain": "VIEW",
                            },
                            {
                                "upstream_object_name": "OTHER_DB.OTHER_SCHEMA.TABLE_1",
                                "upstream_object_domain": "TABLE",
                            },
                        ]
                        if op_idx == 1
                        else []
                    )
                ),
                "UPSTREAM_COLUMNS": json.dumps(
                    [
                        {
                            "column_name": "COL_{}".format(col_idx),
                            "upstreams": [
                                [
                                    {
                                        "object_name": "TEST_DB.TEST_SCHEMA.TABLE_2",
                                        "object_domain": "Table",
                                        "column_name": "COL_{}".format(col_idx),
                                    }
                                ]
                            ],
                        }
                        for col_idx in range(1, num_cols + 1)
                    ]
                    + (  # This additional upstream is only for TABLE_1
                        [
                            {
                                "column_name": "COL_1",
                                "upstreams": [
                                    [
                                        {
                                            "object_name": "OTHER_DB.OTHER_SCHEMA.TABLE_1",
                                            "object_domain": "Table",
                                            "column_name": "COL_1",
                                        }
                                    ]
                                ],
                            }
                        ]
                        if op_idx == 1
                        else []
                    )
                ),
            }
            for op_idx in range(1, num_ops + 1)
        ]
    elif query in (
        snowflake_query.SnowflakeQuery.table_to_table_lineage_history_v2(
            start_time_millis=1654473600000,
            end_time_millis=1654586220000,
            include_view_lineage=False,
            include_column_lineage=False,
        ),
    ):
        return [
            {
                "DOWNSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.TABLE_{}".format(op_idx),
                "DOWNSTREAM_TABLE_DOMAIN": "TABLE",
                "UPSTREAM_TABLES": json.dumps(
                    [
                        {
                            "upstream_object_name": "TEST_DB.TEST_SCHEMA.TABLE_2",
                            "upstream_object_domain": "TABLE",
                        },
                    ]
                    + (  # This additional upstream is only for TABLE_1
                        [
                            {
                                "upstream_object_name": "OTHER_DB.OTHER_SCHEMA.TABLE_1",
                                "upstream_object_domain": "TABLE",
                            },
                        ]
                        if op_idx == 1
                        else []
                    )
                ),
            }
            for op_idx in range(1, num_ops + 1)
        ]
    elif query in [
        snowflake_query.SnowflakeQuery.view_dependencies(),
    ]:
        return [
            {
                "REFERENCED_OBJECT_DOMAIN": "table",
                "REFERENCING_OBJECT_DOMAIN": "view",
                "DOWNSTREAM_VIEW": "TEST_DB.TEST_SCHEMA.VIEW_2",
                "VIEW_UPSTREAM": "TEST_DB.TEST_SCHEMA.TABLE_2",
            }
        ]
    elif query in [
        snowflake_query.SnowflakeQuery.view_dependencies_v2(),
    ]:
        # VIEW_2 has dependency on TABLE_2
        return [
            {
                "DOWNSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.VIEW_2",
                "DOWNSTREAM_TABLE_DOMAIN": "view",
                "UPSTREAM_TABLES": json.dumps(
                    [
                        {
                            "upstream_object_name": "TEST_DB.TEST_SCHEMA.TABLE_2",
                            "upstream_object_domain": "table",
                        }
                    ]
                ),
            }
        ]
    elif query in [
        snowflake_query.SnowflakeQuery.view_lineage_history(
            1654473600000,
            1654586220000,
        ),
        snowflake_query.SnowflakeQuery.view_lineage_history(
            1654473600000, 1654586220000, False
        ),
    ]:
        return [
            {
                "DOWNSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.TABLE_1",
                "VIEW_NAME": "TEST_DB.TEST_SCHEMA.VIEW_1",
                "VIEW_DOMAIN": "VIEW",
                "VIEW_COLUMNS": json.dumps(
                    [
                        {"columnId": 0, "columnName": "COL_{}".format(col_idx)}
                        for col_idx in range(1, num_cols + 1)
                    ]
                ),
                "DOWNSTREAM_TABLE_DOMAIN": "TABLE",
                "DOWNSTREAM_TABLE_COLUMNS": json.dumps(
                    [
                        {
                            "columnId": 0,
                            "columnName": "COL_{}".format(col_idx),
                            "directSources": [
                                {
                                    "columnName": "COL_{}".format(col_idx),
                                    "objectDomain": "Table",
                                    "objectId": 0,
                                    "objectName": "TEST_DB.TEST_SCHEMA.TABLE_2",
                                }
                            ],
                        }
                        for col_idx in range(1, num_cols + 1)
                    ]
                ),
            }
        ]
    elif query in [
        snowflake_query.SnowflakeQuery.view_dependencies_v2(),
        snowflake_query.SnowflakeQuery.view_dependencies(),
        snowflake_query.SnowflakeQuery.show_external_tables(),
    ]:
        return []

    elif (
        query
        == snowflake_query.SnowflakeQuery.get_all_tags_in_database_without_propagation(
            "TEST_DB"
        )
    ):
        return [
            *[
                {
                    "TAG_DATABASE": "TEST_DB",
                    "TAG_SCHEMA": "TEST_SCHEMA",
                    "TAG_NAME": f"my_tag_{ix}",
                    "TAG_VALUE": f"my_value_{ix}",
                    "OBJECT_DATABASE": "TEST_DB",
                    "OBJECT_SCHEMA": "TEST_SCHEMA",
                    "OBJECT_NAME": "VIEW_2",
                    "COLUMN_NAME": None,
                    "DOMAIN": "TABLE",
                }
                for ix in range(3)
            ],
            {
                "TAG_DATABASE": "TEST_DB",
                "TAG_SCHEMA": "TEST_SCHEMA",
                "TAG_NAME": "security",
                "TAG_VALUE": "pii",
                "OBJECT_DATABASE": "TEST_DB",
                "OBJECT_SCHEMA": "TEST_SCHEMA",
                "OBJECT_NAME": "VIEW_1",
                "COLUMN_NAME": "COL_1",
                "DOMAIN": "COLUMN",
            },
            {
                "TAG_DATABASE": "OTHER_DB",
                "TAG_SCHEMA": "OTHER_SCHEMA",
                "TAG_NAME": "my_other_tag",
                "TAG_VALUE": "other",
                "OBJECT_DATABASE": "TEST_DB",
                "OBJECT_SCHEMA": None,
                "OBJECT_NAME": "TEST_SCHEMA",
                "COLUMN_NAME": None,
                "DOMAIN": "SCHEMA",
            },
            {
                "TAG_DATABASE": "OTHER_DB",
                "TAG_SCHEMA": "OTHER_SCHEMA",
                "TAG_NAME": "my_other_tag",
                "TAG_VALUE": "other",
                "OBJECT_DATABASE": None,
                "OBJECT_SCHEMA": None,
                "OBJECT_NAME": "TEST_DB",
                "COLUMN_NAME": None,
                "DOMAIN": "DATABASE",
            },
        ]
    raise ValueError(f"Unexpected query: {query}")
