import json
import random
from datetime import datetime, timezone
from unittest.mock import MagicMock

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BucketDuration
from datahub.ingestion.source.snowflake import snowflake_query
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery

NUM_TABLES = 10
NUM_VIEWS = 2
NUM_COLS = 10
NUM_OPS = 10
NUM_USAGE = 0


def is_secure(view_idx):
    return view_idx == 1


FROZEN_TIME = "2022-06-07 17:00:00"
large_sql_query = """WITH object_access_history AS
        (
            select
                object.value : "objectName"::varchar AS object_name,
                object.value : "objectDomain"::varchar AS object_domain,
                object.value : "columns" AS object_columns,
                query_start_time,
                query_id,
                user_name
            from
                (
                    select
                        query_id,
                        query_start_time,
                        user_name,
                        -- Construct the email in the query, should match the Python behavior.
                        -- The user_email is only used by the email_filter_query.
                        NVL(USERS.email, CONCAT(LOWER(user_name), '')) AS user_email,
                        DIRECT_OBJECTS_ACCESSED
                    from
                        snowflake.account_usage.access_history
                    LEFT JOIN
                        snowflake.account_usage.users USERS
                        ON user_name = users.name
                    WHERE
                        query_start_time >= to_timestamp_ltz(1710720000000, 3)
                        AND query_start_time < to_timestamp_ltz(1713332173148, 3)
                )
                t,
                lateral flatten(input => t.DIRECT_OBJECTS_ACCESSED) object
            where
                NOT RLIKE(object_name,'.*\\.FIVETRAN_.*_STAGING\\..*','i') AND NOT RLIKE(object_name,'.*__DBT_TMP$','i') AND NOT RLIKE(object_name,'.*\\.SEGMENT_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}','i') AND NOT RLIKE(object_name,'.*\\.STAGING_.*_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}','i')
        )
        ,
        field_access_history AS
        (
            select
                o.*,
                col.value : "columnName"::varchar AS column_name
            from
                object_access_history o,
                lateral flatten(input => o.object_columns) col
        )
        ,
        basic_usage_counts AS
        (
            SELECT
                object_name,
                ANY_VALUE(object_domain) AS object_domain,
                DATE_TRUNC('DAY', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
                count(distinct(query_id)) AS total_queries,
                count( distinct(user_name) ) AS total_users
            FROM
                object_access_history
            GROUP BY
                bucket_start_time,
                object_name
        )
        ,
        field_usage_counts AS
        (
            SELECT
                object_name,
                column_name,
                DATE_TRUNC('DAY', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
                count(distinct(query_id)) AS total_queries
            FROM
                field_access_history
            GROUP BY
                bucket_start_time,
                object_name,
                column_name
        )
        ,
        user_usage_counts AS
        (
            SELECT
                object_name,
                DATE_TRUNC('DAY', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
                count(distinct(query_id)) AS total_queries,
                user_name,
                ANY_VALUE(users.email) AS user_email
            FROM
                object_access_history
                LEFT JOIN
                    snowflake.account_usage.users users
                    ON user_name = users.name
            GROUP BY
                bucket_start_time,
                object_name,
                user_name
        )
        ,
        top_queries AS
        (
            SELECT
                object_name,
                DATE_TRUNC('DAY', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
                query_history.query_text AS query_text,
                count(distinct(access_history.query_id)) AS total_queries
            FROM
                object_access_history access_history
            LEFT JOIN
                (
                    SELECT * FROM snowflake.account_usage.query_history
                    WHERE query_history.start_time >= to_timestamp_ltz(1710720000000, 3)
                        AND query_history.start_time < to_timestamp_ltz(1713332173148, 3)
                ) query_history
                ON access_history.query_id = query_history.query_id
            GROUP BY
                bucket_start_time,
                object_name,
                query_text
            QUALIFY row_number() over ( partition by bucket_start_time, object_name
            order by
                total_queries desc, query_text asc ) <= 10
        )
        select
            basic_usage_counts.object_name AS "OBJECT_NAME",
            basic_usage_counts.bucket_start_time AS "BUCKET_START_TIME",
            ANY_VALUE(basic_usage_counts.object_domain) AS "OBJECT_DOMAIN",
            ANY_VALUE(basic_usage_counts.total_queries) AS "TOTAL_QUERIES",
            ANY_VALUE(basic_usage_counts.total_users) AS "TOTAL_USERS",
            ARRAY_UNIQUE_AGG(top_queries.query_text) AS "TOP_SQL_QUERIES",
            ARRAY_UNIQUE_AGG(OBJECT_CONSTRUCT( 'col', field_usage_counts.column_name, 'total', field_usage_counts.total_queries ) ) AS "FIELD_COUNTS",
            ARRAY_UNIQUE_AGG(OBJECT_CONSTRUCT( 'user_name', user_usage_counts.user_name, 'email', user_usage_counts.user_email, 'total', user_usage_counts.total_queries ) ) AS "USER_COUNTS"
        from
            basic_usage_counts basic_usage_counts
            left join
                top_queries top_queries
                on basic_usage_counts.bucket_start_time = top_queries.bucket_start_time
                and basic_usage_counts.object_name = top_queries.object_name
            left join
                field_usage_counts field_usage_counts
                on basic_usage_counts.bucket_start_time = field_usage_counts.bucket_start_time
                and basic_usage_counts.object_name = field_usage_counts.object_name
            left join
                user_usage_counts user_usage_counts
                on basic_usage_counts.bucket_start_time = user_usage_counts.bucket_start_time
                and basic_usage_counts.object_name = user_usage_counts.object_name
        where
            basic_usage_counts.object_domain in ('Table','External table','View','Materialized view','Iceberg table')
            and basic_usage_counts.object_name is not null
        group by
            basic_usage_counts.object_name,
            basic_usage_counts.bucket_start_time
        order by
            basic_usage_counts.bucket_start_time
                                               """


def default_query_results(  # noqa: C901
    query,
    num_tables=NUM_TABLES,
    num_views=NUM_VIEWS,
    num_cols=NUM_COLS,
    num_ops=NUM_OPS,
    num_usages=NUM_USAGE,
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
    elif query == SnowflakeQuery.tables_for_schema("TEST_SCHEMA", "TEST_DB"):
        return [
            {
                "TABLE_SCHEMA": "TEST_SCHEMA",
                "TABLE_NAME": f"TABLE_{tbl_idx}",
                "TABLE_TYPE": "BASE TABLE",
                "CREATED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "LAST_ALTERED": datetime(2021, 6, 8, 0, 0, 0, 0),
                "BYTES": 1024,
                "ROW_COUNT": 10000,
                "COMMENT": "Comment for Table",
                "CLUSTERING_KEY": "LINEAR(COL_1)",
            }
            for tbl_idx in range(1, num_tables + 1)
        ]
    elif query == SnowflakeQuery.show_views_for_database("TEST_DB"):
        # TODO: Add tests for view pagination.
        return [
            {
                "schema_name": "TEST_SCHEMA",
                "name": f"VIEW_{view_idx}",
                "created_on": datetime(2021, 6, 8, 0, 0, 0, 0),
                "comment": "Comment for View",
                "is_secure": "true" if is_secure(view_idx) else "false",
                "text": (
                    f"create view view_{view_idx} as select * from table_{view_idx}"
                    if not is_secure(view_idx)
                    else None
                ),
            }
            for view_idx in range(1, num_views + 1)
        ]
    elif query == SnowflakeQuery.get_secure_view_definitions():
        return [
            {
                "TABLE_CATALOG": "TEST_DB",
                "TABLE_SCHEMA": "TEST_SCHEMA",
                "TABLE_NAME": f"VIEW_{view_idx}",
                "VIEW_DEFINITION": f"create view view_{view_idx} as select * from table_{view_idx}",
            }
            for view_idx in range(1, num_views + 1)
            if is_secure(view_idx)
        ]
    elif query == SnowflakeQuery.columns_for_schema("TEST_SCHEMA", "TEST_DB"):
        return [
            {
                "TABLE_CATALOG": "TEST_DB",
                "TABLE_SCHEMA": "TEST_SCHEMA",
                "TABLE_NAME": table_name,
                "COLUMN_NAME": f"COL_{col_idx}",
                "ORDINAL_POSITION": col_idx,
                "IS_NULLABLE": "NO",
                "DATA_TYPE": "TEXT" if col_idx > 1 else "NUMBER",
                "COMMENT": "Comment for column",
                "CHARACTER_MAXIMUM_LENGTH": 255 if col_idx > 1 else None,
                "NUMERIC_PRECISION": None if col_idx > 1 else 38,
                "NUMERIC_SCALE": None if col_idx > 1 else 0,
            }
            for table_name in (
                [f"TABLE_{tbl_idx}" for tbl_idx in range(1, num_tables + 1)]
                + [f"VIEW_{view_idx}" for view_idx in range(1, num_views + 1)]
            )
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
                                {"columnId": 0, "columnName": f"COL_{col_idx}"}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_2",
                        },
                        {
                            "columns": [
                                {"columnId": 0, "columnName": f"COL_{col_idx}"}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_3",
                        },
                        {
                            "columns": [
                                {"columnId": 0, "columnName": f"COL_{col_idx}"}
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
                                {"columnId": 0, "columnName": f"COL_{col_idx}"}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_2",
                        },
                        {
                            "columns": [
                                {"columnId": 0, "columnName": f"COL_{col_idx}"}
                                for col_idx in range(1, num_cols + 1)
                            ],
                            "objectDomain": "Table",
                            "objectId": 0,
                            "objectName": "TEST_DB.TEST_SCHEMA.TABLE_3",
                        },
                        {
                            "columns": [
                                {"columnId": 0, "columnName": f"COL_{col_idx}"}
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
                                    "columnName": f"COL_{col_idx}",
                                    "directSources": [
                                        {
                                            "columnName": f"COL_{col_idx}",
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
                            "objectName": f"TEST_DB.TEST_SCHEMA.TABLE_{op_idx}",
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
        mock = MagicMock()
        mock.__iter__.return_value = [
            {
                "OBJECT_NAME": f"TEST_DB.TEST_SCHEMA.TABLE_{i}{random.randint(99, 999) if i > num_tables else ''}",
                "BUCKET_START_TIME": datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                    tzinfo=timezone.utc
                ),
                "OBJECT_DOMAIN": "Table",
                "TOTAL_QUERIES": 10,
                "TOTAL_USERS": 1,
                "TOP_SQL_QUERIES": json.dumps([large_sql_query for _ in range(10)]),
                "FIELD_COUNTS": json.dumps(
                    [{"col": f"col{c}", "total": 10} for c in range(num_cols)]
                ),
                "USER_COUNTS": json.dumps(
                    [
                        {"email": f"abc{i}@xyz.com", "user_name": f"abc{i}", "total": 1}
                        for i in range(10)
                    ]
                ),
            }
            for i in range(num_usages)
        ]
        return mock
    elif query in (
        snowflake_query.SnowflakeQuery.table_to_table_lineage_history_v2(
            start_time_millis=1654473600000,
            end_time_millis=1654586220000,
            include_column_lineage=True,
        ),
    ):
        return [
            {
                "DOWNSTREAM_TABLE_NAME": f"TEST_DB.TEST_SCHEMA.TABLE_{op_idx}",
                "DOWNSTREAM_TABLE_DOMAIN": "TABLE",
                "UPSTREAM_TABLES": json.dumps(
                    [
                        {
                            "upstream_object_name": "TEST_DB.TEST_SCHEMA.TABLE_2",
                            "upstream_object_domain": "TABLE",
                            "query_id": f"01b2576e-0804-4957-0034-7d83066cd0ee{op_idx}",
                        }
                    ]
                    + (  # This additional upstream is only for TABLE_1
                        [
                            {
                                "upstream_object_name": "TEST_DB.TEST_SCHEMA.VIEW_1",
                                "upstream_object_domain": "VIEW",
                                "query_id": f"01b2576e-0804-4957-0034-7d83066cd0ee{op_idx}",
                            },
                            {
                                "upstream_object_name": "OTHER_DB.OTHER_SCHEMA.TABLE_1",
                                "upstream_object_domain": "TABLE",
                                "query_id": f"01b2576e-0804-4957-0034-7d83066cd0ee{op_idx}",
                            },
                        ]
                        if op_idx == 1
                        else []
                    )
                ),
                "UPSTREAM_COLUMNS": json.dumps(
                    [
                        {
                            "column_name": f"COL_{col_idx}",
                            "upstreams": [
                                {
                                    "query_id": f"01b2576e-0804-4957-0034-7d83066cd0ee{op_idx}",
                                    "column_upstreams": [
                                        {
                                            "object_name": "TEST_DB.TEST_SCHEMA.TABLE_2",
                                            "object_domain": "Table",
                                            "column_name": f"COL_{col_idx}",
                                        }
                                    ],
                                }
                            ],
                        }
                        for col_idx in range(1, num_cols + 1)
                    ]
                    + (  # This additional upstream is only for TABLE_1
                        [
                            {
                                "column_name": "COL_1",
                                "upstreams": [
                                    {
                                        "query_id": f"01b2576e-0804-4957-0034-7d83066cd0ee{op_idx}",
                                        "column_upstreams": [
                                            {
                                                "object_name": "OTHER_DB.OTHER_SCHEMA.TABLE_1",
                                                "object_domain": "Table",
                                                "column_name": "COL_1",
                                            }
                                        ],
                                    }
                                ],
                            }
                        ]
                        if op_idx == 1
                        else []
                    )
                ),
                "QUERIES": json.dumps(
                    [
                        {
                            "query_text": f"INSERT INTO TEST_DB.TEST_SCHEMA.TABLE_{op_idx} SELECT * FROM TEST_DB.TEST_SCHEMA.TABLE_2",
                            "query_id": f"01b2576e-0804-4957-0034-7d83066cd0ee{op_idx}",
                            "start_time": "06-06-2022",
                        }
                    ]
                ),
            }
            for op_idx in range(1, num_ops + 1)
        ]
    elif query in (
        snowflake_query.SnowflakeQuery.table_to_table_lineage_history_v2(
            start_time_millis=1654473600000,
            end_time_millis=1654586220000,
            include_column_lineage=False,
        ),
    ):
        return [
            {
                "DOWNSTREAM_TABLE_NAME": f"TEST_DB.TEST_SCHEMA.TABLE_{op_idx}",
                "DOWNSTREAM_TABLE_DOMAIN": "TABLE",
                "UPSTREAM_TABLES": json.dumps(
                    [
                        {
                            "upstream_object_name": "TEST_DB.TEST_SCHEMA.TABLE_2",
                            "upstream_object_domain": "TABLE",
                            "query_id": f"01b2576e-0804-4957-0034-7d83066cd0ee{op_idx}",
                        },
                    ]
                    + (  # This additional upstream is only for TABLE_1
                        [
                            {
                                "upstream_object_name": "OTHER_DB.OTHER_SCHEMA.TABLE_1",
                                "upstream_object_domain": "TABLE",
                                "query_id": f"01b2576e-0804-4957-0034-7d83066cd0ee{op_idx}",
                            },
                        ]
                        if op_idx == 1
                        else []
                    )
                ),
                "QUERIES": json.dumps(
                    [
                        {
                            "query_text": f"INSERT INTO TEST_DB.TEST_SCHEMA.TABLE_{op_idx} SELECT * FROM TEST_DB.TEST_SCHEMA.TABLE_2",
                            "query_id": f"01b2576e-0804-4957-0034-7d83066cd0ee{op_idx}",
                            "start_time": datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                                tzinfo=timezone.utc
                            ),
                        }
                    ]
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
        snowflake_query.SnowflakeQuery.view_dependencies_v2(),
        snowflake_query.SnowflakeQuery.view_dependencies(),
        snowflake_query.SnowflakeQuery.show_external_tables(),
        snowflake_query.SnowflakeQuery.copy_lineage_history(
            start_time_millis=1654473600000, end_time_millis=1654621200000
        ),
        snowflake_query.SnowflakeQuery.copy_lineage_history(
            start_time_millis=1654473600000, end_time_millis=1654586220000
        ),
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
