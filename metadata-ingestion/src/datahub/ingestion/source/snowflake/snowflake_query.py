from typing import Optional


class SnowflakeQuery:
    @staticmethod
    def current_version() -> str:
        return "select CURRENT_VERSION()"

    @staticmethod
    def current_role() -> str:
        return "select CURRENT_ROLE()"

    @staticmethod
    def current_warehouse() -> str:
        return "select CURRENT_WAREHOUSE()"

    @staticmethod
    def current_database() -> str:
        return "select CURRENT_DATABASE()"

    @staticmethod
    def current_schema() -> str:
        return "select CURRENT_SCHEMA()"

    @staticmethod
    def show_databases() -> str:
        return "show databases"

    @staticmethod
    def use_database(db_name: str) -> str:
        return f'use database "{db_name}"'

    @staticmethod
    def schemas_for_database(db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT schema_name AS "SCHEMA_NAME",
        created AS "CREATED",
        last_altered AS "LAST_ALTERED",
        comment AS "COMMENT"
        from {db_clause}information_schema.schemata
        WHERE schema_name != 'INFORMATION_SCHEMA'
        order by schema_name"""

    @staticmethod
    def tables_for_database(db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog AS "TABLE_CATALOG",
        table_schema AS "TABLE_SCHEMA",
        table_name AS "TABLE_NAME",
        table_type AS "TABLE_TYPE",
        created AS "CREATED",
        last_altered AS "LAST_ALTERED" ,
        comment AS "COMMENT",
        row_count AS "ROW_COUNT",
        bytes AS "BYTES",
        clustering_key AS "CLUSTERING_KEY",
        auto_clustering_on AS "AUTO_CLUSTERING_ON"
        FROM {db_clause}information_schema.tables t
        WHERE table_schema != 'INFORMATION_SCHEMA'
        and table_type in ( 'BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    @staticmethod
    def tables_for_schema(schema_name: str, db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog AS "TABLE_CATALOG",
        table_schema AS "TABLE_SCHEMA",
        table_name AS "TABLE_NAME",
        table_type AS "TABLE_TYPE",
        created AS "CREATED",
        last_altered AS "LAST_ALTERED" ,
        comment AS "COMMENT",
        row_count AS "ROW_COUNT",
        bytes AS "BYTES",
        clustering_key AS "CLUSTERING_KEY",
        auto_clustering_on AS "AUTO_CLUSTERING_ON"
        FROM {db_clause}information_schema.tables t
        where table_schema='{schema_name}'
        and table_type in ('BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    # View definition is retrived in information_schema query only if role is owner of view. Hence this query is not used.
    # https://community.snowflake.com/s/article/Is-it-possible-to-see-the-view-definition-in-information-schema-views-from-a-non-owner-role
    @staticmethod
    def views_for_database(db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog AS "TABLE_CATALOG",
        table_schema AS "TABLE_SCHEMA",
        table_name AS "TABLE_NAME",
        created AS "CREATED",
        last_altered AS "LAST_ALTERED",
        comment AS "COMMENT",
        view_definition AS "VIEW_DEFINITION"
        FROM {db_clause}information_schema.views t
        WHERE table_schema != 'INFORMATION_SCHEMA'
        order by table_schema, table_name"""

    # View definition is retrived in information_schema query only if role is owner of view. Hence this query is not used.
    # https://community.snowflake.com/s/article/Is-it-possible-to-see-the-view-definition-in-information-schema-views-from-a-non-owner-role
    @staticmethod
    def views_for_schema(schema_name: str, db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog AS "TABLE_CATALOG",
        table_schema AS "TABLE_SCHEMA",
        table_name AS "TABLE_NAME",
        created AS "CREATED",
        last_altered AS "LAST_ALTERED",
        comment AS "COMMENT",
        view_definition AS "VIEW_DEFINITION"
        FROM {db_clause}information_schema.views t
        where table_schema='{schema_name}'
        order by table_schema, table_name"""

    @staticmethod
    def show_views_for_database(db_name: str) -> str:
        return f"""show views in database "{db_name}";"""

    @staticmethod
    def show_views_for_schema(schema_name: str, db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""show views in schema {db_clause}"{schema_name}";"""

    @staticmethod
    def columns_for_schema(schema_name: str, db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        select
        table_catalog AS "TABLE_CATALOG",
        table_schema AS "TABLE_SCHEMA",
        table_name AS "TABLE_NAME",
        column_name AS "COLUMN_NAME",
        ordinal_position AS "ORDINAL_POSITION",
        is_nullable AS "IS_NULLABLE",
        data_type AS "DATA_TYPE",
        comment AS "COMMENT",
        character_maximum_length AS "CHARACTER_MAXIMUM_LENGTH",
        numeric_precision AS "NUMERIC_PRECISION",
        numeric_scale AS "NUMERIC_SCALE",
        column_default AS "COLUMN_DEFAULT",
        is_identity AS "IS_IDENTITY"
        from {db_clause}information_schema.columns
        WHERE table_schema='{schema_name}'
        ORDER BY ordinal_position"""

    @staticmethod
    def columns_for_table(
        table_name: str, schema_name: str, db_name: Optional[str]
    ) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        select
        table_catalog AS "TABLE_CATALOG",
        table_schema AS "TABLE_SCHEMA",
        table_name AS "TABLE_NAME",
        column_name AS "COLUMN_NAME",
        ordinal_position AS "ORDINAL_POSITION",
        is_nullable AS "IS_NULLABLE",
        data_type AS "DATA_TYPE",
        comment AS "COMMENT",
        character_maximum_length AS "CHARACTER_MAXIMUM_LENGTH",
        numeric_precision AS "NUMERIC_PRECISION",
        numeric_scale AS "NUMERIC_SCALE",
        column_default AS "COLUMN_DEFAULT",
        is_identity AS "IS_IDENTITY"
        from {db_clause}information_schema.columns
        WHERE table_schema='{schema_name}' and table_name='{table_name}'
        ORDER BY ordinal_position"""

    @staticmethod
    def show_primary_keys_for_schema(schema_name: str, db_name: str) -> str:
        return f"""
        show primary keys in schema "{db_name}"."{schema_name}" """

    @staticmethod
    def show_foreign_keys_for_schema(schema_name: str, db_name: str) -> str:
        return f"""
        show imported keys in schema "{db_name}"."{schema_name}" """

    @staticmethod
    def operational_data_for_time_window(
        start_time_millis: int, end_time_millis: int
    ) -> str:
        return f"""
        SELECT
            -- access_history.query_id, -- only for debugging purposes
            access_history.query_start_time AS "QUERY_START_TIME",
            query_history.query_text AS "QUERY_TEXT",
            query_history.query_type AS "QUERY_TYPE",
            query_history.rows_inserted AS "ROWS_INSERTED",
            query_history.rows_updated AS "ROWS_UPDATED",
            query_history.rows_deleted AS "ROWS_DELETED",
            access_history.base_objects_accessed AS "BASE_OBJECTS_ACCESSED",
            access_history.direct_objects_accessed AS "DIRECT_OBJECTS_ACCESSED", -- when dealing with views, direct objects will show the view while base will show the underlying table
            access_history.objects_modified AS "OBJECTS_MODIFIED",
            -- query_history.execution_status, -- not really necessary, but should equal "SUCCESS"
            -- query_history.warehouse_name,
            access_history.user_name AS "USER_NAME",
            users.first_name AS "FIRST_NAME",
            users.last_name AS "LAST_NAME",
            users.display_name AS "DISPLAY_NAME",
            users.email AS "EMAIL",
            query_history.role_name AS "ROLE_NAME"
        FROM
            snowflake.account_usage.access_history access_history
        LEFT JOIN
            snowflake.account_usage.query_history query_history
            ON access_history.query_id = query_history.query_id
        LEFT JOIN
            snowflake.account_usage.users users
            ON access_history.user_name = users.name
        WHERE query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
            AND query_start_time < to_timestamp_ltz({end_time_millis}, 3)
            AND query_history.query_type in ('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'CREATE_TABLE', 'CREATE_TABLE_AS_SELECT')
        ORDER BY query_start_time DESC
        ;"""

    @staticmethod
    def table_to_table_lineage_history(
        start_time_millis: int, end_time_millis: int
    ) -> str:
        return f"""
        WITH table_lineage_history AS (
            SELECT
                r.value:"objectName"::varchar AS upstream_table_name,
                r.value:"objectDomain"::varchar AS upstream_table_domain,
                r.value:"columns" AS upstream_table_columns,
                w.value:"objectName"::varchar AS downstream_table_name,
                w.value:"objectDomain"::varchar AS downstream_table_domain,
                w.value:"columns" AS downstream_table_columns,
                t.query_start_time AS query_start_time
            FROM
                (SELECT * from snowflake.account_usage.access_history) t,
                lateral flatten(input => t.DIRECT_OBJECTS_ACCESSED) r,
                lateral flatten(input => t.OBJECTS_MODIFIED) w
            WHERE r.value:"objectId" IS NOT NULL
            AND w.value:"objectId" IS NOT NULL
            AND w.value:"objectName" NOT LIKE '%.GE_TMP_%'
            AND w.value:"objectName" NOT LIKE '%.GE_TEMP_%'
            AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
            AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3))
        SELECT
        upstream_table_name AS "UPSTREAM_TABLE_NAME",
        downstream_table_name AS "DOWNSTREAM_TABLE_NAME",
        upstream_table_columns AS "UPSTREAM_TABLE_COLUMNS",
        downstream_table_columns AS "DOWNSTREAM_TABLE_COLUMNS"
        FROM table_lineage_history
        WHERE upstream_table_domain in ('Table', 'External table') and downstream_table_domain = 'Table'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY downstream_table_name,
            upstream_table_name,
            downstream_table_columns
            ORDER BY query_start_time DESC
        ) = 1"""

    @staticmethod
    def view_dependencies() -> str:
        return """
        SELECT
          concat(
            referenced_database, '.', referenced_schema,
            '.', referenced_object_name
          ) AS "VIEW_UPSTREAM",
          referenced_object_domain as "REFERENCED_OBJECT_DOMAIN",
          concat(
            referencing_database, '.', referencing_schema,
            '.', referencing_object_name
          ) AS "DOWNSTREAM_VIEW",
          referencing_object_domain AS "REFERENCING_OBJECT_DOMAIN"
        FROM
          snowflake.account_usage.object_dependencies
        WHERE
          referencing_object_domain in ('VIEW', 'MATERIALIZED VIEW')
        """

    @staticmethod
    def view_lineage_history(start_time_millis: int, end_time_millis: int) -> str:
        return f"""
        WITH view_lineage_history AS (
          SELECT
            vu.value : "objectName"::varchar AS view_name,
            vu.value : "objectDomain"::varchar AS view_domain,
            vu.value : "columns" AS view_columns,
            w.value : "objectName"::varchar AS downstream_table_name,
            w.value : "objectDomain"::varchar AS downstream_table_domain,
            w.value : "columns" AS downstream_table_columns,
            t.query_start_time AS query_start_time
          FROM
            (
              SELECT
                *
              FROM
                snowflake.account_usage.access_history
            ) t,
            lateral flatten(input => t.DIRECT_OBJECTS_ACCESSED) vu,
            lateral flatten(input => t.OBJECTS_MODIFIED) w
          WHERE
            vu.value : "objectId" IS NOT NULL
            AND w.value : "objectId" IS NOT NULL
            AND w.value : "objectName" NOT LIKE '%.GE_TMP_%'
            AND w.value : "objectName" NOT LIKE '%.GE_TEMP_%'
            AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
            AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3)
        )
        SELECT
          view_name AS "VIEW_NAME",
          view_domain AS "VIEW_DOMAIN",
          view_columns AS "VIEW_COLUMNS",
          downstream_table_name AS "DOWNSTREAM_TABLE_NAME",
          downstream_table_domain AS "DOWNSTREAM_TABLE_DOMAIN",
          downstream_table_columns AS "DOWNSTREAM_TABLE_COLUMNS"
        FROM
          view_lineage_history
        WHERE
          view_domain in ('View', 'Materialized view')
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY view_name,
            downstream_table_name,
            downstream_table_columns
            ORDER BY
              query_start_time DESC
          ) = 1
        """

    @staticmethod
    def show_external_tables() -> str:
        return "show external tables in account"

    @staticmethod
    def external_table_lineage_history(
        start_time_millis: int, end_time_millis: int
    ) -> str:
        return f"""
        WITH external_table_lineage_history AS (
            SELECT
                r.value:"locations" AS upstream_locations,
                w.value:"objectName"::varchar AS downstream_table_name,
                w.value:"objectDomain"::varchar AS downstream_table_domain,
                w.value:"columns" AS downstream_table_columns,
                t.query_start_time AS query_start_time
            FROM
                (SELECT * from snowflake.account_usage.access_history) t,
                lateral flatten(input => t.BASE_OBJECTS_ACCESSED) r,
                lateral flatten(input => t.OBJECTS_MODIFIED) w
            WHERE r.value:"locations" IS NOT NULL
            AND w.value:"objectId" IS NOT NULL
            AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
            AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3))
        SELECT
        upstream_locations AS "UPSTREAM_LOCATIONS",
        downstream_table_name AS "DOWNSTREAM_TABLE_NAME",
        downstream_table_columns AS "DOWNSTREAM_TABLE_COLUMNS"
        FROM external_table_lineage_history
        WHERE downstream_table_domain = 'Table'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY downstream_table_name ORDER BY query_start_time DESC) = 1"""

    @staticmethod
    def get_access_history_date_range() -> str:
        return """
            select
                min(query_start_time) as "MIN_TIME",
                max(query_start_time) as "MAX_TIME"
            from snowflake.account_usage.access_history
        """

    @staticmethod
    def usage_per_object_per_time_bucket_for_time_window(
        start_time_millis: int,
        end_time_millis: int,
        time_bucket_size: str,
        use_base_objects: bool,
        top_n_queries: int,
        include_top_n_queries: bool,
    ) -> str:
        if not include_top_n_queries:
            top_n_queries = 0
        assert time_bucket_size == "DAY" or time_bucket_size == "HOUR"
        objects_column = (
            "BASE_OBJECTS_ACCESSED" if use_base_objects else "DIRECT_OBJECTS_ACCESSED"
        )
        return f"""
        WITH object_access_history AS
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
                        {objects_column}
                    from
                        snowflake.account_usage.access_history
                    WHERE
                        query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
                        AND query_start_time < to_timestamp_ltz({end_time_millis}, 3)
                )
                t,
                lateral flatten(input => t.{objects_column}) object
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
                DATE_TRUNC('{time_bucket_size}', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
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
                DATE_TRUNC('{time_bucket_size}', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
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
                DATE_TRUNC('{time_bucket_size}', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
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
                DATE_TRUNC('{time_bucket_size}', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
                query_history.query_text AS query_text,
                count(distinct(access_history.query_id)) AS total_queries
            FROM
                object_access_history access_history
                LEFT JOIN
                    snowflake.account_usage.query_history query_history
                    ON access_history.query_id = query_history.query_id
            GROUP BY
                bucket_start_time,
                object_name,
                query_text
            QUALIFY row_number() over ( partition by bucket_start_time, object_name
            order by
                total_queries desc, query_text asc ) <= {top_n_queries}
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
            basic_usage_counts.object_domain in
            (
                'Table',
                'View',
                'Materialized view',
                'External table'
            )
        group by
            basic_usage_counts.object_name,
            basic_usage_counts.bucket_start_time
        order by
            basic_usage_counts.bucket_start_time
        """
