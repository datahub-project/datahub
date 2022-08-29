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
        SELECT schema_name AS "schema_name",
        created AS "created",
        last_altered AS "last_altered",
        comment AS "comment"
        from {db_clause}information_schema.schemata
        WHERE schema_name != 'INFORMATION_SCHEMA'
        order by schema_name"""

    @staticmethod
    def tables_for_database(db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog AS "table_catalog",
        table_schema AS "table_schema",
        table_name AS "table_name",
        table_type AS "table_type",
        created AS "created",
        last_altered AS "last_altered" ,
        comment AS "comment",
        row_count AS "row_count",
        bytes AS "bytes",
        clustering_key AS "clustering_key",
        auto_clustering_on AS "auto_clustering_on"
        FROM {db_clause}information_schema.tables t
        WHERE table_schema != 'INFORMATION_SCHEMA'
        and table_type in ( 'BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    @staticmethod
    def tables_for_schema(schema_name: str, db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog AS "table_catalog",
        table_schema AS "table_schema",
        table_name AS "table_name",
        table_type AS "table_type",
        created AS "created",
        last_altered AS "last_altered" ,
        comment AS "comment",
        row_count AS "row_count",
        bytes AS "bytes",
        clustering_key AS "clustering_key",
        auto_clustering_on AS "auto_clustering_on"
        FROM {db_clause}information_schema.tables t
        where schema_name='{schema_name}'
        and table_type in ('BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    # View definition is retrived in information_schema query only if role is owner of view. Hence this query is not used.
    # https://community.snowflake.com/s/article/Is-it-possible-to-see-the-view-definition-in-information-schema-views-from-a-non-owner-role
    @staticmethod
    def views_for_database(db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog AS "table_catalog",
        table_schema AS "table_schema",
        table_name AS "table_name",
        created AS "created",
        last_altered AS "last_altered",
        comment AS "comment",
        view_definition AS "view_definition"
        FROM {db_clause}information_schema.views t
        WHERE table_schema != 'INFORMATION_SCHEMA'
        order by table_schema, table_name"""

    # View definition is retrived in information_schema query only if role is owner of view. Hence this query is not used.
    # https://community.snowflake.com/s/article/Is-it-possible-to-see-the-view-definition-in-information-schema-views-from-a-non-owner-role
    @staticmethod
    def views_for_schema(schema_name: str, db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog AS "table_catalog",
        table_schema AS "table_schema",
        table_name AS "table_name",
        created AS "created",
        last_altered AS "last_altered",
        comment AS "comment",
        view_definition AS "view_definition"
        FROM {db_clause}information_schema.views t
        where schema_name='{schema_name}'
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
        table_catalog AS "table_catalog",
        table_schema AS "table_schema",
        table_name AS "table_name",
        column_name AS "column_name",
        ordinal_position AS "ordinal_position",
        is_nullable AS "is_nullable",
        data_type AS "data_type",
        comment AS "comment",
        character_maximum_length AS "character_maximum_length",
        numeric_precision AS "numeric_precision",
        numeric_scale AS "numeric_scale",
        column_default AS "column_default",
        is_identity AS "is_identity"
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
        table_catalog AS "table_catalog",
        table_schema AS "table_schema",
        table_name AS "table_name",
        column_name AS "column_name",
        ordinal_position AS "ordinal_position",
        is_nullable AS "is_nullable",
        data_type AS "data_type",
        comment AS "comment",
        character_maximum_length AS "character_maximum_length",
        numeric_precision AS "numeric_precision",
        numeric_scale AS "numeric_scale",
        column_default AS "column_default",
        is_identity AS "is_identity"
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
            access_history.query_start_time AS "query_start_time",
            query_history.query_text AS "query_text",
            query_history.query_type AS "query_type",
            query_history.rows_inserted AS "rows_inserted",
            query_history.rows_updated AS "rows_updated",
            query_history.rows_deleted AS "rows_deleted",
            access_history.base_objects_accessed AS "base_objects_accessed",
            access_history.direct_objects_accessed AS "direct_objects_accessed", -- when dealing with views, direct objects will show the view while base will show the underlying table
            access_history.objects_modified AS "objects_modified",
            -- query_history.execution_status, -- not really necessary, but should equal "SUCCESS"
            -- query_history.warehouse_name,
            access_history.user_name AS "user_name",
            users.first_name AS "first_name",
            users.last_name AS "last_name",
            users.display_name AS "display_name",
            users.email AS "email",
            query_history.role_name AS "role_name"
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
        upstream_table_name AS "upstream_table_name",
        downstream_table_name AS "downstream_table_name",
        upstream_table_columns AS "upstream_table_columns",
        downstream_table_columns AS "downstream_table_columns"
        FROM table_lineage_history
        WHERE upstream_table_domain in ('Table', 'External table') and downstream_table_domain = 'Table'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY downstream_table_name, upstream_table_name ORDER BY query_start_time DESC) = 1"""

    @staticmethod
    def view_dependencies() -> str:
        return """
        SELECT
          concat(
            referenced_database, '.', referenced_schema,
            '.', referenced_object_name
          ) AS "view_upstream",
          concat(
            referencing_database, '.', referencing_schema,
            '.', referencing_object_name
          ) AS "downstream_view",
          referencing_object_domain AS "referencing_object_domain"
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
          view_name AS "view_name",
          view_domain AS "view_domain",
          view_columns AS "view_columns",
          downstream_table_name AS "downstream_table_name",
          downstream_table_columns AS "downstream_table_columns"
        FROM
          view_lineage_history
        WHERE
          view_domain in ('View', 'Materialized view')
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY view_name,
            downstream_table_name
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
        upstream_locations AS "upstream_locations",
        downstream_table_name AS "downstream_table_name",
        downstream_table_columns AS "downstream_table_columns"
        FROM external_table_lineage_history
        WHERE downstream_table_domain = 'Table'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY downstream_table_name ORDER BY query_start_time DESC) = 1"""

    @staticmethod
    def usage_per_object_per_time_bucket_for_time_window(
        start_time_millis: int,
        end_time_millis: int,
        time_bucket_size: str,
        use_base_objects: bool,
        top_n_queries: int,
        include_top_n_queries: bool,
    ) -> str:
        # TODO: Do not query for top n queries if include_top_n_queries = False
        # How can we make this pretty
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
                query_text QUALIFY row_number() over ( partition by bucket_start_time, object_name, query_text
            order by
                total_queries desc ) <= {top_n_queries}
        )
        select
            basic_usage_counts.object_name AS "object_name",
            basic_usage_counts.bucket_start_time AS "bucket_start_time",
            ANY_VALUE(basic_usage_counts.object_domain) AS "object_domain",
            ANY_VALUE(basic_usage_counts.total_queries) AS "total_queries",
            ANY_VALUE(basic_usage_counts.total_users) AS "total_users",
            ARRAY_AGG( distinct top_queries.query_text) AS "top_sql_queries",
            ARRAY_AGG( distinct OBJECT_CONSTRUCT( 'column_name', field_usage_counts.column_name, 'total_queries', field_usage_counts.total_queries ) ) AS "field_counts",
            ARRAY_AGG( distinct OBJECT_CONSTRUCT( 'user_name', user_usage_counts.user_name, 'user_email', user_usage_counts.user_email, 'total_queries', user_usage_counts.total_queries ) ) AS "user_counts"
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
