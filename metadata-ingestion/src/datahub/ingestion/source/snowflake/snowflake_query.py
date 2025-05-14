from typing import List, Optional

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BucketDuration
from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_config import (
    DEFAULT_TEMP_TABLES_PATTERNS,
)
from datahub.utilities.prefix_batch_builder import PrefixGroup

SHOW_VIEWS_MAX_PAGE_SIZE = 10000
SHOW_STREAM_MAX_PAGE_SIZE = 10000


def create_deny_regex_sql_filter(
    deny_pattern: List[str], filter_cols: List[str]
) -> str:
    upstream_sql_filter = (
        " AND ".join(
            [
                (f"NOT RLIKE({col_name},'{regexp}','i')")
                for col_name in filter_cols
                for regexp in deny_pattern
            ]
        )
        if deny_pattern
        else ""
    )

    return upstream_sql_filter


class SnowflakeQuery:
    ACCESS_HISTORY_TABLE_VIEW_DOMAINS = {
        SnowflakeObjectDomain.TABLE.capitalize(),
        SnowflakeObjectDomain.EXTERNAL_TABLE.capitalize(),
        SnowflakeObjectDomain.VIEW.capitalize(),
        SnowflakeObjectDomain.MATERIALIZED_VIEW.capitalize(),
        SnowflakeObjectDomain.ICEBERG_TABLE.capitalize(),
        SnowflakeObjectDomain.STREAM.capitalize(),
    }

    ACCESS_HISTORY_TABLE_VIEW_DOMAINS_FILTER = "({})".format(
        ",".join(f"'{domain}'" for domain in ACCESS_HISTORY_TABLE_VIEW_DOMAINS)
    )
    ACCESS_HISTORY_TABLE_DOMAINS_FILTER = (
        "("
        f"'{SnowflakeObjectDomain.TABLE.capitalize()}',"
        f"'{SnowflakeObjectDomain.VIEW.capitalize()}',"
        f"'{SnowflakeObjectDomain.STREAM.capitalize()}',"
        ")"
    )

    @staticmethod
    def current_account() -> str:
        return "select CURRENT_ACCOUNT()"

    @staticmethod
    def current_region() -> str:
        return "select CURRENT_REGION()"

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
    def show_databases() -> str:
        return "show databases"

    @staticmethod
    def show_tags() -> str:
        return "show tags"

    @staticmethod
    def use_database(db_name: str) -> str:
        return f'use database "{db_name}"'

    @staticmethod
    def use_schema(schema_name: str) -> str:
        return f'use schema "{schema_name}"'

    @staticmethod
    def get_databases(db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT database_name AS "DATABASE_NAME",
        created AS "CREATED",
        last_altered AS "LAST_ALTERED",
        comment AS "COMMENT"
        from {db_clause}information_schema.databases
        order by database_name"""

    @staticmethod
    def schemas_for_database(db_name: str) -> str:
        db_clause = f'"{db_name}".'
        return f"""
        SELECT schema_name AS "SCHEMA_NAME",
        created AS "CREATED",
        last_altered AS "LAST_ALTERED",
        comment AS "COMMENT"
        from {db_clause}information_schema.schemata
        WHERE schema_name != 'INFORMATION_SCHEMA'
        order by schema_name"""

    @staticmethod
    def tables_for_database(db_name: str) -> str:
        db_clause = f'"{db_name}".'
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
        auto_clustering_on AS "AUTO_CLUSTERING_ON",
        is_dynamic AS "IS_DYNAMIC",
        is_iceberg AS "IS_ICEBERG",
        is_hybrid AS "IS_HYBRID"
        FROM {db_clause}information_schema.tables t
        WHERE table_schema != 'INFORMATION_SCHEMA'
        and table_type in ( 'BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    @staticmethod
    def tables_for_schema(schema_name: str, db_name: str) -> str:
        db_clause = f'"{db_name}".'
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
        auto_clustering_on AS "AUTO_CLUSTERING_ON",
        is_dynamic AS "IS_DYNAMIC",
        is_iceberg AS "IS_ICEBERG",
        is_hybrid AS "IS_HYBRID"
        FROM {db_clause}information_schema.tables t
        where table_schema='{schema_name}'
        and table_type in ('BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    @staticmethod
    def procedures_for_database(db_name: str) -> str:
        db_clause = f'"{db_name}".'
        return f"""
        SELECT procedure_catalog AS "PROCEDURE_CATALOG",
        procedure_schema AS "PROCEDURE_SCHEMA",
        procedure_name AS "PROCEDURE_NAME",
        procedure_language AS "PROCEDURE_LANGUAGE",
        argument_signature AS "ARGUMENT_SIGNATURE",
        data_type AS "PROCEDURE_RETURN_TYPE",
        procedure_definition AS "PROCEDURE_DEFINITION",
        created AS "CREATED",
        last_altered AS "LAST_ALTERED",
        comment AS "COMMENT"
        FROM {db_clause}information_schema.procedures
        order by procedure_schema, procedure_name"""

    @staticmethod
    def get_all_tags():
        return """
        SELECT tag_database as "TAG_DATABASE",
        tag_schema AS "TAG_SCHEMA",
        tag_name AS "TAG_NAME",
        FROM snowflake.account_usage.tag_references
        GROUP BY TAG_DATABASE , TAG_SCHEMA, tag_name
        ORDER BY TAG_DATABASE, TAG_SCHEMA, TAG_NAME  ASC;
        """

    @staticmethod
    def get_all_tags_on_object_with_propagation(
        db_name: str, quoted_identifier: str, domain: str
    ) -> str:
        # https://docs.snowflake.com/en/sql-reference/functions/tag_references.html
        return f"""
        SELECT tag_database as "TAG_DATABASE",
        tag_schema AS "TAG_SCHEMA",
        tag_name AS "TAG_NAME",
        tag_value AS "TAG_VALUE"
        FROM table("{db_name}".information_schema.tag_references('{quoted_identifier}', '{domain}'));
        """

    @staticmethod
    def get_all_tags_in_database_without_propagation(db_name: str) -> str:
        allowed_object_domains = (
            "("
            f"'{SnowflakeObjectDomain.DATABASE.upper()}',"
            f"'{SnowflakeObjectDomain.SCHEMA.upper()}',"
            f"'{SnowflakeObjectDomain.TABLE.upper()}',"
            f"'{SnowflakeObjectDomain.COLUMN.upper()}'"
            ")"
        )

        # https://docs.snowflake.com/en/sql-reference/account-usage/tag_references.html
        return f"""
        SELECT tag_database as "TAG_DATABASE",
        tag_schema AS "TAG_SCHEMA",
        tag_name AS "TAG_NAME",
        tag_value AS "TAG_VALUE",
        object_database as "OBJECT_DATABASE",
        object_schema AS "OBJECT_SCHEMA",
        object_name AS "OBJECT_NAME",
        column_name AS "COLUMN_NAME",
        domain as "DOMAIN"
        FROM snowflake.account_usage.tag_references
        WHERE (object_database = '{db_name}' OR object_name = '{db_name}')
        AND domain in {allowed_object_domains}
        AND object_deleted IS NULL;
        """

    @staticmethod
    def get_tags_on_columns_with_propagation(
        db_name: str, quoted_table_identifier: str
    ) -> str:
        # https://docs.snowflake.com/en/sql-reference/functions/tag_references_all_columns.html
        return f"""
        SELECT tag_database as "TAG_DATABASE",
        tag_schema AS "TAG_SCHEMA",
        tag_name AS "TAG_NAME",
        tag_value AS "TAG_VALUE",
        column_name AS "COLUMN_NAME"
        FROM table("{db_name}".information_schema.tag_references_all_columns('{quoted_table_identifier}', '{SnowflakeObjectDomain.TABLE}'));
        """

    @staticmethod
    def show_views_for_database(
        db_name: str,
        limit: int = SHOW_VIEWS_MAX_PAGE_SIZE,
        view_pagination_marker: Optional[str] = None,
    ) -> str:
        # While there is an information_schema.views view, that only shows the view definition if the role
        # is an owner of the view. That doesn't work for us.
        # https://community.snowflake.com/s/article/Is-it-possible-to-see-the-view-definition-in-information-schema-views-from-a-non-owner-role

        # SHOW VIEWS can return a maximum of 10000 rows.
        # https://docs.snowflake.com/en/sql-reference/sql/show-views#usage-notes
        assert limit <= SHOW_VIEWS_MAX_PAGE_SIZE

        # To work around this, we paginate through the results using the FROM clause.
        from_clause = (
            f"""FROM '{view_pagination_marker}'""" if view_pagination_marker else ""
        )
        return f"""\
SHOW VIEWS IN DATABASE "{db_name}"
LIMIT {limit} {from_clause};
"""

    @staticmethod
    def get_secure_view_definitions() -> str:
        # https://docs.snowflake.com/en/sql-reference/account-usage/views
        return """
            SELECT
                TABLE_CATALOG as "TABLE_CATALOG",
                TABLE_SCHEMA as "TABLE_SCHEMA",
                TABLE_NAME as "TABLE_NAME",
                VIEW_DEFINITION as "VIEW_DEFINITION"
            FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS
            WHERE IS_SECURE = 'YES' AND VIEW_DEFINITION !='' AND DELETED IS NULL
        """

    @staticmethod
    def columns_for_schema(
        schema_name: str,
        db_name: str,
        prefix_groups: Optional[List[PrefixGroup]] = None,
    ) -> str:
        columns_template = """\
SELECT
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
FROM "{db_name}".information_schema.columns
WHERE table_schema='{schema_name}' AND {extra_clause}"""

        selects = []
        if prefix_groups is None:
            prefix_groups = [PrefixGroup(prefix="", names=[])]
        for prefix_group in prefix_groups:
            if prefix_group.prefix == "":
                extra_clause = "TRUE"
            elif prefix_group.exact_match:
                extra_clause = f"table_name = '{prefix_group.prefix}'"
            else:
                extra_clause = f"table_name LIKE '{prefix_group.prefix}%'"

            selects.append(
                columns_template.format(
                    db_name=db_name, schema_name=schema_name, extra_clause=extra_clause
                )
            )

        return (
            "\nUNION ALL\n".join(selects)
            + """\nORDER BY table_name, ordinal_position"""
        )

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
            (
                SELECT * FROM snowflake.account_usage.query_history
                WHERE query_history.start_time >= to_timestamp_ltz({start_time_millis}, 3)
                    AND query_history.start_time < to_timestamp_ltz({end_time_millis}, 3)
            ) query_history
            ON access_history.query_id = query_history.query_id
        LEFT JOIN
            snowflake.account_usage.users users
            ON access_history.user_name = users.name
        WHERE query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
            AND query_start_time < to_timestamp_ltz({end_time_millis}, 3)
            AND access_history.objects_modified is not null
            AND ARRAY_SIZE(access_history.objects_modified) > 0
        ORDER BY query_start_time DESC
        ;"""

    # Note on use of `upstreams_deny_pattern` to ignore temporary tables:
    # Snowflake access history may include temporary tables in DIRECT_OBJECTS_ACCESSED and
    # OBJECTS_MODIFIED->columns->directSources. We do not need these temporary tables and filter these in the query.
    @staticmethod
    def table_to_table_lineage_history_v2(
        start_time_millis: int,
        end_time_millis: int,
        include_column_lineage: bool = True,
        upstreams_deny_pattern: List[str] = DEFAULT_TEMP_TABLES_PATTERNS,
    ) -> str:
        if include_column_lineage:
            return SnowflakeQuery.table_upstreams_with_column_lineage(
                start_time_millis,
                end_time_millis,
                upstreams_deny_pattern,
            )
        else:
            return SnowflakeQuery.table_upstreams_only(
                start_time_millis,
                end_time_millis,
                upstreams_deny_pattern,
            )

    @staticmethod
    def show_external_tables() -> str:
        return "show external tables in account"

    @staticmethod
    def copy_lineage_history(
        start_time_millis: int,
        end_time_millis: int,
        downstreams_deny_pattern: List[str] = DEFAULT_TEMP_TABLES_PATTERNS,
    ) -> str:
        temp_table_filter = create_deny_regex_sql_filter(
            downstreams_deny_pattern,
            ["DOWNSTREAM_TABLE_NAME"],
        )

        return f"""
        SELECT
            ARRAY_UNIQUE_AGG(h.stage_location) AS "UPSTREAM_LOCATIONS",
            concat(
                h.table_catalog_name, '.', h.table_schema_name,
                '.', h.table_name
            ) AS "DOWNSTREAM_TABLE_NAME"
        FROM
            snowflake.account_usage.copy_history h
        WHERE h.status in ('Loaded','Partially loaded')
            AND DOWNSTREAM_TABLE_NAME IS NOT NULL
            AND h.last_load_time >= to_timestamp_ltz({start_time_millis}, 3)
            AND h.last_load_time < to_timestamp_ltz({end_time_millis}, 3)
            {("AND " + temp_table_filter) if temp_table_filter else ""}
        GROUP BY DOWNSTREAM_TABLE_NAME;
        """

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
        time_bucket_size: BucketDuration,
        use_base_objects: bool,
        top_n_queries: int,
        include_top_n_queries: bool,
        email_domain: Optional[str],
        email_filter: AllowDenyPattern,
        table_deny_pattern: List[str] = DEFAULT_TEMP_TABLES_PATTERNS,
    ) -> str:
        if not include_top_n_queries:
            top_n_queries = 0
        assert (
            time_bucket_size == BucketDuration.DAY
            or time_bucket_size == BucketDuration.HOUR
        )

        temp_table_filter = create_deny_regex_sql_filter(
            table_deny_pattern,
            ["object_name"],
        )

        objects_column = (
            "BASE_OBJECTS_ACCESSED" if use_base_objects else "DIRECT_OBJECTS_ACCESSED"
        )
        email_filter_query = SnowflakeQuery.gen_email_filter_query(email_filter)

        email_domain = f"@{email_domain}" if email_domain else ""

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
                        -- Construct the email in the query, should match the Python behavior.
                        -- The user_email is only used by the email_filter_query.
                        NVL(USERS.email, CONCAT(LOWER(user_name), '{email_domain}')) AS user_email,
                        {objects_column}
                    from
                        snowflake.account_usage.access_history
                    LEFT JOIN
                        snowflake.account_usage.users USERS
                        ON user_name = users.name
                    WHERE
                        query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
                        AND query_start_time < to_timestamp_ltz({end_time_millis}, 3)
                        {email_filter_query}
                )
                t,
                lateral flatten(input => t.{objects_column}) object
            {("where " + temp_table_filter) if temp_table_filter else ""}
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
                DATE_TRUNC('{time_bucket_size.value}', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
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
                DATE_TRUNC('{time_bucket_size.value}', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
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
                DATE_TRUNC('{time_bucket_size.value}', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
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
                DATE_TRUNC('{time_bucket_size.value}', CONVERT_TIMEZONE('UTC', query_start_time)) AS bucket_start_time,
                query_history.query_text AS query_text,
                count(distinct(access_history.query_id)) AS total_queries
            FROM
                object_access_history access_history
            LEFT JOIN
                (
                    SELECT * FROM snowflake.account_usage.query_history
                    WHERE query_history.start_time >= to_timestamp_ltz({start_time_millis}, 3)
                        AND query_history.start_time < to_timestamp_ltz({end_time_millis}, 3)
                ) query_history
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
            basic_usage_counts.object_domain in {SnowflakeQuery.ACCESS_HISTORY_TABLE_VIEW_DOMAINS_FILTER}
            and basic_usage_counts.object_name is not null
        group by
            basic_usage_counts.object_name,
            basic_usage_counts.bucket_start_time
        order by
            basic_usage_counts.bucket_start_time
        """

    @staticmethod
    def gen_email_filter_query(email_filter: AllowDenyPattern) -> str:
        allow_filters = []
        allow_filter = ""
        if len(email_filter.allow) == 1 and email_filter.allow[0] == ".*":
            allow_filter = ""
        else:
            for allow_pattern in email_filter.allow:
                allow_filters.append(
                    f"rlike(user_name, '{allow_pattern}','{'i' if email_filter.ignoreCase else 'c'}')"
                )
            if allow_filters:
                allow_filter = " OR ".join(allow_filters)
                allow_filter = f"AND ({allow_filter})"
        deny_filters = []
        deny_filter = ""
        for deny_pattern in email_filter.deny:
            deny_filters.append(
                f"rlike(user_name, '{deny_pattern}','{'i' if email_filter.ignoreCase else 'c'}')"
            )
        if deny_filters:
            deny_filter = " OR ".join(deny_filters)
            deny_filter = f"({deny_filter})"
        email_filter_query = allow_filter + (
            " AND" + f" NOT {deny_filter}" if deny_filter else ""
        )
        return email_filter_query

    @staticmethod
    def table_upstreams_with_column_lineage(
        start_time_millis: int,
        end_time_millis: int,
        upstreams_deny_pattern: List[str],
    ) -> str:
        allowed_upstream_table_domains = (
            SnowflakeQuery.ACCESS_HISTORY_TABLE_VIEW_DOMAINS_FILTER
        )

        upstream_sql_filter = create_deny_regex_sql_filter(
            upstreams_deny_pattern,
            ["upstream_table_name", "upstream_column_table_name"],
        )
        _MAX_UPSTREAMS_PER_DOWNSTREAM = 20
        _MAX_UPSTREAM_COLUMNS_PER_DOWNSTREAM = 400
        _MAX_QUERIES_PER_DOWNSTREAM = 30

        return f"""
        WITH column_lineage_history AS (
            SELECT
                r.value : "objectName" :: varchar AS upstream_table_name,
                r.value : "objectDomain" :: varchar AS upstream_table_domain,
                REPLACE(w.value : "objectName" :: varchar, '__DBT_TMP', '') AS downstream_table_name,
                w.value : "objectDomain" :: varchar AS downstream_table_domain,
                wcols.value : "columnName" :: varchar AS downstream_column_name,
                wcols_directSources.value : "objectName" as upstream_column_table_name,
                wcols_directSources.value : "columnName" as upstream_column_name,
                wcols_directSources.value : "objectDomain" as upstream_column_object_domain,
                t.query_start_time AS query_start_time,
                t.query_id AS query_id
            FROM
                snowflake.account_usage.access_history t,
                lateral flatten(input => t.DIRECT_OBJECTS_ACCESSED) r,
                lateral flatten(input => t.OBJECTS_MODIFIED) w,
                lateral flatten(input => w.value : "columns", outer => true) wcols,
                lateral flatten(input => wcols.value : "directSources", outer => true) wcols_directSources
            WHERE
                r.value : "objectId" IS NOT NULL
                AND w.value : "objectId" IS NOT NULL
                AND w.value : "objectName" NOT LIKE '%.GE_TMP_%'
                AND w.value : "objectName" NOT LIKE '%.GE_TEMP_%'
                AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
                AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3)
                AND upstream_table_domain in {allowed_upstream_table_domains}
                AND downstream_table_domain = '{SnowflakeObjectDomain.TABLE.capitalize()}'
                {("AND " + upstream_sql_filter) if upstream_sql_filter else ""}
            ),
        column_upstream_jobs AS (
            SELECT
                downstream_table_name,
                downstream_column_name,
                ANY_VALUE(query_start_time) as query_start_time,
                query_id,
                ARRAY_UNIQUE_AGG(
                    OBJECT_CONSTRUCT(
                        'object_name', upstream_column_table_name,
                        'object_domain', upstream_column_object_domain,
                        'column_name', upstream_column_name
                    )
                ) as upstream_columns_for_job
            FROM
                column_lineage_history
            WHERE
                upstream_column_name is not null
                and upstream_column_table_name is not null
            GROUP BY
                downstream_table_name,
                downstream_column_name,
                query_id
            ),-- one row per column's upstream job (repetitive query possible)
        column_upstream_jobs_unique AS (
            SELECT
                downstream_table_name,
                downstream_column_name,
                upstream_columns_for_job,
                MAX_BY(query_id, query_start_time) as query_id,
                MAX(query_start_time) as query_start_time
            FROM
                column_upstream_jobs
            GROUP BY
                downstream_table_name,
                downstream_column_name,
                upstream_columns_for_job
            ),-- one row per column's unique upstream job (keep only latest query)
        column_upstreams AS (
            SELECT
                downstream_table_name,
                downstream_column_name,
                ARRAY_UNIQUE_AGG(
                    OBJECT_CONSTRUCT (
                        'column_upstreams', upstream_columns_for_job,
                        'query_id', query_id
                    )
                ) as upstreams
            FROM
                column_upstream_jobs_unique
            GROUP BY
                downstream_table_name,
                downstream_column_name
            ), -- one row per downstream column
        table_upstream_jobs_unique AS (
            SELECT
                downstream_table_name,
                ANY_VALUE(downstream_table_domain) as downstream_table_domain,
                upstream_table_name,
                ANY_VALUE(upstream_table_domain) as upstream_table_domain,
                MAX_BY(query_id, query_start_time) as query_id
            FROM
                column_lineage_history
            GROUP BY
                downstream_table_name,
                upstream_table_name
            ), -- one row per downstream table
        query_ids AS (
            SELECT distinct downstream_table_name, query_id from table_upstream_jobs_unique
            UNION
            select distinct downstream_table_name, query_id from column_upstream_jobs_unique
        ),
        queries AS (
            select qid.downstream_table_name, qid.query_id, query_history.query_text, query_history.start_time
            from  query_ids qid
            LEFT JOIN (
                SELECT * FROM snowflake.account_usage.query_history
                WHERE query_history.start_time >= to_timestamp_ltz({start_time_millis}, 3)
                    AND query_history.start_time < to_timestamp_ltz({end_time_millis}, 3)
            ) query_history
            on qid.query_id = query_history.query_id
            WHERE qid.query_id is not null
              AND query_history.query_text is not null
        )
        SELECT
            h.downstream_table_name AS "DOWNSTREAM_TABLE_NAME",
            ANY_VALUE(h.downstream_table_domain) AS "DOWNSTREAM_TABLE_DOMAIN",
            ARRAY_SLICE(ARRAY_UNIQUE_AGG(
                OBJECT_CONSTRUCT(
                    'upstream_object_name', h.upstream_table_name,
                    'upstream_object_domain', h.upstream_table_domain,
                    'query_id', h.query_id
                )
            ), 0, {_MAX_UPSTREAMS_PER_DOWNSTREAM}) AS "UPSTREAM_TABLES",
            ARRAY_SLICE(ARRAY_UNIQUE_AGG(
                OBJECT_CONSTRUCT(
                    'column_name', column_upstreams.downstream_column_name,
                    'upstreams', column_upstreams.upstreams
                )
            ), 0, {_MAX_UPSTREAM_COLUMNS_PER_DOWNSTREAM}) AS "UPSTREAM_COLUMNS",
            ARRAY_SLICE(ARRAY_UNIQUE_AGG(
                OBJECT_CONSTRUCT(
                    'query_id', q.query_id,
                    'query_text', q.query_text,
                    'start_time', q.start_time
                )
            ), 0, {_MAX_QUERIES_PER_DOWNSTREAM}) as "QUERIES"
            FROM
                table_upstream_jobs_unique h
            LEFT JOIN column_upstreams column_upstreams
                on h.downstream_table_name = column_upstreams.downstream_table_name
            LEFT JOIN queries q
                on h.downstream_table_name = q.downstream_table_name
            GROUP BY
                h.downstream_table_name
            ORDER BY
                h.downstream_table_name
        """

    # See Note on temporary tables above.
    @staticmethod
    def table_upstreams_only(
        start_time_millis: int,
        end_time_millis: int,
        upstreams_deny_pattern: List[str],
    ) -> str:
        allowed_upstream_table_domains = (
            SnowflakeQuery.ACCESS_HISTORY_TABLE_VIEW_DOMAINS_FILTER
        )

        upstream_sql_filter = create_deny_regex_sql_filter(
            upstreams_deny_pattern,
            ["upstream_table_name"],
        )
        return f"""
        WITH table_lineage_history AS (
            SELECT
                r.value : "objectName" :: varchar AS upstream_table_name,
                r.value : "objectDomain" :: varchar AS upstream_table_domain,
                w.value : "objectName" :: varchar AS downstream_table_name,
                w.value : "objectDomain" :: varchar AS downstream_table_domain,
                t.query_start_time AS query_start_time,
                t.query_id AS query_id
            FROM
                snowflake.account_usage.access_history t,
                lateral flatten(input => t.DIRECT_OBJECTS_ACCESSED) r,
                lateral flatten(input => t.OBJECTS_MODIFIED) w
            WHERE
                r.value : "objectId" IS NOT NULL
                AND w.value : "objectId" IS NOT NULL
                AND w.value : "objectName" NOT LIKE '%.GE_TMP_%'
                AND w.value : "objectName" NOT LIKE '%.GE_TEMP_%'
                AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
                AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3)
                AND upstream_table_domain in {allowed_upstream_table_domains}
                AND downstream_table_domain = '{SnowflakeObjectDomain.TABLE.capitalize()}'
                {("AND " + upstream_sql_filter) if upstream_sql_filter else ""}
            ),
        table_upstream_jobs_unique AS (
            SELECT
                downstream_table_name,
                ANY_VALUE(downstream_table_domain) as downstream_table_domain,
                upstream_table_name,
                ANY_VALUE(upstream_table_domain) as upstream_table_domain,
                MAX_BY(query_id, query_start_time) as query_id
            FROM
                table_lineage_history
            GROUP BY
                downstream_table_name,
                upstream_table_name
            ), -- one row per downstream table
        query_ids AS (
            SELECT distinct downstream_table_name, query_id from table_upstream_jobs_unique
        ),
        queries AS (
            select qid.downstream_table_name, qid.query_id, query_history.query_text, query_history.start_time
            from  query_ids qid
            LEFT JOIN (
                SELECT * FROM snowflake.account_usage.query_history
                WHERE query_history.start_time >= to_timestamp_ltz({start_time_millis}, 3)
                    AND query_history.start_time < to_timestamp_ltz({end_time_millis}, 3)
            ) query_history
            on qid.query_id = query_history.query_id
        )
        SELECT
            h.downstream_table_name AS "DOWNSTREAM_TABLE_NAME",
            ANY_VALUE(h.downstream_table_domain) AS "DOWNSTREAM_TABLE_DOMAIN",
            ARRAY_UNIQUE_AGG(
                OBJECT_CONSTRUCT(
                    'upstream_object_name', h.upstream_table_name,
                    'upstream_object_domain', h.upstream_table_domain,
                    'query_id', h.query_id
                )
            ) AS "UPSTREAM_TABLES",
            ARRAY_UNIQUE_AGG(
                OBJECT_CONSTRUCT(
                    'query_id', q.query_id,
                    'query_text', q.query_text,
                    'start_time', q.start_time
                )
            ) as "QUERIES"
            FROM
                table_upstream_jobs_unique h
            LEFT JOIN queries q
                on h.downstream_table_name = q.downstream_table_name
            GROUP BY
                h.downstream_table_name
            ORDER BY
                h.downstream_table_name
        """

    @staticmethod
    def dmf_assertion_results(start_time_millis: int, end_time_millis: int) -> str:
        pattern = r"datahub\\_\\_%"
        escape_pattern = r"\\"
        return f"""
            SELECT
                MEASUREMENT_TIME AS "MEASUREMENT_TIME",
                METRIC_NAME AS "METRIC_NAME",
                TABLE_NAME AS "TABLE_NAME",
                TABLE_SCHEMA AS "TABLE_SCHEMA",
                TABLE_DATABASE AS "TABLE_DATABASE",
                VALUE::INT AS "VALUE"
            FROM
                SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
            WHERE
                MEASUREMENT_TIME >= to_timestamp_ltz({start_time_millis}, 3)
                AND MEASUREMENT_TIME < to_timestamp_ltz({end_time_millis}, 3)
                AND METRIC_NAME ilike '{pattern}' escape '{escape_pattern}'
                ORDER BY MEASUREMENT_TIME ASC;

            """

    @staticmethod
    def get_all_users() -> str:
        return """SELECT name as "NAME", email as "EMAIL" FROM SNOWFLAKE.ACCOUNT_USAGE.USERS"""

    @staticmethod
    def streams_for_database(
        db_name: str,
        limit: int = SHOW_STREAM_MAX_PAGE_SIZE,
        stream_pagination_marker: Optional[str] = None,
    ) -> str:
        # SHOW STREAMS can return a maximum of 10000 rows.
        # https://docs.snowflake.com/en/sql-reference/sql/show-streams#usage-notes
        assert limit <= SHOW_STREAM_MAX_PAGE_SIZE

        # To work around this, we paginate through the results using the FROM clause.
        from_clause = (
            f"""FROM '{stream_pagination_marker}'""" if stream_pagination_marker else ""
        )
        return f"""SHOW STREAMS IN DATABASE "{db_name}" LIMIT {limit} {from_clause};"""
