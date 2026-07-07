import re
from datetime import datetime, timedelta
from typing import List, Optional, Union


class DremioSQLQueries:
    # VIEW_DEFINITION is fetched separately (QUERY_VIEW_DEFINITIONS_*) rather than
    # joined here: repeating it per column can push the result's VARCHAR vector
    # past Dremio's 2 GiB limit (OversizedAllocationException), dropping datasets.
    #
    # {columns_schema_filter} sits directly inside the INFORMATION_SCHEMA.COLUMNS
    # scan so Dremio's InfoSchemaPushFilterIntoScan rule can push it into the scan
    # (used by container partitioning). Only plain =/LIKE on a bare TABLE_SCHEMA
    # push down — a filter above the join, or wrapped in REGEXP_LIKE/CONCAT, is not
    # pushed. Left empty for the single catalog-wide fetch.
    QUERY_DATASETS_CE_GLOBAL = """
    SELECT * FROM
    (
    SELECT
        T.TABLE_SCHEMA,
        T.TABLE_NAME,
        CONCAT(T.TABLE_SCHEMA, '.', T.TABLE_NAME) AS FULL_TABLE_PATH,
        C.COLUMN_NAME,
        C.ORDINAL_POSITION,
        C.IS_NULLABLE,
        C.DATA_TYPE,
        C.COLUMN_SIZE
    FROM
        INFORMATION_SCHEMA."TABLES" T
        INNER JOIN INFORMATION_SCHEMA.COLUMNS C ON
        C.TABLE_CATALOG = T.TABLE_CATALOG
        AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
        AND C.TABLE_NAME = T.TABLE_NAME
    WHERE
        T.TABLE_TYPE NOT IN ('SYSTEM_TABLE')
        {columns_schema_filter}
    )
    WHERE 1=1
        {schema_pattern}
        {deny_schema_pattern}
    ORDER BY
        TABLE_SCHEMA ASC,
        TABLE_NAME ASC,
        ORDINAL_POSITION ASC
    {limit_clause}
    """

    # One row per view. {view_definition_select} is filled in by the API layer so
    # the definition column can optionally be wrapped in SUBSTR(...) to cap size.
    QUERY_VIEW_DEFINITIONS_CE = """
    SELECT
        CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS FULL_TABLE_PATH,
        TABLE_SCHEMA,
        {view_definition_select} AS VIEW_DEFINITION
    FROM
        INFORMATION_SCHEMA.VIEWS
    WHERE 1=1
        {schema_pattern}
        {deny_schema_pattern}
    ORDER BY
        TABLE_SCHEMA ASC,
        TABLE_NAME ASC
    {limit_clause}
    """

    # VIEW_DEFINITION omitted here; fetched by QUERY_VIEW_DEFINITIONS_EE (see
    # QUERY_DATASETS_CE_GLOBAL).
    QUERY_DATASETS_EE_GLOBAL = """
        SELECT * FROM
        (
        SELECT
            RESOURCE_ID,
            V.TABLE_NAME,
            OWNER,
            PATH AS TABLE_SCHEMA,
            CONCAT(REPLACE(REPLACE(
                REPLACE(V.PATH, ', ', '.'),
                 '[', ''), ']', ''
                 )) AS FULL_TABLE_PATH,
            OWNER_TYPE,
            LOCATION_ID,
            FORMAT_TYPE,
            COLUMN_NAME,
            ORDINAL_POSITION,
            IS_NULLABLE,
            DATA_TYPE,
            COLUMN_SIZE,
            CREATED
        FROM
            (SELECT
                VIEW_ID AS RESOURCE_ID,
                VIEW_NAME AS TABLE_NAME,
                PATH,
                CASE
                    WHEN LENGTH(SCHEMA_ID) = 0 THEN SPACE_ID
                    ELSE SCHEMA_ID
                END AS LOCATION_ID,
                OWNER_ID,
                '' AS FORMAT_TYPE,
                CREATED,
                TYPE
            FROM
                SYS.VIEWS
            UNION ALL
            SELECT
                TABLE_ID AS RESOURCE_ID,
                TABLE_NAME,
                PATH,
                CASE
                    WHEN LENGTH(SCHEMA_ID) = 0 THEN SOURCE_ID
                    ELSE SCHEMA_ID
                END AS LOCATION_ID,
                OWNER_ID,
                FORMAT_TYPE,
                CREATED,
                TYPE
            FROM
                SYS."TABLES"
            ) V
        LEFT JOIN
            (SELECT
                USER_ID AS ID,
                USER_NAME AS "OWNER",
                'USER' AS OWNER_TYPE
            FROM
                SYS.USERS
            UNION ALL
            SELECT
                ROLE_ID AS ID,
                ROLE_NAME AS "OWNER",
                'GROUP' AS OWNER_TYPE
            FROM
                SYS.ROLES
            ) U
        ON
            V.OWNER_ID = U.ID
        LEFT JOIN
        (SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION,
            IS_NULLABLE,
            DATA_TYPE,
            COLUMN_SIZE
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
            {columns_schema_filter}
        ) C
        ON
            CONCAT(REPLACE(REPLACE(REPLACE(V.PATH, ', ', '.'), '[', ''), ']', '')) =
            CONCAT(C.TABLE_SCHEMA, '.', C.TABLE_NAME)
        WHERE
            V.TYPE NOT IN ('SYSTEM_TABLE')
        )
        WHERE 1=1
            {schema_pattern}
            {deny_schema_pattern}
        ORDER BY
            TABLE_SCHEMA ASC,
            TABLE_NAME ASC,
            ORDINAL_POSITION ASC
        {limit_clause}
        """

    # VIEW_DEFINITION omitted here; fetched by QUERY_VIEW_DEFINITIONS_CLOUD (see
    # QUERY_DATASETS_CE_GLOBAL).
    QUERY_DATASETS_CLOUD_GLOBAL = """
        SELECT * FROM
        (
        SELECT
            RESOURCE_ID,
            V.TABLE_NAME,
            OWNER,
            PATH AS TABLE_SCHEMA,
            CONCAT(REPLACE(REPLACE(
            REPLACE(V.PATH, ', ', '.'),
             '[', ''), ']', ''
             )) AS FULL_TABLE_PATH,
            OWNER_TYPE,
            LOCATION_ID,
            FORMAT_TYPE,
            COLUMN_NAME,
            ORDINAL_POSITION,
            IS_NULLABLE,
            DATA_TYPE,
            COLUMN_SIZE,
            CREATED
        FROM
            (SELECT
                VIEW_ID AS RESOURCE_ID,
                VIEW_NAME AS TABLE_NAME,
                PATH,
                CASE
                    WHEN LENGTH(SCHEMA_ID) = 0 THEN SPACE_ID
                    ELSE SCHEMA_ID
                END AS LOCATION_ID,
                OWNER_ID,
                '' AS FORMAT_TYPE,
                CREATED,
                TYPE
            FROM
                SYS.PROJECT.VIEWS
            UNION ALL
            SELECT
                TABLE_ID AS RESOURCE_ID,
                TABLE_NAME,
                PATH,
                CASE
                    WHEN LENGTH(SCHEMA_ID) = 0 THEN SOURCE_ID
                    ELSE SCHEMA_ID
                END AS LOCATION_ID,
                OWNER_ID,
                FORMAT_TYPE,
                CREATED,
                TYPE
            FROM
                SYS.PROJECT."TABLES"
            ) V
        LEFT JOIN
            (SELECT
                USER_ID AS ID,
                USER_NAME AS "OWNER",
                'USER' AS OWNER_TYPE
            FROM
                SYS.ORGANIZATION.USERS
            UNION ALL
            SELECT
                ROLE_ID AS ID,
                ROLE_NAME AS "OWNER",
                'GROUP' AS OWNER_TYPE
            FROM
                SYS.ORGANIZATION.ROLES
            ) U
        ON
            V.OWNER_ID = U.ID
        LEFT JOIN
        (SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION,
            IS_NULLABLE,
            DATA_TYPE,
            COLUMN_SIZE
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
            {columns_schema_filter}
        ) C
        ON
            CONCAT(REPLACE(REPLACE(REPLACE(V.PATH, ', ', '.'), '[', ''), ']', '')) =
            CONCAT(C.TABLE_SCHEMA, '.', C.TABLE_NAME)
        WHERE
            V.TYPE NOT IN ('SYSTEM_TABLE')
        )
        WHERE 1=1
            {schema_pattern}
            {deny_schema_pattern}
        ORDER BY
            TABLE_SCHEMA ASC,
            TABLE_NAME ASC,
            ORDINAL_POSITION ASC
        {limit_clause}
        """

    # One row per view; see QUERY_VIEW_DEFINITIONS_CE for {view_definition_select}.
    QUERY_VIEW_DEFINITIONS_EE = """
        SELECT * FROM
        (
        SELECT
            CONCAT(REPLACE(REPLACE(REPLACE(PATH, ', ', '.'), '[', ''), ']', '')) AS FULL_TABLE_PATH,
            PATH AS TABLE_SCHEMA,
            {view_definition_select} AS VIEW_DEFINITION
        FROM
            SYS.VIEWS
        WHERE
            TYPE NOT IN ('SYSTEM_TABLE')
        )
        WHERE 1=1
            {schema_pattern}
            {deny_schema_pattern}
        ORDER BY
            TABLE_SCHEMA ASC
        {limit_clause}
        """

    # One row per view; see QUERY_VIEW_DEFINITIONS_CE for {view_definition_select}.
    QUERY_VIEW_DEFINITIONS_CLOUD = """
        SELECT * FROM
        (
        SELECT
            CONCAT(REPLACE(REPLACE(REPLACE(PATH, ', ', '.'), '[', ''), ']', '')) AS FULL_TABLE_PATH,
            PATH AS TABLE_SCHEMA,
            {view_definition_select} AS VIEW_DEFINITION
        FROM
            SYS.PROJECT.VIEWS
        WHERE
            TYPE NOT IN ('SYSTEM_TABLE')
        )
        WHERE 1=1
            {schema_pattern}
            {deny_schema_pattern}
        ORDER BY
            TABLE_SCHEMA ASC
        {limit_clause}
        """

    # Normalized schema field for the pattern filters below: strips Dremio's
    # array-style "[a, b]" path formatting down to dotted, UPPER-cased text so
    # REGEXP_LIKE patterns can match it consistently.
    SCHEMA_FILTER_FIELD = "CONCAT(REPLACE(REPLACE(REPLACE(UPPER(TABLE_SCHEMA), ', ', '.'), '[', ''), ']', ''))"

    @staticmethod
    def pattern_condition(
        patterns: Union[str, List[str]], field: str, allow: bool = True
    ) -> str:
        """Build an `AND [NOT] REGEXP_LIKE(field, '...')` schema-pattern predicate."""
        if not patterns:
            return ""

        if isinstance(patterns, str):
            patterns = [patterns.upper()]

        if ".*" in patterns and allow:
            return ""

        patterns = [p.upper() for p in patterns if p != ".*"]
        if not patterns:
            return ""

        operator = "REGEXP_LIKE" if allow else "NOT REGEXP_LIKE"
        pattern_str = "|".join(f"({p})" for p in patterns)
        return f"AND {operator}({field}, '{pattern_str}')"

    @staticmethod
    def container_schema_condition(container_name: str, field: str) -> str:
        """Build an `AND REGEXP_LIKE(...)` predicate scoping to one root container.

        Targets the derived (CONCAT/UPPER-wrapped) schema field in the outer
        query, so it does NOT push into the info-schema scan — it bounds the
        system-table side and the final sort. `container_columns_filter` handles
        the pushable half. Matches the container root and any nested schema path
        beneath it; the name is regex-escaped so special characters match literally.
        """
        pattern = f"{re.escape(container_name)}(\\..*)?"
        return DremioSQLQueries.pattern_condition([pattern], field)

    @staticmethod
    def container_columns_filter(container_name: str, column: str) -> str:
        """Build a pushable `TABLE_SCHEMA` predicate scoping to one root container.

        Emitted directly inside the INFORMATION_SCHEMA.COLUMNS scan so Dremio's
        InfoSchemaPushFilterIntoScan rule pushes it down (bounding the dominant
        scan). Dremio only pushes plain `=`/`LIKE` on a bare `TABLE_SCHEMA`
        (optionally `UPPER`-wrapped) — never `REGEXP_LIKE` or CONCAT-wrapped
        columns — so we match the container root with equality and nested schemas
        with a `LIKE 'root.%'`. LIKE wildcards in the name are escaped.
        """
        literal = container_name.upper().replace("'", "''")
        like_literal = (
            literal.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        return (
            f"AND (UPPER({column}) = '{literal}' "
            f"OR UPPER({column}) LIKE '{like_literal}.%' ESCAPE '\\')"
        )

    @staticmethod
    def view_definition_select(source_column: str, max_length: Optional[int]) -> str:
        """Return the VIEW_DEFINITION select expression, optionally SUBSTR-capped.

        Capping happens server-side so an oversized single definition can't blow
        the result's VARCHAR vector past Dremio's 2 GiB limit.
        """
        if max_length is not None:
            return f"SUBSTR({source_column}, 1, {max_length})"
        return source_column

    @staticmethod
    def _get_default_start_timestamp_millis() -> str:
        """Get default start timestamp (1 day ago) in milliseconds precision format"""
        one_day_ago = datetime.now() - timedelta(days=1)
        return one_day_ago.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ]  # Truncate to milliseconds

    @staticmethod
    def _get_default_end_timestamp_millis() -> str:
        """Get default end timestamp (now) in milliseconds precision format"""
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Truncate to milliseconds

    @staticmethod
    def get_query_all_jobs(
        start_timestamp_millis: Optional[str] = None,
        end_timestamp_millis: Optional[str] = None,
    ) -> str:
        """Get query for all jobs with optional time filtering."""
        if start_timestamp_millis is None:
            start_timestamp_millis = (
                DremioSQLQueries._get_default_start_timestamp_millis()
            )
        if end_timestamp_millis is None:
            end_timestamp_millis = DremioSQLQueries._get_default_end_timestamp_millis()

        return f"""
        SELECT
            job_id,
            user_name,
            submitted_ts,
            query,
            queried_datasets
        FROM
            SYS.JOBS_RECENT
        WHERE
            STATUS = 'COMPLETED'
            AND LENGTH(queried_datasets)>0
            AND user_name != '$dremio$'
            AND query_type not like '%INTERNAL%'
            AND submitted_ts >= TIMESTAMP '{start_timestamp_millis}'
            AND submitted_ts <= TIMESTAMP '{end_timestamp_millis}'
        ORDER BY submitted_ts ASC
        {{limit_clause}}
        """

    @staticmethod
    def get_query_all_jobs_array(
        start_timestamp_millis: Optional[str] = None,
        end_timestamp_millis: Optional[str] = None,
    ) -> str:
        """All-jobs query for Dremio Software 26.1.0+ (queried_datasets is
        ARRAY<VARCHAR>). Renders the array as a bracket-string so the
        existing DremioQuery._get_queried_datasets parser keeps working."""
        if start_timestamp_millis is None:
            start_timestamp_millis = (
                DremioSQLQueries._get_default_start_timestamp_millis()
            )
        if end_timestamp_millis is None:
            end_timestamp_millis = DremioSQLQueries._get_default_end_timestamp_millis()

        return f"""
        SELECT
            job_id,
            user_name,
            submitted_ts,
            query,
            CONCAT('[', ARRAY_TO_STRING(queried_datasets, ','), ']') as queried_datasets
        FROM
            SYS.JOBS_RECENT
        WHERE
            STATUS = 'COMPLETED'
            AND ARRAY_SIZE(queried_datasets)>0
            AND user_name != '$dremio$'
            AND query_type not like '%INTERNAL%'
            AND submitted_ts >= TIMESTAMP '{start_timestamp_millis}'
            AND submitted_ts <= TIMESTAMP '{end_timestamp_millis}'
        ORDER BY submitted_ts ASC
        """

    @staticmethod
    def get_query_all_jobs_cloud(
        start_timestamp_millis: Optional[str] = None,
        end_timestamp_millis: Optional[str] = None,
    ) -> str:
        """Get query for all jobs in Dremio Cloud with optional time filtering."""
        if start_timestamp_millis is None:
            start_timestamp_millis = (
                DremioSQLQueries._get_default_start_timestamp_millis()
            )
        if end_timestamp_millis is None:
            end_timestamp_millis = DremioSQLQueries._get_default_end_timestamp_millis()

        return f"""
        SELECT
            job_id,
            user_name,
            submitted_ts,
            query,
            CONCAT('[', ARRAY_TO_STRING(queried_datasets, ','), ']') as queried_datasets
        FROM
            sys.project.history.jobs
        WHERE
            STATUS = 'COMPLETED'
            AND ARRAY_SIZE(queried_datasets)>0
            AND user_name != '$dremio$'
            AND query_type not like '%INTERNAL%'
            AND submitted_ts >= TIMESTAMP '{start_timestamp_millis}'
            AND submitted_ts <= TIMESTAMP '{end_timestamp_millis}'
        ORDER BY submitted_ts ASC
        {{limit_clause}}
        """
