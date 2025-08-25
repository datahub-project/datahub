import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


class DremioSQLQueries:
    # Optimized query for Community Edition with intelligent view handling
    QUERY_DATASETS_CE = """
    SELECT * FROM (
        SELECT
            T.TABLE_SCHEMA,
            T.TABLE_NAME,
            CONCAT(T.TABLE_SCHEMA, '.', T.TABLE_NAME) AS FULL_TABLE_PATH,
            V.VIEW_DEFINITION,
            LENGTH(V.VIEW_DEFINITION) AS VIEW_DEFINITION_LENGTH,
            C.COLUMN_NAME,
            C.IS_NULLABLE,
            C.DATA_TYPE,
            C.COLUMN_SIZE
        FROM
            INFORMATION_SCHEMA."TABLES" T
            LEFT JOIN INFORMATION_SCHEMA.VIEWS V ON
                V.TABLE_CATALOG = T.TABLE_CATALOG
                AND V.TABLE_SCHEMA = T.TABLE_SCHEMA
                AND V.TABLE_NAME = T.TABLE_NAME
            INNER JOIN INFORMATION_SCHEMA.COLUMNS C ON
                C.TABLE_CATALOG = T.TABLE_CATALOG
                AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
                AND C.TABLE_NAME = T.TABLE_NAME
        WHERE
            T.TABLE_TYPE NOT IN ('SYSTEM_TABLE')
            AND LOCATE('{container_name}', LOWER(T.TABLE_SCHEMA)) = 1
            {system_table_filter}
    )
    WHERE 1=1
        {schema_pattern}
        {deny_schema_pattern}
    ORDER BY
        TABLE_SCHEMA ASC,
        TABLE_NAME ASC,
        COLUMN_NAME ASC
    """

    QUERY_DATASETS_EE = """
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
            VIEW_DEFINITION,
            LENGTH(VIEW_DEFINITION) AS VIEW_DEFINITION_LENGTH,
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
                SQL_DEFINITION AS VIEW_DEFINITION,
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
                NULL AS VIEW_DEFINITION,
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
        WHERE
            LOCATE('{container_name}', LOWER(TABLE_SCHEMA)) = 1
        ) C
        ON
            CONCAT(REPLACE(REPLACE(REPLACE(V.PATH, ', ', '.'), '[', ''), ']', '')) =
            CONCAT(C.TABLE_SCHEMA, '.', C.TABLE_NAME)
        WHERE
            V.TYPE NOT IN ('SYSTEM_TABLE')
            AND LOCATE('{container_name}', LOWER(PATH)) = 2
        )
        WHERE 1=1
            {schema_pattern}
            {deny_schema_pattern}
        ORDER BY
            TABLE_SCHEMA ASC,
            TABLE_NAME ASC
        """

    QUERY_DATASETS_CLOUD = """
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
            VIEW_DEFINITION,
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
                CASE 
                    WHEN {max_view_definition_length} = -1 THEN SQL_DEFINITION
                    WHEN {truncate_large_view_definitions} = true AND LENGTH(SQL_DEFINITION) > {max_view_definition_length} 
                        THEN CONCAT(SUBSTRING(SQL_DEFINITION, 1, {max_view_definition_length}), '... [TRUNCATED]')
                    WHEN {truncate_large_view_definitions} = false AND LENGTH(SQL_DEFINITION) > {max_view_definition_length}
                        THEN NULL
                    ELSE SQL_DEFINITION
                END AS VIEW_DEFINITION,
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
                NULL AS VIEW_DEFINITION,
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
        WHERE
            LOCATE('{container_name}', LOWER(TABLE_SCHEMA)) = 1
        ) C
        ON
            CONCAT(REPLACE(REPLACE(REPLACE(V.PATH, ', ', '.'), '[', ''), ']', '')) =
            CONCAT(C.TABLE_SCHEMA, '.', C.TABLE_NAME)
        WHERE
            V.TYPE NOT IN ('SYSTEM_TABLE')
            AND LOCATE('{container_name}', LOWER(PATH)) = 2
        )
        WHERE 1=1
            {schema_pattern}
            {deny_schema_pattern}
        ORDER BY
            TABLE_SCHEMA ASC,
            TABLE_NAME ASC
            """

    @staticmethod
    def _get_default_start_timestamp_millis() -> str:
        """Get default start timestamp (1 day ago) in milliseconds precision format"""
        one_day_ago = datetime.now() - timedelta(days=1)
        return one_day_ago.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ]  # Truncate to milliseconds

    @staticmethod
    def get_optimized_start_timestamp(
        last_checkpoint: Optional[datetime] = None, default_lookback_hours: int = 24
    ) -> str:
        """
        Get optimized start timestamp for stateful ingestion.

        This method implements intelligent time window selection:
        - Uses last checkpoint if available (stateful ingestion)
        - Falls back to configurable lookback period
        - Adds small buffer to handle clock skew and late-arriving data

        Args:
            last_checkpoint: Last successful ingestion timestamp
            default_lookback_hours: Default lookback period in hours

        Returns:
            Formatted timestamp string for SQL queries
        """
        now = datetime.now()

        if last_checkpoint:
            # Use checkpoint with small buffer for late-arriving data (15 minutes)
            buffer_minutes = 15
            start_time = last_checkpoint - timedelta(minutes=buffer_minutes)

            # Safety check: don't go back more than 7 days from checkpoint
            max_lookback = now - timedelta(days=7)
            if start_time < max_lookback:
                start_time = max_lookback

            logger.info(
                f"Using stateful ingestion: checkpoint={last_checkpoint}, "
                f"start_time={start_time} (with {buffer_minutes}min buffer)"
            )
        else:
            # No checkpoint available, use default lookback
            start_time = now - timedelta(hours=default_lookback_hours)
            logger.info(
                f"No checkpoint found, using {default_lookback_hours}h lookback: {start_time}"
            )

        return start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    @staticmethod
    def _get_default_end_timestamp_millis() -> str:
        """Get default end timestamp (now) in milliseconds precision format"""
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Truncate to milliseconds

    @staticmethod
    def get_query_all_jobs(
        start_timestamp_millis: Optional[str] = None,
        end_timestamp_millis: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> str:
        """
        Get query for all jobs with optional time filtering and pagination.

        Args:
            start_timestamp_millis: Start timestamp in format 'YYYY-MM-DD HH:MM:SS.mmm' (defaults to 1 day ago)
            end_timestamp_millis: End timestamp in format 'YYYY-MM-DD HH:MM:SS.mmm' (defaults to now)
            limit: Maximum number of rows to return (for pagination)
            offset: Number of rows to skip (for pagination)

        Returns:
            SQL query string with time filtering and pagination applied
        """
        if start_timestamp_millis is None:
            start_timestamp_millis = (
                DremioSQLQueries._get_default_start_timestamp_millis()
            )
        if end_timestamp_millis is None:
            end_timestamp_millis = DremioSQLQueries._get_default_end_timestamp_millis()

        base_query = f"""
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
        ORDER BY submitted_ts DESC
        """

        # Add pagination if specified
        if limit is not None:
            base_query += f"\nLIMIT {limit}"
            if offset is not None:
                base_query += f" OFFSET {offset}"

        return base_query

    @staticmethod
    def get_query_all_jobs_cloud(
        start_timestamp_millis: Optional[str] = None,
        end_timestamp_millis: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> str:
        """
        Get query for all jobs in Dremio Cloud with optional time filtering and pagination.

        Args:
            start_timestamp_millis: Start timestamp in format 'YYYY-MM-DD HH:MM:SS.mmm' (defaults to 7 days ago)
            end_timestamp_millis: End timestamp in format 'YYYY-MM-DD HH:MM:SS.mmm' (defaults to now)
            limit: Maximum number of rows to return (for pagination)
            offset: Number of rows to skip (for pagination)

        Returns:
            SQL query string with time filtering and pagination applied
        """
        if start_timestamp_millis is None:
            start_timestamp_millis = (
                DremioSQLQueries._get_default_start_timestamp_millis()
            )
        if end_timestamp_millis is None:
            end_timestamp_millis = DremioSQLQueries._get_default_end_timestamp_millis()

        base_query = f"""
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
        ORDER BY submitted_ts DESC
        """

        # Add pagination if specified
        if limit is not None:
            base_query += f"\nLIMIT {limit}"
            if offset is not None:
                base_query += f" OFFSET {offset}"

        return base_query

    QUERY_TYPES = [
        "ALTER TABLE",
        "ALTER VIEW",
        "COPY INTO",
        "CREATE TABLE",
        "CREATE VIEW",
        "DROP TABLE",
        "DROP VIEW",
        "SELECT",
        "WITH",
    ]

    PROFILE_COLUMNS = """
    SELECT
        {profile_queries}
    FROM(
    SELECT
        *
    FROM
        {dremio_dataset}
    LIMIT {sample_limit}
    )
    """

    # Intelligent chunking queries for large view definitions
    @staticmethod
    def get_view_chunk_query_ce(
        table_schema: str, table_name: str, chunk_start: int, chunk_size: int
    ) -> str:
        """
        Generate a query to fetch a specific chunk of a view definition for Community Edition.

        Args:
            table_schema: Schema name
            table_name: Table name
            chunk_start: Starting position (1-based)
            chunk_size: Size of chunk to fetch

        Returns:
            SQL query to fetch the specified chunk
        """
        return f"""
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            SUBSTRING(VIEW_DEFINITION, {chunk_start}, {chunk_size}) AS VIEW_DEFINITION_CHUNK,
            {chunk_start} AS CHUNK_START_POSITION,
            {chunk_size} AS CHUNK_SIZE,
            LENGTH(VIEW_DEFINITION) AS TOTAL_LENGTH
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = '{table_schema}' 
        AND TABLE_NAME = '{table_name}'
        """

    @staticmethod
    def get_view_chunk_query_ee(
        table_schema: str, table_name: str, chunk_start: int, chunk_size: int
    ) -> str:
        """
        Generate a query to fetch a specific chunk of a view definition for Enterprise Edition.

        Args:
            table_schema: Schema name (path)
            table_name: Table name
            chunk_start: Starting position (1-based)
            chunk_size: Size of chunk to fetch

        Returns:
            SQL query to fetch the specified chunk
        """
        return f"""
        SELECT
            PATH AS TABLE_SCHEMA,
            VIEW_NAME AS TABLE_NAME,
            SUBSTRING(SQL_DEFINITION, {chunk_start}, {chunk_size}) AS VIEW_DEFINITION_CHUNK,
            {chunk_start} AS CHUNK_START_POSITION,
            {chunk_size} AS CHUNK_SIZE,
            LENGTH(SQL_DEFINITION) AS TOTAL_LENGTH
        FROM SYS.VIEWS
        WHERE CONCAT(REPLACE(REPLACE(REPLACE(PATH, ', ', '.'), '[', ''), ']', '')) = '{table_schema}'
        AND VIEW_NAME = '{table_name}'
        """

    @staticmethod
    def get_view_chunk_query_cloud(
        table_schema: str, table_name: str, chunk_start: int, chunk_size: int
    ) -> str:
        """
        Generate a query to fetch a specific chunk of a view definition for Dremio Cloud.

        Args:
            table_schema: Schema name (path)
            table_name: Table name
            chunk_start: Starting position (1-based)
            chunk_size: Size of chunk to fetch

        Returns:
            SQL query to fetch the specified chunk
        """
        return f"""
        SELECT
            PATH AS TABLE_SCHEMA,
            VIEW_NAME AS TABLE_NAME,
            SUBSTRING(SQL_DEFINITION, {chunk_start}, {chunk_size}) AS VIEW_DEFINITION_CHUNK,
            {chunk_start} AS CHUNK_START_POSITION,
            {chunk_size} AS CHUNK_SIZE,
            LENGTH(SQL_DEFINITION) AS TOTAL_LENGTH
        FROM SYS.PROJECT.VIEWS
        WHERE CONCAT(REPLACE(REPLACE(REPLACE(PATH, ', ', '.'), '[', ''), ']', '')) = '{table_schema}'
        AND VIEW_NAME = '{table_name}'
        """

    @staticmethod
    def get_system_limits_query() -> str:
        """
        Generate a query to detect Dremio's single_field_size_bytes limit.

        Works for Enterprise, Software, and Community editions.

        Returns:
            SQL query to fetch system configuration limits
        """
        return """
        SELECT 
            option_name,
            option_value,
            option_type,
            option_scope
        FROM sys.options 
        WHERE LOWER(option_name) LIKE '%single%field%size%'
           OR LOWER(option_name) LIKE '%field%size%bytes%'
           OR LOWER(option_name) LIKE '%max%field%size%'
        """

    @staticmethod
    def get_system_limits_query_cloud() -> str:
        """
        Generate a query to detect Dremio Cloud's system limits.

        Returns:
            SQL query to fetch system configuration limits for Cloud
        """
        return """
        SELECT 
            option_name,
            option_value,
            option_type,
            option_scope
        FROM sys.project.options 
        WHERE LOWER(option_name) LIKE '%single%field%size%'
           OR LOWER(option_name) LIKE '%field%size%bytes%'
           OR LOWER(option_name) LIKE '%max%field%size%'
        """

    @staticmethod
    def get_view_size_analysis_query(schema_list: str) -> str:
        """
        Generate a query to analyze view sizes for intelligent processing.

        Args:
            schema_list: Comma-separated list of schema names in quotes

        Returns:
            SQL query to analyze view definition sizes
        """
        return f"""
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS FULL_TABLE_PATH,
            LENGTH(VIEW_DEFINITION) AS VIEW_DEFINITION_LENGTH,
            SUBSTRING(VIEW_DEFINITION, 1, 100) AS VIEW_DEFINITION_PREVIEW
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA IN ({schema_list})
        ORDER BY VIEW_DEFINITION_LENGTH DESC
        """
