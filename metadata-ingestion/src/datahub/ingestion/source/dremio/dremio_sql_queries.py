from datetime import datetime, timedelta
from typing import Optional


class DremioSQLQueries:
    QUERY_DATASETS_CE = """
    SELECT* FROM
    (
    SELECT
        T.TABLE_SCHEMA,
        T.TABLE_NAME,
        CONCAT(T.TABLE_SCHEMA, '.', T.TABLE_NAME) AS FULL_TABLE_PATH,
        V.VIEW_DEFINITION,
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
    )
    WHERE 1=1
        {schema_pattern}
        {deny_schema_pattern}
    ORDER BY
        TABLE_SCHEMA ASC,
        TABLE_NAME ASC
    """

    QUERY_DATASETS_EE = """
        SELECT* FROM
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
                SQL_DEFINITION AS VIEW_DEFINITION,
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
    def _get_default_end_timestamp_millis() -> str:
        """Get default end timestamp (now) in milliseconds precision format"""
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Truncate to milliseconds

    @staticmethod
    def get_query_all_jobs(
        start_timestamp_millis: Optional[str] = None,
        end_timestamp_millis: Optional[str] = None,
    ) -> str:
        """
        Get query for all jobs with optional time filtering.

        Args:
            start_timestamp_millis: Start timestamp in format 'YYYY-MM-DD HH:MM:SS.mmm' (defaults to 1 day ago)
            end_timestamp_millis: End timestamp in format 'YYYY-MM-DD HH:MM:SS.mmm' (defaults to now)

        Returns:
            SQL query string with time filtering applied
        """
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
        """

    @staticmethod
    def get_query_all_jobs_cloud(
        start_timestamp_millis: Optional[str] = None,
        end_timestamp_millis: Optional[str] = None,
    ) -> str:
        """
        Get query for all jobs in Dremio Cloud with optional time filtering.

        Args:
            start_timestamp_millis: Start timestamp in format 'YYYY-MM-DD HH:MM:SS.mmm' (defaults to 7 days ago)
            end_timestamp_millis: End timestamp in format 'YYYY-MM-DD HH:MM:SS.mmm' (defaults to now)

        Returns:
            SQL query string with time filtering applied
        """
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
        """

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
