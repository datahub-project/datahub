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

    # Dremio Documentation: https://docs.dremio.com/current/reference/sql/system-tables/jobs_recent/
    # queried_datasets incorrectly documented as [varchar]. Observed as varchar.
    # LENGTH used as opposed to ARRAY_SIZE
    QUERY_ALL_JOBS = """
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
    """

    # Dremio Documentation: https://docs.dremio.com/cloud/reference/sql/system-tables/jobs-historical
    # queried_datasets correctly documented as [varchar]
    QUERY_ALL_JOBS_CLOUD = """
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
