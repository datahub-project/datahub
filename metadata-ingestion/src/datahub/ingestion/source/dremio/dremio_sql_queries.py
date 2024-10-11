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
        LEFT JOININFORMATION_SCHEMA.VIEWS V ON
        V.TABLE_CATALOG = T.TABLE_CATALOG
        AND V.TABLE_SCHEMA = T.TABLE_SCHEMA
        AND V.TABLE_NAME = T.TABLE_NAME
        INNER JOININFORMATION_SCHEMA.COLUMNS C ON
        C.TABLE_CATALOG = T.TABLE_CATALOG
        AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
        AND C.TABLE_NAME = T.TABLE_NAME
    WHERE
        T.TABLE_TYPE NOT IN ('SYSTEM_TABLE')
    )
    WHERE 1=1
        {schema_pattern}
        {table_pattern}
        {deny_schema_pattern}
        {deny_table_pattern}
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
        ) C
        ON
            CONCAT(REPLACE(REPLACE(REPLACE(V.PATH, ', ', '.'), '[', ''), ']', '')) =
            CONCAT(C.TABLE_SCHEMA, '.', C.TABLE_NAME)
        WHERE
            V.TYPE NOT IN ('SYSTEM_TABLE')
        )
        WHERE 1=1
            {schema_pattern}
            {table_pattern}
            {deny_schema_pattern}
            {deny_table_pattern}
        ORDER BY
            TABLE_SCHEMA ASC,
            TABLE_NAME ASC
        """

    QUERY_DATASETS_CLOUD = """
        SELECT* FROM
        (
        SELECT
            RESOURCE_ID,
            V.TABLE_NAME,
            OWNER,
            PATH AS TABLE_SCHEMA,
            CONCAT(REPLACE(REPLACE(
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
        ) C
        ON
            CONCAT(REPLACE(REPLACE(REPLACE(V.PATH, ', ', '.'), '[', ''), ']', '')) =
            CONCAT(C.TABLE_SCHEMA, '.', C.TABLE_NAME)
        WHERE
            V.TYPE NOT IN ('SYSTEM_TABLE')
        )
        WHERE 1=1
            {schema_pattern}
            {table_pattern}
            {deny_schema_pattern}
            {deny_table_pattern}
        ORDER BY
            TABLE_SCHEMA ASC,
            TABLE_NAME ASC
            """

    QUERY_ALL_JOBS = """
    SELECT
        *
    FROM
        SYS.JOBS_RECENT
    WHERE
        STATUS = 'COMPLETED'
        AND LENGTH(queried_datasets)>0
        AND user_name != '$dremio$'
        AND query_type not like '%INTERNAL%'
    """

    QUERY_ALL_JOBS_CLOUD = """
        SELECT
            *
        FROM
            SYS.PROJECT.HISTORY.JOBS
        WHERE
            STATUS = 'COMPLETED'
            AND LENGTH(queried_datasets)>0
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

    DISTINCT_COUNT_VALUE = """
    COUNT(DISTINCT {column}) AS {column}_max_value
    """

    # DISTINCT_COUNT_FREQUENCIES = """
    # (SELECTSTRING_AGG(CAST(COUNT(*) AS VARCHAR) || ':' || CAST({column} AS VARCHAR), ',')
    #  FROM(SELECT{column} FROM{dremio_dataset} GROUP BY {column})
    #  GROUP BY {column}
    #  ORDER BY COUNT(*) DESC
    # ) as {column}_value_frequencies
    # """

    HISTOGRAM_VALUES = """
        (SELECTSTRING_AGG(CAST(COUNT(*) AS VARCHAR) || ':' || CAST({column} AS VARCHAR), ',')
         FROM(SELECT{column} FROM{dremio_dataset} GROUP BY {column})
         GROUP BY {column}
         ORDER BY COUNT(*) DESC
        ) as {column}_value_frequencies
        """

    MAX_VALUE = """
    MAX({column}) AS {column}_max_value
    """

    MIN_VALUE = """
        MIN({column}) AS {column}_min_value
    """

    MEAN_VALUE = """
        AVG({column}) AS {column}_mean_value
    """

    MEDIAN_VALUE = """
        MEDIAN({column}) AS {column}_median_value
    """

    NULL_COUNT_VALUE = """
        SUM(CASEWHEN {column} IS NULL THEN 1 ELSE 0 END) as {column}_null_count
    """

    QUANTILES_VALUE = """
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) as {column}_25th_percentile,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column}) as {column}_50th_percentile,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) as {column}_75th_percentile
    """

    STDDEV_VALUE = """
        STDDEV({column}) as {column}_stddev_value
    """
