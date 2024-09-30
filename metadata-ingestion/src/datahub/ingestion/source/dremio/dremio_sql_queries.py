class DremioSQLQueries:
    QUERY_DATASETS = """
    SELECT
        T.TABLE_SCHEMA,
        T.TABLE_NAME,
        T.TABLE_TYPE,
        V.VIEW_DEFINITION
    FROM
        INFORMATION_SCHEMA."TABLES" AS T
        LEFT JOIN INFORMATION_SCHEMA.VIEWS V ON
        V.TABLE_CATALOG = T.TABLE_CATALOG
        AND V.TABLE_SCHEMA = T.TABLE_SCHEMA
        AND V.TABLE_NAME = T.TABLE_NAME
    WHERE
        (({collect_pds} AND {collect_system_tables}) OR
        (NOT({collect_pds}) AND NOT({collect_system_tables}) AND T.TABLE_TYPE = 'VIEW') OR
        ({collect_pds} AND NOT({collect_system_tables}) AND T.TABLE_TYPE IN ('TABLE','VIEW')) OR
        (NOT({collect_pds}) AND {collect_system_tables} AND T.TABLE_TYPE IN ('SYSTEM_TABLE','VIEW')))
        AND T.TABLE_SCHEMA not like 'sys%';
    """

    QUERY_DATASET_SCHEMAS = """
    SELECT
        T.TABLE_SCHEMA,
        T.TABLE_NAME,
        T.TABLE_TYPE,
        C.COLUMN_NAME,
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
        V.TABLE_SCHEMA = '{schema}'
        AND V.TABLE_NAME = '{table_name}'
    """

    QUERY_ALL_TABLES_AND_COLUMNS = """
    SELECT
        T.TABLE_SCHEMA,
        T.TABLE_NAME,
        T.TABLE_TYPE,
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
    ORDER BY
        T.TABLE_SCHEMA ASC,
        T.TABLE_NAME ASC
    """

    QUERY_ALL_JOBS = """
    SELECT
        *
    FROM
        SYS.jobs_recent
    WHERE
        status = 'COMPLETED'
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
