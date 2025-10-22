"""
SAP HANA SQL queries for metadata extraction.

This module contains all SQL queries used to extract metadata from SAP HANA databases.
"""


class HanaQuery:
    """Collection of SQL queries for SAP HANA metadata extraction."""

    @staticmethod
    def get_databases() -> str:
        """Get list of databases."""
        return """
        SELECT database_name AS "DATABASE_NAME",
               created AS "CREATED",
               last_altered AS "LAST_ALTERED",
               comment AS "COMMENT"
        FROM SYS.M_DATABASES
        ORDER BY database_name
        """

    @staticmethod
    def schemas_for_database(db_name: str) -> str:
        """Get schemas for a specific database."""
        return """
        SELECT schema_name AS "SCHEMA_NAME",
               create_time AS "CREATED",
               comments AS "COMMENT"
        FROM SYS.SCHEMAS
        WHERE schema_name != 'SYS'
          AND schema_name != '_SYS_STATISTICS'
          AND schema_name != '_SYS_REPO'
          AND schema_name != '_SYS_BI'
        ORDER BY schema_name
        """

    @staticmethod
    def tables_for_schema(schema_name: str, db_name: str) -> str:
        """Get tables for a specific schema."""
        return f"""
        SELECT table_name AS "TABLE_NAME",
               table_type AS "TABLE_TYPE",
               comments AS "COMMENT",
               create_time AS "CREATED",
               record_count AS "ROW_COUNT",
               table_size AS "BYTES"
        FROM SYS.TABLES
        WHERE schema_name = '{schema_name}'
          AND table_type IN ('ROW', 'COLUMN')
        ORDER BY table_name
        """

    @staticmethod
    def views_for_schema(schema_name: str, db_name: str) -> str:
        """Get views for a specific schema."""
        return f"""
        SELECT view_name AS "VIEW_NAME",
               definition AS "DEFINITION",
               comments AS "COMMENT",
               create_time AS "CREATED",
               view_type AS "VIEW_TYPE"
        FROM SYS.VIEWS
        WHERE schema_name = '{schema_name}'
        ORDER BY view_name
        """

    @staticmethod
    def get_calculation_views() -> str:
        """Get calculation views from repository."""
        return """
        SELECT PACKAGE_ID,
               OBJECT_NAME,
               TO_VARCHAR(CDATA) AS CDATA
        FROM _SYS_REPO.ACTIVE_OBJECT
        WHERE LOWER(OBJECT_SUFFIX) = 'calculationview'
        ORDER BY PACKAGE_ID, OBJECT_NAME
        """

    @staticmethod
    def columns_for_table(table_name: str, schema_name: str, db_name: str) -> str:
        """Get columns for a specific table or view."""
        return f"""
        SELECT COLUMN_NAME,
               COMMENTS,
               DATA_TYPE_NAME,
               IS_NULLABLE,
               POSITION,
               LENGTH,
               SCALE
        FROM (
            SELECT POSITION,
                   COLUMN_NAME,
                   COMMENTS,
                   DATA_TYPE_NAME,
                   IS_NULLABLE,
                   LENGTH,
                   SCALE,
                   SCHEMA_NAME,
                   TABLE_NAME
            FROM SYS.TABLE_COLUMNS 
            UNION ALL
            SELECT POSITION,
                   COLUMN_NAME,
                   COMMENTS,
                   DATA_TYPE_NAME,
                   IS_NULLABLE,
                   LENGTH,
                   SCALE,
                   SCHEMA_NAME,
                   VIEW_NAME AS TABLE_NAME
            FROM SYS.VIEW_COLUMNS
        ) AS COLUMNS
        WHERE LOWER(SCHEMA_NAME) = '{schema_name.lower()}'
          AND LOWER(TABLE_NAME) = '{table_name.lower()}'
        ORDER BY POSITION ASC
        """

    @staticmethod
    def columns_for_calculation_view(view_name: str) -> str:
        """Get columns for a calculation view."""
        return f"""
        SELECT COLUMN_NAME,
               COMMENTS,
               DATA_TYPE_NAME,
               IS_NULLABLE,
               POSITION
        FROM SYS.VIEW_COLUMNS
        WHERE LOWER(SCHEMA_NAME) = '_sys_bic'
          AND LOWER(VIEW_NAME) = '{view_name.lower()}'
        ORDER BY POSITION ASC
        """

    @staticmethod
    def get_table_lineage(schema: str, table_name: str) -> str:
        """Get lineage information for a table or view."""
        return f"""
        SELECT LOWER(BASE_SCHEMA_NAME) AS BASE_SCHEMA_NAME,
               LOWER(BASE_OBJECT_NAME) AS BASE_OBJECT_NAME
        FROM SYS.OBJECT_DEPENDENCIES
        WHERE LOWER(DEPENDENT_SCHEMA_NAME) = '{schema.lower()}'
          AND LOWER(DEPENDENT_OBJECT_NAME) = '{table_name.lower()}'
        """

    @staticmethod
    def get_query_logs() -> str:
        """Get query execution logs."""
        return """
        SELECT STATEMENT_STRING,
               USER_NAME,
               LAST_EXECUTION_TIMESTAMP
        FROM SYS.M_SQL_PLAN_CACHE
        WHERE STATEMENT_STRING IS NOT NULL
        """
