"""
SAP HANA Data Dictionary for metadata extraction.

This module provides a data dictionary interface for extracting metadata
from SAP HANA databases, including tables, views, calculation views, and schemas.
"""

import logging
from typing import Dict, List

from datahub.ingestion.source.sql.hana.hana_query import HanaQuery
from datahub.ingestion.source.sql.hana.hana_schema import (
    HanaCalculationView,
    HanaColumn,
    HanaDatabase,
    HanaSchema,
    HanaTable,
    HanaView,
)

logger = logging.getLogger(__name__)


class HanaDataDictionary:
    """Data dictionary for SAP HANA metadata extraction."""

    def __init__(self, connection, report):
        """Initialize the data dictionary.

        Args:
            connection: SQLAlchemy connection to HANA
            report: Report object for tracking statistics
        """
        self.connection = connection
        self.report = report

    def get_databases(self) -> List[HanaDatabase]:
        """Get list of databases."""
        databases = []

        try:
            query = HanaQuery.get_databases()
            result = self.connection.execute(query).fetchall()

            for row in result:
                database = HanaDatabase(
                    name=row["DATABASE_NAME"],
                    created=row.get("CREATED"),
                    comment=row.get("COMMENT"),
                    last_altered=row.get("LAST_ALTERED"),
                )
                databases.append(database)

        except Exception as e:
            logger.error(f"Error fetching databases: {e}")
            self.report.report_failure("get_databases", str(e))

        return databases

    def get_schemas_for_database(self, db_name: str) -> List[HanaSchema]:
        """Get schemas for a specific database."""
        schemas = []

        try:
            query = HanaQuery.schemas_for_database(db_name)
            result = self.connection.execute(query).fetchall()

            for row in result:
                schema = HanaSchema(
                    name=row["SCHEMA_NAME"],
                    created=row.get("CREATED"),
                    comment=row.get("COMMENT"),
                    last_altered=row.get("LAST_ALTERED"),
                )
                schemas.append(schema)

        except Exception as e:
            logger.error(f"Error fetching schemas for database {db_name}: {e}")
            self.report.report_failure("get_schemas", str(e))

        return schemas

    def get_tables_for_schema(self, schema_name: str, db_name: str) -> List[HanaTable]:
        """Get tables for a specific schema."""
        tables = []

        try:
            query = HanaQuery.tables_for_schema(schema_name, db_name)
            result = self.connection.execute(query).fetchall()

            for row in result:
                table = HanaTable(
                    name=row["TABLE_NAME"],
                    schema_name=schema_name,
                    type=row.get("TABLE_TYPE"),
                    comment=row.get("COMMENT"),
                    created=row.get("CREATED"),
                    last_altered=row.get("LAST_ALTERED"),
                    rows_count=row.get("ROW_COUNT"),
                    size_in_bytes=row.get("BYTES"),
                )
                tables.append(table)

        except Exception as e:
            logger.error(f"Error fetching tables for schema {schema_name}: {e}")
            self.report.report_failure("get_tables", str(e))

        return tables

    def get_views_for_schema(self, schema_name: str, db_name: str) -> List[HanaView]:
        """Get views for a specific schema."""
        views = []

        try:
            query = HanaQuery.views_for_schema(schema_name, db_name)
            result = self.connection.execute(query).fetchall()

            for row in result:
                view = HanaView(
                    name=row["VIEW_NAME"],
                    schema_name=schema_name,
                    comment=row.get("COMMENT"),
                    created=row.get("CREATED"),
                    last_altered=row.get("LAST_ALTERED"),
                    view_definition=row.get("DEFINITION"),
                    view_type=row.get("VIEW_TYPE"),
                )
                views.append(view)

        except Exception as e:
            logger.error(f"Error fetching views for schema {schema_name}: {e}")
            self.report.report_failure("get_views", str(e))

        return views

    def get_calculation_views(self) -> List[HanaCalculationView]:
        """Get calculation views from _SYS_REPO.ACTIVE_OBJECT."""
        calc_views = []

        try:
            query = HanaQuery.get_calculation_views()
            result = self.connection.execute(query).fetchall()

            for row in result:
                calc_view = HanaCalculationView(
                    name=row["OBJECT_NAME"],
                    package_id=row["PACKAGE_ID"],
                    definition=row["CDATA"],
                )
                calc_views.append(calc_view)

        except Exception as e:
            logger.error(f"Error fetching calculation views: {e}")
            self.report.report_failure("get_calculation_views", str(e))

        return calc_views

    def get_columns_for_table(
        self, table_name: str, schema_name: str, db_name: str
    ) -> List[HanaColumn]:
        """Get columns for a specific table or view."""
        columns = []

        try:
            query = HanaQuery.columns_for_table(table_name, schema_name, db_name)
            result = self.connection.execute(query).fetchall()

            for row in result:
                column = HanaColumn(
                    name=row["COLUMN_NAME"],
                    data_type=row["DATA_TYPE_NAME"],
                    comment=row.get("COMMENTS"),
                    nullable=bool(row.get("IS_NULLABLE", "YES") == "YES"),
                    ordinal_position=row.get("POSITION"),
                    character_maximum_length=row.get("LENGTH"),
                    numeric_precision=row.get("SCALE"),
                    numeric_scale=row.get("SCALE"),
                )
                columns.append(column)

        except Exception as e:
            logger.error(f"Error fetching columns for {schema_name}.{table_name}: {e}")
            self.report.report_failure("get_columns", str(e))

        return columns

    def get_columns_for_calculation_view(self, view_name: str) -> List[HanaColumn]:
        """Get columns for a calculation view."""
        columns = []

        try:
            query = HanaQuery.columns_for_calculation_view(view_name)
            result = self.connection.execute(query).fetchall()

            for row in result:
                column = HanaColumn(
                    name=row["COLUMN_NAME"],
                    data_type=row["DATA_TYPE_NAME"],
                    comment=row.get("COMMENTS"),
                    nullable=bool(row.get("IS_NULLABLE", "YES") == "YES"),
                    ordinal_position=row.get("POSITION"),
                )
                columns.append(column)

        except Exception as e:
            logger.error(
                f"Error fetching columns for calculation view {view_name}: {e}"
            )
            self.report.report_failure("get_calculation_view_columns", str(e))

        return columns

    def get_table_lineage(self, schema: str, table_name: str) -> List[Dict[str, str]]:
        """Get lineage information for a table or view."""
        lineage = []

        try:
            query = HanaQuery.get_table_lineage(schema, table_name)
            result = self.connection.execute(query).fetchall()

            for row in result:
                lineage.append(
                    {
                        "base_schema": row["BASE_SCHEMA_NAME"],
                        "base_object": row["BASE_OBJECT_NAME"],
                    }
                )

        except Exception as e:
            logger.error(f"Error fetching lineage for {schema}.{table_name}: {e}")
            self.report.report_failure("get_lineage", str(e))

        return lineage
