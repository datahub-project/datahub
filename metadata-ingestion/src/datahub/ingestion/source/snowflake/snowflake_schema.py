import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from snowflake.connector import SnowflakeConnection

from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeQueryMixin
from datahub.ingestion.source.sql.sql_generic import BaseColumn, BaseTable, BaseView

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class SnowflakePK:
    name: str
    column_names: List[str]


@dataclass
class SnowflakeFK:
    name: str
    column_names: List[str]
    referred_database: str
    referred_schema: str
    referred_table: str
    referred_column_names: List[str]


@dataclass(frozen=True, eq=True)
class SnowflakeColumn(BaseColumn):
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]

    def get_precise_native_type(self):
        precise_native_type = self.data_type
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html
        if (
            self.data_type in ("NUMBER", "NUMERIC", "DECIMAL")
            and self.numeric_precision is not None
            and self.numeric_scale is not None
        ):
            precise_native_type = (
                f"NUMBER({self.numeric_precision},{self.numeric_scale})"
            )
        # https://docs.snowflake.com/en/sql-reference/data-types-text.html
        elif (
            self.data_type in ("TEXT", "STRING", "VARCHAR")
            and self.character_maximum_length is not None
        ):
            precise_native_type = f"VARCHAR({self.character_maximum_length})"
        return precise_native_type


@dataclass
class SnowflakeTable(BaseTable):
    clustering_key: Optional[str] = None
    pk: Optional[SnowflakePK] = None
    columns: List[SnowflakeColumn] = field(default_factory=list)
    foreign_keys: List[SnowflakeFK] = field(default_factory=list)
    sample_data: Optional[pd.DataFrame] = None


@dataclass
class SnowflakeView(BaseView):
    columns: List[SnowflakeColumn] = field(default_factory=list)


@dataclass
class SnowflakeSchema:
    name: str
    created: Optional[datetime]
    last_altered: Optional[datetime]
    comment: Optional[str]
    tables: List[SnowflakeTable] = field(default_factory=list)
    views: List[SnowflakeView] = field(default_factory=list)


@dataclass
class SnowflakeDatabase:
    name: str
    created: Optional[datetime]
    comment: Optional[str]
    last_altered: Optional[datetime] = None
    schemas: List[SnowflakeSchema] = field(default_factory=list)


class SnowflakeDataDictionary(SnowflakeQueryMixin):
    def __init__(self) -> None:
        self.logger = logger

    def show_databases(self, conn: SnowflakeConnection) -> List[SnowflakeDatabase]:

        databases: List[SnowflakeDatabase] = []

        cur = self.query(
            conn,
            SnowflakeQuery.show_databases(),
        )

        for database in cur:
            snowflake_db = SnowflakeDatabase(
                name=database["name"],
                created=database["created_on"],
                comment=database["comment"],
            )
            databases.append(snowflake_db)

        return databases

    def get_databases(
        self, conn: SnowflakeConnection, db_name: str
    ) -> List[SnowflakeDatabase]:

        databases: List[SnowflakeDatabase] = []

        cur = self.query(
            conn,
            SnowflakeQuery.get_databases(db_name),
        )

        for database in cur:
            snowflake_db = SnowflakeDatabase(
                name=database["DATABASE_NAME"],
                created=database["CREATED"],
                last_altered=database["LAST_ALTERED"],
                comment=database["COMMENT"],
            )
            databases.append(snowflake_db)

        return databases

    def get_schemas_for_database(
        self, conn: SnowflakeConnection, db_name: str
    ) -> List[SnowflakeSchema]:

        snowflake_schemas = []

        cur = self.query(
            conn,
            SnowflakeQuery.schemas_for_database(db_name),
        )

        for schema in cur:
            snowflake_schema = SnowflakeSchema(
                name=schema["SCHEMA_NAME"],
                created=schema["CREATED"],
                last_altered=schema["LAST_ALTERED"],
                comment=schema["COMMENT"],
            )
            snowflake_schemas.append(snowflake_schema)
        return snowflake_schemas

    def get_tables_for_database(
        self, conn: SnowflakeConnection, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeTable]]]:
        tables: Dict[str, List[SnowflakeTable]] = {}
        try:
            cur = self.query(
                conn,
                SnowflakeQuery.tables_for_database(db_name),
            )
        except Exception as e:
            logger.debug(
                f"Failed to get all tables for database - {db_name}", exc_info=e
            )
            # Error - Information schema query returned too much data. Please repeat query with more selective predicates.
            return None

        for table in cur:
            if table["TABLE_SCHEMA"] not in tables:
                tables[table["TABLE_SCHEMA"]] = []
            tables[table["TABLE_SCHEMA"]].append(
                SnowflakeTable(
                    name=table["TABLE_NAME"],
                    created=table["CREATED"],
                    last_altered=table["LAST_ALTERED"],
                    size_in_bytes=table["BYTES"],
                    rows_count=table["ROW_COUNT"],
                    comment=table["COMMENT"],
                    clustering_key=table["CLUSTERING_KEY"],
                )
            )
        return tables

    def get_tables_for_schema(
        self, conn: SnowflakeConnection, schema_name: str, db_name: str
    ) -> List[SnowflakeTable]:
        tables: List[SnowflakeTable] = []

        cur = self.query(
            conn,
            SnowflakeQuery.tables_for_schema(schema_name, db_name),
        )

        for table in cur:
            tables.append(
                SnowflakeTable(
                    name=table["TABLE_NAME"],
                    created=table["CREATED"],
                    last_altered=table["LAST_ALTERED"],
                    size_in_bytes=table["BYTES"],
                    rows_count=table["ROW_COUNT"],
                    comment=table["COMMENT"],
                    clustering_key=table["CLUSTERING_KEY"],
                )
            )
        return tables

    def get_views_for_database(
        self, conn: SnowflakeConnection, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeView]]]:
        views: Dict[str, List[SnowflakeView]] = {}
        try:
            cur = self.query(conn, SnowflakeQuery.show_views_for_database(db_name))
        except Exception as e:
            logger.debug(
                f"Failed to get all views for database - {db_name}", exc_info=e
            )
            # Error - Information schema query returned too much data. Please repeat query with more selective predicates.
            return None

        for table in cur:
            if table["schema_name"] not in views:
                views[table["schema_name"]] = []
            views[table["schema_name"]].append(
                SnowflakeView(
                    name=table["name"],
                    created=table["created_on"],
                    # last_altered=table["last_altered"],
                    comment=table["comment"],
                    view_definition=table["text"],
                    last_altered=table["created_on"],
                )
            )
        return views

    def get_views_for_schema(
        self, conn: SnowflakeConnection, schema_name: str, db_name: str
    ) -> List[SnowflakeView]:
        views: List[SnowflakeView] = []

        cur = self.query(
            conn, SnowflakeQuery.show_views_for_schema(schema_name, db_name)
        )
        for table in cur:
            views.append(
                SnowflakeView(
                    name=table["name"],
                    created=table["created_on"],
                    # last_altered=table["last_altered"],
                    comment=table["comment"],
                    view_definition=table["text"],
                    last_altered=table["created_on"],
                )
            )
        return views

    def get_columns_for_schema(
        self, conn: SnowflakeConnection, schema_name: str, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeColumn]]]:
        columns: Dict[str, List[SnowflakeColumn]] = {}
        try:
            cur = self.query(
                conn, SnowflakeQuery.columns_for_schema(schema_name, db_name)
            )
        except Exception as e:
            logger.debug(
                f"Failed to get all columns for schema - {schema_name}", exc_info=e
            )
            # Error - Information schema query returned too much data.
            # Please repeat query with more selective predicates.
            return None

        for column in cur:
            if column["TABLE_NAME"] not in columns:
                columns[column["TABLE_NAME"]] = []
            columns[column["TABLE_NAME"]].append(
                SnowflakeColumn(
                    name=column["COLUMN_NAME"],
                    ordinal_position=column["ORDINAL_POSITION"],
                    is_nullable=column["IS_NULLABLE"] == "YES",
                    data_type=column["DATA_TYPE"],
                    comment=column["COMMENT"],
                    character_maximum_length=column["CHARACTER_MAXIMUM_LENGTH"],
                    numeric_precision=column["NUMERIC_PRECISION"],
                    numeric_scale=column["NUMERIC_SCALE"],
                )
            )
        return columns

    def get_columns_for_table(
        self, conn: SnowflakeConnection, table_name: str, schema_name: str, db_name: str
    ) -> List[SnowflakeColumn]:
        columns: List[SnowflakeColumn] = []

        cur = self.query(
            conn,
            SnowflakeQuery.columns_for_table(table_name, schema_name, db_name),
        )

        for column in cur:
            columns.append(
                SnowflakeColumn(
                    name=column["COLUMN_NAME"],
                    ordinal_position=column["ORDINAL_POSITION"],
                    is_nullable=column["IS_NULLABLE"] == "YES",
                    data_type=column["DATA_TYPE"],
                    comment=column["COMMENT"],
                    character_maximum_length=column["CHARACTER_MAXIMUM_LENGTH"],
                    numeric_precision=column["NUMERIC_PRECISION"],
                    numeric_scale=column["NUMERIC_SCALE"],
                )
            )
        return columns

    def get_pk_constraints_for_schema(
        self, conn: SnowflakeConnection, schema_name: str, db_name: str
    ) -> Dict[str, SnowflakePK]:
        constraints: Dict[str, SnowflakePK] = {}
        cur = self.query(
            conn,
            SnowflakeQuery.show_primary_keys_for_schema(schema_name, db_name),
        )

        for row in cur:
            if row["table_name"] not in constraints:
                constraints[row["table_name"]] = SnowflakePK(
                    name=row["constraint_name"], column_names=[]
                )
            constraints[row["table_name"]].column_names.append(row["column_name"])
        return constraints

    def get_fk_constraints_for_schema(
        self, conn: SnowflakeConnection, schema_name: str, db_name: str
    ) -> Dict[str, List[SnowflakeFK]]:
        constraints: Dict[str, List[SnowflakeFK]] = {}
        fk_constraints_map: Dict[str, SnowflakeFK] = {}

        cur = self.query(
            conn,
            SnowflakeQuery.show_foreign_keys_for_schema(schema_name, db_name),
        )

        for row in cur:
            if row["fk_name"] not in constraints:
                fk_constraints_map[row["fk_name"]] = SnowflakeFK(
                    name=row["fk_name"],
                    column_names=[],
                    referred_database=row["pk_database_name"],
                    referred_schema=row["pk_schema_name"],
                    referred_table=row["pk_table_name"],
                    referred_column_names=[],
                )

            if row["fk_table_name"] not in constraints:
                constraints[row["fk_table_name"]] = []

            fk_constraints_map[row["fk_name"]].column_names.append(
                row["fk_column_name"]
            )
            fk_constraints_map[row["fk_name"]].referred_column_names.append(
                row["pk_column_name"]
            )
            constraints[row["fk_table_name"]].append(fk_constraints_map[row["fk_name"]])

        return constraints
