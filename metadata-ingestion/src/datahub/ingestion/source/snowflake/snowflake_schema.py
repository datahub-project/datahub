import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from snowflake.connector import SnowflakeConnection

from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeQueryMixin

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


@dataclass
class SnowflakeColumn:
    name: str
    ordinal_position: int
    is_nullable: bool
    data_type: str
    comment: str


@dataclass
class SnowflakeTable:
    name: str
    created: datetime
    last_altered: datetime
    size_in_bytes: int
    rows_count: int
    comment: str
    clustering_key: str
    pk: Optional[SnowflakePK] = None
    columns: List[SnowflakeColumn] = field(default_factory=list)
    foreign_keys: List[SnowflakeFK] = field(default_factory=list)


@dataclass
class SnowflakeView:
    name: str
    created: datetime
    comment: Optional[str]
    view_definition: str
    last_altered: Optional[datetime] = None
    columns: List[SnowflakeColumn] = field(default_factory=list)


@dataclass
class SnowflakeSchema:
    name: str
    created: datetime
    last_altered: datetime
    comment: str
    tables: List[SnowflakeTable] = field(default_factory=list)
    views: List[SnowflakeView] = field(default_factory=list)


@dataclass
class SnowflakeDatabase:
    name: str
    created: datetime
    comment: str
    schemas: List[SnowflakeSchema] = field(default_factory=list)


class SnowflakeDataDictionary(SnowflakeQueryMixin):
    def __init__(self) -> None:
        self.logger = logger

    def get_databases(self, conn: SnowflakeConnection) -> List[SnowflakeDatabase]:

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
                name=schema["schema_name"],
                created=schema["created"],
                last_altered=schema["last_altered"],
                comment=schema["comment"],
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
            logger.debug(e)
            # Error - Information schema query returned too much data. Please repeat query with more selective predicates.
            return None

        for table in cur:
            if table["table_schema"] not in tables:
                tables[table["table_schema"]] = []
            tables[table["table_schema"]].append(
                SnowflakeTable(
                    name=table["table_name"],
                    created=table["created"],
                    last_altered=table["last_altered"],
                    size_in_bytes=table["bytes"],
                    rows_count=table["row_count"],
                    comment=table["comment"],
                    clustering_key=table["clustering_key"],
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
                    name=table["table_name"],
                    created=table["created"],
                    last_altered=table["last_altered"],
                    size_in_bytes=table["bytes"],
                    rows_count=table["row_count"],
                    comment=table["comment"],
                    clustering_key=table["clustering_key"],
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
            logger.debug(e)
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
            logger.debug(e)
            # Error - Information schema query returned too much data.
            # Please repeat query with more selective predicates.
            return None

        for column in cur:
            if column["table_name"] not in columns:
                columns[column["table_name"]] = []
            columns[column["table_name"]].append(
                SnowflakeColumn(
                    name=column["column_name"],
                    ordinal_position=column["ordinal_position"],
                    is_nullable=column["is_nullable"] == "YES",
                    data_type=column["data_type"],
                    comment=column["comment"],
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
                    name=column["column_name"],
                    ordinal_position=column["ordinal_position"],
                    is_nullable=column["is_nullable"] == "YES",
                    data_type=column["data_type"],
                    comment=column["comment"],
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
