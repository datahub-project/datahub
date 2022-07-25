import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from snowflake.connector import DictCursor, SnowflakeConnection

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
    last_altered: datetime
    comment: str
    view_definition: str
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


class SnowflakeQuery:
    @staticmethod
    def show_databases() -> str:
        return "show databases"

    @staticmethod
    def use_database(db_name: str) -> str:
        return f'use database "{db_name}"'

    @staticmethod
    def schemas_for_database(db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT schema_name as "schema_name",
        created as "created",
        last_altered as "last_altered",
        comment as "comment"
        from {db_clause}information_schema.schemata
        WHERE schema_name != 'INFORMATION_SCHEMA'
        order by schema_name"""

    @staticmethod
    def tables_for_database(db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog as "table_catalog",
        table_schema as "table_schema",
        table_name as "table_name",
        table_type as "table_type",
        created as "created",
        last_altered as "last_altered" ,
        comment as "comment",
        row_count as "row_count",
        bytes as "bytes",
        clustering_key as "clustering_key",
        auto_clustering_on as "auto_clustering_on"
        FROM {db_clause}information_schema.tables t
        WHERE table_schema != 'INFORMATION_SCHEMA'
        and table_type in ( 'BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    @staticmethod
    def tables_for_schema(schema_name: str, db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog as "table_catalog",
        table_schema as "table_schema",
        table_name as "table_name",
        table_type as "table_type",
        created as "created",
        last_altered as "last_altered" ,
        comment as "comment",
        row_count as "row_count",
        bytes as "bytes",
        clustering_key as "clustering_key",
        auto_clustering_on as "auto_clustering_on"
        FROM {db_clause}information_schema.tables t
        where schema_name='{schema_name}'
        and table_type in ('BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    @staticmethod
    def views_for_database(db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog as "table_catalog",
        table_schema as "table_schema",
        table_name as "table_name",
        created as "created",
        last_altered as "last_altered",
        comment as "comment",
        view_definition as "view_definition"
        FROM {db_clause}information_schema.views t
        WHERE table_schema != 'INFORMATION_SCHEMA'
        order by table_schema, table_name"""

    @staticmethod
    def views_for_schema(schema_name: str, db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        SELECT table_catalog as "table_catalog",
        table_schema as "table_schema",
        table_name as "table_name",
        created as "created",
        last_altered as "last_altered",
        comment as "comment",
        view_definition as "view_definition"
        FROM {db_clause}information_schema.views t
        where schema_name='{schema_name}'
        order by table_schema, table_name"""

    @staticmethod
    def columns_for_schema(schema_name: str, db_name: Optional[str]) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        select
        table_catalog as "table_catalog",
        table_schema as "table_schema",
        table_name as "table_name",
        column_name as "column_name",
        ordinal_position as "ordinal_position",
        is_nullable as "is_nullable",
        data_type as "data_type",
        comment as "comment",
        character_maximum_length as "character_maximum_length",
        numeric_precision as "numeric_precision",
        numeric_scale as "numeric_scale",
        column_default as "column_default",
        is_identity as "is_identity"
        from {db_clause}information_schema.columns
        WHERE table_schema='{schema_name}'
        ORDER BY ordinal_position"""

    @staticmethod
    def columns_for_table(
        table_name: str, schema_name: str, db_name: Optional[str]
    ) -> str:
        db_clause = f'"{db_name}".' if db_name is not None else ""
        return f"""
        select
        table_catalog as "table_catalog",
        table_schema as "table_schema",
        table_name as "table_name",
        column_name as "column_name",
        ordinal_position as "ordinal_position",
        is_nullable as "is_nullable",
        data_type as "data_type",
        comment as "comment",
        character_maximum_length as "character_maximum_length",
        numeric_precision as "numeric_precision",
        numeric_scale as "numeric_scale",
        column_default as "column_default",
        is_identity as "is_identity"
        from {db_clause}information_schema.columns
        WHERE table_schema='{schema_name}' and table_name='{table_name}'
        ORDER BY ordinal_position"""

    @staticmethod
    def show_primary_keys_for_schema(schema_name: str, db_name: str) -> str:
        return f"""
        show primary keys in schema "{db_name}"."{schema_name}" """

    @staticmethod
    def show_foreign_keys_for_schema(schema_name: str, db_name: str) -> str:
        return f"""
        show imported keys in schema "{db_name}"."{schema_name}" """


class SnowflakeDataDictionary:
    def query(self, conn, query):
        logger.debug("Query : {}".format(query))
        resp = conn.cursor(DictCursor).execute(query)
        return resp

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
            cur = self.query(conn, SnowflakeQuery.views_for_database(db_name))
        except Exception as e:
            logger.debug(e)
            # Error - Information schema query returned too much data. Please repeat query with more selective predicates.
            return None

        for table in cur:
            if table["table_schema"] not in views:
                views[table["table_schema"]] = []
            views[table["table_schema"]].append(
                SnowflakeView(
                    name=table["table_name"],
                    created=table["created"],
                    last_altered=table["last_altered"],
                    comment=table["comment"],
                    view_definition=table["view_definition"],
                )
            )
        return views

    def get_views_for_schema(
        self, conn: SnowflakeConnection, schema_name: str, db_name: str
    ) -> List[SnowflakeView]:
        views: List[SnowflakeView] = []

        cur = self.query(conn, SnowflakeQuery.views_for_schema(schema_name, db_name))
        for table in cur:
            views.append(
                SnowflakeView(
                    name=table["table_name"],
                    created=table["created"],
                    last_altered=table["last_altered"],
                    comment=table["comment"],
                    view_definition=table["view_definition"],
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
