import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Optional

import pandas as pd
from snowflake.connector import SnowflakeConnection

from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
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


@dataclass
class SnowflakeTag:
    database: str
    schema: str
    name: str
    value: str

    def display_name(self) -> str:
        return f"{self.name}: {self.value}"

    def identifier(self) -> str:
        return f"{self._id_prefix_as_str()}:{self.value}"

    def _id_prefix_as_str(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}"


@dataclass
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
    type: Optional[str] = None
    clustering_key: Optional[str] = None
    pk: Optional[SnowflakePK] = None
    columns: List[SnowflakeColumn] = field(default_factory=list)
    foreign_keys: List[SnowflakeFK] = field(default_factory=list)
    tags: Optional[List[SnowflakeTag]] = None
    column_tags: Dict[str, List[SnowflakeTag]] = field(default_factory=dict)
    sample_data: Optional[pd.DataFrame] = None


@dataclass
class SnowflakeView(BaseView):
    materialized: bool = False
    columns: List[SnowflakeColumn] = field(default_factory=list)
    tags: Optional[List[SnowflakeTag]] = None
    column_tags: Dict[str, List[SnowflakeTag]] = field(default_factory=dict)


@dataclass
class SnowflakeSchema:
    name: str
    created: Optional[datetime]
    last_altered: Optional[datetime]
    comment: Optional[str]
    tables: List[str] = field(default_factory=list)
    views: List[str] = field(default_factory=list)
    tags: Optional[List[SnowflakeTag]] = None


@dataclass
class SnowflakeDatabase:
    name: str
    created: Optional[datetime]
    comment: Optional[str]
    last_altered: Optional[datetime] = None
    schemas: List[SnowflakeSchema] = field(default_factory=list)
    tags: Optional[List[SnowflakeTag]] = None


class _SnowflakeTagCache:
    def __init__(self) -> None:
        # self._database_tags[<database_name>] = list of tags applied to database
        self._database_tags: Dict[str, List[SnowflakeTag]] = defaultdict(list)

        # self._schema_tags[<database_name>][<schema_name>] = list of tags applied to schema
        self._schema_tags: Dict[str, Dict[str, List[SnowflakeTag]]] = defaultdict(
            lambda: defaultdict(list)
        )

        # self._table_tags[<database_name>][<schema_name>][<table_name>] = list of tags applied to table
        self._table_tags: Dict[
            str, Dict[str, Dict[str, List[SnowflakeTag]]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        # self._column_tags[<database_name>][<schema_name>][<table_name>][<column_name>] = list of tags applied to column
        self._column_tags: Dict[
            str, Dict[str, Dict[str, Dict[str, List[SnowflakeTag]]]]
        ] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        )

    def add_database_tag(self, db_name: str, tag: SnowflakeTag) -> None:
        self._database_tags[db_name].append(tag)

    def get_database_tags(self, db_name: str) -> List[SnowflakeTag]:
        return self._database_tags[db_name]

    def add_schema_tag(self, schema_name: str, db_name: str, tag: SnowflakeTag) -> None:
        self._schema_tags[db_name][schema_name].append(tag)

    def get_schema_tags(self, schema_name: str, db_name: str) -> List[SnowflakeTag]:
        return self._schema_tags.get(db_name, {}).get(schema_name, [])

    def add_table_tag(
        self, table_name: str, schema_name: str, db_name: str, tag: SnowflakeTag
    ) -> None:
        self._table_tags[db_name][schema_name][table_name].append(tag)

    def get_table_tags(
        self, table_name: str, schema_name: str, db_name: str
    ) -> List[SnowflakeTag]:
        return self._table_tags[db_name][schema_name][table_name]

    def add_column_tag(
        self,
        column_name: str,
        table_name: str,
        schema_name: str,
        db_name: str,
        tag: SnowflakeTag,
    ) -> None:
        self._column_tags[db_name][schema_name][table_name][column_name].append(tag)

    def get_column_tags_for_table(
        self, table_name: str, schema_name: str, db_name: str
    ) -> Dict[str, List[SnowflakeTag]]:
        return (
            self._column_tags.get(db_name, {}).get(schema_name, {}).get(table_name, {})
        )


class SnowflakeDataDictionary(SnowflakeQueryMixin):
    def __init__(self) -> None:
        self.logger = logger
        self.connection: Optional[SnowflakeConnection] = None

    def set_connection(self, connection: SnowflakeConnection) -> None:
        self.connection = connection

    def get_connection(self) -> SnowflakeConnection:
        # Connection is already present by the time this is called
        assert self.connection is not None
        return self.connection

    def show_databases(self) -> List[SnowflakeDatabase]:
        databases: List[SnowflakeDatabase] = []

        cur = self.query(
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

    def get_databases(self, db_name: str) -> List[SnowflakeDatabase]:
        databases: List[SnowflakeDatabase] = []

        cur = self.query(
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

    def get_schemas_for_database(self, db_name: str) -> List[SnowflakeSchema]:
        snowflake_schemas = []

        cur = self.query(
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

    @lru_cache(maxsize=1)
    def get_tables_for_database(
        self, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeTable]]]:
        tables: Dict[str, List[SnowflakeTable]] = {}
        try:
            cur = self.query(
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
                    type=table["TABLE_TYPE"],
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
        self, schema_name: str, db_name: str
    ) -> List[SnowflakeTable]:
        tables: List[SnowflakeTable] = []

        cur = self.query(
            SnowflakeQuery.tables_for_schema(schema_name, db_name),
        )

        for table in cur:
            tables.append(
                SnowflakeTable(
                    name=table["TABLE_NAME"],
                    type=table["TABLE_TYPE"],
                    created=table["CREATED"],
                    last_altered=table["LAST_ALTERED"],
                    size_in_bytes=table["BYTES"],
                    rows_count=table["ROW_COUNT"],
                    comment=table["COMMENT"],
                    clustering_key=table["CLUSTERING_KEY"],
                )
            )
        return tables

    @lru_cache(maxsize=1)
    def get_views_for_database(
        self, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeView]]]:
        views: Dict[str, List[SnowflakeView]] = {}
        try:
            cur = self.query(SnowflakeQuery.show_views_for_database(db_name))
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
                    materialized=table.get("is_materialized", "false").lower()
                    == "true",
                )
            )
        return views

    def get_views_for_schema(
        self, schema_name: str, db_name: str
    ) -> List[SnowflakeView]:
        views: List[SnowflakeView] = []

        cur = self.query(SnowflakeQuery.show_views_for_schema(schema_name, db_name))
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

    @lru_cache(maxsize=1)
    def get_columns_for_schema(
        self, schema_name: str, db_name: str
    ) -> Optional[Dict[str, List[SnowflakeColumn]]]:
        columns: Dict[str, List[SnowflakeColumn]] = {}
        try:
            cur = self.query(SnowflakeQuery.columns_for_schema(schema_name, db_name))
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
        self, table_name: str, schema_name: str, db_name: str
    ) -> List[SnowflakeColumn]:
        columns: List[SnowflakeColumn] = []

        cur = self.query(
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

    @lru_cache(maxsize=1)
    def get_pk_constraints_for_schema(
        self, schema_name: str, db_name: str
    ) -> Dict[str, SnowflakePK]:
        constraints: Dict[str, SnowflakePK] = {}
        cur = self.query(
            SnowflakeQuery.show_primary_keys_for_schema(schema_name, db_name),
        )

        for row in cur:
            if row["table_name"] not in constraints:
                constraints[row["table_name"]] = SnowflakePK(
                    name=row["constraint_name"], column_names=[]
                )
            constraints[row["table_name"]].column_names.append(row["column_name"])
        return constraints

    @lru_cache(maxsize=1)
    def get_fk_constraints_for_schema(
        self, schema_name: str, db_name: str
    ) -> Dict[str, List[SnowflakeFK]]:
        constraints: Dict[str, List[SnowflakeFK]] = {}
        fk_constraints_map: Dict[str, SnowflakeFK] = {}

        cur = self.query(
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

    def get_tags_for_database_without_propagation(
        self,
        db_name: str,
    ) -> _SnowflakeTagCache:
        cur = self.query(
            SnowflakeQuery.get_all_tags_in_database_without_propagation(db_name)
        )

        tags = _SnowflakeTagCache()

        for tag in cur:
            snowflake_tag = SnowflakeTag(
                database=tag["TAG_DATABASE"],
                schema=tag["TAG_SCHEMA"],
                name=tag["TAG_NAME"],
                value=tag["TAG_VALUE"],
            )

            # This is the name of the object, unless the object is a column, in which
            # case the name is in the `COLUMN_NAME` field.
            object_name = tag["OBJECT_NAME"]
            # This will be null if the object is a database or schema
            object_schema = tag["OBJECT_SCHEMA"]
            # This will be null if the object is a database
            object_database = tag["OBJECT_DATABASE"]

            domain = tag["DOMAIN"].lower()
            if domain == SnowflakeObjectDomain.DATABASE:
                tags.add_database_tag(object_name, snowflake_tag)
            elif domain == SnowflakeObjectDomain.SCHEMA:
                tags.add_schema_tag(object_name, object_database, snowflake_tag)
            elif domain == SnowflakeObjectDomain.TABLE:  # including views
                tags.add_table_tag(
                    object_name, object_schema, object_database, snowflake_tag
                )
            elif domain == SnowflakeObjectDomain.COLUMN:
                column_name = tag["COLUMN_NAME"]
                tags.add_column_tag(
                    column_name,
                    object_name,
                    object_schema,
                    object_database,
                    snowflake_tag,
                )
            else:
                # This should never happen.
                self.logger.error(f"Encountered an unexpected domain: {domain}")
                continue

        return tags

    def get_tags_for_object_with_propagation(
        self,
        domain: str,
        quoted_identifier: str,
        db_name: str,
    ) -> List[SnowflakeTag]:
        tags: List[SnowflakeTag] = []

        cur = self.query(
            SnowflakeQuery.get_all_tags_on_object_with_propagation(
                db_name, quoted_identifier, domain
            ),
        )

        for tag in cur:
            tags.append(
                SnowflakeTag(
                    database=tag["TAG_DATABASE"],
                    schema=tag["TAG_SCHEMA"],
                    name=tag["TAG_NAME"],
                    value=tag["TAG_VALUE"],
                )
            )
        return tags

    def get_tags_on_columns_for_table(
        self, quoted_table_name: str, db_name: str
    ) -> Dict[str, List[SnowflakeTag]]:
        tags: Dict[str, List[SnowflakeTag]] = defaultdict(list)
        cur = self.query(
            SnowflakeQuery.get_tags_on_columns_with_propagation(
                db_name, quoted_table_name
            ),
        )

        for tag in cur:
            column_name = tag["COLUMN_NAME"]
            snowflake_tag = SnowflakeTag(
                database=tag["TAG_DATABASE"],
                schema=tag["TAG_SCHEMA"],
                name=tag["TAG_NAME"],
                value=tag["TAG_VALUE"],
            )
            tags[column_name].append(snowflake_tag)

        return tags
