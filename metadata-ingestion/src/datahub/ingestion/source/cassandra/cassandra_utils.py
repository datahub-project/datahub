import json
import logging
from typing import Any, Dict, Generator, List, Optional, Type

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)


# we always skip over ingesting metadata about these keyspaces
SYSTEM_KEYSPACE_LIST = set(
    ["system", "system_auth", "system_schema", "system_distributed", "system_traces"]
)

# - Referencing https://docs.datastax.com/en/cql-oss/3.x/cql/cql_using/useQuerySystem.html#Table3.ColumnsinSystem_SchemaTables-Cassandra3.0 - #
# this keyspace contains details about the cassandra cluster's keyspaces, tables, and columns
SYSTEM_SCHEMA_KESPACE_NAME = "system_schema"
# these are the names of the tables we're interested in querying metadata from
CASSANDRA_SYSTEM_SCHEMA_TABLES = {
    "keyspaces": "keyspaces",
    "tables": "tables",
    "views": "views",
    "columns": "columns",
}
# these column names are present on the system_schema tables
CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES = {
    "keyspace_name": "keyspace_name",  # present on all tables
    "table_name": "table_name",  # present on tables table
    "column_name": "column_name",  # present on columns table
    "column_type": "type",  # present on columns table
    "view_name": "view_name",  # present on views table
    "base_table_name": "base_table_name",  # present on views table
    "where_clause": "where_clause",  # present on views table
}


class CassandraQueries:
    # get all keyspaces
    GET_KEYSPACES_QUERY = f"SELECT * FROM {SYSTEM_SCHEMA_KESPACE_NAME}.{CASSANDRA_SYSTEM_SCHEMA_TABLES['keyspaces']}"
    # get all tables for a keyspace
    GET_TABLES_QUERY = f"SELECT * FROM {SYSTEM_SCHEMA_KESPACE_NAME}.{CASSANDRA_SYSTEM_SCHEMA_TABLES['tables']} WHERE {CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES['keyspace_name']} = %s"
    # get all columns for a table
    GET_COLUMNS_QUERY = f"SELECT * FROM {SYSTEM_SCHEMA_KESPACE_NAME}.{CASSANDRA_SYSTEM_SCHEMA_TABLES['columns']} WHERE {CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES['keyspace_name']} = %s AND {CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES['table_name']} = %s"
    # get all views for a keyspace
    GET_VIEWS_QUERY = f"SELECT * FROM {SYSTEM_SCHEMA_KESPACE_NAME}.{CASSANDRA_SYSTEM_SCHEMA_TABLES['views']} WHERE {CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES['keyspace_name']} = %s"


# This class helps convert cassandra column types to SchemaFieldDataType for use by the datahaub metadata schema
class CassandraToSchemaFieldConverter:
    # FieldPath format version.
    version_string: str = "[version=2.0]"

    # Mapping from cassandra field types to SchemaFieldDataType.
    # https://cassandra.apache.org/doc/stable/cassandra/cql/types.html (version 4.1)
    _field_type_to_schema_field_type: Dict[str, Type] = {
        # Bool
        "boolean": BooleanTypeClass,
        # Binary
        "blob": BytesTypeClass,
        # Numbers
        "bigint": NumberTypeClass,
        "counter": NumberTypeClass,
        "decimal": NumberTypeClass,
        "double": NumberTypeClass,
        "float": NumberTypeClass,
        "int": NumberTypeClass,
        "smallint": NumberTypeClass,
        "tinyint": NumberTypeClass,
        "varint": NumberTypeClass,
        # Dates
        "date": DateTypeClass,
        # Times
        "duration": TimeTypeClass,
        "time": TimeTypeClass,
        "timestamp": TimeTypeClass,
        # Strings
        "text": StringTypeClass,
        "ascii": StringTypeClass,
        "inet": StringTypeClass,
        "timeuuid": StringTypeClass,
        "uuid": StringTypeClass,
        "varchar": StringTypeClass,
        # Records
        "geo_point": RecordTypeClass,
        # Arrays
        "histogram": ArrayTypeClass,
    }

    @staticmethod
    def get_column_type(cassandra_column_type: str) -> SchemaFieldDataType:
        type_class: Optional[
            Type
        ] = CassandraToSchemaFieldConverter._field_type_to_schema_field_type.get(
            cassandra_column_type
        )
        if type_class is None:
            logger.warning(
                f"Cannot map {cassandra_column_type!r} to SchemaFieldDataType, using NullTypeClass."
            )
            type_class = NullTypeClass

        return SchemaFieldDataType(type=type_class())

    def __init__(self) -> None:
        self._prefix_name_stack: List[str] = [self.version_string]

    def _get_cur_field_path(self) -> str:
        return ".".join(self._prefix_name_stack)

    def _get_schema_fields(
        self, cassandra_column_infos: List[dict[str, Any]]
    ) -> Generator[SchemaField, None, None]:
        # append each schema field (sort so output is consistent)
        for column_info in cassandra_column_infos:
            # convert namedtuple to dictionary if it isn't already one
            column_info = (
                column_info._asdict()
                if hasattr(column_info, "_asdict")
                else column_info
            )
            column_info["column_name_bytes"] = None
            column_name: str = column_info[
                CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["column_name"]
            ]
            cassandra_type: str = column_info[
                CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["column_type"]
            ]
            # if column_info.get("table_name") == "movie_reviews":
            #     breakpoint()
            if cassandra_type is not None:
                self._prefix_name_stack.append(f"[type={cassandra_type}].{column_name}")
                schema_field_data_type: SchemaFieldDataType = self.get_column_type(
                    cassandra_type
                )
                schema_field: SchemaField = SchemaField(
                    fieldPath=self._get_cur_field_path(),
                    nativeDataType=cassandra_type,
                    type=schema_field_data_type,
                    description=None,
                    nullable=True,
                    recursive=False,
                )
                yield schema_field
                self._prefix_name_stack.pop()
            else:
                # Unexpected! Log a warning.
                logger.warning(
                    f"Cassandra schema does not have 'type'!"
                    f" Schema={json.dumps(column_info)}"
                )
                continue

    @classmethod
    def get_schema_fields(
        cls, cassandra_column_infos: List[dict[str, Any]]
    ) -> Generator[SchemaField, None, None]:
        converter = cls()
        yield from converter._get_schema_fields(cassandra_column_infos)
