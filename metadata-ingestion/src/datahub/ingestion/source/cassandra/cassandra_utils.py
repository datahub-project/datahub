import logging
from typing import Dict, Generator, List, Optional, Type

from datahub.ingestion.source.cassandra.cassandra_api import CassandraColumn
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


# This class helps convert cassandra column types to SchemaFieldDataType for use by the datahaub metadata schema
class CassandraToSchemaFieldConverter:
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

    def _get_schema_fields(
        self, cassandra_column_infos: List[CassandraColumn]
    ) -> Generator[SchemaField, None, None]:
        # append each schema field (sort so output is consistent)
        for column_info in cassandra_column_infos:
            column_name: str = column_info.column_name
            cassandra_type: str = column_info.type

            schema_field_data_type: SchemaFieldDataType = self.get_column_type(
                cassandra_type
            )
            schema_field: SchemaField = SchemaField(
                fieldPath=column_name,
                nativeDataType=cassandra_type,
                type=schema_field_data_type,
                description=None,
                nullable=True,
                recursive=False,
            )
            yield schema_field

    @classmethod
    def get_schema_fields(
        cls, cassandra_column_infos: List[CassandraColumn]
    ) -> Generator[SchemaField, None, None]:
        converter = cls()
        yield from converter._get_schema_fields(cassandra_column_infos)
