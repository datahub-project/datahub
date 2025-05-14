import logging
from dataclasses import dataclass, field
from typing import Dict, Generator, List, Optional, Type

from datahub.ingestion.source.cassandra.cassandra_api import CassandraColumn
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
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
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict

logger = logging.getLogger(__name__)


# we always skip over ingesting metadata about these keyspaces
SYSTEM_KEYSPACE_LIST = set(
    ["system", "system_auth", "system_schema", "system_distributed", "system_traces"]
)


@dataclass
class CassandraSourceReport(StaleEntityRemovalSourceReport, IngestionStageReport):
    num_tables_failed: int = 0
    num_views_failed: int = 0
    tables_scanned: int = 0
    views_scanned: int = 0
    entities_profiled: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_entity_scanned(self, name: str, ent_type: str = "View") -> None:
        """
        Entity could be a view or a table
        """
        if ent_type == "Table":
            self.tables_scanned += 1
        elif ent_type == "View":
            self.views_scanned += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")

    # TODO Need to create seperate common config for profiling report
    profiling_skipped_other: TopKDict[str, int] = field(default_factory=int_top_k_dict)
    profiling_skipped_table_profile_pattern: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    def report_entity_profiled(self, name: str) -> None:
        self.entities_profiled += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)


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
        type_class: Optional[Type] = (
            CassandraToSchemaFieldConverter._field_type_to_schema_field_type.get(
                cassandra_column_type
            )
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
