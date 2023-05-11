# Supported types are available at
# https://api-docs.databricks.com/rest/latest/unity-catalog-api-specification-2-1.html?_ga=2.151019001.1795147704.1666247755-2119235717.1666247755
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from databricks.sdk.service.sql import QueryStatementType

from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OperationTypeClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

DATA_TYPE_REGISTRY: dict = {
    "BOOLEAN": BooleanTypeClass,
    "BYTE": BytesTypeClass,
    "DATE": DateTypeClass,
    "SHORT": NumberTypeClass,
    "INT": NumberTypeClass,
    "LONG": NumberTypeClass,
    "FLOAT": NumberTypeClass,
    "DOUBLE": NumberTypeClass,
    "TIMESTAMP": TimeTypeClass,
    "STRING": StringTypeClass,
    "BINARY": BytesTypeClass,
    "DECIMAL": NumberTypeClass,
    "INTERVAL": TimeTypeClass,
    "ARRAY": ArrayTypeClass,
    "STRUCT": RecordTypeClass,
    "MAP": MapTypeClass,
    "CHAR": StringTypeClass,
    "NULL": NullTypeClass,
}


# Does not parse other statement types, besides SELECT
OPERATION_STATEMENT_TYPES = {
    QueryStatementType.INSERT: OperationTypeClass.INSERT,
    QueryStatementType.COPY: OperationTypeClass.INSERT,
    QueryStatementType.UPDATE: OperationTypeClass.UPDATE,
    QueryStatementType.MERGE: OperationTypeClass.UPDATE,
    QueryStatementType.DELETE: OperationTypeClass.DELETE,
    QueryStatementType.TRUNCATE: OperationTypeClass.DELETE,
    QueryStatementType.CREATE: OperationTypeClass.CREATE,
    QueryStatementType.REPLACE: OperationTypeClass.CREATE,
    QueryStatementType.ALTER: OperationTypeClass.ALTER,
    QueryStatementType.DROP: OperationTypeClass.DROP,
    QueryStatementType.OTHER: OperationTypeClass.UNKNOWN,
}
ALLOWED_STATEMENT_TYPES = {*OPERATION_STATEMENT_TYPES.keys(), QueryStatementType.SELECT}


@dataclass
class CommonProperty:
    id: str
    name: str
    type: str
    comment: Optional[str]


@dataclass
class Metastore(CommonProperty):
    metastore_id: str
    owner: Optional[str]


@dataclass
class Catalog(CommonProperty):
    metastore: Metastore
    owner: Optional[str]


@dataclass
class Schema(CommonProperty):
    catalog: Catalog
    owner: Optional[str]


@dataclass
class Column(CommonProperty):
    type_text: str
    type_name: SchemaFieldDataTypeClass
    type_precision: int
    type_scale: int
    position: int
    nullable: bool
    comment: Optional[str]


@dataclass
class ColumnLineage:
    source: str
    destination: str


@dataclass
class ServicePrincipal:
    id: str
    application_id: str  # uuid used to reference the service principal
    display_name: str
    active: Optional[bool]


@dataclass(frozen=True, order=True)
class TableReference:
    metastore_id: str
    catalog: str
    schema: str
    table: str

    @classmethod
    def create(cls, table: "Table") -> "TableReference":
        return cls(
            table.schema.catalog.metastore.id,
            table.schema.catalog.name,
            table.schema.name,
            table.name,
        )

    def __str__(self) -> str:
        return f"{self.metastore_id}.{self.catalog}.{self.schema}.{self.table}"

    @property
    def qualified_table_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass
class Table(CommonProperty):
    schema: Schema
    columns: List[Column]
    storage_location: Optional[str]
    data_source_format: Optional[str]
    comment: Optional[str]
    table_type: str
    owner: Optional[str]
    generation: int
    created_at: datetime
    created_by: str
    updated_at: Optional[datetime]
    updated_by: Optional[str]
    table_id: str
    view_definition: Optional[str]
    properties: Dict[str, str]
    upstreams: Dict[TableReference, Dict[str, List[str]]] = field(default_factory=dict)

    ref: TableReference = field(init=False)
    # lineage: Optional[Lineage]

    def __post_init__(self):
        self.ref = TableReference.create(self)


@dataclass
class Query:
    query_id: str
    query_text: str
    statement_type: QueryStatementType
    start_time: datetime
    end_time: datetime
    # User who ran the query
    user_id: int
    user_name: str  # Email or username
    # User whose credentials were used to run the query
    executed_as_user_id: int
    executed_as_user_name: str
