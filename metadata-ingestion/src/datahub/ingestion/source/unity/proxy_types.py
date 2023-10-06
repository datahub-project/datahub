# Supported types are available at
# https://api-docs.databricks.com/rest/latest/unity-catalog-api-specification-2-1.html?_ga=2.151019001.1795147704.1666247755-2119235717.1666247755
import dataclasses
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, FrozenSet, List, Optional, Set

from databricks.sdk.service.catalog import (
    CatalogType,
    ColumnTypeName,
    DataSourceFormat,
    TableType,
)
from databricks.sdk.service.sql import QueryStatementType
from databricks.sdk.service.workspace import Language

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
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)

DATA_TYPE_REGISTRY: dict = {
    ColumnTypeName.BOOLEAN: BooleanTypeClass,
    ColumnTypeName.BYTE: BytesTypeClass,
    ColumnTypeName.DATE: DateTypeClass,
    ColumnTypeName.SHORT: NumberTypeClass,
    ColumnTypeName.INT: NumberTypeClass,
    ColumnTypeName.LONG: NumberTypeClass,
    ColumnTypeName.FLOAT: NumberTypeClass,
    ColumnTypeName.DOUBLE: NumberTypeClass,
    ColumnTypeName.TIMESTAMP: TimeTypeClass,
    ColumnTypeName.TIMESTAMP_NTZ: TimeTypeClass,
    ColumnTypeName.STRING: StringTypeClass,
    ColumnTypeName.BINARY: BytesTypeClass,
    ColumnTypeName.DECIMAL: NumberTypeClass,
    ColumnTypeName.INTERVAL: TimeTypeClass,
    ColumnTypeName.ARRAY: ArrayTypeClass,
    ColumnTypeName.STRUCT: RecordTypeClass,
    ColumnTypeName.MAP: MapTypeClass,
    ColumnTypeName.CHAR: StringTypeClass,
    ColumnTypeName.NULL: NullTypeClass,
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


NotebookId = int


@dataclass
class CommonProperty:
    id: str
    name: str
    comment: Optional[str]


@dataclass
class Metastore(CommonProperty):
    global_metastore_id: str  # Global across clouds and regions
    metastore_id: str
    owner: Optional[str]
    cloud: Optional[str]
    region: Optional[str]


@dataclass
class Catalog(CommonProperty):
    metastore: Optional[Metastore]
    owner: Optional[str]
    type: CatalogType


@dataclass
class Schema(CommonProperty):
    catalog: Catalog
    owner: Optional[str]


@dataclass
class Column(CommonProperty):
    type_text: str
    type_name: ColumnTypeName
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
    metastore: Optional[str]
    catalog: str
    schema: str
    table: str

    @classmethod
    def create(cls, table: "Table") -> "TableReference":
        return cls(
            table.schema.catalog.metastore.id
            if table.schema.catalog.metastore
            else None,
            table.schema.catalog.name,
            table.schema.name,
            table.name,
        )

    @classmethod
    def create_from_lineage(
        cls, d: dict, metastore: Optional[Metastore]
    ) -> Optional["TableReference"]:
        try:
            return cls(
                metastore.id if metastore else None,
                d["catalog_name"],
                d["schema_name"],
                d.get("table_name", d["name"]),  # column vs table query output
            )
        except Exception as e:
            logger.warning(f"Failed to create TableReference from {d}: {e}")
            return None

    def __str__(self) -> str:
        if self.metastore:
            return f"{self.metastore}.{self.catalog}.{self.schema}.{self.table}"
        else:
            return self.qualified_table_name

    @property
    def qualified_table_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    @property
    def external_path(self) -> str:
        return f"{self.catalog}/{self.schema}/{self.table}"


@dataclass
class Table(CommonProperty):
    schema: Schema
    columns: List[Column]
    storage_location: Optional[str]
    data_source_format: Optional[DataSourceFormat]
    table_type: TableType
    owner: Optional[str]
    generation: Optional[int]
    created_at: datetime
    created_by: str
    updated_at: Optional[datetime]
    updated_by: Optional[str]
    table_id: str
    view_definition: Optional[str]
    properties: Dict[str, str]
    upstreams: Dict[TableReference, Dict[str, List[str]]] = field(default_factory=dict)
    upstream_notebooks: Set[NotebookId] = field(default_factory=set)
    downstream_notebooks: Set[NotebookId] = field(default_factory=set)

    ref: TableReference = field(init=False)

    def __post_init__(self):
        self.ref = TableReference.create(self)
        self.is_view = self.table_type in [TableType.VIEW, TableType.MATERIALIZED_VIEW]


@dataclass
class Query:
    query_id: str
    query_text: str
    statement_type: QueryStatementType
    start_time: datetime
    end_time: datetime
    # User who ran the query
    user_id: int
    user_name: Optional[str]  # Email or username
    # User whose credentials were used to run the query
    executed_as_user_id: int
    executed_as_user_name: Optional[str]


@dataclass
class TableProfile:
    num_rows: Optional[int]
    num_columns: Optional[int]
    total_size: Optional[int]
    column_profiles: List["ColumnProfile"]

    def __bool__(self):
        return any(
            (
                self.num_rows is not None,
                self.num_columns is not None,
                self.total_size is not None,
                any(self.column_profiles),
            )
        )


@dataclass
class ColumnProfile:
    name: str
    null_count: Optional[int]
    distinct_count: Optional[int]
    min: Optional[str]
    max: Optional[str]

    version: Optional[str]
    avg_len: Optional[str]
    max_len: Optional[str]

    def __bool__(self):
        return any(
            (
                self.null_count is not None,
                self.distinct_count is not None,
                self.min is not None,
                self.max is not None,
            )
        )


@dataclass
class Notebook:
    id: NotebookId
    path: str
    language: Language
    created_at: datetime
    modified_at: datetime

    upstreams: FrozenSet[TableReference] = field(default_factory=frozenset)

    @classmethod
    def add_upstream(cls, upstream: TableReference, notebook: "Notebook") -> "Notebook":
        return cls(
            **{  # type: ignore
                **dataclasses.asdict(notebook),
                "upstreams": frozenset([*notebook.upstreams, upstream]),
            }
        )
