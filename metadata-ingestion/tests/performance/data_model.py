from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from typing_extensions import Literal

StatementType = Literal[  # SELECT + values from OperationTypeClass
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "ALTER",
    "DROP",
    "CUSTOM",
    "UNKNOWN",
]


@dataclass
class Container:
    name: str
    parent: Optional["Container"] = None


class ColumnType(str, Enum):
    # Can add types that take parameters in the future

    INTEGER = "INTEGER"
    FLOAT = "FLOAT"  # Double precision (64 bit)
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    DATETIME = "DATETIME"


@dataclass
class Column:
    name: str
    type: ColumnType
    nullable: bool


ColumnRef = str
ColumnMapping = Dict[ColumnRef, Column]


@dataclass
class Table:
    name: str
    container: Container
    columns: List[ColumnRef]
    column_mapping: Optional[ColumnMapping]

    def is_view(self) -> bool:
        return False


@dataclass
class View(Table):
    definition: str
    parents: List[Table]

    def is_view(self) -> bool:
        return True


@dataclass
class FieldAccess:
    column: ColumnRef
    table: Table


@dataclass
class Query:
    text: str
    type: StatementType
    actor: str
    timestamp: datetime
    fields_accessed: List[FieldAccess]  # Has at least one entry
    object_modified: Optional[Table] = None  # Can be only part of a table
