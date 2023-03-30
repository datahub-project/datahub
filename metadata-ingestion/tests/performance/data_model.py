from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from typing_extensions import Literal

Column = str
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


@dataclass
class Table:
    name: str
    container: Container
    columns: List[Column]

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
    column: Column
    table: Table


@dataclass
class Query:
    text: str
    type: StatementType
    actor: str
    timestamp: datetime
    fields_accessed: List[FieldAccess]  # Has at least one entry
    object_modified: Optional[Table] = None  # Can be only part of a table
