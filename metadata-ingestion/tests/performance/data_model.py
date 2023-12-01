import typing
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union

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
    type: ColumnType = ColumnType.STRING
    nullable: bool = False


ColumnRef = str
ColumnMapping = Dict[ColumnRef, Column]


@dataclass(init=False)
class Table:
    name: str
    container: Container
    columns: typing.OrderedDict[ColumnRef, Column] = field(repr=False)
    upstreams: List["Table"] = field(repr=False)

    def __init__(
        self,
        name: str,
        container: Container,
        columns: Union[List[str], Dict[str, Column]],
        upstreams: List["Table"],
    ):
        self.name = name
        self.container = container
        self.upstreams = upstreams
        if isinstance(columns, list):
            self.columns = OrderedDict((col, Column(col)) for col in columns)
        elif isinstance(columns, dict):
            self.columns = OrderedDict(columns)

    @property
    def name_components(self) -> List[str]:
        lst = [self.name]
        container: Optional[Container] = self.container
        while container:
            lst.append(container.name)
            container = container.parent
        return lst[::-1]

    def is_view(self) -> bool:
        return False


@dataclass(init=False)
class View(Table):
    definition: str

    def __init__(
        self,
        name: str,
        container: Container,
        columns: Union[List[str], Dict[str, Column]],
        upstreams: List["Table"],
        definition: str,
    ):
        super().__init__(name, container, columns, upstreams)
        self.definition = definition

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
