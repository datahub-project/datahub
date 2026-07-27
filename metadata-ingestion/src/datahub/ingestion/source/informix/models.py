from dataclasses import dataclass
from typing import List, Optional

from pydantic import BaseModel

from datahub.metadata.schema_classes import SchemaFieldDataTypeClass


@dataclass(frozen=True)
class InformixType:
    # A syscolumns.coltype base code's DataHub type class and canonical native name.
    datahub_type: type
    native_name: str


@dataclass(frozen=True)
class MappedColumn:
    data_type: SchemaFieldDataTypeClass
    nullable: bool
    native: str


class InformixColumn(BaseModel):
    name: str
    coltype: int
    length: int
    colno: int
    is_pk: bool = False


class InformixTable(BaseModel):
    name: str
    owner: str
    is_view: bool = False
    nrows: Optional[int] = None


class InformixForeignKey(BaseModel):
    name: str
    child_columns: List[str]
    parent_table: str
    parent_owner: str
    parent_columns: List[str]
