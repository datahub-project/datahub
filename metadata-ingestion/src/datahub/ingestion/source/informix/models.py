from typing import List, Optional

from pydantic import BaseModel


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
