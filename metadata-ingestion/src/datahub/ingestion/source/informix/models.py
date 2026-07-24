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
