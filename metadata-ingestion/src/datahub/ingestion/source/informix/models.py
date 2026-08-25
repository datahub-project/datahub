from dataclasses import dataclass
from typing import List, Optional

from pydantic import BaseModel, model_validator

from datahub.metadata.schema_classes import SchemaFieldDataTypeClass


@dataclass(frozen=True)
class InformixType:
    # A syscolumns.coltype base code's DataHub type class and canonical native name.
    datahub_type: type
    native_name: str


@dataclass(frozen=True)
class ExtendedType:
    """A column's ``sysxtdtypes`` row, joined via ``syscolumns.extended_id``.

    ``source_name`` is the name of the type a DISTINCT was declared over
    (``sysxtdtypes.source`` resolved back through ``sysxtdtypes``), and is None
    for every other mode -- and for a DISTINCT over an ordinary built-in, whose
    ``source`` is 0. The three fields always arrive together from one catalog
    row, so bundling them keeps a half-populated combination unrepresentable.
    """

    name: str
    mode: Optional[str]
    source_name: Optional[str] = None


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
    # The column's sysxtdtypes row, absent for ordinary types (extended_id = 0).
    # See constants._resolve_extended_type.
    extended: Optional[ExtendedType] = None


class InformixTable(BaseModel):
    name: str
    owner: str
    is_view: bool = False
    nrows: Optional[int] = None


class InformixForeignKey(BaseModel):
    """A referential constraint whose child/parent columns are pairwise aligned.

    ``child_columns[i]`` references ``parent_columns[i]``. Unequal lengths are
    rejected at construction so callers cannot emit a silently misaligned
    constraint.
    """

    name: str
    child_columns: List[str]
    parent_table: str
    parent_owner: str
    parent_columns: List[str]

    @model_validator(mode="after")
    def _require_aligned_columns(self) -> "InformixForeignKey":
        if len(self.child_columns) != len(self.parent_columns):
            raise ValueError(
                f"Foreign key '{self.name}' has mismatched column counts: "
                f"child={len(self.child_columns)} parent={len(self.parent_columns)}"
            )
        return self
