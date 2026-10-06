import functools
from typing import Any, Optional, Tuple

from pydantic import BaseModel


class _ParserBaseModel(
    BaseModel,
    arbitrary_types_allowed=True,
):
    def json(self, *args: Any, **kwargs: Any) -> str:
        return super().model_dump_json(*args, **kwargs)  # type: ignore


@functools.total_ordering
class _FrozenModel(_ParserBaseModel, frozen=True):
    def __lt__(self, other: "_FrozenModel") -> bool:
        for field in self.__class__.model_fields:
            self_v = getattr(self, field)
            other_v = getattr(other, field)

            # Handle None values by pushing them to the end of the ordering.
            if self_v is None and other_v is not None:
                return False
            elif self_v is not None and other_v is None:
                return True
            elif self_v != other_v:
                return self_v < other_v

        return False


class _TableName(_FrozenModel):
    # TODO: Move this into the schema_resolver.py file.

    database: Optional[str] = None
    db_schema: Optional[str] = None
    table: str
    parts: Optional[Tuple[str, ...]] = None

    @property
    def identity(
        self,
    ) -> Tuple[Optional[str], Optional[str], str, Optional[Tuple[str, ...]]]:
        """
        Table identity for hashing and equality.

        Includes parts only when >3 parts to distinguish multi-part tables
        while maintaining backward compatibility for standard 3-part names.
        """
        if self.parts and len(self.parts) > 3:
            return (self.database, self.db_schema, self.table, self.parts)
        return (self.database, self.db_schema, self.table, None)

    def __hash__(self) -> int:
        return hash(self.identity)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _TableName):
            return False
        return self.identity == other.identity

    def qualified(
        self,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> "_TableName":
        return _TableName(
            database=self.database or default_db,
            db_schema=self.db_schema or default_schema,
            table=self.table,
            parts=self.parts,
        )
