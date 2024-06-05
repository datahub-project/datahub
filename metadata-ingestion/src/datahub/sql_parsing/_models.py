import functools
from typing import Any, Optional

import sqlglot
from pydantic import BaseModel

from datahub.configuration.pydantic_migration_helpers import PYDANTIC_VERSION_2
from datahub.metadata.schema_classes import SchemaFieldDataTypeClass


class _ParserBaseModel(
    BaseModel,
    arbitrary_types_allowed=True,
    json_encoders={
        SchemaFieldDataTypeClass: lambda v: v.to_obj(),
    },
):
    def json(self, *args: Any, **kwargs: Any) -> str:
        if PYDANTIC_VERSION_2:
            return super().model_dump_json(*args, **kwargs)  # type: ignore
        else:
            return super().json(*args, **kwargs)


@functools.total_ordering
class _FrozenModel(_ParserBaseModel, frozen=True):
    def __lt__(self, other: "_FrozenModel") -> bool:
        # TODO: The __fields__ attribute is deprecated in Pydantic v2.
        for field in self.__fields__:
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
    database: Optional[str] = None
    db_schema: Optional[str] = None
    table: str

    def as_sqlglot_table(self) -> sqlglot.exp.Table:
        return sqlglot.exp.Table(
            catalog=(
                sqlglot.exp.Identifier(this=self.database) if self.database else None
            ),
            db=sqlglot.exp.Identifier(this=self.db_schema) if self.db_schema else None,
            this=sqlglot.exp.Identifier(this=self.table),
        )

    def qualified(
        self,
        dialect: sqlglot.Dialect,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> "_TableName":
        database = self.database or default_db
        db_schema = self.db_schema or default_schema

        return _TableName(
            database=database,
            db_schema=db_schema,
            table=self.table,
        )

    @classmethod
    def from_sqlglot_table(
        cls,
        table: sqlglot.exp.Table,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> "_TableName":
        return cls(
            database=table.catalog or default_db,
            db_schema=table.db or default_schema,
            table=table.this.name,
        )
