import functools
from typing import Any, Callable, List, Optional, Tuple

import sqlglot
from pydantic import BaseModel


def _default_leaf_name(exp: sqlglot.exp.Expression) -> str:
    return exp.name


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
            parts=self.parts,
        )

    @classmethod
    def from_sqlglot_table(
        cls,
        table: sqlglot.exp.Table,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
        leaf_name_transform: Optional[Callable[[sqlglot.exp.Expression], str]] = None,
    ) -> "_TableName":
        transform = leaf_name_transform or _default_leaf_name

        # Handle Snowflake semantic views: SEMANTIC_VIEW(table_name ...)
        if isinstance(table.this, sqlglot.exp.SemanticView):
            inner_table = table.this.this
            if isinstance(inner_table, sqlglot.exp.Table):
                return cls.from_sqlglot_table(
                    inner_table,
                    default_db=default_db,
                    default_schema=default_schema,
                    leaf_name_transform=leaf_name_transform,
                )
            elif isinstance(inner_table, sqlglot.exp.Identifier):
                return cls(
                    database=table.catalog or default_db,
                    db_schema=table.db or default_schema,
                    table=transform(inner_table),
                    parts=None,
                )

        if isinstance(table.this, sqlglot.exp.Dot):
            # Dot is left-associative (a.b.c = Dot(Dot(a,b),c)), so collect
            # right-side identifiers while walking left, then reverse.
            all_parts_exp: List[sqlglot.exp.Expression] = []
            exp: sqlglot.exp.Expression = table.this
            while isinstance(exp, sqlglot.exp.Dot):
                all_parts_exp.append(exp.expression)
                exp = exp.this
            all_parts_exp.append(exp)
            all_parts_exp.reverse()
            parts = [p.name for p in all_parts_exp[:-1]] + [
                transform(all_parts_exp[-1])
            ]
            table_name = ".".join(parts)
        else:
            table_name = transform(table.this)

        parts_tuple = tuple(p.name for p in table.parts) if table.parts else None

        return cls(
            database=table.catalog or default_db,
            db_schema=table.db or default_schema,
            table=table_name,
            parts=parts_tuple,
        )
