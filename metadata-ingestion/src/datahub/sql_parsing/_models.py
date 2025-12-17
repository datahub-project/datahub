import functools
from typing import Any, Optional

import sqlglot
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
        # Track if this is a temp table (for MSSQL # prefix restoration)
        is_local_temp = False
        is_global_temp = False

        if isinstance(table.this, sqlglot.exp.Dot):
            # For tables that are more than 3 parts, the extra parts will be in a Dot.
            # For now, we just merge them into the table name.
            parts = []
            exp = table.this
            while isinstance(exp, sqlglot.exp.Dot):
                parts.append(exp.this.name)
                exp = exp.expression
            parts.append(exp.name)
            table_name = ".".join(parts)

            # For multi-part names, check the final identifier for temp flags
            if hasattr(exp, "args"):
                is_local_temp = exp.args.get("temporary", False)
                is_global_temp = exp.args.get("global_", False)
        else:
            table_name = table.this.name

            # Check the identifier for temp flags
            if hasattr(table.this, "args"):
                is_local_temp = table.this.args.get("temporary", False)
                is_global_temp = table.this.args.get("global_", False)

        # For MSSQL dialect, SQLGlot strips the # or ## prefix from temp tables
        # but sets flags on the identifier. We need to restore the prefix so that
        # downstream temp table detection (which checks for startswith("#")) works.
        # - Local temp tables (#name): 'temporary' flag is set
        # - Global temp tables (##name): 'global_' flag is set
        # Note: Redshift also uses # for temp tables but SQLGlot keeps the prefix intact.
        #
        # The following functions depend on the # prefix for temp table detection:
        #   - datahub.ingestion.source.sql.mssql.source.SQLServerSource.is_temp_table()
        #   - datahub.ingestion.source.sql_queries.SqlQueriesSource.is_temp_table()
        #   - datahub.ingestion.source.bigquery_v2.queries_extractor.BigQueryQueriesExtractor.is_temp_table()
        #   - datahub.ingestion.source.snowflake.snowflake_queries.SnowflakeQueriesExtractor.is_temp_table()
        #   - datahub.sql_parsing.sql_parsing_aggregator.SqlParsingAggregator.is_temp_table()
        if is_global_temp and not table_name.startswith("##"):
            table_name = f"##{table_name}"
        elif is_local_temp and not table_name.startswith("#"):
            table_name = f"#{table_name}"

        return cls(
            database=table.catalog or default_db,
            db_schema=table.db or default_schema,
            table=table_name,
        )
