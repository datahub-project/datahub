import pathlib
from typing import Any, ClassVar, List, Optional, Sequence, Tuple

import duckdb

from datahub.lite.duckdb_lite_config import DuckDBLiteConfig
from datahub.lite.sql_backed_lite import SqlBackedLite


class DuckDBLite(SqlBackedLite[DuckDBLiteConfig]):
    STRING_TYPE: ClassVar[str] = "VARCHAR"
    JSON_TYPE: ClassVar[str] = "JSON"
    BIGINT_TYPE: ClassVar[str] = "BIGINT"

    @classmethod
    def create(cls, config_dict: dict) -> "DuckDBLite":
        config: DuckDBLiteConfig = DuckDBLiteConfig.model_validate(config_dict)
        return DuckDBLite(config)

    def location(self) -> str:
        return self.config.file

    @property
    def read_only(self) -> bool:
        return self.config.read_only

    def _connect(self, fpath: pathlib.Path) -> None:
        self.duckdb_client = duckdb.connect(
            str(fpath), read_only=self.config.read_only, config=self.config.options
        )

    def _execute(self, query: str, params: Sequence[Any] = ()) -> List[Tuple]:
        if params:
            return self.duckdb_client.execute(query, list(params)).fetchall()
        return self.duckdb_client.execute(query).fetchall()

    def _execute_one(self, query: str, params: Sequence[Any] = ()) -> Optional[Tuple]:
        if params:
            return self.duckdb_client.execute(query, list(params)).fetchone()
        return self.duckdb_client.execute(query).fetchone()

    def _begin(self) -> None:
        self.duckdb_client.begin()

    def _commit(self) -> None:
        self.duckdb_client.commit()

    def _rollback(self) -> None:
        self.duckdb_client.rollback()

    def _close_connection(self) -> None:
        self.duckdb_client.close()

    def _create_unique_index(
        self, index_name: str, table_name: str, columns: List[str]
    ) -> None:
        try:
            self.duckdb_client.execute(
                f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({', '.join(columns)})"
            )
        except duckdb.CatalogException as e:
            if "already exists" not in str(e).lower():
                raise
