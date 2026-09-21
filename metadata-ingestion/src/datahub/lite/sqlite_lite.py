import logging
import pathlib
import sqlite3
from typing import Any, ClassVar, List, Optional, Sequence, Tuple

from datahub.lite.sql_backed_lite import SqlBackedLite
from datahub.lite.sqlite_lite_config import SqliteLiteConfig

logger = logging.getLogger(__name__)


class SqliteLite(SqlBackedLite[SqliteLiteConfig]):
    STRING_TYPE: ClassVar[str] = "TEXT"
    JSON_TYPE: ClassVar[str] = "TEXT"
    BIGINT_TYPE: ClassVar[str] = "INTEGER"

    @classmethod
    def create(cls, config_dict: dict) -> "SqliteLite":
        config: SqliteLiteConfig = SqliteLiteConfig.model_validate(config_dict)
        return SqliteLite(config)

    def location(self) -> str:
        return self.config.file

    @property
    def read_only(self) -> bool:
        return self.config.read_only

    def _connect(self, fpath: pathlib.Path) -> None:
        self._warn_if_duckdb_sibling(fpath)
        if self.config.read_only:
            # mode=ro fails outright if the database does not exist yet, which is
            # the behavior we want: a read-only open should never create one.
            self.sqlite_client = sqlite3.connect(
                f"{fpath.resolve().as_uri()}?mode=ro",
                uri=True,
                check_same_thread=False,
            )
        else:
            self.sqlite_client = sqlite3.connect(str(fpath), check_same_thread=False)
        for pragma, value in self.config.options.items():
            # PRAGMA values cannot be bound as parameters.
            self.sqlite_client.execute(f"PRAGMA {pragma} = {value}")

    @staticmethod
    def _warn_if_duckdb_sibling(fpath: pathlib.Path) -> None:
        # The default engine and the default filename both changed, so a sink
        # that relied on the defaults silently starts a fresh, empty store next
        # to the DuckDB one it used to write. Say so rather than look wiped.
        if fpath.exists():
            return
        duckdb_sibling = fpath.with_suffix(".duckdb")
        if duckdb_sibling.exists():
            logger.warning(
                f"Creating a new, empty SQLite DataHub Lite instance at {fpath}, "
                f"but {duckdb_sibling} already exists. DataHub Lite now defaults to "
                "sqlite; the two file formats are not interchangeable. To keep using "
                "the existing instance, set the lite type to 'duckdb' and point it at "
                "that file. To migrate, export it with the duckdb engine and re-import."
            )

    def _execute(self, query: str, params: Sequence[Any] = ()) -> List[Tuple]:
        return self.sqlite_client.execute(query, params).fetchall()

    def _execute_one(self, query: str, params: Sequence[Any] = ()) -> Optional[Tuple]:
        return self.sqlite_client.execute(query, params).fetchone()

    def _commit(self) -> None:
        self.sqlite_client.commit()

    def _rollback(self) -> None:
        self.sqlite_client.rollback()

    def _close_connection(self) -> None:
        self.sqlite_client.close()

    @classmethod
    def _json_text(cls, column: str, path: str) -> str:
        # The `->>` operator needs SQLite 3.38 (2022-02), which is newer than
        # the system library on distros this engine exists to support --
        # Ubuntu 22.04 ships 3.37. json_extract goes back to 3.9 and unquotes
        # string values identically.
        return f"json_extract({column}, '{path}')"

    def _create_unique_index(
        self, index_name: str, table_name: str, columns: List[str]
    ) -> None:
        self.sqlite_client.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name} ({', '.join(columns)})"
        )
