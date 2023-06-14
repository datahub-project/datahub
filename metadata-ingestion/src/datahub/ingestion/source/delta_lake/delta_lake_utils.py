import pathlib
from typing import Dict, Optional

from deltalake import DeltaTable

try:
    from deltalake.exceptions import TableNotFoundError

    _MUST_CHECK_TABLE_NOT_FOUND_MESSAGE = False
except ImportError:
    # For deltalake < 0.10.0.
    from deltalake import PyDeltaTableError as TableNotFoundError  # type: ignore

    _MUST_CHECK_TABLE_NOT_FOUND_MESSAGE = True

from datahub.ingestion.source.delta_lake.config import DeltaLakeSourceConfig


def read_delta_table(
    path: str, opts: Dict[str, str], delta_lake_config: DeltaLakeSourceConfig
) -> Optional[DeltaTable]:
    if not delta_lake_config.is_s3 and not pathlib.Path(path).exists():
        # The DeltaTable() constructor will create the path if it doesn't exist.
        # Hence we need an extra, manual check here.
        return None

    try:
        return DeltaTable(
            path,
            storage_options=opts,
            without_files=not delta_lake_config.require_files,
        )
    except TableNotFoundError as e:
        # For deltalake < 0.10.0, we need to check the error message to make sure
        # that this is a table not found error. Newer versions have a dedicated
        # exception class for this.
        if _MUST_CHECK_TABLE_NOT_FOUND_MESSAGE and "Not a Delta table" not in str(e):
            raise e
        else:
            # Otherwise, the table was genuinely not found and we return None.
            pass
    return None


def get_file_count(delta_table: DeltaTable) -> int:
    return len(delta_table.files())
