from typing import Optional

from deltalake import DeltaTable, PyDeltaTableError

from datahub.ingestion.source.delta_lake.config import DeltaLakeSourceConfig


def read_delta_table(
    path: str, delta_lake_config: DeltaLakeSourceConfig
) -> Optional[DeltaTable]:
    delta_table = None
    try:
        opts = {}
        if delta_lake_config.is_s3():
            if delta_lake_config.s3 is None:
                raise ValueError("aws_config not set. Cannot browse s3")
            if delta_lake_config.s3.aws_config is None:
                raise ValueError("aws_config not set. Cannot browse s3")
            opts = {
                "AWS_ACCESS_KEY_ID": delta_lake_config.s3.aws_config.aws_access_key_id,
                "AWS_SECRET_ACCESS_KEY": delta_lake_config.s3.aws_config.aws_secret_access_key,
            }
        delta_table = DeltaTable(path, storage_options=opts)

    except PyDeltaTableError as e:
        if "Not a Delta table" not in str(e):
            import pdb

            pdb.set_trace()
            raise e

    return delta_table


def get_file_count(delta_table: DeltaTable) -> int:
    return len(delta_table.files())
