from typing import Optional

from deltalake import DeltaTable, PyDeltaTableError

from datahub.ingestion.source.delta_lake.config import DeltaLakeSourceConfig


def read_delta_table(
    path: str, delta_lake_config: DeltaLakeSourceConfig
) -> Optional[DeltaTable]:
    delta_table = None
    try:
        opts = {}
        if delta_lake_config.is_s3:
            if (
                delta_lake_config.s3 is not None
                and delta_lake_config.s3.aws_config is not None
            ):
                creds = delta_lake_config.s3.aws_config.get_credentials()
                opts = {
                    "AWS_ACCESS_KEY_ID": creds.get("aws_access_key_id", ""),
                    "AWS_SECRET_ACCESS_KEY": creds.get("aws_secret_access_key", ""),
                    # Allow http connections, this is required for minio
                    "AWS_STORAGE_ALLOW_HTTP": "true",
                }
                if delta_lake_config.s3.aws_config.aws_region:
                    opts["AWS_REGION"] = delta_lake_config.s3.aws_config.aws_region
                if delta_lake_config.s3.aws_config.aws_endpoint_url:
                    opts[
                        "AWS_ENDPOINT_URL"
                    ] = delta_lake_config.s3.aws_config.aws_endpoint_url
        delta_table = DeltaTable(
            path,
            storage_options=opts,
            without_files=not delta_lake_config.require_files,
        )

    except PyDeltaTableError as e:
        if "Not a Delta table" not in str(e):
            raise e
    return delta_table


def get_file_count(delta_table: DeltaTable) -> int:
    return len(delta_table.files())
