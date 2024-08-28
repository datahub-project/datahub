from typing import Optional

from dagster._core.storage.db_io_manager import DbIOManager
from dagster_snowflake.snowflake_io_manager import SnowflakeDbClient
from dagster_snowflake_pandas import SnowflakePandasIOManager
from pydantic import Field

from datahub_dagster_plugin.modules.storage.datahub_db_io_manager import (
    DataHubDbIoManager,
)


class DataHubSnowflakePandasIOManager(SnowflakePandasIOManager):
    datahub_env: Optional[str] = Field(
        default=None,
        description="The DataHub env where the materialized assets belongs to.",
    )
    datahub_base_url: Optional[str] = Field(
        default=None,
        description="The DataHub base url for generated DataHub url in asset metadata.",
    )

    def create_io_manager(self, context) -> DbIOManager:  # type: ignore[no-untyped-def]
        return DataHubDbIoManager(
            db_client=SnowflakeDbClient(),
            io_manager_name="DataHubSnowflakeIOManager",
            database=self.database,
            schema=self.schema_,
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
            datahub_env=self.datahub_env,
            datahub_base_url=self.datahub_base_url,
        )
