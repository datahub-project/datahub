from typing import Dict, Optional, Sequence, Type

from dagster import MetadataValue, OutputContext
from dagster._core.storage.db_io_manager import DbClient, DbIOManager, DbTypeHandler

imported_db_clients = {}

try:
    from dagster_snowflake.snowflake_io_manager import SnowflakeDbClient

    imported_db_clients["snowflake"] = SnowflakeDbClient
except ImportError:
    pass

try:
    from dagster_snowflake.snowflake_io_manager import SnowflakeDbClient

    imported_db_clients["snowflake"] = SnowflakeDbClient
except ImportError:
    pass


class DataHubDbIoManager(DbIOManager):
    def __init__(
        self,
        *,
        type_handlers: Sequence[DbTypeHandler],
        db_client: DbClient,
        database: str,
        schema: Optional[str] = None,
        io_manager_name: Optional[str] = None,
        default_load_type: Optional[Type] = None,
        datahub_env: Optional[str] = "PROD",
        datahub_base_url: Optional[str] = None,
    ):
        super().__init__(
            type_handlers=type_handlers,
            db_client=db_client,
            database=database,
            schema=schema,
            io_manager_name=io_manager_name,
            default_load_type=default_load_type,
        )
        self.datahub_env = datahub_env if datahub_env else "PROD"
        self.datahub_base_url = datahub_base_url

    def handle_output(self, context: OutputContext, obj: object) -> None:
        super().handle_output(context, obj=obj)
        # Custom logic here

        table_slice = self._get_table_slice(context, context)
        platform: Optional[str] = None

        for db_client in imported_db_clients:
            if isinstance(self._db_client, imported_db_clients[db_client]):
                platform = db_client
                break

        if not platform:
            context.log.debug(
                "Unable to get platform from db_client or it is not supported"
            )
            return

        if table_slice.database:
            urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{table_slice.database.lower()}.{table_slice.schema.lower()}.{table_slice.table.lower()},{self.datahub_env})"
        else:
            urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{table_slice.schema.lower()}.{table_slice.table.lower()},{self.datahub_env})"

        metadata: Dict = {
            "datahub_urn": urn,
        }

        if self.datahub_base_url:
            metadata["datahub_url"] = MetadataValue.url(
                f"{self.datahub_base_url}/dataset/{urn}"
            )

        context.add_output_metadata(metadata)
