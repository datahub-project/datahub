import logging
from typing import Any, List

from pydantic import Field
from sqlalchemy.engine import Inspector
from sqlalchemy_singlestoredb import JSON, VECTOR

from datahub._version import __version__
from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.sql.sql_common import register_custom_type
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BytesTypeClass,
    RecordTypeClass,
)

logger = logging.getLogger(__name__)

register_custom_type(JSON, RecordTypeClass)
register_custom_type(VECTOR, BytesTypeClass)


class SingleStoreConfig(TwoTierSQLAlchemyConfig):
    def get_identifier(self, *, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    # defaults
    host_port: str = Field(
        default="localhost:3306", description="SingleStore host URL."
    )
    scheme: HiddenFromDocs[str] = "singlestoredb"

    include_stored_procedures: bool = Field(
        default=True,
        description="Include ingest of stored procedures.",
    )

    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion."
        "Specify regex to match the entire procedure name in database.schema.procedure_name format. e.g. to match all procedures starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )


@platform_name("SingleStore")
@config_class(SingleStoreConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class SingleStoreSource(TwoTierSQLAlchemySource):
    """
    Source connector for extracting metadata and profiling information from SingleStore databases.

    This class supports:
    - Metadata extraction for databases, schemas, tables, and columns.
    - Table and column statistics via optional data profiling.
    - Ingestion of stored procedures and their definitions.
    - Mapping of SingleStore-specific types (e.g., JSON, VECTOR) to DataHub schema types.

    Configuration options allow filtering of stored procedures and enabling/disabling profiling.
    """

    config: SingleStoreConfig

    def __init__(self, config: SingleStoreConfig, ctx: Any):
        # Add SingleStore connection attributes for identification
        if config.options is None:
            config.options = {}
        connect_args = config.options.setdefault("connect_args", {})
        conn_attrs = connect_args.setdefault("conn_attrs", {})
        conn_attrs["_connector_name"] = "SingleStore DataHub Metadata Ingestion Source"
        conn_attrs["_connector_version"] = __version__
        conn_attrs["_product_version"] = __version__

        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "singlestore"

    @classmethod
    def create(cls, config_dict, ctx):
        config = SingleStoreConfig.model_validate(config_dict)
        return cls(config, ctx)

    def add_profile_metadata(self, inspector: Inspector) -> None:
        if not self.config.is_profiling_enabled():
            return
        with inspector.engine.connect() as conn:
            # https://support.singlestore.com/hc/en-us/articles/360061597292-The-size-of-a-table-and-an-index
            # data in memory
            for row in conn.execute(
                "SELECT DATABASE_NAME, TABLE_NAME, SUM(MEMORY_USE) :> BIGINT AS MEMORY_USE FROM INFORMATION_SCHEMA.TABLE_STATISTICS GROUP BY 1, 2"
            ):
                table_id = f"{row.DATABASE_NAME}.{row.TABLE_NAME}"
                self.profile_metadata_info.dataset_name_to_storage_bytes[table_id] = (
                    row.MEMORY_USE
                )

            # data on disk
            for row in conn.execute(
                "SELECT DATABASE_NAME, TABLE_NAME, SUM(UNCOMPRESSED_SIZE) :> BIGINT AS UNCOMPRESSED_SIZE FROM INFORMATION_SCHEMA.COLUMNAR_SEGMENTS GROUP BY 1, 2"
            ):
                table_id = f"{row.DATABASE_NAME}.{row.TABLE_NAME}"
                if not table_id in self.profile_metadata_info.dataset_name_to_storage_bytes:
                    self.profile_metadata_info.dataset_name_to_storage_bytes[table_id] = 0
                self.profile_metadata_info.dataset_name_to_storage_bytes[table_id] += (
                    row.UNCOMPRESSED_SIZE
                )

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """
        Get stored procedures for a specific schema.
        """
        base_procedures = []
        with inspector.engine.connect() as conn:
            procedures = conn.execute(
                """
                SELECT ROUTINE_NAME,
                    ROUTINE_DEFINITION,
                    CREATED,
                    LAST_ALTERED,
                    ROUTINE_COMMENT,
                    EXTERNAL_LANGUAGE,
                    ROUTINE_SCHEMA
                FROM information_schema.ROUTINES
                WHERE ROUTINE_TYPE = 'PROCEDURE'
                AND ROUTINE_SCHEMA = %s
                """,
                (schema,),
            )

            procedure_rows = list(procedures)
            for row in procedure_rows:
                base_procedures.append(
                    BaseProcedure(
                        name=row.ROUTINE_NAME,
                        procedure_definition=row.ROUTINE_DEFINITION,
                        created=row.CREATED,
                        last_altered=row.LAST_ALTERED,
                        comment=row.ROUTINE_COMMENT,
                        argument_signature=None,
                        return_type=None,
                        extra_properties=None,
                        language=row.EXTERNAL_LANGUAGE,
                        default_db=row.ROUTINE_SCHEMA,
                        default_schema=None,
                    )
                )
            return base_procedures
