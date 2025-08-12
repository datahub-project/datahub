import re
from typing import Iterable, List, Union

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import text, util
from sqlalchemy.dialects.mysql import BIT, base
from sqlalchemy.dialects.mysql.enumerated import SET
from sqlalchemy.engine import Row
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    SqlWorkUnit,
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.sql_utils import (
    gen_database_key,
)
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
    generate_procedure_container_workunits,
    generate_procedure_workunits,
)
from datahub.ingestion.source.sql.stored_procedures.config import (
    StoredProcedureConfigMixin,
)
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.schema_classes import BytesTypeClass

SET.__repr__ = util.generic_repr  # type:ignore

GEOMETRY = make_sqlalchemy_type("GEOMETRY")
POINT = make_sqlalchemy_type("POINT")
LINESTRING = make_sqlalchemy_type("LINESTRING")
POLYGON = make_sqlalchemy_type("POLYGON")
DECIMAL128 = make_sqlalchemy_type("DECIMAL128")

register_custom_type(GEOMETRY)
register_custom_type(POINT)
register_custom_type(LINESTRING)
register_custom_type(POLYGON)
register_custom_type(DECIMAL128)
register_custom_type(BIT, BytesTypeClass)

base.ischema_names["geometry"] = GEOMETRY
base.ischema_names["point"] = POINT
base.ischema_names["linestring"] = LINESTRING
base.ischema_names["polygon"] = POLYGON
base.ischema_names["decimal128"] = DECIMAL128

# SQL query constants for better maintainability
STORED_PROCEDURES_QUERY = """
SELECT
    ROUTINE_NAME as name,
    ROUTINE_DEFINITION as definition,
    ROUTINE_COMMENT as comment,
    CREATED,
    LAST_ALTERED,
    SQL_DATA_ACCESS,
    SECURITY_TYPE,
    DEFINER
FROM information_schema.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE'
AND ROUTINE_SCHEMA = :schema
"""


class MySQLConnectionConfig(SQLAlchemyConnectionConfig):
    # defaults
    host_port: str = Field(default="localhost:3306", description="MySQL host URL.")
    scheme: str = "mysql+pymysql"


class MySQLConfig(
    MySQLConnectionConfig, TwoTierSQLAlchemyConfig, StoredProcedureConfigMixin
):
    # MySQLConfig now inherits stored procedure configuration from StoredProcedureConfigMixin
    # This includes: include_stored_procedures, procedure_pattern
    pass

    def get_identifier(self, *, schema: str, table: str) -> str:
        return f"{schema}.{table}"


@platform_name("MySQL")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class MySQLSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, tables, and stored procedures
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling
    """

    config: MySQLConfig

    def __init__(self, config, ctx):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self) -> str:
        return "mysql"

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def is_temp_table(self, name: str) -> bool:
        """
        Check if a table name represents a temporary table in MySQL.
        MySQL temporary tables typically start with # or _tmp patterns.
        """
        # MySQL temporary table patterns
        temp_patterns = [
            r"^#.*",  # Tables starting with #
            r"^(tmp|temp)_.*",  # Tables starting with tmp_ or temp_
            r".*_(tmp|temp)$",  # Tables ending with _tmp or _temp
            r".*_(tmp|temp)_.*",  # Tables containing _tmp_ or _temp_
        ]

        table_name = name.split(".")[
            -1
        ].lower()  # Get just the table name, case insensitive

        return any(re.match(pattern, table_name) for pattern in temp_patterns)

    def get_schema_level_workunits(
        self,
        inspector: Inspector,
        schema: str,
        database: str,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from super().get_schema_level_workunits(
            inspector=inspector,
            schema=schema,
            database=database,
        )

        if self.config.include_stored_procedures:
            try:
                yield from self.loop_stored_procedures(inspector, schema, self.config)
            except Exception as e:
                self.report.failure(
                    title="Failed to list stored procedures for schema",
                    message="An error occurred while listing procedures for the schema.",
                    context=f"{database}.{schema}",
                    exc=e,
                )

    def _process_procedures(
        self,
        procedures: List[BaseProcedure],
        db_name: str,
        schema: str,
    ) -> Iterable[MetadataWorkUnit]:
        if procedures:
            yield from generate_procedure_container_workunits(
                database_key=gen_database_key(
                    database=db_name,
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                ),
                schema_key=None,  # MySQL is two-tier
            )

        for procedure in procedures:
            yield from self._process_procedure(procedure, schema, db_name)

    def _process_procedure(
        self,
        procedure: BaseProcedure,
        schema: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Process a single stored procedure for MySQL (two-tier database)."""
        try:
            yield from generate_procedure_workunits(
                procedure=procedure,
                database_key=gen_database_key(
                    database=db_name,
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                ),
                schema_key=None,  # MySQL is two-tier - no schema key needed
                schema_resolver=self.get_schema_resolver(),
                is_temp_table=self.is_temp_table,
            )
        except Exception as e:
            self.report.warning(
                title="Failed to emit stored procedure",
                message="An error occurred while emitting stored procedure",
                context=procedure.name,
                exc=e,
            )

    def add_profile_metadata(self, inspector: Inspector) -> None:
        if not self.config.is_profiling_enabled():
            return
        with inspector.engine.connect() as conn:
            for row in conn.execute(
                "SELECT table_schema, table_name, data_length from information_schema.tables"
            ):
                self.profile_metadata_info.dataset_name_to_storage_bytes[
                    f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
                ] = row.DATA_LENGTH

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """
        Get stored procedures for a specific schema.
        """
        base_procedures = []
        with inspector.engine.connect() as conn:
            procedures = conn.execute(
                text(STORED_PROCEDURES_QUERY),
                {"schema": schema},
            )

            procedure_rows: List[Row] = list(procedures)
            for row in procedure_rows:
                # Extract name with fallback - name is required for BaseProcedure
                procedure_name = getattr(row, "name", None)
                if not procedure_name:
                    # Skip procedures without names
                    continue

                base_procedures.append(
                    BaseProcedure(
                        name=procedure_name,
                        language="SQL",
                        argument_signature=None,
                        return_type=None,
                        procedure_definition=getattr(row, "definition", None),
                        created=getattr(row, "CREATED", None),
                        last_altered=getattr(row, "LAST_ALTERED", None),
                        comment=getattr(row, "comment", None),
                        extra_properties={
                            k: v
                            for k, v in {
                                "sql_data_access": getattr(
                                    row, "SQL_DATA_ACCESS", None
                                ),
                                "security_type": getattr(row, "SECURITY_TYPE", None),
                                "definer": getattr(row, "DEFINER", None),
                            }.items()
                            if v is not None
                        },
                    )
                )
            return base_procedures
