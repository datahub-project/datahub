# This import verifies that the dependencies are available.

from typing import Iterable, List, Union

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import util
from sqlalchemy.dialects.mysql import BIT, base
from sqlalchemy.dialects.mysql.enumerated import SET
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern
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
    gen_schema_key,
)
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
    generate_procedure_container_workunits,
    generate_procedure_workunits,
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


class MySQLConnectionConfig(SQLAlchemyConnectionConfig):
    # defaults
    host_port: str = Field(default="localhost:3306", description="MySQL host URL.")
    scheme: str = "mysql+pymysql"


class MySQLConfig(MySQLConnectionConfig, TwoTierSQLAlchemyConfig):
    def get_identifier(self, *, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    include_stored_procedures: bool = Field(
        default=True,
        description="Include ingest of stored procedures.",
    )
    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion."
        "Specify regex to match the entire procedure name in database.schema.procedure_name format. e.g. to match all procedures starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )


@platform_name("MySQL")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class MySQLSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    Metadata for databases, schemas, and tables
    Column types and schema associated with each table
    Table, row, and column statistics via optional SQL profiling
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "mysql"

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)

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

    def loop_stored_procedures(
        self,
        inspector: Inspector,
        schema: str,
        config: MySQLConfig,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Loop schema data for get stored procedures as dataJob-s.
        """
        db_name = self.get_db_name(inspector)

        procedures = self.fetch_procedures_for_schema(inspector, schema, db_name)
        if procedures:
            yield from self._process_procedures(procedures, db_name, schema)

    def fetch_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        try:
            raw_procedures: List[BaseProcedure] = self.get_procedures_for_schema(
                inspector, schema, db_name
            )
            procedures: List[BaseProcedure] = []
            for procedure in raw_procedures:
                procedure_qualified_name = self.get_identifier(
                    schema=schema,
                    entity=procedure.name,
                    inspector=inspector,
                )

                if not self.config.procedure_pattern.allowed(procedure_qualified_name):
                    self.report.report_dropped(procedure_qualified_name)
                else:
                    procedures.append(procedure)
            return procedures
        except Exception as e:
            self.report.warning(
                title="Failed to get procedures for schema",
                message="An error occurred while fetching procedures for the schema.",
                context=f"{db_name}.{schema}",
                exc=e,
            )
            return []

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
                    SELECT ROUTINE_NAME AS name, 
                    ROUTINE_DEFINITION AS definition, 
                    EXTERNAL_LANGUAGE AS language
                    FROM information_schema.ROUTINES
                    WHERE ROUTINE_TYPE = 'PROCEDURE'
                    AND ROUTINE_SCHEMA = '"""
                + schema
                + """'
                    """
            )

            procedure_rows = list(procedures)
            for row in procedure_rows:
                base_procedures.append(
                    BaseProcedure(
                        name=row.name,
                        language=row.language,
                        argument_signature=None,
                        return_type=None,
                        procedure_definition=row.definition,
                        created=None,
                        last_altered=None,
                        extra_properties=None,
                        comment=None,
                    )
                )
            return base_procedures

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
                schema_key=gen_schema_key(
                    db_name=db_name,
                    schema=schema,
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                ),
            )
        for procedure in procedures:
            yield from self._process_procedure(procedure, schema, db_name)

    def _process_procedure(
        self,
        procedure: BaseProcedure,
        schema: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        try:
            yield from generate_procedure_workunits(
                procedure=procedure,
                database_key=gen_database_key(
                    database=db_name,
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                ),
                schema_key=gen_schema_key(
                    db_name=db_name,
                    schema=schema,
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                ),
                schema_resolver=self.get_schema_resolver(),
            )
        except Exception as e:
            self.report.warning(
                title="Failed to emit stored procedure",
                message="An error occurred while emitting stored procedure",
                context=procedure.name,
                exc=e,
            )
