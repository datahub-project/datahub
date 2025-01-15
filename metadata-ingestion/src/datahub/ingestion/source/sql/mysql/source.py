import logging
from typing import Dict, Iterable, List, Union

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import util
from sqlalchemy.dialects.mysql import BIT, base
from sqlalchemy.dialects.mysql.enumerated import SET
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import StructuredLogLevel
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.mysql.job_models import (
    MySQLDataFlow,
    MySQLDataJob,
    MySQLProcedureContainer,
    MySQLStoredProcedure,
)
from datahub.ingestion.source.sql.mysql.stored_procedure_lineage import (
    generate_procedure_lineage,
)
from datahub.ingestion.source.sql.sql_common import (
    SqlWorkUnit,
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.schema_classes import BytesTypeClass
from datahub.utilities.file_backed_collections import FileBackedList

logger = logging.getLogger(__name__)

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
    include_stored_procedures: bool = Field(
        default=True, description="Include ingest of stored procedures."
    )
    include_stored_procedures_code: bool = Field(
        default=True, description="Include information about stored procedure code."
    )
    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion. "
        "Specify regex to match the entire procedure name in database.schema.procedure_name format.",
    )
    include_lineage: bool = Field(
        default=True,
        description="Enable lineage extraction for stored procedures",
    )

    def get_identifier(self, *, schema: str, table: str) -> str:
        return f"{schema}.{table}"


@platform_name("MySQL")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class MySQLSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:
    - Metadata for databases, schemas, and tables
    - Column types and schema associated with each table
    - Table, row, and column statistics via optional SQL profiling
    - Stored procedure metadata and lineage
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, self.get_platform())
        self.config: MySQLConfig = config
        self.stored_procedures: FileBackedList[MySQLStoredProcedure] = FileBackedList()

    def get_platform(self):
        return "mysql"

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)

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
                self.report.report_failure(
                    "stored_procedures",
                    f"Failed to extract stored procedures due to error {e}",
                )

    def loop_stored_procedures(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: MySQLConfig,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Loop schema data to get stored procedures as dataJob-s.
        """
        db_name = self.get_db_name(inspector)
        procedure_flow_name = f"{db_name}.{schema}.stored_procedures"
        mysql_procedure_container = MySQLProcedureContainer(
            name=procedure_flow_name,
            env=sql_config.env,
            db=db_name,
            platform_instance=sql_config.platform_instance,
        )
        data_flow = MySQLDataFlow(entity=mysql_procedure_container)

        with inspector.engine.connect() as conn:
            procedures_data = self._get_stored_procedures(conn, db_name, schema)
            procedures: List[MySQLStoredProcedure] = []

            for procedure_data in procedures_data:
                procedure_full_name = (
                    f"{db_name}.{schema}.{procedure_data['routine_name']}"
                )
                if not self.config.procedure_pattern.allowed(procedure_full_name):
                    self.report.report_dropped(procedure_full_name)
                    continue
                procedures.append(
                    MySQLStoredProcedure(
                        flow=mysql_procedure_container, **procedure_data
                    )
                )

            if procedures:
                yield from self.construct_flow_workunits(data_flow=data_flow)
            for procedure in procedures:
                yield from self._process_stored_procedure(conn, procedure)

    def _get_stored_procedures(
        self,
        conn: Connection,
        db_name: str,
        schema: str,
    ) -> List[Dict[str, str]]:
        stored_procedures_data = conn.execute(
            f"""
SELECT
    ROUTINE_SCHEMA,
    ROUTINE_NAME,
    ROUTINE_DEFINITION,
    ROUTINE_COMMENT,
    CREATED,
    LAST_ALTERED,
    SQL_DATA_ACCESS,
    SECURITY_TYPE,
    DEFINER
FROM information_schema.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE'
AND ROUTINE_SCHEMA = '{schema}'
            """
        )

        procedures_list = []
        for row in stored_procedures_data:
            procedures_list.append(
                dict(
                    db=db_name,
                    routine_schema=row["ROUTINE_SCHEMA"],
                    routine_name=row["ROUTINE_NAME"],
                    code=row["ROUTINE_DEFINITION"],
                    description=row["ROUTINE_COMMENT"]
                    if row["ROUTINE_COMMENT"]
                    else None,
                )
            )
        return procedures_list

    def _process_stored_procedure(
        self,
        conn: Connection,
        procedure: MySQLStoredProcedure,
    ) -> Iterable[MetadataWorkUnit]:
        data_job = MySQLDataJob(entity=procedure)

        # Get procedure metadata
        procedure_metadata = conn.execute(
            f"""
SELECT
    SECURITY_TYPE,
    SQL_DATA_ACCESS,
    CREATED,
    LAST_ALTERED,
    DEFINER
FROM information_schema.ROUTINES
WHERE ROUTINE_SCHEMA = '{procedure.routine_schema}'
AND ROUTINE_NAME = '{procedure.routine_name}'
            """
        ).fetchone()

        if procedure_metadata:
            for key, value in procedure_metadata.items():
                if value is not None:
                    data_job.add_property(key.lower(), str(value))

        # Get procedure parameters
        parameters = conn.execute(
            f"""
SELECT
    PARAMETER_NAME,
    PARAMETER_MODE,
    DATA_TYPE
FROM information_schema.PARAMETERS
WHERE SPECIFIC_SCHEMA = '{procedure.routine_schema}'
AND SPECIFIC_NAME = '{procedure.routine_name}'
ORDER BY ORDINAL_POSITION
            """
        )

        param_list = []
        for param in parameters:
            if param["PARAMETER_NAME"]:  # Skip RETURNS for functions
                param_list.append(
                    f"{param['PARAMETER_MODE']} {param['PARAMETER_NAME']} {param['DATA_TYPE']}"
                )

        if param_list:
            data_job.add_property("parameters", ", ".join(param_list))

        if procedure.code and self.config.include_stored_procedures_code:
            data_job.add_property("code", procedure.code)

        if self.config.include_lineage:
            self.stored_procedures.append(procedure)

        yield from self.construct_job_workunits(
            data_job,
            include_lineage=False,  # Lineage will be processed later
        )

    def construct_job_workunits(
        self,
        data_job: MySQLDataJob,
        include_lineage: bool = True,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=data_job.urn,
            aspect=data_job.as_datajob_info_aspect,
        ).as_workunit()

        data_platform_instance_aspect = data_job.as_maybe_platform_instance_aspect
        if data_platform_instance_aspect:
            yield MetadataChangeProposalWrapper(
                entityUrn=data_job.urn,
                aspect=data_platform_instance_aspect,
            ).as_workunit()

        if include_lineage:
            yield MetadataChangeProposalWrapper(
                entityUrn=data_job.urn,
                aspect=data_job.as_datajob_input_output_aspect,
            ).as_workunit()

    def construct_flow_workunits(
        self,
        data_flow: MySQLDataFlow,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=data_flow.urn,
            aspect=data_flow.as_dataflow_info_aspect,
        ).as_workunit()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from super().get_workunits_internal()

        # Process stored procedure lineage after all other metadata has been collected
        for procedure in self.stored_procedures:
            with self.report.report_exc(
                message="Failed to parse stored procedure lineage",
                context=procedure.full_name,
                level=StructuredLogLevel.WARN,
            ):
                yield from auto_workunit(
                    generate_procedure_lineage(
                        schema_resolver=self.get_schema_resolver(),
                        procedure=procedure,
                        procedure_job_urn=MySQLDataJob(entity=procedure).urn,
                        is_temp_table=self.is_temp_table,
                    )
                )

    def is_temp_table(self, name: str) -> bool:
        """
        Check if a table is temporary in MySQL.
        """
        try:
            parts = name.split(".")
            table_name = parts[-1]

            # Check for temporary table markers
            if table_name.startswith("#") or table_name.startswith("_tmp"):
                return True

            # This is also a temp table if:
            # 1. This name would be allowed by the dataset patterns
            # 2. We have a list of discovered tables
            # 3. It's not in the discovered tables list
            if (
                len(parts) >= 2
                and self.config.schema_pattern.allowed(parts[-2])
                and self.config.table_pattern.allowed(name)
                and name not in self.discovered_datasets
            ):
                logger.debug(f"Inferred as temp table: {name}")
                return True

        except Exception:
            logger.warning(f"Error parsing table name {name}")
        return False

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
