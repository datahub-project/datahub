import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from pydantic.fields import Field
from sqlalchemy.engine import Inspector
from sqlalchemy.engine.base import Connection

from datahub.emitter.mcp_builder import DatabaseKey
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
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.mysql.job_models import (
    MySQLDataJob,
    MySQLProcedureContainer,
    MySQLStoredProcedure,
)
from datahub.ingestion.source.sql.stored_procedures.base import (
    generate_procedure_container_workunits,
    generate_procedure_lineage,
)

logger: logging.Logger = logging.getLogger(__name__)


class MariaDBConfig(MySQLConfig):
    host_port: str = Field(default="localhost:3306", description="MariaDB host URL.")


@platform_name("MariaDB")
@config_class(MariaDBConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class MariaDBSource(MySQLSource):
    def get_platform(self):
        return "mariadb"

    def _get_stored_procedures(
        self,
        conn: Connection,
        db_name: str,
        schema: str,
    ) -> List[Dict[str, str]]:
        stored_procedures_data = conn.execute(  # type: ignore
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
            # For MariaDB, always try to get the procedure definition using SHOW CREATE
            # as it's more reliable for large procedures
            try:
                create_proc = conn.execute(  # type: ignore
                    f"SHOW CREATE PROCEDURE `{schema}`.`{row['ROUTINE_NAME']}`"
                ).fetchone()
                code = (
                    create_proc[2] if create_proc else row["ROUTINE_DEFINITION"]
                )  # MariaDB returns (Procedure, body, something)
            except Exception as e:
                logger.warning(
                    f"Failed to get procedure definition for {schema}.{row['ROUTINE_NAME']}: {e}"
                )
                code = row["ROUTINE_DEFINITION"]

            procedures_list.append(
                dict(
                    routine_schema=schema,
                    routine_name=row["ROUTINE_NAME"],
                    code=code,
                    comment=row.get("ROUTINE_COMMENT"),
                    created=row.get("CREATED"),
                    last_altered=row.get("LAST_ALTERED"),
                )
            )
        return procedures_list

    def loop_stored_procedures(
        self,
        inspector: Inspector,
        schema: str,  # In two-tier this is actually the database name
        sql_config: MySQLConfig,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Override to ensure MariaDB source is set correctly while maintaining two-tier structure.
        """
        db_name = self.get_db_name(inspector)
        procedure_flow_name = f"{db_name}.stored_procedures"
        mariadb_procedure_container = MySQLProcedureContainer(
            name=procedure_flow_name,
            env=sql_config.env,
            db=db_name,
            platform_instance=sql_config.platform_instance,
            source="mariadb",
        )

        # Define database key for the base procedure implementation
        database_key = DatabaseKey(
            database=db_name,
            platform=self.get_platform(),
            instance=sql_config.platform_instance,
            env=sql_config.env,
        )

        schema_key = None  # MariaDB is two-tier

        with inspector.engine.connect() as conn:
            procedures_data = self._get_stored_procedures(conn, db_name, schema)
            procedures: List[MySQLStoredProcedure] = []

            for procedure_data in procedures_data:
                procedure_full_name = f"{db_name}.{procedure_data['routine_name']}"
                if not self.config.procedure_pattern.allowed(procedure_full_name):
                    self.report.report_dropped(procedure_full_name)
                    continue

                # Create a procedure
                procedure = MySQLStoredProcedure(
                    name=procedure_data["routine_name"],
                    code=procedure_data.get("code"),
                    routine_schema=schema,
                    comment=procedure_data.get("comment"),
                    created=self._parse_datetime(procedure_data.get("created")),
                    last_altered=self._parse_datetime(
                        procedure_data.get("last_altered")
                    ),
                    flow=mariadb_procedure_container,
                )
                procedures.append(procedure)

                # Also add to stored_procedures list for lineage processing
                if self.config.include_lineage:
                    self.stored_procedures.append(procedure)

            if procedures:
                # Use the base container workunit generator
                yield from generate_procedure_container_workunits(
                    database_key=database_key,
                    schema_key=schema_key,
                )

            for procedure in procedures:
                # Get a data job for this procedure
                data_job = MySQLDataJob(entity=procedure)

                # Generate basic metadata
                yield from self.construct_job_workunits(data_job)

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
                        procedure=procedure.to_base_procedure(),
                        procedure_job_urn=MySQLDataJob(entity=procedure).urn,
                        default_db=None,
                        default_schema=procedure.routine_schema,
                        is_temp_table=self.is_temp_table,
                    )
                )

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        """Convert a string timestamp to datetime or return None"""
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except (ValueError, TypeError):
            return None
