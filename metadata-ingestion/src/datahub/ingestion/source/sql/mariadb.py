import logging
from typing import Dict, Iterable, List

from pydantic.fields import Field
from sqlalchemy.engine import Inspector
from sqlalchemy.engine.base import Connection

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.mysql.job_models import (
    MySQLDataFlow,
    MySQLProcedureContainer,
    MySQLStoredProcedure,
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
            # For MariaDB, always try to get the procedure definition using SHOW CREATE
            # as it's more reliable for large procedures
            try:
                create_proc = conn.execute(
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
        data_flow = MySQLDataFlow(entity=mariadb_procedure_container)

        with inspector.engine.connect() as conn:
            procedures_data = self._get_stored_procedures(conn, db_name, schema)
            procedures: List[MySQLStoredProcedure] = []

            for procedure_data in procedures_data:
                procedure_full_name = f"{db_name}.{procedure_data['routine_name']}"
                if not self.config.procedure_pattern.allowed(procedure_full_name):
                    self.report.report_dropped(procedure_full_name)
                    continue
                procedures.append(
                    MySQLStoredProcedure(
                        flow=mariadb_procedure_container, **procedure_data
                    )
                )

            if procedures:
                yield from self.construct_flow_workunits(data_flow=data_flow)
            for procedure in procedures:
                yield from self._process_stored_procedure(conn, procedure)
