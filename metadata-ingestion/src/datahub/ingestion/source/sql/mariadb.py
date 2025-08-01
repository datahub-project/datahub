import logging
from datetime import datetime
from typing import Any, Iterable, List, Optional

from pydantic.fields import Field
from sqlalchemy.engine import Inspector
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql import text

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
from datahub.ingestion.source.sql.sql_utils import (
    gen_database_key,
)
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
    generate_procedure_container_workunits,
)

# MariaDB inherits shared stored procedure config from MySQL via StoredProcedureConfigMixin

logger: logging.Logger = logging.getLogger(__name__)


class MariaDBConfig(MySQLConfig):
    host_port: str = Field(default="localhost:3306", description="MariaDB host URL.")
    # MariaDB inherits stored procedure configuration from MySQLConfig
    # This includes include_stored_procedures and procedure_pattern


@platform_name("MariaDB")
@config_class(MariaDBConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class MariaDBSource(MySQLSource):
    def get_platform(self):
        return "mariadb"

    def _get_stored_procedures(
        self,
        conn: Connection,
        db_name: str,
        schema: str,
    ) -> List[BaseProcedure]:
        query = text(f"""
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
        """)

        procedures_list = []

        for row in conn.execute(query):  # type: ignore
            try:
                routine_name = row["ROUTINE_NAME"]
                if not routine_name:
                    logger.warning(f"Skipping procedure with empty name in {schema}")
                    continue

                # Always extract procedure code (needed for lineage)
                code = self._extract_procedure_definition(
                    conn, schema, routine_name, row
                )

                # Helper function to safely access columns that might not exist
                def safe_get(row_obj: Any, column: str) -> Optional[str]:
                    try:
                        return row_obj[column]
                    except (KeyError, IndexError):
                        return None

                procedures_list.append(
                    BaseProcedure(
                        name=routine_name,
                        language="SQL",
                        argument_signature=None,
                        return_type=None,
                        procedure_definition=code,
                        created=self._parse_datetime(safe_get(row, "CREATED")),
                        last_altered=self._parse_datetime(
                            safe_get(row, "LAST_ALTERED")
                        ),
                        comment=safe_get(row, "ROUTINE_COMMENT"),
                        extra_properties={
                            k: v
                            for k, v in {
                                "sql_data_access": safe_get(row, "SQL_DATA_ACCESS"),
                                "security_type": safe_get(row, "SECURITY_TYPE"),
                                "definer": safe_get(row, "DEFINER"),
                            }.items()
                            if v is not None
                        },
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Error processing procedure {schema}.{routine_name if 'routine_name' in locals() else 'unknown'}: {e}"
                )

        return procedures_list

    def _extract_procedure_definition(
        self, conn: Connection, schema: str, routine_name: str, row: Any
    ) -> str:
        """Extract a stored procedure definition with SHOW CREATE PROCEDURE fallback."""
        try:
            # Escape identifiers for SQL safety
            escaped_schema = schema.replace("`", "``")
            escaped_routine = routine_name.replace("`", "``")
            show_query = text(
                f"SHOW CREATE PROCEDURE `{escaped_schema}`.`{escaped_routine}`"
            )

            create_proc = conn.execute(show_query).fetchone()  # type: ignore

            # MariaDB typically returns procedure definition at position 2
            if create_proc and len(create_proc) > 2:
                return create_proc[2]

            # Fall back to ROUTINE_DEFINITION
            return row["ROUTINE_DEFINITION"]
        except Exception as e:
            logger.warning(
                f"Failed to get procedure definition for {schema}.{routine_name}: {e}"
            )
            return row["ROUTINE_DEFINITION"]

    def loop_stored_procedures(
        self,
        inspector: Inspector,
        schema: str,  # In two-tier this is actually the database name
        sql_config: MySQLConfig,
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
            with inspector.engine.connect() as conn:
                raw_procedures: List[BaseProcedure] = self._get_stored_procedures(
                    conn, db_name, schema
                )
            procedures: List[BaseProcedure] = []
            for procedure in raw_procedures:
                procedure_qualified_name = f"{db_name}.{procedure.name}"

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
                schema_key=None,  # MariaDB is two-tier
            )

        for procedure in procedures:
            # Generate procedure metadata and lineage immediately (inherited from MySQL)
            yield from self._process_procedure(procedure, schema, db_name)

    # _process_procedure method removed - MariaDB now inherits immediate lineage processing from MySQL

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        """Convert a string timestamp to datetime or return None"""
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except (ValueError, TypeError):
            return None
