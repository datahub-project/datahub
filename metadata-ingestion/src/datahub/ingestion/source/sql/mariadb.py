import logging
from datetime import datetime
from typing import List, Optional

from pydantic.fields import Field
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
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
)

# MariaDB inherits shared stored procedure config from MySQL via StoredProcedureConfigMixin

logger: logging.Logger = logging.getLogger(__name__)

STORED_PROCEDURES_QUERY = """
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
AND ROUTINE_SCHEMA = :schema
"""


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
    def get_platform(self) -> str:
        return "mariadb"

    def _get_stored_procedures(
        self,
        conn: Connection,
        db_name: str,
        schema: str,
    ) -> List[BaseProcedure]:
        query = text(STORED_PROCEDURES_QUERY)

        procedures_list = []

        for row in conn.execute(query, {"schema": schema}):
            # Convert SQLAlchemy Row to dict for easier and safer access
            row_dict = dict(row)
            procedure_name = row_dict.get("ROUTINE_NAME", "unknown")

            try:
                routine_name = row_dict.get("ROUTINE_NAME")
                if not routine_name:
                    self.report.warning(
                        title="Skipping procedure with empty name",
                        message=f"Found procedure with empty name in schema {schema}",
                        context=f"Schema: {schema}",
                    )
                    continue

                # Use ROUTINE_DEFINITION directly from information_schema (same as MySQL)
                code = row_dict.get("ROUTINE_DEFINITION")

                procedures_list.append(
                    BaseProcedure(
                        name=routine_name,
                        language="SQL",
                        argument_signature=None,
                        return_type=None,
                        procedure_definition=code,
                        created=self._parse_datetime(row_dict.get("CREATED")),
                        last_altered=self._parse_datetime(row_dict.get("LAST_ALTERED")),
                        comment=row_dict.get("ROUTINE_COMMENT"),
                        extra_properties={
                            k: v
                            for k, v in {
                                "sql_data_access": row_dict.get("SQL_DATA_ACCESS"),
                                "security_type": row_dict.get("SECURITY_TYPE"),
                                "definer": row_dict.get("DEFINER"),
                            }.items()
                            if v is not None
                        },
                    )
                )
            except Exception as e:
                self.report.warning(
                    title="Error processing stored procedure",
                    message=f"Failed to process procedure {schema}.{procedure_name}",
                    context=f"Schema: {schema}, Procedure: {procedure_name}",
                    exc=e,
                )

        return procedures_list

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        """Convert a string timestamp to datetime or return None"""
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except (ValueError, TypeError):
            return None
