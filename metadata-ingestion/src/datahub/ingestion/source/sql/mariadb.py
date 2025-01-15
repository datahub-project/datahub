import logging
from typing import Dict, List

from sqlalchemy.engine.base import Connection

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource

logger: logging.Logger = logging.getLogger(__name__)


@platform_name("MariaDB")
@config_class(MySQLConfig)
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
                code = create_proc[1] if create_proc else row["ROUTINE_DEFINITION"]
            except Exception as e:
                logger.warning(
                    f"Failed to get procedure definition for {schema}.{row['ROUTINE_NAME']}: {e}"
                )
                code = row["ROUTINE_DEFINITION"]

            procedures_list.append(
                dict(
                    db=db_name,
                    routine_schema=row["ROUTINE_SCHEMA"],
                    routine_name=row["ROUTINE_NAME"],
                    code=code,
                    description=row["ROUTINE_COMMENT"]
                    if row["ROUTINE_COMMENT"]
                    else None,
                )
            )
        return procedures_list
