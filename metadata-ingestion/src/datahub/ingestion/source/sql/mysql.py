# This import verifies that the dependencies are available.
import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Iterable, List, Optional

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import create_engine, event, inspect, util
from sqlalchemy.dialects.mysql import BIT, base
from sqlalchemy.dialects.mysql.enumerated import SET
from sqlalchemy.engine import Row
from sqlalchemy.engine.reflection import Inspector

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import (
    AwsConnectionConfig,
    RDSIAMTokenManager,
)
from datahub.ingestion.source.sql.sql_common import (
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.sql_utils import gen_database_key
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
    fetch_procedures_from_query,
    generate_procedure_container_workunits,
    make_temp_table_checker,
)
from datahub.ingestion.source.sql.stored_procedures.config import (
    StoredProcedureConfigMixin,
)
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.schema_classes import BytesTypeClass
from datahub.utilities.str_enum import StrEnum

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

# Regex for CREATE [GLOBAL] TEMPORARY TABLE [IF NOT EXISTS] [db.]table
TEMP_TABLE_PATTERN = re.compile(
    r"CREATE\s+(?:GLOBAL\s+)?TEMPORARY\s+TABLE\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:`?(\w+)`?\.)?`?(\w+)`?",
    re.IGNORECASE,
)

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


class MySQLAuthMode(StrEnum):
    """Authentication mode for MySQL connection."""

    PASSWORD = "PASSWORD"
    AWS_IAM = "AWS_IAM"


class MySQLConnectionConfig(SQLAlchemyConnectionConfig):
    # defaults
    host_port: str = Field(default="localhost:3306", description="MySQL host URL.")
    scheme: HiddenFromDocs[str] = "mysql+pymysql"

    # Authentication configuration
    auth_mode: MySQLAuthMode = Field(
        default=MySQLAuthMode.PASSWORD,
        description="Authentication mode to use for the MySQL connection. "
        "Options are 'PASSWORD' (default) for standard username/password authentication, "
        "or 'AWS_IAM' for AWS RDS IAM authentication.",
    )
    aws_config: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description="AWS configuration for RDS IAM authentication (only used when auth_mode is AWS_IAM). "
        "Provides full control over AWS credentials, region, profiles, role assumption, retry logic, and proxy settings. "
        "If not explicitly configured, boto3 will automatically use the default credential chain and region from "
        "environment variables (AWS_DEFAULT_REGION, AWS_REGION), AWS config files (~/.aws/config), or IAM role metadata.",
    )


class MySQLConfig(
    MySQLConnectionConfig, TwoTierSQLAlchemyConfig, StoredProcedureConfigMixin
):
    # Override procedure_pattern with MySQL-specific description and example
    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion. "
        "Specify regex to match the entire procedure name in database.procedure_name format (two-tier). "
        "e.g. to match all procedures starting with 'sp_customer' in 'sales' database, use the regex 'sales.sp_customer.*'",
    )

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

    def __init__(self, config: MySQLConfig, ctx: Any):
        super().__init__(config, ctx, self.get_platform())

        self._rds_iam_token_manager: Optional[RDSIAMTokenManager] = None
        if config.auth_mode == MySQLAuthMode.AWS_IAM:
            hostname, port = parse_host_port(config.host_port, default_port=3306)
            if port is None:
                raise ValueError("Port must be specified for RDS IAM authentication")

            if not config.username:
                raise ValueError("username is required for RDS IAM authentication")

            self._rds_iam_token_manager = RDSIAMTokenManager(
                endpoint=hostname,
                username=config.username,
                port=port,
                aws_config=config.aws_config,
            )

    def get_platform(self):
        return "mysql"

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _parse_datetime(value: Optional[Any]) -> Optional[datetime]:
        """
        Convert a timestamp value to datetime or return None.

        Handles both string and datetime objects from SQLAlchemy.
        """
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except (ValueError, TypeError):
            return None

    # Note: MySQL relies on base class get_schema_level_workunits which handles
    # stored procedure ingestion with two-layer error handling:
    # 1. Base class catch-all in get_schema_level_workunits
    # 2. MySQL-specific error handling in get_procedures_for_schema
    # This ensures errors are properly handled and table ingestion continues

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

    def get_temp_table_checker(
        self, procedure: BaseProcedure, schema: str, db_name: str
    ) -> Optional[Callable[[str], bool]]:
        """Return a function to check if a table name is a MySQL temporary table."""
        return make_temp_table_checker(
            procedure.procedure_definition, TEMP_TABLE_PATTERN, str.lower
        )

    def _setup_rds_iam_event_listener(
        self, engine: "Engine", database_name: Optional[str] = None
    ) -> None:
        """Setup SQLAlchemy event listener to inject RDS IAM tokens."""
        if not (
            self.config.auth_mode == MySQLAuthMode.AWS_IAM
            and self._rds_iam_token_manager
        ):
            return

        def do_connect_listener(_dialect, _conn_rec, _cargs, cparams):
            if not self._rds_iam_token_manager:
                raise RuntimeError("RDS IAM Token Manager is not initialized")
            cparams["password"] = self._rds_iam_token_manager.get_token()
            # PyMySQL requires SSL to be enabled for RDS IAM authentication.
            # Preserve any existing SSL configuration, otherwise enable with default settings.
            # The {"ssl": True} dict is a workaround to make PyMySQL recognize that SSL
            # should be enabled, since the library requires a truthy value in the ssl parameter.
            # See https://pymysql.readthedocs.io/en/latest/modules/connections.html#pymysql.connections.Connection
            cparams["ssl"] = cparams.get("ssl") or {"ssl": True}

        event.listen(engine, "do_connect", do_connect_listener)  # type: ignore[misc]

    def get_inspectors(self):
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, **self.config.options)
        self._setup_rds_iam_event_listener(engine)

        with engine.connect() as conn:
            inspector = inspect(conn)
            if self.config.database and self.config.database != "":
                databases = [self.config.database]
            else:
                databases = inspector.get_schema_names()
            for db in databases:
                if self.config.database_pattern.allowed(db):
                    url = self.config.get_sql_alchemy_url(current_db=db)
                    db_engine = create_engine(url, **self.config.options)
                    self._setup_rds_iam_event_listener(db_engine, database_name=db)

                    with db_engine.connect() as conn:
                        inspector = inspect(conn)
                        yield inspector

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

        def map_row(row: Row) -> Optional[BaseProcedure]:
            if not row.name:
                return None

            return BaseProcedure(
                name=row.name,
                language="SQL",
                argument_signature=None,
                return_type=None,
                procedure_definition=row.definition,
                created=self._parse_datetime(row.CREATED),
                last_altered=self._parse_datetime(row.LAST_ALTERED),
                comment=row.comment,
                extra_properties={
                    k: v
                    for k, v in {
                        "sql_data_access": row.SQL_DATA_ACCESS,
                        "security_type": row.SECURITY_TYPE,
                        "definer": row.DEFINER,
                    }.items()
                    if v is not None
                },
            )

        return fetch_procedures_from_query(
            inspector=inspector,
            query=STORED_PROCEDURES_QUERY,
            params={"schema": schema},
            row_mapper=map_row,
            source_name="MySQL",
            schema=schema,
            report=self.report,
            permission_error_message="Failed to access stored procedure metadata. Grant SELECT permission on information_schema.ROUTINES or disable with 'include_stored_procedures: false'.",
            system_table="information_schema.ROUTINES",
        )
