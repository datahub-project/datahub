import re
from typing import Iterable, List, Union

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import text, util
# This import verifies that the dependencies are available.
import logging
from typing import TYPE_CHECKING, Any, List, Optional

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
    SqlWorkUnit,
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.sql_utils import (
    gen_database_key,
)
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port
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
        base_procedures = []
        with inspector.engine.connect() as conn:
            procedures = conn.execute(
                text(STORED_PROCEDURES_QUERY),
                {"schema": schema},
            )

            procedure_rows: List[Row] = list(procedures)
            for row in procedure_rows:
                # Convert SQLAlchemy Row to dict for easier and safer access
                row_dict = dict(row)

                # Extract name - name is required for BaseProcedure
                procedure_name = row_dict.get("name")
                if not procedure_name:
                    # Skip procedures without names
                    continue

                base_procedures.append(
                    BaseProcedure(
                        name=procedure_name,
                        language="SQL",
                        argument_signature=None,
                        return_type=None,
                        procedure_definition=row_dict.get("definition"),
                        created=row_dict.get("CREATED"),
                        last_altered=row_dict.get("LAST_ALTERED"),
                        comment=row_dict.get("comment"),
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
            return base_procedures
