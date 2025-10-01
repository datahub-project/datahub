# This import verifies that the dependencies are available.
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, List, Optional

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import util
from sqlalchemy.dialects.mysql import BIT, base
from sqlalchemy.dialects.mysql.enumerated import SET
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
from datahub.ingestion.source.aws.aws_common import (
    AwsConnectionConfig,
    RDSIAMTokenManager,
)
from datahub.ingestion.source.sql.sql_common import (
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port
from datahub.ingestion.source.sql.stored_procedures.base import (
    BaseProcedure,
)
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata.schema_classes import BytesTypeClass

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


class MySQLAuthMode(str, Enum):
    """Authentication mode for MySQL connection."""

    PASSWORD = "PASSWORD"
    IAM = "IAM"


class MySQLConnectionConfig(SQLAlchemyConnectionConfig):
    # defaults
    host_port: str = Field(default="localhost:3306", description="MySQL host URL.")
    scheme: HiddenFromDocs[str] = "mysql+pymysql"

    # Authentication configuration
    auth_mode: MySQLAuthMode = Field(
        default=MySQLAuthMode.PASSWORD,
        description="Authentication mode to use for the MySQL connection. "
        "Options are 'PASSWORD' (default) for standard username/password authentication, "
        "or 'IAM' for AWS RDS IAM authentication.",
    )
    aws_config: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description="AWS configuration for RDS IAM authentication (only used when auth_mode is IAM). "
        "Provides full control over AWS credentials, region, profiles, role assumption, retry logic, and proxy settings. "
        "If not explicitly configured, boto3 will automatically use the default credential chain and region from "
        "environment variables (AWS_DEFAULT_REGION, AWS_REGION), AWS config files (~/.aws/config), or IAM role metadata.",
    )


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

    config: MySQLConfig
    _rds_iam_hostname: Optional[str] = None
    _rds_iam_port: Optional[int] = None

    def __init__(self, config: MySQLConfig, ctx: Any):
        super().__init__(config, ctx, self.get_platform())

        self._rds_iam_token_manager: Optional[RDSIAMTokenManager] = None
        if config.auth_mode == MySQLAuthMode.IAM:
            # wh Extract and store hostname/port for reuse
            self._rds_iam_hostname, parsed_port = parse_host_port(
                config.host_port, default_port=3306
            )
            # parse_host_port returns Optional[int], but we need int for RDSIAMTokenManager
            self._rds_iam_port = parsed_port if parsed_port is not None else 3306

            if not config.username:
                raise ValueError("username is required for RDS IAM authentication")

            self._rds_iam_token_manager = RDSIAMTokenManager(
                endpoint=self._rds_iam_hostname,
                username=config.username,
                port=self._rds_iam_port,
                aws_config=config.aws_config,
            )

    def get_platform(self):
        return "mysql"

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _setup_rds_iam_event_listener(
        self, engine: "Engine", database_name: Optional[str] = None
    ) -> None:
        """Setup SQLAlchemy event listener to inject RDS IAM tokens."""
        from sqlalchemy import event

        if not (
            self.config.auth_mode == MySQLAuthMode.IAM and self._rds_iam_token_manager
        ):
            return

        # Type narrowing: assert token manager is not None after the guard check
        assert self._rds_iam_token_manager is not None

        @event.listens_for(engine, "do_connect")  # type: ignore[misc]
        def provide_token(
            dialect: Any, conn_rec: Any, cargs: Any, cparams: Any
        ) -> None:
            """Inject fresh RDS IAM token and SSL config before each connection."""
            assert self._rds_iam_token_manager is not None
            token = self._rds_iam_token_manager.get_token()
            cparams["host"] = self._rds_iam_hostname
            cparams["port"] = self._rds_iam_port
            cparams["user"] = self.config.username
            cparams["password"] = token
            cparams["database"] = database_name or self.config.database

            # Merge user-provided SSL settings with required RDS IAM settings
            user_ssl = cparams.get("ssl", {})
            if isinstance(user_ssl, dict):
                user_ssl["ssl"] = True
                cparams["ssl"] = user_ssl
            else:
                cparams["ssl"] = {"ssl": True}

            # Merge user-provided auth_plugin_map with required RDS IAM plugin
            user_auth_plugins = cparams.get("auth_plugin_map", {})
            if isinstance(user_auth_plugins, dict):
                user_auth_plugins["mysql_clear_password"] = None
                cparams["auth_plugin_map"] = user_auth_plugins
            else:
                cparams["auth_plugin_map"] = {"mysql_clear_password": None}

            logger.debug(
                f"Injected RDS IAM token for connection to {self._rds_iam_hostname}:{self._rds_iam_port}"
                + (f" (database: {database_name})" if database_name else "")
            )

    def get_inspectors(self):
        from sqlalchemy import create_engine, inspect

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")

        # Configure engine options for RDS IAM authentication
        engine_options = dict(self.config.options)
        if self.config.auth_mode == MySQLAuthMode.IAM:
            engine_options.setdefault("pool_recycle", 600)
            engine_options.setdefault("pool_pre_ping", True)
            logger.debug(
                "RDS IAM enabled: setting pool_recycle=600 and pool_pre_ping=True"
            )

        engine = create_engine(url, **engine_options)
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

                    # Configure engine options for database-specific connections
                    db_engine_options = dict(self.config.options)
                    if self.config.auth_mode == MySQLAuthMode.IAM:
                        db_engine_options.setdefault("pool_recycle", 600)
                        db_engine_options.setdefault("pool_pre_ping", True)

                    db_engine = create_engine(url, **db_engine_options)
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
                """
                SELECT ROUTINE_NAME AS name, 
                    ROUTINE_DEFINITION AS definition, 
                    EXTERNAL_LANGUAGE AS language
                FROM information_schema.ROUTINES
                WHERE ROUTINE_TYPE = 'PROCEDURE'
                AND ROUTINE_SCHEMA = %s
                """,
                (schema,),
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
