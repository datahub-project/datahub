import logging
from typing import Any, Optional

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor
from typing_extensions import Protocol

from datahub.configuration.common import MetaError
from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.ingestion.source.snowflake.constants import (
    GENERIC_PERMISSION_ERROR_KEY,
    SNOWFLAKE_DEFAULT_CLOUD,
    SNOWFLAKE_REGION_CLOUD_REGION_MAPPING,
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakePermissionError(MetaError):
    """A permission error has happened"""


# Required only for mypy, since we are using mixin classes, and not inheritance.
# Reference - https://mypy.readthedocs.io/en/latest/more_types.html#mixin-classes
class SnowflakeLoggingProtocol(Protocol):
    logger: logging.Logger


class SnowflakeQueryProtocol(SnowflakeLoggingProtocol, Protocol):
    def get_connection(self) -> SnowflakeConnection:
        ...


class SnowflakeQueryMixin:
    def query(self: SnowflakeQueryProtocol, query: str) -> Any:
        try:
            self.logger.debug("Query : {}".format(query))
            resp = self.get_connection().cursor(DictCursor).execute(query)
            return resp

        except Exception as e:
            if is_permission_error(e):
                raise SnowflakePermissionError(e) from e
            raise


class SnowflakeCommonProtocol(SnowflakeLoggingProtocol, Protocol):
    config: SnowflakeV2Config
    report: SnowflakeV2Report

    def get_dataset_identifier(
        self, table_name: str, schema_name: str, db_name: str
    ) -> str:
        ...

    def get_dataset_identifier_from_qualified_name(self, qualified_name: str) -> str:
        ...

    def snowflake_identifier(self, identifier: str) -> str:
        ...

    def report_warning(self, key: str, reason: str) -> None:
        ...

    def report_error(self, key: str, reason: str) -> None:
        ...


class SnowflakeCommonMixin:
    platform = "snowflake"

    @staticmethod
    def create_snowsight_base_url(
        account_locator: str,
        cloud_region_id: str,
        cloud: str,
        privatelink: bool = False,
    ) -> Optional[str]:
        if privatelink:
            url = f"https://app.{account_locator}.{cloud_region_id}.privatelink.snowflakecomputing.com/"
        elif cloud == SNOWFLAKE_DEFAULT_CLOUD:
            url = f"https://app.snowflake.com/{cloud_region_id}/{account_locator}/"
        else:
            url = f"https://app.snowflake.com/{cloud_region_id}.{cloud}/{account_locator}/"
        return url

    @staticmethod
    def get_cloud_region_from_snowflake_region_id(region):
        if region in SNOWFLAKE_REGION_CLOUD_REGION_MAPPING.keys():
            cloud, cloud_region_id = SNOWFLAKE_REGION_CLOUD_REGION_MAPPING[region]
        elif region.startswith(("aws_", "gcp_", "azure_")):
            # e.g. aws_us_west_2, gcp_us_central1, azure_northeurope
            cloud, cloud_region_id = region.split("_", 1)
            cloud_region_id = cloud_region_id.replace("_", "-")
        else:
            raise Exception(f"Unknown snowflake region {region}")
        return cloud, cloud_region_id

    def _is_dataset_pattern_allowed(
        self: SnowflakeCommonProtocol,
        dataset_name: Optional[str],
        dataset_type: Optional[str],
        is_upstream: bool = False,
    ) -> bool:
        if is_upstream and not self.config.validate_upstreams_against_patterns:
            return True
        if not dataset_type or not dataset_name:
            return True
        dataset_params = dataset_name.split(".")
        if dataset_type.lower() not in (
            SnowflakeObjectDomain.TABLE,
            SnowflakeObjectDomain.EXTERNAL_TABLE,
            SnowflakeObjectDomain.VIEW,
            SnowflakeObjectDomain.MATERIALIZED_VIEW,
        ):
            return False
        if len(dataset_params) != 3:
            self.report_warning(
                "invalid-dataset-pattern",
                f"Found {dataset_params} of type {dataset_type}",
            )
            # NOTE: this case returned `True` earlier when extracting lineage
            return False

        if not self.config.database_pattern.allowed(
            dataset_params[0].strip('"')
        ) or not is_schema_allowed(
            self.config.schema_pattern,
            dataset_params[1].strip('"'),
            dataset_params[0].strip('"'),
            self.config.match_fully_qualified_names,
        ):
            return False

        if dataset_type.lower() in {
            SnowflakeObjectDomain.TABLE
        } and not self.config.table_pattern.allowed(
            self.get_dataset_identifier_from_qualified_name(dataset_name)
        ):
            return False

        if dataset_type.lower() in {
            "view",
            "materialized_view",
        } and not self.config.view_pattern.allowed(
            self.get_dataset_identifier_from_qualified_name(dataset_name)
        ):
            return False

        return True

    def snowflake_identifier(self: SnowflakeCommonProtocol, identifier: str) -> str:
        # to be in in sync with older connector, convert name to lowercase
        if self.config.convert_urns_to_lowercase:
            return identifier.lower()
        return identifier

    @staticmethod
    def get_quoted_identifier_for_database(db_name):
        return f'"{db_name}"'

    @staticmethod
    def get_quoted_identifier_for_schema(db_name, schema_name):
        return f'"{db_name}"."{schema_name}"'

    @staticmethod
    def get_quoted_identifier_for_table(db_name, schema_name, table_name):
        return f'"{db_name}"."{schema_name}"."{table_name}"'

    def get_dataset_identifier(
        self: SnowflakeCommonProtocol, table_name: str, schema_name: str, db_name: str
    ) -> str:
        return self.snowflake_identifier(f"{db_name}.{schema_name}.{table_name}")

    # Qualified Object names from snowflake audit logs have quotes for for snowflake quoted identifiers,
    # For example "test-database"."test-schema".test_table
    # whereas we generate urns without quotes even for quoted identifiers for backward compatibility
    # and also unavailability of utility function to identify whether current table/schema/database
    # name should be quoted in above method get_dataset_identifier
    def get_dataset_identifier_from_qualified_name(
        self: SnowflakeCommonProtocol, qualified_name: str
    ) -> str:
        name_parts = qualified_name.split(".")
        if len(name_parts) != 3:
            self.report.report_warning(
                "invalid-dataset-pattern",
                f"Found non-parseable {name_parts} for {qualified_name}",
            )
            return self.snowflake_identifier(qualified_name.replace('"', ""))
        return self.get_dataset_identifier(
            name_parts[2].strip('"'), name_parts[1].strip('"'), name_parts[0].strip('"')
        )

    # Note - decide how to construct user urns.
    # Historically urns were created using part before @ from user's email.
    # Users without email were skipped from both user entries as well as aggregates.
    # However email is not mandatory field in snowflake user, user_name is always present.
    def get_user_identifier(
        self: SnowflakeCommonProtocol,
        user_name: str,
        user_email: Optional[str],
        email_as_user_identifier: bool,
    ) -> str:
        if user_email:
            return self.snowflake_identifier(
                user_email
                if email_as_user_identifier is True
                else user_email.split("@")[0]
            )
        return self.snowflake_identifier(user_name)

    # TODO: Revisit this after stateful ingestion can commit checkpoint
    # for failures that do not affect the checkpoint
    def warn_if_stateful_else_error(
        self: SnowflakeCommonProtocol, key: str, reason: str
    ) -> None:
        if (
            self.config.stateful_ingestion is not None
            and self.config.stateful_ingestion.enabled
        ):
            self.report_warning(key, reason)
        else:
            self.report_error(key, reason)

    def report_warning(self: SnowflakeCommonProtocol, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        self.logger.warning(f"{key} => {reason}")

    def report_error(self: SnowflakeCommonProtocol, key: str, reason: str) -> None:
        self.report.report_failure(key, reason)
        self.logger.error(f"{key} => {reason}")


class SnowflakeConnectionProtocol(SnowflakeLoggingProtocol, Protocol):
    connection: Optional[SnowflakeConnection]
    config: SnowflakeV2Config
    report: SnowflakeV2Report

    def create_connection(self) -> Optional[SnowflakeConnection]:
        ...

    def report_error(self, key: str, reason: str) -> None:
        ...


class SnowflakeConnectionMixin:
    def get_connection(self: SnowflakeConnectionProtocol) -> SnowflakeConnection:
        if self.connection is None:
            # Ideally this is never called here
            self.logger.info("Did you forget to initialize connection for module?")
            self.connection = self.create_connection()

        # Connection is already present by the time its used for query
        # Every module initializes the connection or fails and returns
        assert self.connection is not None
        return self.connection

    # If connection succeeds, return connection, else return None and report failure
    def create_connection(
        self: SnowflakeConnectionProtocol,
    ) -> Optional[SnowflakeConnection]:
        try:
            conn = self.config.get_connection()
        except Exception as e:
            logger.debug(e, exc_info=e)
            if "not granted to this user" in str(e):
                self.report_error(
                    GENERIC_PERMISSION_ERROR_KEY,
                    f"Failed to connect with snowflake due to error {e}",
                )
            else:
                logger.debug(e, exc_info=e)
                self.report_error(
                    "snowflake-connection",
                    f"Failed to connect to snowflake instance due to error {e}.",
                )
            return None
        else:
            return conn

    def close(self: SnowflakeConnectionProtocol) -> None:
        if self.connection is not None and not self.connection.is_closed():
            self.connection.close()


def is_permission_error(e: Exception) -> bool:
    msg = str(e)
    # 002003 (02000): SQL compilation error: Database/SCHEMA 'XXXX' does not exist or not authorized.
    # Insufficient privileges to operate on database 'XXXX'
    return "Insufficient privileges" in msg or "not authorized" in msg
