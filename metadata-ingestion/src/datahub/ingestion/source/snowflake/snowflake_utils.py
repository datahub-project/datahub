import abc
from functools import cached_property
from typing import ClassVar, List, Literal, Optional, Tuple

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.snowflake.constants import (
    SNOWFLAKE_REGION_CLOUD_REGION_MAPPING,
    SnowflakeCloudProvider,
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeFilterConfig,
    SnowflakeIdentifierConfig,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report


class SnowflakeStructuredReportMixin(abc.ABC):
    @property
    @abc.abstractmethod
    def structured_reporter(self) -> SourceReport: ...


class SnowsightUrlBuilder:
    CLOUD_REGION_IDS_WITHOUT_CLOUD_SUFFIX: ClassVar = [
        "us-west-2",
        "us-east-1",
        "eu-west-1",
        "eu-central-1",
        "ap-southeast-1",
        "ap-southeast-2",
    ]

    snowsight_base_url: str

    def __init__(self, account_locator: str, region: str, privatelink: bool = False):
        cloud, cloud_region_id = self.get_cloud_region_from_snowflake_region_id(region)
        self.snowsight_base_url = self.create_snowsight_base_url(
            account_locator, cloud_region_id, cloud, privatelink
        )

    @staticmethod
    def create_snowsight_base_url(
        account_locator: str,
        cloud_region_id: str,
        cloud: str,
        privatelink: bool = False,
    ) -> str:
        if cloud:
            url_cloud_provider_suffix = f".{cloud}"

        if cloud == SnowflakeCloudProvider.AWS:
            # Some AWS regions do not have cloud suffix. See below the list:
            # https://docs.snowflake.com/en/user-guide/admin-account-identifier#non-vps-account-locator-formats-by-cloud-platform-and-region
            if (
                cloud_region_id
                in SnowsightUrlBuilder.CLOUD_REGION_IDS_WITHOUT_CLOUD_SUFFIX
            ):
                url_cloud_provider_suffix = ""
            else:
                url_cloud_provider_suffix = f".{cloud}"
        if privatelink:
            url = f"https://app.{account_locator}.{cloud_region_id}.privatelink.snowflakecomputing.com/"
        else:
            url = f"https://app.snowflake.com/{cloud_region_id}{url_cloud_provider_suffix}/{account_locator}/"
        return url

    @staticmethod
    def get_cloud_region_from_snowflake_region_id(
        region: str,
    ) -> Tuple[str, str]:
        cloud: str
        if region in SNOWFLAKE_REGION_CLOUD_REGION_MAPPING.keys():
            cloud, cloud_region_id = SNOWFLAKE_REGION_CLOUD_REGION_MAPPING[region]
        elif region.startswith(("aws_", "gcp_", "azure_")):
            # e.g. aws_us_west_2, gcp_us_central1, azure_northeurope
            cloud, cloud_region_id = region.split("_", 1)
            cloud_region_id = cloud_region_id.replace("_", "-")
        else:
            raise Exception(f"Unknown snowflake region {region}")
        return cloud, cloud_region_id

    # domain is either "view" or "table"
    def get_external_url_for_table(
        self,
        table_name: str,
        schema_name: str,
        db_name: str,
        domain: Literal[SnowflakeObjectDomain.TABLE, SnowflakeObjectDomain.VIEW],
    ) -> Optional[str]:
        return f"{self.snowsight_base_url}#/data/databases/{db_name}/schemas/{schema_name}/{domain}/{table_name}/"

    def get_external_url_for_schema(
        self, schema_name: str, db_name: str
    ) -> Optional[str]:
        return f"{self.snowsight_base_url}#/data/databases/{db_name}/schemas/{schema_name}/"

    def get_external_url_for_database(self, db_name: str) -> Optional[str]:
        return f"{self.snowsight_base_url}#/data/databases/{db_name}/"


class SnowflakeFilter:
    def __init__(
        self, filter_config: SnowflakeFilterConfig, structured_reporter: SourceReport
    ) -> None:
        self.filter_config = filter_config
        self.structured_reporter = structured_reporter

    # TODO: Refactor remaining filtering logic into this class.

    def is_dataset_pattern_allowed(
        self,
        dataset_name: Optional[str],
        dataset_type: Optional[str],
    ) -> bool:
        if not dataset_type or not dataset_name:
            return True
        if dataset_type.lower() not in (
            SnowflakeObjectDomain.TABLE,
            SnowflakeObjectDomain.EXTERNAL_TABLE,
            SnowflakeObjectDomain.VIEW,
            SnowflakeObjectDomain.MATERIALIZED_VIEW,
            SnowflakeObjectDomain.ICEBERG_TABLE,
        ):
            return False
        if _is_sys_table(dataset_name):
            return False

        dataset_params = _split_qualified_name(dataset_name)
        if len(dataset_params) != 3:
            self.structured_reporter.info(
                title="Unexpected dataset pattern",
                message=f"Found a {dataset_type} with an unexpected number of parts. Database and schema filtering will not work as expected, but table filtering will still work.",
                context=dataset_name,
            )
            # We fall-through here so table/view filtering still works.

        if (
            len(dataset_params) >= 1
            and not self.filter_config.database_pattern.allowed(
                dataset_params[0].strip('"')
            )
        ) or (
            len(dataset_params) >= 2
            and not is_schema_allowed(
                self.filter_config.schema_pattern,
                dataset_params[1].strip('"'),
                dataset_params[0].strip('"'),
                self.filter_config.match_fully_qualified_names,
            )
        ):
            return False

        if dataset_type.lower() in {
            SnowflakeObjectDomain.TABLE
        } and not self.filter_config.table_pattern.allowed(
            _cleanup_qualified_name(dataset_name, self.structured_reporter)
        ):
            return False

        if dataset_type.lower() in {
            SnowflakeObjectDomain.VIEW,
            SnowflakeObjectDomain.MATERIALIZED_VIEW,
        } and not self.filter_config.view_pattern.allowed(
            _cleanup_qualified_name(dataset_name, self.structured_reporter)
        ):
            return False

        return True


def _combine_identifier_parts(
    *, table_name: str, schema_name: str, db_name: str
) -> str:
    return f"{db_name}.{schema_name}.{table_name}"


def _is_sys_table(table_name: str) -> bool:
    # Often will look like `SYS$_UNPIVOT_VIEW1737` or `sys$_pivot_view19`.
    return table_name.lower().startswith("sys$")


def _split_qualified_name(qualified_name: str) -> List[str]:
    """
    Split a qualified name into its constituent parts.

    >>> _split_qualified_name("db.my_schema.my_table")
    ['db', 'my_schema', 'my_table']
    >>> _split_qualified_name('"db"."my_schema"."my_table"')
    ['db', 'my_schema', 'my_table']
    >>> _split_qualified_name('TEST_DB.TEST_SCHEMA."TABLE.WITH.DOTS"')
    ['TEST_DB', 'TEST_SCHEMA', 'TABLE.WITH.DOTS']
    >>> _split_qualified_name('TEST_DB."SCHEMA.WITH.DOTS".MY_TABLE')
    ['TEST_DB', 'SCHEMA.WITH.DOTS', 'MY_TABLE']
    """

    # Fast path - no quotes.
    if '"' not in qualified_name:
        return qualified_name.split(".")

    # First pass - split on dots that are not inside quotes.
    in_quote = False
    parts: List[List[str]] = [[]]
    for char in qualified_name:
        if char == '"':
            in_quote = not in_quote
        elif char == "." and not in_quote:
            parts.append([])
        else:
            parts[-1].append(char)

    # Second pass - remove outer pairs of quotes.
    result = []
    for part in parts:
        if len(part) > 2 and part[0] == '"' and part[-1] == '"':
            part = part[1:-1]

        result.append("".join(part))

    return result


# Qualified Object names from snowflake audit logs have quotes for for snowflake quoted identifiers,
# For example "test-database"."test-schema".test_table
# whereas we generate urns without quotes even for quoted identifiers for backward compatibility
# and also unavailability of utility function to identify whether current table/schema/database
# name should be quoted in above method get_dataset_identifier
def _cleanup_qualified_name(
    qualified_name: str, structured_reporter: SourceReport
) -> str:
    name_parts = _split_qualified_name(qualified_name)
    if len(name_parts) != 3:
        if not _is_sys_table(qualified_name):
            structured_reporter.info(
                title="Unexpected dataset pattern",
                message="We failed to parse a Snowflake qualified name into its constituent parts. "
                "DB/schema/table filtering may not work as expected on these entities.",
                context=f"{qualified_name} has {len(name_parts)} parts",
            )
        return qualified_name.replace('"', "")
    return _combine_identifier_parts(
        db_name=name_parts[0],
        schema_name=name_parts[1],
        table_name=name_parts[2],
    )


class SnowflakeIdentifierBuilder:
    platform = "snowflake"

    def __init__(
        self,
        identifier_config: SnowflakeIdentifierConfig,
        structured_reporter: SourceReport,
    ) -> None:
        self.identifier_config = identifier_config
        self.structured_reporter = structured_reporter

    def snowflake_identifier(self, identifier: str) -> str:
        # to be in in sync with older connector, convert name to lowercase
        if self.identifier_config.convert_urns_to_lowercase:
            return identifier.lower()
        return identifier

    def get_dataset_identifier(
        self, table_name: str, schema_name: str, db_name: str
    ) -> str:
        return self.snowflake_identifier(
            _combine_identifier_parts(
                table_name=table_name, schema_name=schema_name, db_name=db_name
            )
        )

    def gen_dataset_urn(self, dataset_identifier: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_identifier,
            platform_instance=self.identifier_config.platform_instance,
            env=self.identifier_config.env,
        )

    def get_dataset_identifier_from_qualified_name(self, qualified_name: str) -> str:
        return self.snowflake_identifier(
            _cleanup_qualified_name(qualified_name, self.structured_reporter)
        )

    @staticmethod
    def get_quoted_identifier_for_database(db_name):
        return f'"{db_name}"'

    @staticmethod
    def get_quoted_identifier_for_schema(db_name, schema_name):
        return f'"{db_name}"."{schema_name}"'

    @staticmethod
    def get_quoted_identifier_for_table(db_name, schema_name, table_name):
        return f'"{db_name}"."{schema_name}"."{table_name}"'

    # Note - decide how to construct user urns.
    # Historically urns were created using part before @ from user's email.
    # Users without email were skipped from both user entries as well as aggregates.
    # However email is not mandatory field in snowflake user, user_name is always present.
    def get_user_identifier(
        self,
        user_name: str,
        user_email: Optional[str],
    ) -> str:
        if user_email:
            return self.snowflake_identifier(
                user_email
                if self.identifier_config.email_as_user_identifier is True
                else user_email.split("@")[0]
            )
        return self.snowflake_identifier(
            f"{user_name}@{self.identifier_config.email_domain}"
            if self.identifier_config.email_as_user_identifier is True
            and self.identifier_config.email_domain is not None
            else user_name
        )


class SnowflakeCommonMixin(SnowflakeStructuredReportMixin):
    platform = "snowflake"

    config: SnowflakeV2Config
    report: SnowflakeV2Report

    @property
    def structured_reporter(self) -> SourceReport:
        return self.report

    @cached_property
    def identifiers(self) -> SnowflakeIdentifierBuilder:
        return SnowflakeIdentifierBuilder(self.config, self.report)

    # TODO: Revisit this after stateful ingestion can commit checkpoint
    # for failures that do not affect the checkpoint
    # TODO: Add additional parameters to match the signature of the .warning and .failure methods
    def warn_if_stateful_else_error(self, key: str, reason: str) -> None:
        if (
            self.config.stateful_ingestion is not None
            and self.config.stateful_ingestion.enabled
        ):
            self.structured_reporter.warning(key, reason)
        else:
            self.structured_reporter.failure(key, reason)
