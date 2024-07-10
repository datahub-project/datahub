import abc
from typing import ClassVar, Literal, Optional, Tuple

from typing_extensions import Protocol

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
    def structured_reporter(self) -> SourceReport:
        ...

    # TODO: Eventually I want to deprecate these methods and use the structured_reporter directly.
    def report_warning(self, key: str, reason: str) -> None:
        self.structured_reporter.warning(key, reason)

    def report_error(self, key: str, reason: str) -> None:
        self.structured_reporter.failure(key, reason)


# Required only for mypy, since we are using mixin classes, and not inheritance.
# Reference - https://mypy.readthedocs.io/en/latest/more_types.html#mixin-classes
class SnowflakeCommonProtocol(Protocol):
    platform: str = "snowflake"

    config: SnowflakeV2Config
    report: SnowflakeV2Report

    def get_dataset_identifier(
        self, table_name: str, schema_name: str, db_name: str
    ) -> str:
        ...

    def cleanup_qualified_name(self, qualified_name: str) -> str:
        ...

    def get_dataset_identifier_from_qualified_name(self, qualified_name: str) -> str:
        ...

    def snowflake_identifier(self, identifier: str) -> str:
        ...

    def report_warning(self, key: str, reason: str) -> None:
        ...

    def report_error(self, key: str, reason: str) -> None:
        ...


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


class SnowflakeFilterMixin(SnowflakeStructuredReportMixin):
    @property
    @abc.abstractmethod
    def filter_config(self) -> SnowflakeFilterConfig:
        ...

    @staticmethod
    def _combine_identifier_parts(
        table_name: str, schema_name: str, db_name: str
    ) -> str:
        return f"{db_name}.{schema_name}.{table_name}"

    def is_dataset_pattern_allowed(
        self,
        dataset_name: Optional[str],
        dataset_type: Optional[str],
    ) -> bool:
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

        if not self.filter_config.database_pattern.allowed(
            dataset_params[0].strip('"')
        ) or not is_schema_allowed(
            self.filter_config.schema_pattern,
            dataset_params[1].strip('"'),
            dataset_params[0].strip('"'),
            self.filter_config.match_fully_qualified_names,
        ):
            return False

        if dataset_type.lower() in {
            SnowflakeObjectDomain.TABLE
        } and not self.filter_config.table_pattern.allowed(
            self.cleanup_qualified_name(dataset_name)
        ):
            return False

        if dataset_type.lower() in {
            SnowflakeObjectDomain.VIEW,
            SnowflakeObjectDomain.MATERIALIZED_VIEW,
        } and not self.filter_config.view_pattern.allowed(
            self.cleanup_qualified_name(dataset_name)
        ):
            return False

        return True

    # Qualified Object names from snowflake audit logs have quotes for for snowflake quoted identifiers,
    # For example "test-database"."test-schema".test_table
    # whereas we generate urns without quotes even for quoted identifiers for backward compatibility
    # and also unavailability of utility function to identify whether current table/schema/database
    # name should be quoted in above method get_dataset_identifier
    def cleanup_qualified_name(self, qualified_name: str) -> str:
        name_parts = qualified_name.split(".")
        if len(name_parts) != 3:
            self.structured_reporter.report_warning(
                title="Unexpected dataset pattern",
                message="We failed to parse a Snowflake qualified name into its constituent parts. "
                "DB/schema/table filtering may not work as expected on these entities.",
                context=f"{qualified_name} has {len(name_parts)} parts",
            )
            return qualified_name.replace('"', "")
        return SnowflakeFilterMixin._combine_identifier_parts(
            table_name=name_parts[2].strip('"'),
            schema_name=name_parts[1].strip('"'),
            db_name=name_parts[0].strip('"'),
        )


class SnowflakeIdentifierMixin(abc.ABC):
    platform = "snowflake"

    @property
    @abc.abstractmethod
    def identifier_config(self) -> SnowflakeIdentifierConfig:
        ...

    def snowflake_identifier(self, identifier: str) -> str:
        # to be in in sync with older connector, convert name to lowercase
        if self.identifier_config.convert_urns_to_lowercase:
            return identifier.lower()
        return identifier

    def get_dataset_identifier(
        self, table_name: str, schema_name: str, db_name: str
    ) -> str:
        return self.snowflake_identifier(
            SnowflakeCommonMixin._combine_identifier_parts(
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


# TODO: We're most of the way there on fully removing SnowflakeCommonProtocol.
class SnowflakeCommonMixin(SnowflakeFilterMixin, SnowflakeIdentifierMixin):
    @property
    def structured_reporter(self: SnowflakeCommonProtocol) -> SourceReport:
        return self.report

    @property
    def filter_config(self: SnowflakeCommonProtocol) -> SnowflakeFilterConfig:
        return self.config

    @property
    def identifier_config(self: SnowflakeCommonProtocol) -> SnowflakeIdentifierConfig:
        return self.config

    @staticmethod
    def get_quoted_identifier_for_database(db_name):
        return f'"{db_name}"'

    @staticmethod
    def get_quoted_identifier_for_schema(db_name, schema_name):
        return f'"{db_name}"."{schema_name}"'

    def get_dataset_identifier_from_qualified_name(self, qualified_name: str) -> str:
        return self.snowflake_identifier(self.cleanup_qualified_name(qualified_name))

    @staticmethod
    def get_quoted_identifier_for_table(db_name, schema_name, table_name):
        return f'"{db_name}"."{schema_name}"."{table_name}"'

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
