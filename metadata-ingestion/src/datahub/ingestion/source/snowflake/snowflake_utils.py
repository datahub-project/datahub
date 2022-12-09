import logging
from enum import Enum
from typing import Any, Optional

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor
from typing_extensions import Protocol

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType
from datahub.metadata.schema_classes import _Aspect

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeCloudProvider(str, Enum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


# See https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#region-ids
# This needs an update if and wuhen snowflake supports new region
SNOWFLAKE_REGION_CLOUD_REGION_MAPPING = {
    "aws_us_west_2": (SnowflakeCloudProvider.AWS, "us-west-2"),
    "aws_us_gov_west_1": (SnowflakeCloudProvider.AWS, "us-gov-west-1"),
    "aws_us_east_2": (SnowflakeCloudProvider.AWS, "us-east-2"),
    "aws_us_east_1": (SnowflakeCloudProvider.AWS, "us-east-1"),
    "aws_us_east_1_gov": (SnowflakeCloudProvider.AWS, "us-east-1"),
    "aws_ca_central_1": (SnowflakeCloudProvider.AWS, "ca-central-1"),
    "aws_sa_east_1": (SnowflakeCloudProvider.AWS, "sa-east-1"),
    "aws_eu_west_1": (SnowflakeCloudProvider.AWS, "eu-west-1"),
    "aws_eu_west_2": (SnowflakeCloudProvider.AWS, "eu-west-2"),
    "aws_eu_west_3": (SnowflakeCloudProvider.AWS, "eu-west-3"),
    "aws_eu_central_1": (SnowflakeCloudProvider.AWS, "eu-central-1"),
    "aws_eu_north_1": (SnowflakeCloudProvider.AWS, "eu-north-1"),
    "aws_ap_northeast_1": (SnowflakeCloudProvider.AWS, "ap-northeast-1"),
    "aws_ap_northeast_3": (SnowflakeCloudProvider.AWS, "ap-northeast-3"),
    "aws_ap_northeast_2": (SnowflakeCloudProvider.AWS, "ap-northeast-2"),
    "aws_ap_south_1": (SnowflakeCloudProvider.AWS, "ap-south-1"),
    "aws_ap_southeast_1": (SnowflakeCloudProvider.AWS, "ap-southeast-1"),
    "aws_ap_southeast_2": (SnowflakeCloudProvider.AWS, "ap-southeast-2"),
    "gcp_us_central1": (SnowflakeCloudProvider.GCP, "us-central1"),
    "gcp_us_east4": (SnowflakeCloudProvider.GCP, "us-east4"),
    "gcp_europe_west2": (SnowflakeCloudProvider.GCP, "europe-west2"),
    "gcp_europe_west4": (SnowflakeCloudProvider.GCP, "europe-west4"),
    "azure_westus2": (SnowflakeCloudProvider.AZURE, "westus2"),
    "azure_centralus": (SnowflakeCloudProvider.AZURE, "centralus"),
    "azure_southcentralus": (SnowflakeCloudProvider.AZURE, "southcentralus"),
    "azure_eastus2": (SnowflakeCloudProvider.AZURE, "eastus2"),
    "azure_usgovvirginia": (SnowflakeCloudProvider.AZURE, "usgovvirginia"),
    "azure_canadacentral": (SnowflakeCloudProvider.AZURE, "canadacentral"),
    "azure_uksouth": (SnowflakeCloudProvider.AZURE, "uk-south"),
    "azure_northeurope": (SnowflakeCloudProvider.AZURE, "northeurope"),
    "azure_westeurope": (SnowflakeCloudProvider.AZURE, "westeurope"),
    "azure_switzerlandnorth": (SnowflakeCloudProvider.AZURE, "switzerlandnorth"),
    "azure_uaenorth": (SnowflakeCloudProvider.AZURE, "uaenorth"),
    "azure_centralindia": (SnowflakeCloudProvider.AZURE, "central-india.azure"),
    "azure_japaneast": (SnowflakeCloudProvider.AZURE, "japaneast"),
    "azure_southeastasia": (SnowflakeCloudProvider.AZURE, "southeastasia"),
    "azure_australiaeast": (SnowflakeCloudProvider.AZURE, "australiaeast"),
}

SNOWFLAKE_DEFAULT_CLOUD = SnowflakeCloudProvider.AWS


# Required only for mypy, since we are using mixin classes, and not inheritance.
# Reference - https://mypy.readthedocs.io/en/latest/more_types.html#mixin-classes
class SnowflakeLoggingProtocol(Protocol):
    logger: logging.Logger


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


class SnowflakeQueryMixin:
    def query(
        self: SnowflakeLoggingProtocol, conn: SnowflakeConnection, query: str
    ) -> Any:
        self.logger.debug("Query : {}".format(query))
        resp = conn.cursor(DictCursor).execute(query)
        return resp


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

    def _is_dataset_pattern_allowed(
        self: SnowflakeCommonProtocol,
        dataset_name: Optional[str],
        dataset_type: Optional[str],
    ) -> bool:
        if not dataset_type or not dataset_name:
            return True
        dataset_params = dataset_name.split(".")
        if len(dataset_params) != 3:
            self.report.report_warning(
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

        if dataset_type.lower() in {"table"} and not self.config.table_pattern.allowed(
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
        self: SnowflakeCommonProtocol, user_name: str, user_email: Optional[str]
    ) -> str:
        if user_email:
            return user_email.split("@")[0]
        return self.snowflake_identifier(user_name)

    def wrap_aspect_as_workunit(
        self: SnowflakeCommonProtocol,
        entityName: str,
        entityUrn: str,
        aspectName: str,
        aspect: _Aspect,
    ) -> MetadataWorkUnit:
        id = f"{aspectName}-for-{entityUrn}"
        if "timestampMillis" in aspect._inner_dict:
            id = f"{aspectName}-{aspect.timestampMillis}-for-{entityUrn}"  # type: ignore
        wu = MetadataWorkUnit(
            id=id,
            mcp=MetadataChangeProposalWrapper(
                entityType=entityName,
                entityUrn=entityUrn,
                aspectName=aspectName,
                aspect=aspect,
                changeType=ChangeType.UPSERT,
            ),
        )
        self.report.report_workunit(wu)
        return wu
