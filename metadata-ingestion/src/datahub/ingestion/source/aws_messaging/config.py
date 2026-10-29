from typing import Any, Dict, Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws_messaging.constants import AwsService
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class AwsMessagingConfigBase(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Settings shared by the sqs and sns sources. Use the same env,
    platform_instance and account_platform_instance_map in both recipes so
    lineage between queues and topics joins."""

    aws_config: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description=(
            "AWS connection config (credentials, region, role assumption, endpoint "
            "override). When omitted, boto3 resolves everything from the standard "
            "credential chain. One recipe covers one region."
        ),
    )
    extract_tags: bool = Field(
        default=True,
        description=(
            "Extract AWS resource tags as DataHub tags. SNS allows 10 "
            "ListTagsForResource calls per second, so turn this off on the sns "
            "source for accounts with tens of thousands of topics."
        ),
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default_factory=dict,
        description=(
            "Domain urn or id to regex patterns on resource names; a matching queue "
            "or topic is assigned that domain."
        ),
    )
    account_platform_instance_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "AWS account id to platform_instance, for lineage to resources in other "
            "accounts whose own recipes set a platform_instance other than the "
            "account id. Accounts not listed use their account id."
        ),
    )
    remove_stale_lineage: bool = Field(
        default=True,
        description=(
            "Remove lineage edges this source wrote on an earlier run that no longer "
            "exist in AWS, such as a removed redrive policy or subscription. Only "
            "edges from resources this run read in full are removed. Needs a "
            "DataHub graph connection (the datahub-rest sink or datahub_api); costs "
            "one DataHub lookup per queue or topic."
        ),
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration for stale entity removal.",
    )

    def make_client(self, session: Any, service: AwsService) -> Any:
        # boto3 clients have no common static type; callers annotate the result.
        return session.client(
            service.value,
            endpoint_url=self.aws_config.aws_endpoint_url,
            config=self.aws_config._aws_config(),
        )


class SqsSourceConfig(AwsMessagingConfigBase):
    queue_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns on queue names to include.",
    )


class SnsDataProtectionConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description=(
            "Read each topic's message data protection policy "
            "(sns:GetDataProtectionPolicy, one call per topic) and tag the topic with "
            "the data identifiers it covers."
        ),
    )
    tag_prefix: str = Field(
        default="aws_data_identifier",
        description="Tags are named `<tag_prefix>:<identifier>`, e.g. `aws_data_identifier:EmailAddress`.",
    )
    glossary_term_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Data identifier name (e.g. `EmailAddress`) to an existing glossary term "
            "urn or name. Only listed identifiers get a term; none are created."
        ),
    )


class SnsSourceConfig(AwsMessagingConfigBase):
    topic_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns on topic names to include.",
    )
    include_subscription_attributes: bool = Field(
        default=False,
        description=(
            "Call sns:GetSubscriptionAttributes for each subscription to read its "
            "dead-letter queue. One call per subscription, for every protocol, on "
            "every run; subscription-to-queue edges need no extra calls."
        ),
    )
    data_protection: SnsDataProtectionConfig = Field(
        default_factory=SnsDataProtectionConfig,
        description="SNS message data protection policies as tags or glossary terms.",
    )
