from typing import Any, Dict, Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
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
        description="Extract AWS resource tags as DataHub tags.",
    )
    account_platform_instance_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "AWS account id to platform_instance, for lineage to resources in other "
            "accounts whose own recipes set a platform_instance other than the "
            "account id. Accounts not listed use their account id."
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
