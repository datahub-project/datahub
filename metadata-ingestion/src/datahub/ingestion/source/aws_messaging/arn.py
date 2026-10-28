import json
from typing import Dict, Optional
from urllib.parse import urlsplit

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.aws_messaging.constants import (
    ARN_PATTERN,
    SNS_PLATFORM,
    SQS_PLATFORM,
)
from datahub.ingestion.source.aws_messaging.models import Arn, RedrivePolicy


def parse_arn(value: Optional[str]) -> Optional[Arn]:
    if not value:
        return None
    match = ARN_PATTERN.match(value)
    if match is None:
        return None
    return Arn(
        partition=match.group("partition"),
        service=match.group("service"),
        region=match.group("region"),
        account=match.group("account") or "",
        resource=match.group("resource"),
    )


def parse_redrive_policy(value: Optional[str]) -> Optional[RedrivePolicy]:
    if not value:
        return None
    try:
        return RedrivePolicy.model_validate(json.loads(value))
    except ValueError:
        return None


def endpoint_host(url: str) -> Optional[str]:
    # Only the host is kept: query strings and paths can carry tokens.
    return urlsplit(url).hostname or None


class UrnBuilder:
    """The one place an AWS ARN becomes a DataHub URN.

    The sqs and sns sources both build every queue and topic URN here, so a
    lineage edge written by one lands on the dataset the other emits."""

    def __init__(
        self,
        *,
        env: str,
        account_id: Optional[str],
        platform_instance: Optional[str],
        account_platform_instance_map: Optional[Dict[str, str]] = None,
    ):
        self.env = env
        self.account_id = account_id
        self.platform_instance = platform_instance
        self.account_platform_instance_map = account_platform_instance_map or {}

    def instance_for(self, account: str) -> Optional[str]:
        if account in self.account_platform_instance_map:
            return self.account_platform_instance_map[account]
        # With no resolved account id, everything is treated as this recipe's, so
        # datasets and the region container share one platform instance.
        if not account or not self.account_id or account == self.account_id:
            return self.platform_instance
        return account

    @staticmethod
    def resource_id(arn: Arn) -> str:
        name = f"{arn.region}.{arn.resource_name}"
        # make_dataset_urn_with_platform_instance lowercases under
        # DATAHUB_DATASET_URN_TO_LOWER but the SDK Dataset doesn't, so the name is
        # lowercased here to keep entities, tags and lineage on one URN.
        return name.lower() if mce_builder.DATASET_URN_TO_LOWER else name

    def dataset_urn(self, platform: str, arn: Arn) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=platform,
            name=self.resource_id(arn),
            platform_instance=self.instance_for(arn.account),
            env=self.env,
        )

    def queue_urn(self, arn: Arn) -> str:
        return self.dataset_urn(SQS_PLATFORM, arn)

    def topic_urn(self, arn: Arn) -> str:
        return self.dataset_urn(SNS_PLATFORM, arn)
