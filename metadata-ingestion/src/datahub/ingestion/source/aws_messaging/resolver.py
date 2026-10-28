from typing import Optional, Set

from datahub.ingestion.source.aws_messaging.arn import UrnBuilder
from datahub.ingestion.source.aws_messaging.constants import AwsService
from datahub.ingestion.source.aws_messaging.models import Arn


class ResourceResolver:
    """Decides which ARNs of one service may appear in lineage.

    Each source only writes edges whose dataset end is its own service, so the
    sqs and sns sources never write the same edge. A resource in the recipe's
    own account and region is only referenced if it is known to exist. One
    elsewhere belongs to another recipe, and its URN follows from the ARN."""

    def __init__(self, *, urns: UrnBuilder, region: str, service: AwsService):
        self.urns = urns
        self.region = region
        self.service = service
        self.known: Set[str] = set()

    def is_local(self, arn: Arn) -> bool:
        own_account = not self.urns.account_id or arn.account == self.urns.account_id
        return own_account and arn.region == self.region

    @staticmethod
    def key(account: str, region: str, name: str) -> str:
        return f"{account}/{region}/{name}"

    def add(self, arn: Arn) -> None:
        self.known.add(self.key(arn.account, arn.region, arn.resource_name))

    def exists(self, arn: Arn) -> bool:
        if not self.is_local(arn):
            return True
        return self.key(arn.account, arn.region, arn.resource_name) in self.known

    def urn(self, arn: Optional[Arn]) -> Optional[str]:
        """The dataset URN for an ARN of this service that may be referenced, else None."""
        if arn is None or arn.service != self.service or not self.exists(arn):
            return None
        if self.service == AwsService.SQS:
            return self.urns.queue_urn(arn)
        return self.urns.topic_urn(arn)
