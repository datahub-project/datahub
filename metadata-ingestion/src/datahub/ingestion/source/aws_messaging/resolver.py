from typing import Optional, Set

from datahub.ingestion.source.aws_messaging.arn import UrnBuilder
from datahub.ingestion.source.aws_messaging.constants import AwsService
from datahub.ingestion.source.aws_messaging.models import Arn


class ResourceResolver:
    """Decides which queue and topic ARNs may appear in lineage.

    A queue or topic in the recipe's own account and region is only referenced
    if this run ingested it. One elsewhere belongs to another recipe, and its URN
    follows from the ARN alone."""

    def __init__(self, *, urns: UrnBuilder, region: str):
        self.urns = urns
        self.region = region
        self.queue_arns: Set[str] = set()
        self.topic_arns: Set[str] = set()

    def is_local(self, arn: Arn) -> bool:
        own_account = not self.urns.account_id or arn.account == self.urns.account_id
        return own_account and arn.region == self.region

    def queue_exists(self, arn: Arn) -> bool:
        return str(arn) in self.queue_arns if self.is_local(arn) else True

    def topic_exists(self, arn: Arn) -> bool:
        return str(arn) in self.topic_arns if self.is_local(arn) else True

    def messaging_urn(self, arn: Optional[Arn]) -> Optional[str]:
        """The dataset URN for a queue or topic ARN that may be referenced, else None."""
        if arn is None:
            return None
        if arn.service == AwsService.SQS and self.queue_exists(arn):
            return self.urns.queue_urn(arn)
        if arn.service == AwsService.SNS and self.topic_exists(arn):
            return self.urns.topic_urn(arn)
        return None
