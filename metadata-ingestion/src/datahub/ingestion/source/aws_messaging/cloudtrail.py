from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Optional, Set

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.clients import CloudTrailLakeClient
from datahub.ingestion.source.aws_messaging.config import CloudTrailProducersConfig
from datahub.ingestion.source.aws_messaging.constants import (
    ASSUMED_ROLE_PATTERN,
    OBSERVED_PRODUCERS_PROPERTY,
    PRODUCER_COLUMN,
    QUEUE_URL_PATTERN,
    RESOURCE_COLUMN,
    AwsService,
)
from datahub.ingestion.source.aws_messaging.lambda_lineage import LambdaLineage
from datahub.ingestion.source.aws_messaging.models import Arn
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.specific.dataset import DatasetPatchBuilder

# Data events that put a message on a queue or topic, and the request
# parameter naming the target.
SEND_EVENTS = {
    AwsService.SQS: (
        "sqs.amazonaws.com",
        ("SendMessage", "SendMessageBatch"),
        "queueUrl",
    ),
    AwsService.SNS: ("sns.amazonaws.com", ("Publish", "PublishBatch"), "topicArn"),
}


class ProducerLineage:
    """Producers seen in CloudTrail Lake data events.

    A caller that is a Lambda function in this account and region becomes an
    upstream job of the queue or topic. Lambda's execution role session is named
    after the function, which is how a caller is matched to a function; this has
    not been checked against every runtime and invocation path. Any other caller
    (an ECS task role, a user) is only listed in the `observed_producers` property:
    a role is not a DataHub entity, and guessing the workload behind it would be
    wrong."""

    def __init__(
        self,
        *,
        config: CloudTrailProducersConfig,
        report: AwsMessagingReportBase,
        client: CloudTrailLakeClient,
        urns: UrnBuilder,
        resolver: ResourceResolver,
        lambdas: Optional[LambdaLineage],
    ):
        self.config = config
        self.report = report
        self.client = client
        self.urns = urns
        self.resolver = resolver
        self.lambdas = lambdas
        self.observed: Dict[str, Set[str]] = defaultdict(set)

    def statement(self, now: datetime) -> str:
        assert (
            self.config.event_data_store is not None
        )  # checked by the config validator
        event_source, event_names, parameter = SEND_EVENTS[self.resolver.service]
        store_id = self.config.event_data_store.rsplit("/", 1)[-1]
        since = (now - timedelta(days=self.config.lookback_days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        names = ", ".join(f"'{name}'" for name in event_names)
        return (
            f"SELECT userIdentity.arn AS {PRODUCER_COLUMN}, "
            f"element_at(requestParameters, '{parameter}') AS {RESOURCE_COLUMN} "
            f"FROM {store_id} "
            f"WHERE eventTime > '{since}' AND eventSource = '{event_source}' "
            f"AND eventName IN ({names}) "
            f"GROUP BY 1, 2"
        )

    def _target_arn(self, resource: str) -> Optional[Arn]:
        if self.resolver.service == AwsService.SNS:
            return parse_arn(resource)
        match = QUEUE_URL_PATTERN.search(resource)
        if match is None:
            return None
        return Arn(
            partition="aws",
            service=AwsService.SQS.value,
            region=self.resolver.region,
            account=match.group("account"),
            resource=match.group("name"),
        )

    def collect(self, now: Optional[datetime] = None) -> None:
        assert self.config.event_data_store is not None
        rows = self.client.run_query(
            self.statement(now or datetime.now(timezone.utc)),
            self.config.event_data_store,
            self.config.query_timeout_seconds,
        )
        for row in rows or []:
            producer = row.get(PRODUCER_COLUMN)
            target = self._target_arn(row.get(RESOURCE_COLUMN) or "")
            # Only resources this run ingested; the query also sees others.
            target_urn = (
                self.resolver.urn(target)
                if target is not None and self.resolver.is_local(target)
                else None
            )
            if not producer or target_urn is None:
                continue
            if not self._add_lambda_edge(producer, target_urn):
                self.observed[target_urn].add(self._role_arn(producer))

    def _add_lambda_edge(self, producer: str, target_urn: str) -> bool:
        match = ASSUMED_ROLE_PATTERN.match(producer)
        if self.lambdas is None or match is None:
            return False
        if self.urns.account_id and match.group("account") != self.urns.account_id:
            return False
        function_arn = self.lambdas.function_arns_by_name().get(match.group("session"))
        if function_arn is None:
            return False
        edges = self.lambdas.function(function_arn, "cloudtrail producer")
        if edges is None:
            return False
        edges.outputs.add(target_urn)
        self.report.producer_lambda_edges += 1
        return True

    @staticmethod
    def _role_arn(producer: str) -> str:
        # The session name changes per task or request, so record the role.
        match = ASSUMED_ROLE_PATTERN.match(producer)
        if match is None:
            return producer
        partition = producer.split(":", 2)[1]
        return (
            f"arn:{partition}:iam::{match.group('account')}:role/{match.group('role')}"
        )

    def workunits(self) -> Iterable[MetadataWorkUnit]:
        for urn in sorted(self.observed):
            producers = sorted(self.observed[urn])
            self.report.producer_roles_recorded += len(producers)
            patch = DatasetPatchBuilder(urn)
            patch.add_custom_property(OBSERVED_PRODUCERS_PROPERTY, ", ".join(producers))
            for mcp in patch.build():
                yield MetadataWorkUnit(id=f"{urn}-observed-producers", mcp_raw=mcp)
