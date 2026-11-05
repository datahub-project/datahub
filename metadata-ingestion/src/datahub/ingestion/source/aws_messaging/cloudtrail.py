from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

from datahub.ingestion.source.aws_messaging.arn import (
    UrnBuilder,
    parse_arn,
    parse_queue_url,
)
from datahub.ingestion.source.aws_messaging.clients import CloudTrailLakeClient
from datahub.ingestion.source.aws_messaging.config import CloudTrailProducersConfig
from datahub.ingestion.source.aws_messaging.constants import (
    ASSUMED_ROLE_PATTERN,
    AWS_REGION_PATTERN,
    PRODUCER_COLUMN,
    RESOURCE_COLUMN,
    AwsService,
)
from datahub.ingestion.source.aws_messaging.lambda_lineage import LambdaLineage
from datahub.ingestion.source.aws_messaging.models import Arn
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver

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
    after the function, so a caller is matched on that session name and the
    function's execution role; this has
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
        self.succeeded: Optional[bool] = None
        self.roles: Dict[str, Set[str]] = defaultdict(set)
        self.lambda_outputs: List[Tuple[str, Arn]] = []

    def statement(self, now: datetime) -> str:
        assert (
            self.config.event_data_store is not None
        )  # checked by the config validator
        # The region comes from the recipe or the AWS profile rather than the
        # config schema, so it's checked here before it reaches the SQL.
        if not AWS_REGION_PATTERN.match(self.resolver.region):
            raise ValueError(
                f"Not an AWS region, so it can't be used in the CloudTrail query: {self.resolver.region!r}"
            )
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
            f"AND awsRegion = '{self.resolver.region}' "
            f"AND eventName IN ({names}) AND errorCode IS NULL "
            f"GROUP BY 1, 2"
        )

    def _target_arn(self, resource: str) -> Optional[Arn]:
        if self.resolver.service == AwsService.SNS:
            return parse_arn(resource)
        # The region comes from the URL: a multi-region trail also records
        # same-named queues in other regions. SendMessage also takes the queue
        # ARN in queueUrl, and CloudTrail records it as sent.
        return parse_queue_url(resource) or parse_arn(resource)

    def query(self, now: Optional[datetime] = None) -> bool:
        """Runs the Lake query once. True when it succeeded, so its rows are the
        whole lookback."""
        if self.succeeded is None:
            self.succeeded = self._run_query(now or datetime.now(timezone.utc))
        return self.succeeded

    def _run_query(self, now: datetime) -> bool:
        assert self.config.event_data_store is not None
        store = parse_arn(self.config.event_data_store)
        # An event data store shared from another account is queried with its
        # owner's account id; the store's resource policy must allow this role.
        # With no resolved account id there's no telling whether the store is
        # another account's, and naming an owner for one's own store fails the
        # query, so the owner is only sent when it's known to differ.
        owner = (
            store.account
            if store is not None
            and store.account
            and self.urns.account_id
            and store.account != self.urns.account_id
            else None
        )
        rows = self.client.run_query(
            self.statement(now),
            self.config.event_data_store,
            self.config.query_timeout_seconds,
            owner_account_id=owner,
        )
        if rows is None:
            return False
        for row in rows:
            producer = row.get(PRODUCER_COLUMN)
            target = self._target_arn(row.get(RESOURCE_COLUMN) or "")
            # The query also sees other services' and regions' resources.
            if (
                not producer
                or target is None
                or target.service != self.resolver.service
                or not self.resolver.is_local(target)
            ):
                continue
            function_arn = self._lambda_function_arn(producer)
            if function_arn is not None:
                self.lambda_outputs.append((function_arn, target))
            else:
                self.roles[self._dataset_urn(target)].add(self._role_arn(producer))
        return True

    def _dataset_urn(self, target: Arn) -> str:
        if self.resolver.service == AwsService.SQS:
            return self.urns.queue_urn(target)
        return self.urns.topic_urn(target)

    def observed_producers(self, urn: str) -> Optional[str]:
        """This run's `observed_producers` value for a dataset, after a
        successful query."""
        roles = self.roles.get(urn)
        if not roles:
            return None
        self.report.producer_roles_recorded += len(roles)
        return ", ".join(sorted(roles))

    def add_lambda_edges(self) -> None:
        for function_arn, target in self.lambda_outputs:
            # Only resources this run ingested.
            target_urn = self.resolver.urn(target)
            if target_urn is None or self.lambdas is None:
                continue
            edges = self.lambdas.function(function_arn, "cloudtrail producer")
            if edges is None:
                continue
            if target_urn not in edges.outputs:
                edges.outputs.add(target_urn)
                self.report.producer_lambda_edges += 1

    def _lambda_function_arn(self, producer: str) -> Optional[str]:
        match = ASSUMED_ROLE_PATTERN.match(producer)
        if self.lambdas is None or match is None:
            return None
        if self.urns.account_id and match.group("account") != self.urns.account_id:
            return None
        session = match.group("session")
        # A session named after a function only counts when the role is that
        # function's execution role; any role can pick its session name.
        if self.lambdas.execution_role_name(session) != match.group("role"):
            return None
        return self.lambdas.function_arns_by_name().get(session)

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
