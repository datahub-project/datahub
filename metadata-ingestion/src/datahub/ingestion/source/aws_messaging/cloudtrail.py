from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Optional, Set

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
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
    OBSERVED_PRODUCERS_PROPERTY,
    PRODUCER_COLUMN,
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
        graph: Optional[DataHubGraph] = None,
    ):
        self.graph = graph
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
            f"AND eventName IN ({names}) "
            f"GROUP BY 1, 2"
        )

    def _target_arn(self, resource: str) -> Optional[Arn]:
        if self.resolver.service == AwsService.SNS:
            return parse_arn(resource)
        # The region comes from the URL: a multi-region trail also records
        # same-named queues in other regions.
        return parse_queue_url(resource)

    def collect(self, now: Optional[datetime] = None) -> None:
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
            self.statement(now or datetime.now(timezone.utc)),
            self.config.event_data_store,
            self.config.query_timeout_seconds,
            owner_account_id=owner,
        )
        if rows is None:
            self._keep_previous_producers()
            return
        for row in rows:
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

    def _keep_previous_producers(self) -> None:
        # The previous value is already in this run's datasetProperties (read
        # before they were written, see dataset_custom_properties), so a failed
        # query leaves it alone. Without a DataHub connection it couldn't be read.
        if self.graph is None:
            self.report.warning(
                title="Observed producers not kept",
                message=(
                    "The CloudTrail query failed and there is no DataHub connection to "
                    "read the previous observed_producers, so they are cleared until the "
                    "next successful run."
                ),
            )

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
