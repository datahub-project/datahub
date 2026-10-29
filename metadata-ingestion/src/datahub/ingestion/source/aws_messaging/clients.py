from typing import Any, Callable, Dict, Iterator, List, Optional, Union

from botocore.exceptions import BotoCoreError, ClientError

from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws_messaging.arn import parse_arn
from datahub.ingestion.source.aws_messaging.constants import ALL_ATTRIBUTES
from datahub.ingestion.source.aws_messaging.models import (
    EventBridgePipe,
    RuleTarget,
    SnsSubscription,
)
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase


def paginate(
    *,
    client: Any,
    operation: str,
    result_key: str,
    report: AwsMessagingReportBase,
    api_label: str,
    optional: bool = False,
    on_error: Optional[Callable[[Union[ClientError, BotoCoreError]], None]] = None,
    **kwargs: Any,
) -> Iterator[Any]:
    # boto3 paginators raise from inside iteration, so the page count at the time
    # of the exception tells a denied call apart from a truncated listing.
    pages_fetched = 0
    try:
        for page in client.get_paginator(operation).paginate(**kwargs):
            pages_fetched += 1
            yield from page.get(result_key) or []
    except (ClientError, BotoCoreError) as e:
        report.report_listing_failure(
            pages_fetched=pages_fetched, exc=e, api_label=api_label, optional=optional
        )
        if on_error is not None:
            on_error(e)


def call_or_warn(
    *,
    call: Callable[[], Dict[str, Any]],
    report: AwsMessagingReportBase,
    api_label: str,
    context: str,
) -> Optional[Dict[str, Any]]:
    try:
        return call()
    except (ClientError, BotoCoreError) as e:
        report.warning(
            title=f"{api_label} failed",
            message="An AWS call for one resource failed; that part is skipped.",
            context=f"{context}: {aws_error_code(e)}",
            exc=e,
        )
        return None


def tags_to_dict(tags: List[Dict[str, str]]) -> Dict[str, str]:
    return {t["Key"]: t.get("Value", "") for t in tags if t.get("Key")}


class SqsClient:
    def __init__(self, client: Any, report: AwsMessagingReportBase):
        self._client = client
        self.report = report

    def list_queue_urls(self, optional: bool = False) -> Iterator[str]:
        # ListQueues only returns a NextToken when MaxResults is set.
        yield from paginate(
            client=self._client,
            operation="list_queues",
            result_key="QueueUrls",
            report=self.report,
            api_label="sqs:ListQueues",
            optional=optional,
            PaginationConfig={"PageSize": 1000},
        )

    def first_queue_url(self) -> Optional[str]:
        urls = self._client.list_queues(MaxResults=1).get("QueueUrls") or []
        return urls[0] if urls else None

    def get_attributes(self, url: str) -> Optional[Dict[str, str]]:
        response = call_or_warn(
            call=lambda: self._client.get_queue_attributes(
                QueueUrl=url, AttributeNames=[ALL_ATTRIBUTES]
            ),
            report=self.report,
            api_label="sqs:GetQueueAttributes",
            context=f"queue={url}",
        )
        return None if response is None else dict(response.get("Attributes") or {})

    def get_tags(self, url: str) -> Dict[str, str]:
        response = call_or_warn(
            call=lambda: self._client.list_queue_tags(QueueUrl=url),
            report=self.report,
            api_label="sqs:ListQueueTags",
            context=f"queue={url}",
        )
        return dict((response or {}).get("Tags") or {})


class TaggingClient:
    """Tags for every resource of one type in a single paginated call, instead
    of one ListQueueTags or ListTagsForResource call per resource."""

    def __init__(self, client: Any, report: AwsMessagingReportBase):
        self._client = client
        self.report = report

    def get_tags_by_arn(
        self, resource_type: str
    ) -> Optional[Dict[str, Dict[str, str]]]:
        """Tags keyed by ARN, or None when the API can't be used, in which case
        the caller reads tags per resource. Untagged resources are absent."""
        tags: Dict[str, Dict[str, str]] = {}
        try:
            for page in self._client.get_paginator("get_resources").paginate(
                ResourceTypeFilters=[resource_type]
            ):
                for item in page.get("ResourceTagMappingList") or []:
                    tags[item["ResourceARN"]] = tags_to_dict(item.get("Tags") or [])
        except (ClientError, BotoCoreError) as e:
            self.report.tags_read_per_resource = True
            self.report.warning(
                title="Tag API unavailable",
                message=(
                    "tag:GetResources failed, so tags are read one resource at a time. "
                    "Grant tag:GetResources to make this a single call."
                ),
                context=aws_error_code(e),
                exc=e,
            )
            return None
        return tags


class SnsClient:
    def __init__(self, client: Any, report: AwsMessagingReportBase):
        self._client = client
        self.report = report

    def list_topic_arns(self) -> Iterator[str]:
        for topic in paginate(
            client=self._client,
            operation="list_topics",
            result_key="Topics",
            report=self.report,
            api_label="sns:ListTopics",
        ):
            yield topic["TopicArn"]

    def first_topic_arn(self) -> Optional[str]:
        topics = self._client.list_topics().get("Topics") or []
        return topics[0].get("TopicArn") if topics else None

    def get_topic_attributes(self, arn: str) -> Optional[Dict[str, str]]:
        response = call_or_warn(
            call=lambda: self._client.get_topic_attributes(TopicArn=arn),
            report=self.report,
            api_label="sns:GetTopicAttributes",
            context=f"topic={arn}",
        )
        return None if response is None else dict(response.get("Attributes") or {})

    def get_tags(self, arn: str) -> Dict[str, str]:
        response = call_or_warn(
            call=lambda: self._client.list_tags_for_resource(ResourceArn=arn),
            report=self.report,
            api_label="sns:ListTagsForResource",
            context=f"topic={arn}",
        )
        return tags_to_dict((response or {}).get("Tags") or [])

    def list_subscriptions(
        self,
        topic_arn: str,
        on_error: Callable[[Union[ClientError, BotoCoreError]], None],
    ) -> Iterator[SnsSubscription]:
        # Per topic, because ListSubscriptions only returns the caller's own
        # subscriptions and misses ones other accounts created on our topics.
        # SNS allows 30 of these calls per second.
        for item in paginate(
            client=self._client,
            operation="list_subscriptions_by_topic",
            result_key="Subscriptions",
            report=self.report,
            api_label="sns:ListSubscriptionsByTopic",
            # Subscriptions only add properties and lineage, not datasets.
            optional=True,
            on_error=on_error,
            TopicArn=topic_arn,
        ):
            yield SnsSubscription(
                subscription_arn=item.get("SubscriptionArn", ""),
                topic_arn=item.get("TopicArn", ""),
                protocol=item.get("Protocol", ""),
                endpoint=item.get("Endpoint", ""),
                owner=item.get("Owner"),
            )

    def get_subscription_attributes(self, arn: str) -> Optional[Dict[str, str]]:
        response = call_or_warn(
            call=lambda: self._client.get_subscription_attributes(SubscriptionArn=arn),
            report=self.report,
            api_label="sns:GetSubscriptionAttributes",
            context=f"subscription={arn}",
        )
        return None if response is None else dict(response.get("Attributes") or {})

    def get_data_protection_policy(self, arn: str) -> Optional[str]:
        response = call_or_warn(
            call=lambda: self._client.get_data_protection_policy(ResourceArn=arn),
            report=self.report,
            api_label="sns:GetDataProtectionPolicy",
            context=f"topic={arn}",
        )
        return (response or {}).get("DataProtectionPolicy") or None


class LambdaClient:
    def __init__(self, client: Any, report: AwsMessagingReportBase):
        self._client = client
        self.report = report

    def list_event_source_mappings(
        self, on_error: Callable[[Union[ClientError, BotoCoreError]], None]
    ) -> Iterator[Dict[str, Any]]:
        yield from paginate(
            client=self._client,
            operation="list_event_source_mappings",
            result_key="EventSourceMappings",
            report=self.report,
            api_label="lambda:ListEventSourceMappings",
            optional=True,
            on_error=on_error,
        )

    def list_functions(
        self, on_error: Callable[[Union[ClientError, BotoCoreError]], None]
    ) -> Iterator[Dict[str, Any]]:
        yield from paginate(
            client=self._client,
            operation="list_functions",
            result_key="Functions",
            report=self.report,
            api_label="lambda:ListFunctions",
            optional=True,
            on_error=on_error,
        )

    def list_async_destinations(
        self,
        function_name: str,
        on_error: Callable[[Union[ClientError, BotoCoreError]], None],
    ) -> Iterator[str]:
        for item in paginate(
            client=self._client,
            operation="list_function_event_invoke_configs",
            result_key="FunctionEventInvokeConfigs",
            report=self.report,
            api_label="lambda:ListFunctionEventInvokeConfigs",
            optional=True,
            on_error=on_error,
            FunctionName=function_name,
        ):
            # Each config is for one version or alias; destinations hang off the
            # unqualified function either way.
            destinations = item.get("DestinationConfig") or {}
            for outcome in ("OnSuccess", "OnFailure"):
                destination = (destinations.get(outcome) or {}).get("Destination")
                if destination:
                    yield destination


class EventBridgeClient:
    def __init__(self, client: Any, report: AwsMessagingReportBase):
        self._client = client
        self.report = report

    def list_event_bus_arns(
        self, on_error: Callable[[Union[ClientError, BotoCoreError]], None]
    ) -> Iterator[str]:
        # boto3 has no paginator for ListEventBuses.
        token: Optional[str] = None
        pages_fetched = 0
        while True:
            kwargs: Dict[str, Any] = {"NextToken": token} if token else {}
            try:
                response = self._client.list_event_buses(**kwargs)
            except (ClientError, BotoCoreError) as e:
                self.report.report_listing_failure(
                    pages_fetched=pages_fetched,
                    exc=e,
                    api_label="events:ListEventBuses",
                    optional=True,
                )
                on_error(e)
                return
            pages_fetched += 1
            for bus in response.get("EventBuses") or []:
                if bus.get("Arn"):
                    yield bus["Arn"]
            token = response.get("NextToken")
            if not token:
                return

    def list_rules(
        self,
        bus_name: str,
        on_error: Callable[[Union[ClientError, BotoCoreError]], None],
    ) -> Iterator[Dict[str, Any]]:
        yield from paginate(
            client=self._client,
            operation="list_rules",
            result_key="Rules",
            report=self.report,
            api_label="events:ListRules",
            optional=True,
            on_error=on_error,
            EventBusName=bus_name,
        )

    def list_targets(
        self,
        rule_name: str,
        bus_name: str,
        on_error: Callable[[Union[ClientError, BotoCoreError]], None],
    ) -> Iterator[RuleTarget]:
        for target in paginate(
            client=self._client,
            operation="list_targets_by_rule",
            result_key="Targets",
            report=self.report,
            api_label="events:ListTargetsByRule",
            optional=True,
            on_error=on_error,
            Rule=rule_name,
            EventBusName=bus_name,
        ):
            if target.get("Arn"):
                yield RuleTarget(arn=target["Arn"])


class PipesClient:
    def __init__(self, client: Any, report: AwsMessagingReportBase):
        self._client = client
        self.report = report

    def list_pipes(
        self, on_error: Callable[[Union[ClientError, BotoCoreError]], None]
    ) -> Iterator[EventBridgePipe]:
        for item in paginate(
            client=self._client,
            operation="list_pipes",
            result_key="Pipes",
            report=self.report,
            api_label="pipes:ListPipes",
            optional=True,
            on_error=on_error,
        ):
            arn = parse_arn(item.get("Arn"))
            if arn is None or not item.get("Source") or not item.get("Target"):
                continue
            yield EventBridgePipe(
                arn=arn,
                name=item.get("Name") or arn.resource_name,
                source=item["Source"],
                target=item["Target"],
                enrichment=item.get("Enrichment"),
                state=item.get("CurrentState"),
            )

    def input_template(self, name: str) -> Optional[str]:
        """The pipe's target input template, "" when it has none, or None if the
        pipe can't be described."""
        response = call_or_warn(
            call=lambda: self._client.describe_pipe(Name=name),
            report=self.report,
            api_label="pipes:DescribePipe",
            context=f"pipe={name}",
        )
        if response is None:
            return None
        return (response.get("TargetParameters") or {}).get("InputTemplate") or ""
