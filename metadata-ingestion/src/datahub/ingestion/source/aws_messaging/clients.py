from typing import Any, Callable, Dict, Iterator, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws_messaging.arn import parse_arn
from datahub.ingestion.source.aws_messaging.constants import ALL_ATTRIBUTES
from datahub.ingestion.source.aws_messaging.models import (
    EventBridgePipe,
    SnsSubscription,
)
from datahub.ingestion.source.aws_messaging.report import AwsMessagingSourceReport


def paginate(
    *,
    client: Any,
    operation: str,
    result_key: str,
    report: AwsMessagingSourceReport,
    api_label: str,
    **kwargs: Any,
) -> Iterator[Any]:
    # boto3 paginators raise from inside iteration, so the page count at the time
    # of the exception tells a denied section apart from a truncated listing.
    pages_fetched = 0
    try:
        for page in client.get_paginator(operation).paginate(**kwargs):
            pages_fetched += 1
            yield from page.get(result_key) or []
    except (ClientError, BotoCoreError) as e:
        report.report_listing_failure(
            pages_fetched=pages_fetched, exc=e, api_label=api_label
        )


def call_or_warn(
    *,
    call: Callable[[], Dict[str, Any]],
    report: AwsMessagingSourceReport,
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
    def __init__(self, client: Any, report: AwsMessagingSourceReport):
        self._client = client
        self.report = report

    def list_queue_urls(self) -> Iterator[str]:
        # ListQueues only returns a NextToken when MaxResults is set.
        yield from paginate(
            client=self._client,
            operation="list_queues",
            result_key="QueueUrls",
            report=self.report,
            api_label="sqs:ListQueues",
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


class SnsClient:
    def __init__(self, client: Any, report: AwsMessagingSourceReport):
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

    def list_subscriptions(self) -> Iterator[SnsSubscription]:
        # One account-wide listing instead of a call per topic; SNS caps both at 30 TPS.
        for item in paginate(
            client=self._client,
            operation="list_subscriptions",
            result_key="Subscriptions",
            report=self.report,
            api_label="sns:ListSubscriptions",
        ):
            yield SnsSubscription(
                subscription_arn=item.get("SubscriptionArn", ""),
                topic_arn=item.get("TopicArn", ""),
                protocol=item.get("Protocol", ""),
                endpoint=item.get("Endpoint", ""),
                owner=item.get("Owner"),
            )

    def get_subscription_attributes(self, arn: str) -> Dict[str, str]:
        response = call_or_warn(
            call=lambda: self._client.get_subscription_attributes(SubscriptionArn=arn),
            report=self.report,
            api_label="sns:GetSubscriptionAttributes",
            context=f"subscription={arn}",
        )
        return dict((response or {}).get("Attributes") or {})


class LambdaClient:
    def __init__(self, client: Any, report: AwsMessagingSourceReport):
        self._client = client
        self.report = report

    def list_event_source_mappings(self) -> Iterator[Dict[str, Any]]:
        yield from paginate(
            client=self._client,
            operation="list_event_source_mappings",
            result_key="EventSourceMappings",
            report=self.report,
            api_label="lambda:ListEventSourceMappings",
        )

    def list_functions(self) -> Iterator[Dict[str, Any]]:
        yield from paginate(
            client=self._client,
            operation="list_functions",
            result_key="Functions",
            report=self.report,
            api_label="lambda:ListFunctions",
        )

    def list_async_destinations(self, function_name: str) -> Iterator[str]:
        for item in paginate(
            client=self._client,
            operation="list_function_event_invoke_configs",
            result_key="FunctionEventInvokeConfigs",
            report=self.report,
            api_label="lambda:ListFunctionEventInvokeConfigs",
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
    def __init__(self, client: Any, report: AwsMessagingSourceReport):
        self._client = client
        self.report = report

    def list_event_bus_arns(self) -> Iterator[str]:
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
                )
                return
            pages_fetched += 1
            for bus in response.get("EventBuses") or []:
                if bus.get("Arn"):
                    yield bus["Arn"]
            token = response.get("NextToken")
            if not token:
                return

    def list_rules(self, bus_name: str) -> Iterator[Dict[str, Any]]:
        yield from paginate(
            client=self._client,
            operation="list_rules",
            result_key="Rules",
            report=self.report,
            api_label="events:ListRules",
            EventBusName=bus_name,
        )

    def list_target_arns(self, rule_name: str, bus_name: str) -> Iterator[str]:
        for target in paginate(
            client=self._client,
            operation="list_targets_by_rule",
            result_key="Targets",
            report=self.report,
            api_label="events:ListTargetsByRule",
            Rule=rule_name,
            EventBusName=bus_name,
        ):
            if target.get("Arn"):
                yield target["Arn"]


class PipesClient:
    def __init__(self, client: Any, report: AwsMessagingSourceReport):
        self._client = client
        self.report = report

    def list_pipes(self) -> Iterator[EventBridgePipe]:
        for item in paginate(
            client=self._client,
            operation="list_pipes",
            result_key="Pipes",
            report=self.report,
            api_label="pipes:ListPipes",
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
