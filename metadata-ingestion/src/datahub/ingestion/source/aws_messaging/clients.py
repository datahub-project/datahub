from typing import Any, Callable, Dict, Iterator, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws_messaging.constants import ALL_ATTRIBUTES
from datahub.ingestion.source.aws_messaging.models import SnsSubscription
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase


def paginate(
    *,
    client: Any,
    operation: str,
    result_key: str,
    report: AwsMessagingReportBase,
    api_label: str,
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
            pages_fetched=pages_fetched, exc=e, api_label=api_label
        )


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

    def get_data_protection_policy(self, arn: str) -> Optional[str]:
        response = call_or_warn(
            call=lambda: self._client.get_data_protection_policy(ResourceArn=arn),
            report=self.report,
            api_label="sns:GetDataProtectionPolicy",
            context=f"topic={arn}",
        )
        return (response or {}).get("DataProtectionPolicy") or None
