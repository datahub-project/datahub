from collections import Counter
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence
from urllib.parse import quote

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.aws_messaging.arn import (
    endpoint_host,
    parse_redrive_policy,
)
from datahub.ingestion.source.aws_messaging.constants import (
    TRUE_STRING,
    WEB_PROTOCOLS,
    EncryptionType,
    QueueAttribute,
    TopicAttribute,
)
from datahub.ingestion.source.aws_messaging.models import (
    SnsSubscription,
    SnsTopic,
    SqsQueue,
)


class AwsRegionKey(ContainerKey):
    region: str


def tag_urns(tags: Dict[str, str]) -> Optional[List[str]]:
    # `Key:Value`, or just `Key` when AWS holds an empty value.
    urns = [
        make_tag_urn(f"{key}:{value}" if value else key) for key, value in tags.items()
    ]
    return sorted(urns) or None


def _non_empty(props: Dict[str, Optional[str]]) -> Dict[str, str]:
    return {key: value for key, value in props.items() if value}


def queue_properties(queue: SqsQueue) -> Dict[str, str]:
    attributes = queue.attributes
    kms_key = attributes.get(QueueAttribute.KMS_MASTER_KEY_ID)
    if kms_key:
        encryption = EncryptionType.KMS
    elif attributes.get(QueueAttribute.SQS_MANAGED_SSE_ENABLED) == TRUE_STRING:
        encryption = EncryptionType.SSE_SQS
    else:
        encryption = EncryptionType.NONE
    redrive = parse_redrive_policy(attributes.get(QueueAttribute.REDRIVE_POLICY))
    return _non_empty(
        {
            "arn": str(queue.arn),
            "url": queue.url,
            "fifo": attributes.get(QueueAttribute.FIFO_QUEUE) or "false",
            "content_based_deduplication": attributes.get(
                QueueAttribute.CONTENT_BASED_DEDUPLICATION
            ),
            "visibility_timeout_seconds": attributes.get(
                QueueAttribute.VISIBILITY_TIMEOUT
            ),
            "message_retention_seconds": attributes.get(
                QueueAttribute.MESSAGE_RETENTION_PERIOD
            ),
            "delay_seconds": attributes.get(QueueAttribute.DELAY_SECONDS),
            "max_message_size": attributes.get(QueueAttribute.MAXIMUM_MESSAGE_SIZE),
            "encryption": encryption.value,
            "kms_key_id": kms_key,
            "dead_letter_target": redrive.dead_letter_target_arn if redrive else None,
            "max_receive_count": (
                str(redrive.max_receive_count)
                if redrive and redrive.max_receive_count is not None
                else None
            ),
        }
    )


def subscription_properties(
    subscriptions: Sequence[SnsSubscription],
) -> Dict[str, str]:
    # Email and SMS endpoints are personal data and web endpoints can carry
    # tokens, so only protocol counts and web hosts are kept.
    counts = Counter(sub.protocol for sub in subscriptions)
    web_hosts = (
        endpoint_host(sub.endpoint)
        for sub in subscriptions
        if sub.protocol in WEB_PROTOCOLS
    )
    hosts = sorted({host for host in web_hosts if host})
    return _non_empty(
        {
            "subscriber_protocols": ", ".join(
                f"{protocol}={count}" for protocol, count in sorted(counts.items())
            ),
            "https_endpoint_hosts": ", ".join(hosts),
        }
    )


def topic_properties(
    topic: SnsTopic, subscriptions: Sequence[SnsSubscription] = ()
) -> Dict[str, str]:
    attributes = topic.attributes
    return subscription_properties(subscriptions) | _non_empty(
        {
            "arn": str(topic.arn),
            "display_name": attributes.get(TopicAttribute.DISPLAY_NAME),
            "fifo": attributes.get(TopicAttribute.FIFO_TOPIC) or "false",
            "content_based_deduplication": attributes.get(
                TopicAttribute.CONTENT_BASED_DEDUPLICATION
            ),
            "kms_key_id": attributes.get(TopicAttribute.KMS_MASTER_KEY_ID),
            "subscriptions_confirmed": attributes.get(
                TopicAttribute.SUBSCRIPTIONS_CONFIRMED
            ),
            "subscriptions_pending": attributes.get(
                TopicAttribute.SUBSCRIPTIONS_PENDING
            ),
            "archive_policy": attributes.get(TopicAttribute.ARCHIVE_POLICY),
        }
    )


def epoch_seconds(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except ValueError:
        return None


def queue_console_url(queue: SqsQueue) -> str:
    region = queue.arn.region
    return (
        f"https://{region}.console.aws.amazon.com/sqs/v3/home?region={region}"
        f"#/queues/{quote(queue.url, safe='')}"
    )


def topic_console_url(topic: SnsTopic) -> str:
    region = topic.arn.region
    return (
        f"https://{region}.console.aws.amazon.com/sns/v3/home?region={region}"
        f"#/topic/{topic.arn}"
    )
