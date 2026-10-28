from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.arn import parse_redrive_policy
from datahub.ingestion.source.aws_messaging.constants import (
    TRUE_STRING,
    EncryptionType,
    QueueAttribute,
)
from datahub.ingestion.source.aws_messaging.models import SqsQueue


class AwsRegionKey(ContainerKey):
    region: str


def tag_urns(tags: Dict[str, str]) -> Optional[List[str]]:
    # `Key:Value`, or just `Key` when AWS holds an empty value.
    urns = [
        make_tag_urn(f"{key}:{value}" if value else key) for key, value in tags.items()
    ]
    return sorted(urns) or None


def secondary(workunits: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
    """Mark workunits for entities this source doesn't own (lineage patches on
    another account's queue, say) so stale-entity removal never deletes them."""
    for workunit in workunits:
        workunit.is_primary_source = False
        yield workunit


def non_empty(props: Dict[str, Optional[str]]) -> Dict[str, str]:
    return {key: value for key, value in props.items() if value}


def epoch_seconds(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except ValueError:
        return None


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
    return non_empty(
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


def queue_console_url(queue: SqsQueue) -> str:
    region = queue.arn.region
    return (
        f"https://{region}.console.aws.amazon.com/sqs/v3/home?region={region}"
        f"#/queues/{quote(queue.url, safe='')}"
    )
