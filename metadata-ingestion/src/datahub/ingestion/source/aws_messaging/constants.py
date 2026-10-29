import re

from datahub.utilities.str_enum import StrEnum

SQS_PLATFORM = "sqs"
SNS_PLATFORM = "sns"


class AwsService(StrEnum):
    SQS = "sqs"
    SNS = "sns"
    TAGGING = "resourcegroupstaggingapi"


QUEUE_URL_PATTERN = re.compile(r"/(?P<account>\d{12})/(?P<name>[^/?#]+)/?$")

ARN_PATTERN = re.compile(
    r"^arn:(?P<partition>aws[a-z-]*):(?P<service>[a-z0-9-]+):"
    r"(?P<region>[a-z0-9-]*):(?P<account>\d{12})?:(?P<resource>.+)$"
)

ALL_ATTRIBUTES = "All"
TRUE_STRING = "true"


class QueueAttribute(StrEnum):
    QUEUE_ARN = "QueueArn"
    CREATED_TIMESTAMP = "CreatedTimestamp"
    LAST_MODIFIED_TIMESTAMP = "LastModifiedTimestamp"
    VISIBILITY_TIMEOUT = "VisibilityTimeout"
    MESSAGE_RETENTION_PERIOD = "MessageRetentionPeriod"
    DELAY_SECONDS = "DelaySeconds"
    MAXIMUM_MESSAGE_SIZE = "MaximumMessageSize"
    FIFO_QUEUE = "FifoQueue"
    CONTENT_BASED_DEDUPLICATION = "ContentBasedDeduplication"
    KMS_MASTER_KEY_ID = "KmsMasterKeyId"
    SQS_MANAGED_SSE_ENABLED = "SqsManagedSseEnabled"
    REDRIVE_POLICY = "RedrivePolicy"


class EncryptionType(StrEnum):
    KMS = "kms"
    SSE_SQS = "sse-sqs"
    NONE = "none"


class LineageRelationship(StrEnum):
    DEAD_LETTER = "dead_letter"


LINEAGE_RELATIONSHIP_PROPERTY = "relationship"
DOWNSTREAM_OF_RELATIONSHIP = "DownstreamOf"
