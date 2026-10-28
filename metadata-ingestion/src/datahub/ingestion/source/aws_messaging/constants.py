import re

from datahub.utilities.str_enum import StrEnum

SQS_PLATFORM = "sqs"
SNS_PLATFORM = "sns"


class AwsService(StrEnum):
    SQS = "sqs"
    SNS = "sns"


class SubscriptionProtocol(StrEnum):
    SQS = "sqs"
    LAMBDA = "lambda"
    FIREHOSE = "firehose"
    HTTP = "http"
    HTTPS = "https"
    EMAIL = "email"
    EMAIL_JSON = "email-json"
    SMS = "sms"
    APPLICATION = "application"


# Endpoints for these protocols are email addresses and phone numbers. Only
# per-protocol counts ever leave the connector.
PERSONAL_DATA_PROTOCOLS = frozenset(
    {
        SubscriptionProtocol.EMAIL,
        SubscriptionProtocol.EMAIL_JSON,
        SubscriptionProtocol.SMS,
    }
)
WEB_PROTOCOLS = frozenset({SubscriptionProtocol.HTTP, SubscriptionProtocol.HTTPS})

# ListSubscriptions reports an unconfirmed subscription with this literal in
# place of a subscription ARN.
PENDING_CONFIRMATION = "PendingConfirmation"

ARN_PATTERN = re.compile(
    r"^arn:(?P<partition>aws[a-z-]*):(?P<service>[a-z0-9-]+):"
    r"(?P<region>[a-z0-9-]*):(?P<account>\d{12})?:(?P<resource>.+)$"
)

ALL_ATTRIBUTES = "All"


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
    REDRIVE_ALLOW_POLICY = "RedriveAllowPolicy"


class TopicAttribute(StrEnum):
    DISPLAY_NAME = "DisplayName"
    FIFO_TOPIC = "FifoTopic"
    CONTENT_BASED_DEDUPLICATION = "ContentBasedDeduplication"
    KMS_MASTER_KEY_ID = "KmsMasterKeyId"
    SUBSCRIPTIONS_CONFIRMED = "SubscriptionsConfirmed"
    SUBSCRIPTIONS_PENDING = "SubscriptionsPending"
    ARCHIVE_POLICY = "ArchivePolicy"


class EncryptionType(StrEnum):
    KMS = "kms"
    SSE_SQS = "sse-sqs"
    NONE = "none"


TRUE_STRING = "true"
