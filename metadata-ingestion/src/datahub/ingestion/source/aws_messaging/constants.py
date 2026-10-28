import re

from datahub.utilities.str_enum import StrEnum

SQS_PLATFORM = "sqs"
SNS_PLATFORM = "sns"
LAMBDA_PLATFORM = "lambda"
EVENTBRIDGE_PLATFORM = "eventbridge"
EVENTBRIDGE_PIPES_PLATFORM = "eventbridge-pipes"
# Owned by the kinesis source; only referenced here for SNS-to-Firehose lineage.
FIREHOSE_PLATFORM = "kinesis-firehose"
FIREHOSE_DELIVERY_JOB_ID = "delivery"

LAMBDA_JOB_ID = "invoke"
EVENTBRIDGE_RULE_JOB_ID = "route"
EVENTBRIDGE_PIPE_JOB_ID = "pipe"


class AwsService(StrEnum):
    SQS = "sqs"
    SNS = "sns"
    TAGGING = "resourcegroupstaggingapi"
    GLUE = "glue"
    LAMBDA = "lambda"
    EVENTS = "events"
    PIPES = "pipes"


QUEUE_URL_PATTERN = re.compile(r"/(?P<account>\d{12})/(?P<name>[^/?#]+)/?$")

ARN_PATTERN = re.compile(
    r"^arn:(?P<partition>aws[a-z-]*):(?P<service>[a-z0-9-]+):"
    r"(?P<region>[a-z0-9-]*):(?P<account>\d{12})?:(?P<resource>.+)$"
)


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


WEB_PROTOCOLS = frozenset({SubscriptionProtocol.HTTP, SubscriptionProtocol.HTTPS})

# ListSubscriptions reports an unconfirmed subscription with this literal in
# place of a subscription ARN.
PENDING_CONFIRMATION = "PendingConfirmation"

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


class TopicAttribute(StrEnum):
    DISPLAY_NAME = "DisplayName"
    FIFO_TOPIC = "FifoTopic"
    CONTENT_BASED_DEDUPLICATION = "ContentBasedDeduplication"
    KMS_MASTER_KEY_ID = "KmsMasterKeyId"
    SUBSCRIPTIONS_CONFIRMED = "SubscriptionsConfirmed"
    SUBSCRIPTIONS_PENDING = "SubscriptionsPending"
    ARCHIVE_POLICY = "ArchivePolicy"


class SubscriptionAttribute(StrEnum):
    REDRIVE_POLICY = "RedrivePolicy"
    RAW_MESSAGE_DELIVERY = "RawMessageDelivery"


class EncryptionType(StrEnum):
    KMS = "kms"
    SSE_SQS = "sse-sqs"
    NONE = "none"


class LineageRelationship(StrEnum):
    DEAD_LETTER = "dead_letter"
    SUBSCRIPTION = "subscription"
    SUBSCRIPTION_DEAD_LETTER = "subscription_dead_letter"


LINEAGE_RELATIONSHIP_PROPERTY = "relationship"

GLUE_ENTITY_NOT_FOUND = "EntityNotFoundException"
