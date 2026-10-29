import re

from datahub.utilities.str_enum import StrEnum

SQS_PLATFORM = "sqs"
SNS_PLATFORM = "sns"
LAMBDA_PLATFORM = "lambda"
EVENTBRIDGE_PLATFORM = "eventbridge"
EVENTBRIDGE_PIPES_PLATFORM = "eventbridge-pipes"
S3_PLATFORM = "s3"
# Owned by the kinesis source; only referenced here for SNS-to-Firehose lineage.
FIREHOSE_PLATFORM = "kinesis-firehose"
FIREHOSE_DELIVERY_JOB_ID = "delivery"

# AWS console paths for the region containers; EventBridge's console lives under
# `events`, not its platform name.
CONSOLE_PATHS = {
    SQS_PLATFORM: "sqs/v3/home",
    SNS_PLATFORM: "sns/v3/home",
    EVENTBRIDGE_PLATFORM: "events/home",
}

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
    S3 = "s3"
    CLOUDTRAIL = "cloudtrail"


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
    S3_NOTIFICATION = "s3_notification"


LINEAGE_RELATIONSHIP_PROPERTY = "relationship"

GLUE_ENTITY_NOT_FOUND = "EntityNotFoundException"

S3_PREFIX_FILTER = "prefix"

OBSERVED_PRODUCERS_PROPERTY = "observed_producers"

# CloudTrail Lake column aliases used in the producer query.
PRODUCER_COLUMN = "producer"
RESOURCE_COLUMN = "resource"

ASSUMED_ROLE_PATTERN = re.compile(
    r"^arn:aws[a-z-]*:sts::(?P<account>\d{12}):assumed-role/(?P<role>[^/]+)/(?P<session>.+)$"
)


class QueryStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"


QUERY_POLL_SECONDS = 2

# SQS queue URL hosts: `sqs.<region>.amazonaws.com[.cn]`, the legacy
# `<region>.queue.amazonaws.com`, and `queue.amazonaws.com` for us-east-1.
QUEUE_URL_HOST_PATTERN = re.compile(
    r"^(?:sqs\.(?P<region>[a-z0-9-]+)|(?P<legacy_region>[a-z0-9-]+)\.queue|(?P<global>queue))"
    r"\.amazonaws\.com(?P<china>\.cn)?$"
)
LEGACY_GLOBAL_QUEUE_REGION = "us-east-1"

BUCKET_REGION_PARAMETER = "BucketRegion"
# botocore 1.35.42 added ListBuckets' bucket-region filter (pagination came in 1.34.162).
MIN_BOTOCORE_FOR_BUCKET_REGION = "1.35.42"

# Values interpolated into the CloudTrail Lake SQL are checked against these
# first, so nothing from the recipe can change the query's shape.
AWS_REGION_PATTERN = re.compile(r"^[a-z]{2}(?:-[a-z]+)+-\d+$")
EVENT_DATA_STORE_PATTERN = re.compile(
    r"^(?:arn:aws[a-z-]*:cloudtrail:[a-z0-9-]+:\d{12}:eventdatastore/)?[A-Za-z0-9-]{1,128}$"
)
