from typing import Final, Tuple

from typing_extensions import LiteralString

# https://docs.confluent.io/cloud/current/stream-governance/graphql.html
CATALOG_GRAPHQL_PATH: Final[str] = "/catalog/graphql"

DEFAULT_PAGE_SIZE: Final[int] = 100
DEFAULT_TIMEOUT_SECONDS: Final[int] = 30

CONFLUENT_CLOUD_DOMAIN_SUFFIX: Final[str] = ".confluent.cloud"

# Confluent Cloud rejects page sizes above this.
MAX_PAGE_SIZE: Final[int] = 1000

# Hard stop if the catalog ignores {offset} and keeps returning full pages.
MAX_CATALOG_PAGES: Final[int] = 10_000

DATA_KEY: Final[str] = "data"
ERRORS_KEY: Final[str] = "errors"
MESSAGE_KEY: Final[str] = "message"

# Catalog GraphQL rejects a variables map; inline limit/offset instead.
LIMIT_PLACEHOLDER: Final[str] = "{limit}"
OFFSET_PLACEHOLDER: Final[str] = "{offset}"

MAX_ERROR_BODY_CHARS: Final[int] = 500

# Transient Kafka REST failures while listing topics for Confluent Cloud lineage.
# urllib3 Retry.total is additional retries after the first attempt.
MAX_KAFKA_TOPIC_FETCH_ATTEMPTS: Final[int] = 3

# Safety valve for the v3 topic-list pagination loop, in case a server keeps
# returning a `metadata.next` link.
MAX_KAFKA_TOPIC_PAGES: Final[int] = 10_000
KAFKA_TOPIC_FETCH_RETRY_STATUS_CODES: Final[Tuple[int, ...]] = (
    429,
    500,
    502,
    503,
    504,
)

KAFKA_REST_NO_ENDPOINT: LiteralString = (
    "Could not resolve the Kafka REST endpoint for the live-cluster topic list"
)
KAFKA_REST_NO_ENDPOINT_CATALOG: LiteralString = (
    "Could not resolve the Kafka REST endpoint for the live-cluster topic list, "
    "so Stream Catalog lineage will not be cross-checked against the broker"
)
KAFKA_REST_NO_AUTH: LiteralString = (
    "No authentication credentials available for the Kafka REST API"
)
KAFKA_REST_NO_AUTH_CATALOG: LiteralString = (
    "No authentication credentials available for the Kafka REST API, "
    "so Stream Catalog lineage will not be cross-checked against the broker"
)
KAFKA_REST_FETCH_FAILED: LiteralString = "Failed to get topics from the Kafka REST API"
KAFKA_REST_FETCH_FAILED_CATALOG: LiteralString = (
    "Failed to get topics from the Kafka REST API, so Stream Catalog lineage "
    "will not be cross-checked against the broker"
)
KAFKA_REST_UNEXPECTED_RESPONSE: LiteralString = (
    "Unexpected response format from the Kafka REST API topic list"
)
KAFKA_REST_UNEXPECTED_RESPONSE_CATALOG: LiteralString = (
    "Unexpected response format from the Kafka REST API topic list, "
    "so Stream Catalog lineage will not be cross-checked against the broker"
)
