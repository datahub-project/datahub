from typing import Final

# Path appended to the Schema Registry endpoint to reach the Stream Catalog GraphQL
# API. See https://docs.confluent.io/cloud/current/stream-governance/graphql.html
CATALOG_GRAPHQL_PATH: Final[str] = "/catalog/graphql"

DEFAULT_PAGE_SIZE: Final[int] = 100
DEFAULT_TIMEOUT_SECONDS: Final[int] = 30

# Confluent Cloud caps a single catalog page; values above this are rejected.
MAX_PAGE_SIZE: Final[int] = 1000

DATA_KEY: Final[str] = "data"
ERRORS_KEY: Final[str] = "errors"
MESSAGE_KEY: Final[str] = "message"
