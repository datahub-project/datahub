from typing import Final

# Path appended to the Schema Registry endpoint to reach the Stream Catalog GraphQL
# API. See https://docs.confluent.io/cloud/current/stream-governance/graphql.html
CATALOG_GRAPHQL_PATH: Final[str] = "/catalog/graphql"

DEFAULT_PAGE_SIZE: Final[int] = 100
DEFAULT_TIMEOUT_SECONDS: Final[int] = 30

# Recipe path of the Stream Catalog block, used to point config errors at the right place.
CATALOG_CONFIG_PATH: Final[str] = "confluent_catalog"

# Every Confluent Cloud endpoint is served from this domain; the Stream Catalog does not
# exist anywhere else.
CONFLUENT_CLOUD_DOMAIN_SUFFIX: Final[str] = ".confluent.cloud"

# Confluent Cloud caps a single catalog page; values above this are rejected.
MAX_PAGE_SIZE: Final[int] = 1000

DATA_KEY: Final[str] = "data"
ERRORS_KEY: Final[str] = "errors"
MESSAGE_KEY: Final[str] = "message"

# Live catalog endpoint returns HTTP 500 for any GraphQL variables map
# (verified 2026-08-05), so pagination is inlined via these placeholders.
LIMIT_PLACEHOLDER: Final[str] = "{limit}"
OFFSET_PLACEHOLDER: Final[str] = "{offset}"

MAX_ERROR_BODY_CHARS: Final[int] = 500
