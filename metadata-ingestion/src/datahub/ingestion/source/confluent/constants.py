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

# The live Confluent Cloud catalog endpoint returns HTTP 500 for any operation that
# carries a GraphQL variables map (verified 2026-08-05), so every catalog query spells
# its pagination arguments with these placeholders and the client inlines the integers
# before sending. GraphQL validation used to catch a query/variable mismatch for us;
# with variables gone, the client checks both placeholders are present instead.
LIMIT_PLACEHOLDER: Final[str] = "{limit}"
OFFSET_PLACEHOLDER: Final[str] = "{offset}"

# Enough of a rejected response to identify the cause without flooding the report.
MAX_ERROR_BODY_CHARS: Final[int] = 500
