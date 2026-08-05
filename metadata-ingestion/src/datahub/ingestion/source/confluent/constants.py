from typing import Final

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

# Live catalog 500s on any GraphQL variables map (verified 2026-08-05), so
# pagination is inlined via these placeholders.
LIMIT_PLACEHOLDER: Final[str] = "{limit}"
OFFSET_PLACEHOLDER: Final[str] = "{offset}"

MAX_ERROR_BODY_CHARS: Final[int] = 500
