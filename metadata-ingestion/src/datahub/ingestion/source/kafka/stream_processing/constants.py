from typing import Final

# Flink statements are DataJobs on the flink platform; the topics they read and write
# stay datasets on the kafka platform.
FLINK_PLATFORM: Final[str] = "flink"
DATA_JOB_TYPE: Final[str] = "COMMAND"

# Custom-property keys on a statement's DataJob.
PROP_QUERY: Final[str] = "query"
PROP_STATE: Final[str] = "state"
PROP_COMPUTE_POOL: Final[str] = "compute_pool_id"

# Keep a single query from bloating a DataJob's custom properties.
MAX_QUERY_PROPERTY_CHARS: Final[int] = 10_000

# Confluent Cloud Flink REST API (sql/v1):
# https://docs.confluent.io/cloud/current/flink/operate-and-deploy/flink-rest-api.html
FLINK_HOST_TEMPLATE: Final[str] = "https://flink.{region}.{cloud}.confluent.cloud"
FLINK_STATEMENTS_PATH_TEMPLATE: Final[str] = (
    "/sql/v1/organizations/{organization_id}/environments/{environment_id}/statements"
)
FLINK_PAGE_SIZE_PARAM: Final[str] = "page_size"
FLINK_DEFAULT_PAGE_SIZE: Final[int] = 100
# Safety valve if the API keeps returning a next link.
FLINK_MAX_PAGES: Final[int] = 1_000
FLINK_PHASE_RUNNING: Final[str] = "RUNNING"
# Statement properties that set the catalog (environment) and database (Kafka cluster)
# for unqualified table names.
FLINK_PROP_CURRENT_CATALOG: Final[str] = "sql.current-catalog"
FLINK_PROP_CURRENT_DATABASE: Final[str] = "sql.current-database"

# sqlglot has no Flink dialect. Flink SQL (Calcite) quotes identifiers with backticks and
# strings with single quotes, as Spark's dialect does; Postgres quotes identifiers with
# double quotes instead.
FLINK_SQL_DIALECT: Final[str] = "spark"
