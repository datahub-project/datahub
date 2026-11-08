from typing import Dict, Final, Tuple

from datahub.utilities.str_enum import StrEnum

# Dataset platform for Kafka topics - matches the platform the Kafka source emits
# topics under, so processing-job input/output URNs line up with the topic entities.
KAFKA_PLATFORM: Final[str] = "kafka"

# All processing DataJobs of a given engine hang off one synthetic DataFlow so they
# group in the UI instead of scattering one flow per query/statement/app.
DATA_JOB_TYPE: Final[str] = "COMMAND"


class StreamProcessingEngine(StrEnum):
    FLINK = "flink"


# engine -> (flow_id, flow_name, flow_description)
ENGINE_FLOW_METADATA: Final[Dict[StreamProcessingEngine, Tuple[str, str, str]]] = {
    StreamProcessingEngine.FLINK: (
        "flink_statements",
        "Flink SQL Statements",
        "Confluent Cloud Flink SQL statements reading from and writing to this Kafka cluster.",
    ),
}

# Custom-property keys on the processing DataJob.
PROP_ENGINE: Final[str] = "engine"
PROP_QUERY: Final[str] = "query"
PROP_STATE: Final[str] = "state"

# Keep a single query from bloating a DataJob's custom properties.
MAX_QUERY_PROPERTY_CHARS: Final[int] = 10_000

# --- Confluent Cloud Flink API ----------------------------------------------
FLINK_HOST_TEMPLATE: Final[str] = "https://flink.{region}.{cloud}.confluent.cloud"
FLINK_STATEMENTS_PATH_TEMPLATE: Final[str] = (
    "/sql/v1/organizations/{organization_id}/environments/{environment_id}/statements"
)
FLINK_KEY_DATA: Final[str] = "data"
FLINK_KEY_METADATA: Final[str] = "metadata"
FLINK_KEY_NEXT: Final[str] = "next"
FLINK_KEY_NAME: Final[str] = "name"
FLINK_KEY_SPEC: Final[str] = "spec"
FLINK_KEY_STATEMENT: Final[str] = "statement"
FLINK_KEY_STATUS: Final[str] = "status"
FLINK_KEY_PHASE: Final[str] = "phase"
FLINK_KEY_COMPUTE_POOL: Final[str] = "compute_pool_id"
FLINK_PAGE_SIZE_PARAM: Final[str] = "page_size"
FLINK_DEFAULT_PAGE_SIZE: Final[int] = 100
# Safety valve if the API keeps returning a next link.
FLINK_MAX_PAGES: Final[int] = 1_000
# sqlglot has no Flink dialect. Flink SQL (Calcite) quotes identifiers with backticks and
# strings with single quotes, as Spark's dialect does; Postgres quotes identifiers with
# double quotes instead.
FLINK_SQL_DIALECT: Final[str] = "spark"
# Statement properties that set the catalog (environment) and database (Kafka cluster)
# for unqualified table names.
FLINK_PROP_CURRENT_CATALOG: Final[str] = "sql.current-catalog"
FLINK_PROP_CURRENT_DATABASE: Final[str] = "sql.current-database"
FLINK_KEY_PROPERTIES: Final[str] = "properties"
FLINK_PHASE_RUNNING: Final[str] = "RUNNING"
