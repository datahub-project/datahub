from typing import Dict, FrozenSet, Tuple

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.utilities.str_enum import StrEnum

PLATFORM = "boomi-data-integration"


class DIRegion(StrEnum):
    US = "us"
    EU = "eu"
    IL = "il"
    AU = "au"


REGION_API_URLS: Dict[DIRegion, str] = {
    DIRegion.US: "https://api.rivery.io",
    DIRegion.EU: "https://api.eu-west-1.rivery.io",
    DIRegion.IL: "https://api.il-central-1.rivery.io",
    DIRegion.AU: "https://api.ap-southeast-2.rivery.io",
}

# Only the US console host is confirmed. Other regions get console links only
# when console_url is set.
API_CONSOLE_URLS: Dict[str, str] = {
    REGION_API_URLS[DIRegion.US]: "https://console.rivery.io",
}
RUN_CONSOLE_URL_TEMPLATE = (
    "{console_url}/activities/{account_id}/{environment_id}/activities/{river_id}"
    "?param=run_group_id&run={run_group_id}"
)

HTTPS_SCHEME = "https"
HTTP_SCHEME = "http"
LOOPBACK_HOSTS: FrozenSet[str] = frozenset({"localhost", "127.0.0.1", "::1"})

API_PATH_TEMPLATE = "{api_url}/v1/accounts/{account_id}"
ENVIRONMENTS_PATH = "environments"
ENVIRONMENT_PATH_TEMPLATE = "environments/{environment_id}"
RIVERS_PATH_TEMPLATE = "environments/{environment_id}/rivers"
RIVER_PATH_TEMPLATE = "environments/{environment_id}/rivers/{river_id}"
ACTIVITIES_PATH_TEMPLATE = "environments/{environment_id}/activities"
RIVER_RUNS_PATH_TEMPLATE = "environments/{environment_id}/rivers/{river_id}/runs"
USERS_PATH = "users"

PARAM_PAGE = "page"
PARAM_ITEMS_PER_PAGE = "items_per_page"
PARAM_START_TIME = "start_time"
PARAM_END_TIME = "end_time"
# UTC without a zone suffix; the API rejects a bare date with HTTP 422.
RUN_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
# The API caps items_per_page at 500 on every list endpoint.
ENVIRONMENTS_PAGE_SIZE = 100
RIVERS_PAGE_SIZE = 500
RIVERS_PROBE_PAGE_SIZE = 1
# The activities and runs endpoints cap items_per_page at 200.
ACTIVITIES_PAGE_SIZE = 200
RUNS_PAGE_SIZE = 200
USERS_PAGE_SIZE = 200
# Guards against an API that keeps returning a next_page link; 500 pages of
# 500 rivers is far beyond any real environment.
MAX_PAGES = 500

HEADER_ACCEPT = "Accept"
HEADER_AUTHORIZATION = "Authorization"
CONTENT_TYPE_JSON = "application/json"
BEARER_TEMPLATE = "Bearer {token}"
HTTP_GET = "GET"

HTTP_BAD_REQUEST = 400
HTTP_UNAUTHORIZED = 401
HTTP_FORBIDDEN = 403
ACCESS_DENIED_STATUS_CODES: FrozenSet[int] = frozenset(
    {HTTP_UNAUTHORIZED, HTTP_FORBIDDEN}
)

RETRY_STATUS_CODES: Tuple[int, ...] = (429, 500, 502, 503, 504)
RETRY_METHODS: FrozenSet[str] = frozenset({HTTP_GET})
RETRY_TOTAL = 5
RETRY_BACKOFF_FACTOR = 1.0

# No general rate limit is documented, so stay conservative by default.
DEFAULT_MAX_REQUESTS_PER_SECOND = 5
MAX_REQUESTS_PER_SECOND_LIMIT = 50
DEFAULT_REQUEST_TIMEOUT_SECONDS = 60


class RiverType(StrEnum):
    SOURCE_TO_TARGET = "source_to_target"
    ACTIONS = "actions"
    LOGIC = "logic"
    CONNECTOR_EXECUTOR = "connector_executor"


RIVER_CONSOLE_URL_TEMPLATE = (
    "{console_url}/rivers/{account_id}/{environment_id}/{river_id}"
)
# The console path above is only confirmed for logic rivers. Boomi documents a
# different path for the other types, which has not been verified yet, so
# they get no link.
CONSOLE_LINKED_RIVER_TYPES: FrozenSet[str] = frozenset({RiverType.LOGIC})

LOAD_JOB_ID = "load"
RUN_JOB_ID = "run"


class LogicNodeType(StrEnum):
    RUN_ONCE = "run_once"
    LOOP = "loop"
    CONDITION = "condition"
    RIVER = "river"
    ACTION = "action"
    LOGICODE = "logicode"


SQL_QUERY_STEP_SUFFIX = "_sql_query"
SQL_SCRIPT_STEP_SUFFIX = "_sql_script"
STEP_TARGET_TYPE_TABLE = "table"


class TargetLayout(StrEnum):
    DATABASE_SCHEMA_TABLE = "database_schema_table"
    PROJECT_DATASET_TABLE = "project_dataset_table"
    SCHEMA_TABLE = "schema_table"
    CATALOG_SCHEMA_TABLE = "catalog_schema_table"
    OBJECT_STORAGE = "object_storage"


ATHENA_TARGET = "athena"

# Keyed by the Data Integration target name (`properties.target.name`).
DI_PLATFORM_MAP: Dict[str, str] = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "postgres_rds": "postgres",
    "azure_sql": "mssql",
    "azure_synapse_analytics": "mssql",
    "athena": "athena",
    "s3": "s3",
    "gcs": "gcs",
    "onelake": "fabric-onelake",
}

# OneLake is left out: rivers name the workspace and lakehouse, while
# fabric-onelake URNs are keyed by their ids, so the URNs would never join.
TARGET_LAYOUTS: Dict[str, TargetLayout] = {
    "snowflake": TargetLayout.DATABASE_SCHEMA_TABLE,
    "bigquery": TargetLayout.PROJECT_DATASET_TABLE,
    "redshift": TargetLayout.SCHEMA_TABLE,
    "postgres_rds": TargetLayout.SCHEMA_TABLE,
    "azure_sql": TargetLayout.SCHEMA_TABLE,
    "azure_synapse_analytics": TargetLayout.SCHEMA_TABLE,
    "athena": TargetLayout.SCHEMA_TABLE,
    "databricks": TargetLayout.CATALOG_SCHEMA_TABLE,
    "s3": TargetLayout.OBJECT_STORAGE,
    "gcs": TargetLayout.OBJECT_STORAGE,
    "blob_storage": TargetLayout.OBJECT_STORAGE,
}

# Logic step types are `<prefix>_sql_query` and friends; the prefix names the
# warehouse with a different spelling than the river target names.
LOGIC_STEP_TARGET_NAMES: Dict[str, str] = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "postgres": "postgres_rds",
    "azure_sql": "azure_sql",
    "azure_synapse": "azure_synapse_analytics",
    "athena": "athena",
}


class SourceRunType(StrEnum):
    MULTI_TABLES = "multi_tables"
    CUSTOM_QUERY = "custom_query"


class SourceLayout(StrEnum):
    # The database comes from connection_to_platform and is required.
    DATABASE_SCHEMA_TABLE = "database_schema_table"
    # Two-tier platforms: the river's schema name is the database.
    DATABASE_TABLE = "database_table"
    # The database is prefixed when connection_to_platform has one.
    SCHEMA_TABLE = "schema_table"


# Keyed by the Data Integration source name (`properties.source.name`), which
# spells some platforms differently from target names, such as `postgresql`.
SOURCE_PLATFORM_MAP: Dict[str, str] = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "mssql": "mssql",
    "oracle": "oracle",
    "snowflake": "snowflake",
    "redshift": "redshift",
    "bigquery": "bigquery",
}

# Oracle URNs leave the database out unless the oracle source is told to add
# it, so the database is only used when connection_to_platform names one.
SOURCE_LAYOUTS: Dict[str, SourceLayout] = {
    "postgresql": SourceLayout.DATABASE_SCHEMA_TABLE,
    "mssql": SourceLayout.DATABASE_SCHEMA_TABLE,
    "snowflake": SourceLayout.DATABASE_SCHEMA_TABLE,
    "redshift": SourceLayout.DATABASE_SCHEMA_TABLE,
    "bigquery": SourceLayout.DATABASE_SCHEMA_TABLE,
    "mysql": SourceLayout.DATABASE_TABLE,
    "mariadb": SourceLayout.DATABASE_TABLE,
    "oracle": SourceLayout.SCHEMA_TABLE,
}


class LineageSkipReason(StrEnum):
    UNSUPPORTED_SOURCE = "unsupported_source"
    UNSUPPORTED_RUN_TYPE = "unsupported_run_type"
    MISSING_SOURCE_DATABASE = "missing_source_database"
    MISSING_SOURCE_TABLE = "missing_source_table"
    MISSING_QUERY = "missing_query"
    SQL_PARSE_FAILED = "sql_parse_failed"
    UNQUALIFIED_SQL_TABLE = "unqualified_sql_table"
    UNSUPPORTED_STEP_PLATFORM = "unsupported_step_platform"
    INVALID_QUERY_SETTINGS = "invalid_query_settings"


class ColumnSkipReason(StrEnum):
    EXPRESSION = "expression"
    METADATA_COLUMN = "metadata_column"
    MISSING_NAME = "missing_name"
    CUSTOM_FIELD_MAPPING = "custom_field_mapping"
    MULTI_STATEMENT_SCRIPT = "multi_statement_script"
    INSERT_WITHOUT_COLUMN_LIST = "insert_without_column_list"
    UNMAPPED_SQL_COLUMN = "unmapped_sql_column"
    NO_TARGET_TABLE = "no_target_table"
    NO_SOURCE_COLUMN = "no_source_column"
    NOT_IN_SCHEMA = "not_in_schema"
    MISSING_TARGET = "missing_target"
    INVALID_COLUMN = "invalid_column"
    CUSTOM_QUERY_NOT_SELECT = "custom_query_not_select"


TRANSFORM_IDENTITY = "IDENTITY"
TRANSFORM_SQL = "SQL"

TABLE_NAME_SEPARATOR = "."
QUOTE_CHARS = '"`[]'

PROP_RIVER_TYPE = "river_type"
PROP_RIVER_STATUS = "river_status"
PROP_RUN_TYPE = "run_type"
PROP_SCHEDULE = "schedule"
PROP_DATASOURCE_ID = "datasource_id"
PROP_SOURCE_NAME = "source"
PROP_TARGET_NAME = "target"
PROP_STEP_TYPE = "step_type"
PROP_CALLED_RIVER_ID = "river_id"
PROP_ENVIRONMENT_ID = "environment_id"
PROP_IS_DEFAULT = "is_default"
PROP_GROUP_ID = "group_id"
PROP_RUN_STATUS = "status"
PROP_TRIGGER_NAME = "trigger_name"
PROP_UNITS = "units"
PROP_RPU = "rpu"
PROP_ERROR_DESCRIPTION = "error_description"
PROP_RUN_TARGET_NAME = "target_name"
PROP_TABLE_COUNT = "table_count"
PROP_RUN_GROUP_ID = "run_group_id"
PROP_SUB_RIVER_ID = "sub_river_id"
SCHEDULE_SEPARATOR = ", "

UNREACHABLE_ENVIRONMENTS_TEMPLATE = (
    "Environments that could not be read: {environments}"
)
UNREACHABLE_SEPARATOR = "; "

# Run logs are kept for two weeks.
MAX_RUN_HISTORY_DAYS = 14
DEFAULT_RUN_HISTORY_DAYS = 7
DEFAULT_MAX_RUNS_PER_RIVER = 50
# Run groups are pruned back to the cap once this many times the cap are held,
# so pruning cost stays linear in the rows scanned.
RUN_GROUP_PRUNE_FACTOR = 2
MAX_ERROR_DESCRIPTION_LENGTH = 500
VALUE_SEPARATOR = ", "


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"
    SKIPPED = "skipped"


RUN_STATUSES_IN_PROGRESS: FrozenSet[str] = frozenset(
    {RunStatus.PENDING, RunStatus.RUNNING}
)
RUN_STATUSES_SKIPPED: FrozenSet[str] = frozenset(
    {RunStatus.CANCELED, RunStatus.SKIPPED}
)
RUN_STATUS_RESULTS: Dict[str, InstanceRunResult] = {
    RunStatus.SUCCEEDED: InstanceRunResult.SUCCESS,
    RunStatus.FAILED: InstanceRunResult.FAILURE,
    RunStatus.CANCELED: InstanceRunResult.SKIPPED,
    RunStatus.SKIPPED: InstanceRunResult.SKIPPED,
}
SCHEDULED_TRIGGERS: FrozenSet[str] = frozenset({"scheduled", "rescheduled"})
