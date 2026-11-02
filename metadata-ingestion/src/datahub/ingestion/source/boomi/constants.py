from typing import Dict, FrozenSet, Tuple

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.utilities.str_enum import StrEnum

PLATFORM = "boomi"

DEFAULT_API_URL = "https://api.boomi.com"
API_PATH_TEMPLATE = "{api_url}/api/rest/v1/{account_id}"
TOKEN_USERNAME_PREFIX = "BOOMI_TOKEN."
SCHEME_HTTP = "http"
SCHEME_HTTPS = "https"
LOOPBACK_HOSTS: FrozenSet[str] = frozenset({"localhost", "127.0.0.1", "::1"})

OBJECT_COMPONENT = "Component"
OBJECT_COMPONENT_METADATA = "ComponentMetadata"
OBJECT_DEPLOYED_PACKAGE = "DeployedPackage"
OBJECT_ENVIRONMENT = "Environment"
OBJECT_EXECUTION_RECORD = "ExecutionRecord"
OBJECT_FOLDER = "Folder"
QUERY_SUFFIX = "query"
QUERY_FILTER_KEY = "QueryFilter"
QUERY_EXPRESSION_KEY = "expression"
HTTP_GET = "GET"
HTTP_POST = "POST"
QUERY_MORE_SUFFIX = "queryMore"

CONTENT_TYPE_JSON = "application/json"
CONTENT_TYPE_TEXT = "text/plain"
CONTENT_TYPE_XML = "application/xml"
HEADER_ACCEPT = "Accept"
HEADER_CONTENT_TYPE = "Content-Type"

OPERATOR_AND = "and"
OPERATOR_EQUALS = "EQUALS"
OPERATOR_GREATER_THAN_OR_EQUAL = "GREATER_THAN_OR_EQUAL"
FIELD_TYPE = "type"
FIELD_CURRENT_VERSION = "currentVersion"
FIELD_DELETED = "deleted"
FIELD_BRANCH_NAME = "branchName"
FIELD_COMPONENT_TYPE = "componentType"
FIELD_ACTIVE = "active"
VALUE_TRUE = "true"
VALUE_FALSE = "false"
COMPONENT_TYPE_PROCESS = "process"
FIELD_EXECUTION_TIME = "executionTime"
EXECUTION_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Query responses can render a long as ["Long", n] instead of a plain number.
LONG_TYPE_TAG = "Long"

# Boomi signals rate limiting with 503, not 429; 429 is kept in case a proxy
# in front of the API uses it.
RETRY_STATUS_CODES: Tuple[int, ...] = (429, 500, 502, 503, 504)
RETRY_METHODS: FrozenSet[str] = frozenset({"GET", "POST"})
RETRY_TOTAL = 5
RETRY_BACKOFF_FACTOR = 1.0
HTTP_FORBIDDEN = 403
HTTP_BAD_REQUEST = 400
# ExecutionRecord/query answers HTTP 400 with this message when the account
# has no runtimes (Atoms) the token can see, which just means no runs.
NO_RUNTIMES_ERROR_MESSAGE = "do not have access to any containers"

# Boomi documents 10 requests/second per account; stay below it by default.
DEFAULT_MAX_REQUESTS_PER_SECOND = 8
MAX_REQUESTS_PER_SECOND_LIMIT = 10
DEFAULT_REQUEST_TIMEOUT_SECONDS = 60

PROCESS_JOB_ID = "process"

# Only these API hosts have a confirmed UI host; unknown hosts get no link.
API_HOST_TO_UI_HOST: Dict[str, str] = {
    "api.boomi.com": "platform.boomi.com",
    "api.platform.gb.boomi.com": "platform.gb.boomi.com",
}
PROCESS_URL_TEMPLATE = (
    "https://{ui_host}/AtomSphere.html#build;accountId={account_id};components={id};"
    "componentIdOnFocus={id}"
)

XML_ATTR_FOLDER_FULL_PATH = "folderFullPath"
XML_ELEMENT_DESCRIPTION = "description"
XML_NAMESPACE_END = "}"

PROP_COMPONENT_ID = "componentId"
PROP_VERSION = "version"
PROP_BRANCH_NAME = "branchName"
PROP_MODIFIED_DATE = "modifiedDate"
PROP_FOLDER_PATH = "folderFullPath"
PROP_DEPLOYMENTS = "deployments"
DEPLOYMENT_SEPARATOR = ", "
DEPLOYMENT_FORMAT = "{environment}={version}"

PROP_STATUS = "status"
PROP_ATOM_NAME = "atomName"
PROP_EXECUTION_TYPE = "executionType"
PROP_PARENT_EXECUTION_ID = "parentExecutionId"
PROP_TOP_LEVEL_EXECUTION_ID = "topLevelExecutionId"
PROP_INBOUND_DOCUMENT_COUNT = "inboundDocumentCount"
PROP_OUTBOUND_DOCUMENT_COUNT = "outboundDocumentCount"
PROP_INBOUND_ERROR_DOCUMENT_COUNT = "inboundErrorDocumentCount"

EXECUTION_TYPE_MANUAL = "exec_manual"
EXECUTION_TYPE_SUB_PROCESS = "sub_process"

# Boomi purges run history after the runtime's purgeHistoryDays, 30 by default.
MAX_EXECUTION_HISTORY_DAYS = 30
DEFAULT_EXECUTION_HISTORY_DAYS = 7
DEFAULT_MAX_EXECUTIONS_PER_PROCESS = 50

EXECUTION_STATUS_RESULTS: Dict[str, InstanceRunResult] = {
    "COMPLETE": InstanceRunResult.SUCCESS,
    "COMPLETE_WARN": InstanceRunResult.SUCCESS,
    "ERROR": InstanceRunResult.FAILURE,
    "ABORTED": InstanceRunResult.FAILURE,
    "DISCARDED": InstanceRunResult.SKIPPED,
}
# ABORTED_RECOVERING is still being recovered by the runtime, so it is not final.
EXECUTION_STATUSES_IN_PROGRESS: FrozenSet[str] = frozenset(
    {"INPROCESS", "STARTED", "ENQUEUED", "ABORTED_RECOVERING"}
)

XML_ATTR_TYPE = "type"
XML_ATTR_SUB_TYPE = "subType"
XML_ATTR_SHAPE_TYPE = "shapetype"
XML_ATTR_ID = "id"
XML_ATTR_VALUE = "value"
XML_ELEMENT_SHAPE = "shape"
XML_ELEMENT_FIELD = "field"
SHAPE_TYPE_CONNECTOR_ACTION = "connectoraction"
SHAPE_TYPE_PROCESS_CALL = "processcall"
XML_ELEMENT_CONNECTOR_ACTION = "connectoraction"
XML_ELEMENT_PROCESS_CALL = "processcall"
XML_ATTR_CONNECTOR_TYPE = "connectorType"
XML_ATTR_ACTION_TYPE = "actionType"
XML_ATTR_CONNECTION_ID = "connectionId"
XML_ATTR_OPERATION_ID = "operationId"
XML_ATTR_PROCESS_ID = "processId"

XML_ELEMENT_DB_CONNECTION_SETTINGS = "DatabaseConnectionSettings"
XML_ATTR_DRIVER_ID = "driverId"
XML_ATTR_HOST = "host"
XML_ATTR_PORT = "port"
XML_ATTR_DB_NAME = "dbname"
XML_ATTR_URL_FORMAT = "urlFormat"
XML_ELEMENT_GENERIC_CONNECTION_CONFIG = "GenericConnectionConfig"
FIELD_ID_URL = "url"
FIELD_ID_SCHEMA_NAME = "schemaName"

XML_ELEMENT_GENERIC_OPERATION_CONFIG = "GenericOperationConfig"
XML_ATTR_OBJECT_TYPE_ID = "objectTypeId"
XML_ATTR_OBJECT_TYPE_NAME = "objectTypeName"
XML_ATTR_OPERATION_TYPE = "operationType"
XML_ATTR_CUSTOM_OPERATION_TYPE = "customOperationType"
FIELD_ID_QUERY = "query"
XML_ELEMENT_DB_GET_ACTION = "DatabaseGetAction"
XML_ELEMENT_DB_SEND_ACTION = "DatabaseSendAction"
XML_ELEMENT_SALESFORCE_GET_ACTION = "SalesforceGetAction"
XML_ELEMENT_SALESFORCE_SEND_ACTION = "SalesforceSendAction"
XML_ATTR_OBJECT_NAME = "objectName"
XML_ATTR_OBJECT_ACTION = "objectAction"
GET_ACTION_SUFFIX = "GetAction"
SEND_ACTION_SUFFIX = "SendAction"
# The attribute that names the database profile on a legacy Database operation
# is not documented; these are tried in order.
DB_ACTION_PROFILE_ATTRIBUTES: Tuple[str, ...] = (
    "profileId",
    "profile",
    "requestProfile",
    "responseProfile",
)

XML_ELEMENT_DB_STATEMENT = "DBStatement"
XML_ATTR_TABLE_NAME = "tableName"
XML_ATTR_STATEMENT_TYPE = "statementType"
XML_ELEMENT_SQL = "sql"

COMPONENT_TYPE_CONNECTION = "connector-settings"
COMPONENT_TYPE_OPERATION = "connector-action"
COMPONENT_TYPE_DB_PROFILE = "profile.db"

SUBTYPE_DATABASE = "database"
SUBTYPE_DATABASE_V2 = "officialboomi-X3979C-dbv2da-prod"
SUBTYPE_SALESFORCE = "salesforce"

# Connectors whose endpoints are not datasets (RFCs, URLs, file paths,
# mailboxes). Their shapes are counted per subType rather than reported as
# unresolved lineage.
NON_DATASET_SUBTYPES: FrozenSet[str] = frozenset(
    {
        "sapjco",
        "http",
        "ftp",
        "sftp",
        "officialboomi-X3979C-sftpv2-prod",
        "officialboomi-X3979C-sftpvx-prod",
        "disk",
        "disk-sdk",
        "mail",
    }
)

ACTION_TYPE_LISTEN = "listen"

READ_OPERATION_TYPES: FrozenSet[str] = frozenset({"QUERY", "GET"})
WRITE_OPERATION_TYPES: FrozenSet[str] = frozenset(
    {"CREATE", "INSERT", "UPSERT", "UPDATE", "DELETE", "EXECUTE"}
)

DRIVER_ID_CUSTOM = "custom"
DRIVER_ID_PLATFORM_MAP: Dict[str, str] = {
    "sqlserver": "mssql",
    "sqlserver-jtds": "mssql",
    "oracle": "oracle",
    "mysql": "mysql",
    "postgresql": "postgres",
    "db2": "db2",
    "sap hana": "hana",
    "snowflake": "snowflake",
}

JDBC_PREFIX = "jdbc:"
JDBC_SCHEME_PLATFORM_MAP: Dict[str, str] = {
    "sqlserver": "mssql",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "postgresql": "postgres",
    "oracle": "oracle",
    "snowflake": "snowflake",
    "redshift": "redshift",
    "db2": "db2",
}
JDBC_SQLSERVER_DATABASE_KEYS: FrozenSet[str] = frozenset({"databasename", "database"})
JDBC_SNOWFLAKE_DATABASE_KEY = "db"
JDBC_SNOWFLAKE_SCHEMA_KEY = "schema"
# Platforms whose JDBC URL carries the database as the first path segment.
JDBC_PATH_DATABASE_PLATFORMS: FrozenSet[str] = frozenset(
    {"mysql", "mariadb", "postgres", "redshift", "db2"}
)

# DataHub names MySQL and MariaDB tables `<database>.<table>`, with no schema.
TWO_TIER_PLATFORMS: FrozenSet[str] = frozenset({"mysql", "mariadb"})
# The Oracle source leaves the database (SID or service) out of its URNs by
# default, so a database read from the connection would not join.
PLATFORMS_WITHOUT_DATABASE: FrozenSet[str] = frozenset({"oracle"})
SALESFORCE_PLATFORM = "salesforce"
MSSQL_PLATFORM = "mssql"
SNOWFLAKE_PLATFORM = "snowflake"


class OperationDirection(StrEnum):
    READ = "read"
    WRITE = "write"
    UNKNOWN = "unknown"


class UnresolvedReason(StrEnum):
    COMPONENT_UNREADABLE = "component_unreadable"
    UNKNOWN_DIRECTION = "unknown_direction"
    UNSUPPORTED_CONNECTOR = "unsupported_connector"
    NO_TABLE = "no_table"
    SQL_PARSE_FAILED = "sql_parse_failed"
    MISSING_OPERATION = "missing_operation"
