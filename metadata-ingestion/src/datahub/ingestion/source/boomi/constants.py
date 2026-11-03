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
XML_ATTR_NAME = "name"
XML_ATTR_KEY = "key"
XML_ATTR_TO_SHAPE = "toShape"
XML_ELEMENT_DRAGPOINT = "dragpoint"
SHAPE_TYPE_MAP = "map"
SHAPE_TYPE_PROCESS_ROUTE = "processroute"
SHAPE_TYPE_STOP = "stop"
SHAPE_TYPE_RETURN_DOCUMENTS = "returndocuments"
SHAPE_TYPE_EXCEPTION = "exception"
# Shapes that hand documents to another process or end the flow, so the
# documents after them are not the ones a Map before them produced.
FLOW_BREAK_SHAPE_TYPES = frozenset(
    {
        SHAPE_TYPE_PROCESS_CALL,
        SHAPE_TYPE_PROCESS_ROUTE,
        SHAPE_TYPE_STOP,
        SHAPE_TYPE_RETURN_DOCUMENTS,
        SHAPE_TYPE_EXCEPTION,
    }
)
XML_ELEMENT_MAP_SHAPE = "map"
XML_ATTR_MAP_ID = "mapId"

XML_ELEMENT_DB_CONNECTION_SETTINGS = "DatabaseConnectionSettings"
XML_ATTR_DRIVER_ID = "driverId"
XML_ATTR_HOST = "host"
XML_ATTR_PORT = "port"
XML_ATTR_DB_NAME = "dbname"
XML_ATTR_ADDITIONAL = "additional"
XML_ATTR_URL_FORMAT = "urlFormat"
XML_ELEMENT_GENERIC_CONNECTION_CONFIG = "GenericConnectionConfig"
FIELD_ID_URL = "url"
FIELD_ID_SCHEMA_NAME = "schemaName"
# Connectors other than Database V2 name their JDBC URL field differently, so
# a generic connection is matched on the value rather than the field id.
JDBC_VALUE_PREFIX = "jdbc:"

XML_ELEMENT_GENERIC_OPERATION_CONFIG = "GenericOperationConfig"
XML_ATTR_OBJECT_TYPE_ID = "objectTypeId"
XML_ATTR_OBJECT_TYPE_NAME = "objectTypeName"
XML_ATTR_OPERATION_TYPE = "operationType"
XML_ATTR_CUSTOM_OPERATION_TYPE = "customOperationType"
FIELD_ID_QUERY = "query"
# Lowercase fragments of operation field ids that may hold SQL.
SQL_FIELD_ID_MARKERS: Tuple[str, ...] = ("sql", "query")
XML_ELEMENT_DB_GET_ACTION = "DatabaseGetAction"
XML_ELEMENT_DB_SEND_ACTION = "DatabaseSendAction"
XML_ELEMENT_SALESFORCE_GET_ACTION = "SalesforceGetAction"
XML_ELEMENT_SALESFORCE_SEND_ACTION = "SalesforceSendAction"
XML_ATTR_OBJECT_NAME = "objectName"
XML_ATTR_OBJECT_ACTION = "objectAction"
GET_ACTION_SUFFIX = "GetAction"
SEND_ACTION_SUFFIX = "SendAction"
# Legacy Database operations name their profile on a child element, for
# example <DatabaseGetAction><ReadProfile profileId=".."/>.
XML_DB_PROFILE_ELEMENTS: Tuple[str, ...] = ("ReadProfile", "WriteProfile")
XML_ATTR_PROFILE_ID = "profileId"
# Attribute forms on the action element itself are kept as a fallback.
DB_ACTION_PROFILE_ATTRIBUTES: Tuple[str, ...] = (
    "profileId",
    "profile",
    "requestProfile",
    "responseProfile",
)

# Profile ids that GUI-built generic connector operations, such as Database
# V2, carry on GenericOperationConfig.
GENERIC_OPERATION_PROFILE_ATTRIBUTES: Tuple[str, ...] = (
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
STATEMENT_TYPE_DYNAMIC_PREFIX = "dynamic"

XML_ELEMENT_DATA_ELEMENTS = "DataElements"
XML_ELEMENT_MAP = "Map"
XML_ELEMENT_MAPPING = "Mapping"
XML_ELEMENT_FUNCTION_STEP = "FunctionStep"
XML_ATTR_FROM_PROFILE = "fromProfile"
XML_ATTR_TO_PROFILE = "toProfile"
XML_ATTR_FROM_KEY = "fromKey"
XML_ATTR_FROM_TYPE = "fromType"
XML_ATTR_FROM_FUNCTION = "fromFunction"
XML_ATTR_TO_KEY = "toKey"
XML_ATTR_TO_TYPE = "toType"
XML_ATTR_TO_FUNCTION = "toFunction"
XML_ATTR_IS_MAPPABLE = "isMappable"
MAPPING_TYPE_PROFILE = "profile"
MAPPING_TYPE_FUNCTION = "function"
# Profile elements that hold a value a map can read or write. JSON objects and
# arrays are containers without a value of their own.
PROFILE_FIELD_ELEMENTS: FrozenSet[str] = frozenset(
    {
        "DatabaseElement",
        "XMLElement",
        "JSONRootValue",
        "JSONObjectEntry",
        "JSONArrayElement",
        "FlatFileElement",
    }
)
PROFILE_PATH_SEPARATOR = "/"
TRANSFORM_IDENTITY = "IDENTITY"
# A column computed by an expression in SQL rather than copied.
TRANSFORM_SQL = "SQL"
# A map function whose FunctionStep has no type.
TRANSFORM_FUNCTION = "FUNCTION"

SUBTYPE_DATABASE = "database"
SUBTYPE_DATABASE_V2 = "officialboomi-X3979C-dbv2da-prod"
SUBTYPE_SALESFORCE = "salesforce"
# Connectors with a dedicated parser; every other connector is scanned for a
# JDBC URL.
PARSED_CONNECTION_SUBTYPES: FrozenSet[str] = frozenset(
    {SUBTYPE_DATABASE, SUBTYPE_DATABASE_V2, SUBTYPE_SALESFORCE}
)

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
# A custom driver's urlFormat is a template such as
# `jdbc:postgresql://{0}:{1}/{2}{3}`, filled from the connection's host, port,
# dbname and additional attributes, in that order.
URL_FORMAT_PLACEHOLDER_START = "{"
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

# DataHub names MySQL and MariaDB tables `<database>.<table>`, with no schema.
TWO_TIER_PLATFORMS: FrozenSet[str] = frozenset({"mysql", "mariadb"})
# The Oracle source leaves the database (SID or service) out of its URNs by
# default, so a database read from the connection would not join.
PLATFORMS_WITHOUT_DATABASE: FrozenSet[str] = frozenset({"oracle"})
SALESFORCE_PLATFORM = "salesforce"


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
    AMBIGUOUS_JDBC_URL = "ambiguous_jdbc_url"
    AMBIGUOUS_SQL = "ambiguous_sql"


class ColumnSkipReason(StrEnum):
    # The SQL reads more than one table, so a field cannot be tied to one.
    MULTI_TABLE_SQL = "multi_table_sql"
    NOT_IN_SELECT = "not_in_select"
    NO_SOURCE_COLUMN = "no_source_column"
    NOT_IN_INSERT_COLUMNS = "not_in_insert_columns"
    NESTED_FIELD = "nested_field"
    UNKNOWN_FIELD_KEY = "unknown_field_key"
    NOT_IN_SCHEMA = "not_in_schema"
    UNMAPPED_SQL_COLUMN = "unmapped_sql_column"
    INSERT_WITHOUT_COLUMN_LIST = "insert_without_column_list"
    # A map field targets a column the write's own SQL fills from a table.
    WRITTEN_BY_SQL = "written_by_sql"
    # A map field targets a write whose SQL reads other tables, but which of
    # the target's columns it fills is not known.
    UNMAPPED_SQL_WRITE = "unmapped_sql_write"
    # The parser does not tie a MERGE's INSERT and UPDATE branches to the
    # target table's columns.
    MERGE_STATEMENT = "merge_statement"


class MapSkipReason(StrEnum):
    COMPONENT_UNREADABLE = "component_unreadable"
    NO_MATCHING_READ = "no_matching_read"
    NO_MATCHING_WRITE = "no_matching_write"
    UNSUPPORTED_SOURCE = "unsupported_source"
    UNSUPPORTED_TARGET = "unsupported_target"
