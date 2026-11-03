from typing import Dict, FrozenSet, Tuple

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
FIELD_TYPE = "type"
FIELD_CURRENT_VERSION = "currentVersion"
FIELD_DELETED = "deleted"
FIELD_BRANCH_NAME = "branchName"
FIELD_COMPONENT_TYPE = "componentType"
FIELD_ACTIVE = "active"
VALUE_TRUE = "true"
VALUE_FALSE = "false"
COMPONENT_TYPE_PROCESS = "process"

# Query responses can render a long as ["Long", n] instead of a plain number.
LONG_TYPE_TAG = "Long"

# Boomi signals rate limiting with 503, not 429; 429 is kept in case a proxy
# in front of the API uses it.
RETRY_STATUS_CODES: Tuple[int, ...] = (429, 500, 502, 503, 504)
RETRY_METHODS: FrozenSet[str] = frozenset({"GET", "POST"})
RETRY_TOTAL = 5
RETRY_BACKOFF_FACTOR = 1.0
HTTP_FORBIDDEN = 403

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
