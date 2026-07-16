import re

# --- endpoints ---------------------------------------------------------------
APPLICATION_INFO = "/rest/2.0/application/info"
OAUTH_TOKEN = "/rest/oauth/v2/token"
SESSION_LOGIN = "/rest/2.0/auth/sessions"
GRAPHQL = "/graphql/knowledgeGraph/v1"

ASSETS = "/rest/2.0/assets"
COMMUNITIES = "/rest/2.0/communities"
DOMAINS = "/rest/2.0/domains"
ATTRIBUTES = "/rest/2.0/attributes"
RELATIONS = "/rest/2.0/relations"
ASSET_TYPES = "/rest/2.0/assetTypes"
ATTRIBUTE_TYPES = "/rest/2.0/attributeTypes"
RELATION_TYPES = "/rest/2.0/relationTypes"
DOMAIN_TYPES = "/rest/2.0/domainTypes"
USERS = "/rest/2.0/users"
USER_GROUPS = "/rest/2.0/userGroups"
ROLES = "/rest/2.0/roles"
RESPONSIBILITIES = "/rest/2.0/responsibilities"
OUTPUT_MODULE_JOB = "/rest/2.0/outputModule/export/json-job"
JOBS = "/rest/2.0/jobs"
OUTPUT_MODULE_FILES = "/rest/2.0/outputModule/files"

# --- paging / query params ---------------------------------------------------
LIMIT = "limit"
COUNT_LIMIT = "countLimit"
CURSOR = "cursor"
OFFSET = "offset"
TYPE_ID = "typeId"
ASSET_ID = "assetId"
RELATION_TYPE_ID = "relationTypeId"

# --- response envelope fields ------------------------------------------------
# VERIFY these against the target env's schema — the public docs are thin on the
# exact GraphQL/cursor shapes.
RESULTS_FIELD = "results"
CURSOR_FIELD = "nextCursor"
TOTAL_FIELD = "total"
QUERY_FIELD = "query"
VARIABLES_FIELD = "variables"
ERRORS_FIELD = "errors"
DATA_FIELD = "data"
ACCESS_TOKEN_FIELD = "access_token"
ID_FIELD = "id"
RESULT_FIELD = "result"
MESSAGE_FIELD = "message"
USERNAME_FIELD = "username"
PASSWORD_FIELD = "password"

# --- auth --------------------------------------------------------------------
GRANT_CLIENT_CREDENTIALS = {"grant_type": "client_credentials"}
AUTHORIZATION = "Authorization"
BEARER = "Bearer {}"

# --- jobs --------------------------------------------------------------------
JOB_STATE_FIELD = "state"
JOB_SUCCESS = "COMPLETED"
JOB_TERMINAL = frozenset({"COMPLETED", "ERROR", "CANCELED"})

# --- http --------------------------------------------------------------------
PAGE_SIZE = 1000  # DGC "Enable maximum paging limit" cap: 1000 elements / call
RETRY_STATUS = [429, 500, 502, 503, 504]
HTTP_UNAUTHORIZED = 401

# Knowledge Graph GraphQL and cursor paging require a recent DGC release.
# VERIFY the exact thresholds against the target Collibra version.
GRAPHQL_MIN_VERSION = (2023, 5)
CURSOR_MIN_VERSION = (2022, 8)

VERSION_RE = re.compile(r"(\d+)\.(\d+)")
