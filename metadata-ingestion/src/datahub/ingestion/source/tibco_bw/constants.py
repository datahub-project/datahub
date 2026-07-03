from typing import FrozenSet, Tuple

TIBCO_BW_PLATFORM = "tibco-bw"

# The connector targets two TIBCO integration runtimes behind one platform:
#   - on_prem: ActiveMatrix BusinessWorks (BW/BWCE) via the bwagent REST API.
#   - cloud:   TIBCO Cloud Integration (TCI) via the public cloud REST API.
DEPLOYMENT_ON_PREM = "on_prem"
DEPLOYMENT_CLOUD = "cloud"

DEFAULT_CLOUD_BASE_URL = "https://api.cloud.tibco.com"

# bwagent (on-prem) resource paths, relative to base_url. bwagent listens on
# port 8079 by default and exposes the deployment topology under /bw/v1.
BW_DOMAINS_PATH = "bw/v1/domains"
BW_APPSPACES_TEMPLATE = "bw/v1/domains/{domain}/appspaces"
BW_APPNODES_TEMPLATE = "bw/v1/domains/{domain}/appspaces/{appspace}/appnodes"
BW_APPLICATIONS_TEMPLATE = "bw/v1/domains/{domain}/appspaces/{appspace}/applications"

# TIBCO Cloud Integration (cloud) resource paths, relative to base_url.
TCI_USERINFO_PATH = "v1/userinfo"
TCI_APPS_TEMPLATE = "v1/subscriptions/{subscription}/apps"
# Key under which /v1/userinfo returns the caller's accessible subscriptions.
TCI_SUBSCRIPTIONS_KEY = "subscriptions"

HTTP_SCHEME_HTTP = "http://"
HTTP_SCHEME_HTTPS = "https://"
HEADER_AUTHORIZATION = "Authorization"
HEADER_ACCEPT = "Accept"
CONTENT_TYPE_JSON = "application/json"
AUTH_BEARER_PREFIX = "Bearer "

HTTP_RETRY_MAX_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_FACTOR = 1.0
HTTP_RETRY_STATUS_CODES: Tuple[int, ...] = (429, 500, 502, 503, 504)
HTTP_RETRY_ALLOWED_METHODS: FrozenSet[str] = frozenset({"GET"})

# A DataFlow groups deployed applications; on-prem its id is `domain/appspace`
# and on cloud it is the subscription id. Applications become DataJobs whose
# ids are unique within their scope, so no extra prefixing is required.
SCOPE_ID_DELIMITER = "/"

# Custom-property keys emitted on the DataFlow (scope) and DataJob (application).
PROPERTY_DEPLOYMENT_TYPE = "deployment_type"
PROPERTY_DOMAIN = "domain"
PROPERTY_APPSPACE = "appspace"
PROPERTY_APPNODE_COUNT = "appnode_count"
PROPERTY_APPNODES = "appnodes"
PROPERTY_ORGANIZATION = "organization"
PROPERTY_SUBSCRIPTION = "subscription"
PROPERTY_REGION = "region"
PROPERTY_VERSION = "version"
PROPERTY_STATE = "state"
PROPERTY_STATUS = "status"
PROPERTY_APP_TYPE = "app_type"

# Joins appnode "name (status)" entries into the single appnodes property value.
APPNODE_ENTRY_DELIMITER = ", "
