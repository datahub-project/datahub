import re
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

# Separates the deployment scope from the application name in an
# `application_lineage` key, matching the "domain/appspace" scope id itself.
LINEAGE_KEY_DELIMITER = "/"

# --- Application archives (EAR) ---------------------------------------------
# A BusinessWorks message schema is declared at design time, in the process that
# publishes it, and the only artefact carrying that declaration is the deployed
# application archive. The bwagent REST API is deployment topology and runtime
# statistics; its archive endpoints stop at config/encrypt/decrypt, so there is
# no way to fetch an EAR over HTTP. The operator supplies the file instead, the
# way the dbt source consumes a manifest.

# An EAR is a zip of module bundles; the parts we read are the BPEL-based process
# definitions and the XSDs their message elements resolve to.
EAR_PROCESS_SUFFIX = ".bwp"
EAR_SCHEMA_SUFFIX = ".xsd"
EAR_SUBSTVAR_SUFFIX = ".substvar"
# Nested module bundles: an EAR contains JARs which in turn hold the processes.
EAR_NESTED_ARCHIVE_SUFFIXES = (".jar", ".zip")

XSD_NAMESPACE = "http://www.w3.org/2001/XMLSchema"

# BW serialises a JMS activity as an extension activity tagged with its type id.
# BW5, BW6 and BWCE spell the surrounding XML differently but agree on this
# prefix, so activities are found by type rather than by element path.
JMS_ACTIVITY_TYPE_PREFIX = "bw.jms."
# Only the activities that carry a message body. Reply reuses the request's
# schema and would double-count the destination.
JMS_PUBLISH_ACTIVITY_TYPES = frozenset(
    {"bw.jms.SendMessage", "bw.jms.RequestReply", "bw.jms.SendReply"}
)
JMS_CONSUME_ACTIVITY_TYPES = frozenset(
    {"bw.jms.GetMessage", "bw.jms.WaitForMessage", "bw.jms.ReceiveMessage"}
)

# Attribute names carrying the destination and the message element vary across BW
# versions, so each is looked up by trying the known spellings in order.
DESTINATION_ATTRIBUTE_NAMES = (
    "destination",
    "destinationName",
    "jmsDestination",
    "queueName",
    "topicName",
)
MESSAGE_ELEMENT_ATTRIBUTE_NAMES = (
    "element",
    "inputElement",
    "messageElement",
    "bodyElement",
    "requestElement",
)
# Present on the activity when the destination kind is explicit rather than
# implied by the activity's own configuration.
DESTINATION_TYPE_ATTRIBUTE_NAMES = ("destinationType", "type")

DESTINATION_TYPE_QUEUE = "queue"
DESTINATION_TYPE_TOPIC = "topic"

# A destination is more often a module property reference than a literal, e.g.
# %%Orders.Queue%%. Unresolvable references are reported, never guessed - a wrong
# destination silently attaches a schema to the wrong topic.
PROPERTY_REFERENCE_PATTERN = re.compile(r"%%([^%]+)%%")

# Provenance recorded on a destination whose schema came from an archive, so it
# reads as the publisher's declaration rather than an estimate.
PROPERTY_SCHEMA_SOURCE = "schema_source"
PROPERTY_SCHEMA_DECLARED_BY = "schema_declared_by"
PROPERTY_SCHEMA_CONTRACT = "schema_contract"
SCHEMA_SOURCE_EAR = "tibco-bw-ear"

# Field paths of nested XSD elements are flattened with a dot, matching how the
# rest of DataHub addresses a nested schema field.
FIELD_PATH_DELIMITER = "."
# A self-referencing or mutually recursive type would otherwise flatten forever.
MAX_SCHEMA_DEPTH = 12

# EMS destination datasets are named "<server group>.<queue|topic>.<name>" by the
# TIBCO EMS source; lineage from a publisher has to reproduce that exactly.
EMS_PLATFORM = "tibco-ems"
EMS_NAME_DELIMITER = "."
EMS_DEFAULT_SERVER_GROUP = "default"
