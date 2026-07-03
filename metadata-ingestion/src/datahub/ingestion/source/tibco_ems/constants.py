import re
from typing import FrozenSet, Tuple

TIBCO_EMS_PLATFORM = "tibco-ems"

# EMS REST Proxy (admin/monitoring API) resource paths, relative to base_url.
# `/connect` establishes a server session and returns the session cookie that
# authorises every subsequent monitoring call.
CONNECT_PATH = "connect"
QUEUES_PATH = "system/ems/queues"
TOPICS_PATH = "system/ems/topics"
BRIDGES_PATH = "system/ems/configuration/bridges"

HTTP_SCHEME_HTTP = "http://"
HTTP_SCHEME_HTTPS = "https://"
HEADER_AUTHORIZATION = "Authorization"
HEADER_CONTENT_TYPE = "Content-Type"
CONTENT_TYPE_JSON = "application/json"
AUTH_BEARER_PREFIX = "Bearer "

HTTP_RETRY_MAX_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_FACTOR = 1.0
HTTP_RETRY_STATUS_CODES: Tuple[int, ...] = (429, 500, 502, 503, 504)
HTTP_RETRY_ALLOWED_METHODS: FrozenSet[str] = frozenset({"GET", "POST"})

# EMS destination namespaces are independent: a queue and a topic can share the
# same name. We prefix the dataset name with the destination type so their urns
# never collide, while the display name stays the bare destination name.
DEST_TYPE_QUEUE = "queue"
DEST_TYPE_TOPIC = "topic"
NAME_DELIMITER = "."

# Custom-property keys emitted on destination datasets.
PROPERTY_DESTINATION_TYPE = "destination_type"
PROPERTY_GLOBAL = "global"
PROPERTY_SECURE = "secure"
PROPERTY_MAX_MSGS = "max_msgs"
PROPERTY_MAX_BYTES = "max_bytes"
PROPERTY_PREFETCH = "prefetch"
PROPERTY_EXPIRATION = "expiration"
PROPERTY_PENDING_MESSAGES = "pending_message_count"
PROPERTY_CONSUMER_COUNT = "consumer_count"

# EMS creates internal destinations (monitoring, undelivered messages, temporary
# destinations) whose names start with "$sys." or "$TMP$". They are excluded by
# default because they are not business data flows.
SYSTEM_DESTINATION_PATTERN = re.compile(r"^\$(sys\.|TMP\$)")
