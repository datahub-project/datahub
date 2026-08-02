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

# Every list response is an envelope: the records live under a resource-specific
# key, alongside an "errors" array and the pagination cursors. The record key must
# be named explicitly - "errors" is itself an array and is emitted first, so
# picking the envelope's first array value returns no records at all.
RESPONSE_KEY_QUEUES = "queues"
RESPONSE_KEY_TOPICS = "topics"
RESPONSE_KEY_BRIDGES = "bridges"
RESPONSE_KEY_ERRORS = "errors"
RESPONSE_KEY_NEXT_CURSOR = "next"

# Responses are capped at the proxy's `page_limit`; the remainder is reached by
# replaying the request with the previous page's "next" cursor until it is empty.
QUERY_PARAM_CURSOR = "cursor"
# A proxy that keeps handing back a cursor would otherwise loop forever. EMS
# estates are thousands of destinations at most, so this bound is never reached
# in practice by a correctly behaving server.
MAX_PAGES = 10_000

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

# One REST Proxy can administer several fault-tolerant EMS server groups at once,
# and each group is an independent destination namespace - the same queue name in
# two groups is two unrelated queues. The group therefore leads the dataset name
# and is the container the destination sits in.
DEFAULT_SERVER_GROUP = "default"

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

# A bridge endpoint can be a wildcard subscription rather than a single
# destination: "*" matches one name element and ">" matches the remainder.
# Neither maps to a concrete dataset urn, so lineage for such endpoints is
# skipped. Both tokens are reserved, so a concrete name never contains them.
WILDCARD_DESTINATION_PATTERN = re.compile(r"[*>]")
