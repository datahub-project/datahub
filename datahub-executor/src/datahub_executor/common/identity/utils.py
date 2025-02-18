import hashlib
import logging
import random
import socket
import string
from urllib.parse import urlparse

from datahub_executor.common.constants import (
    DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME,
    DATAHUB_URN_MAX_LEN,
)
from datahub_executor.config import (
    DATAHUB_EXECUTOR_INTERNAL_WORKER,
    DATAHUB_EXECUTOR_NAMESPACE,
    DATAHUB_EXECUTOR_POOL_NAME,
    DATAHUB_GMS_URL,
)

logger = logging.getLogger(__name__)


def get_string_hash(addr: str) -> str:
    return hashlib.sha256(bytes(addr, encoding="ascii")).hexdigest()[0:8]


def get_hostname() -> str:
    try:
        return socket.gethostname()
    except Exception as e:
        logger.exception("gethostname(): %s", e)
        return "error"


def get_host_address(host: str) -> str:
    try:
        return socket.gethostbyname(host)
    except Exception as e:
        logger.exception("gethostbyname(): %s", e)
        return "error"


def get_remote_executor_id_from_urn(urn: str) -> str:
    return urn.replace(f"urn:li:{DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME}:", "")


def get_hostname_from_url(url: str) -> str:
    if "//" not in url:
        url = f"http://{url}"

    hostname = urlparse(url).hostname
    if hostname is None:
        return "undefined"

    return hostname


def get_random_string(length: int) -> str:
    letters = string.ascii_lowercase + string.digits
    return "".join(random.choice(letters) for i in range(length))


def create_instance_id(addr: str) -> str:
    # InstanceID has the following format:
    #     {address-hash8}-{random4}.{executor-id}.{gms-host}
    #
    # For internal use cases, gms-host is always set to "dh-datahub-gms",
    # which is non-descriptive and thus namespace is used instead.

    if DATAHUB_EXECUTOR_INTERNAL_WORKER and DATAHUB_EXECUTOR_NAMESPACE is not None:
        hostname = DATAHUB_EXECUTOR_NAMESPACE
    else:
        hostname = get_hostname_from_url(DATAHUB_GMS_URL)

    instance_id = "{}-{}.{}.{}".format(
        get_string_hash(addr),
        get_random_string(8),
        DATAHUB_EXECUTOR_POOL_NAME,
        hostname,
    )

    # Max URN length is 500 chars, including prefix. Truncate if ID length exceeds the limit
    # to make it consistent across fields that do not enforce the limit. Keep the left-most part
    # of the ID intact.
    max_len = DATAHUB_URN_MAX_LEN - len(DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME) - 10
    if len(instance_id) > max_len:
        logger.warning(
            f"Identity: instance_id is longer than {max_len} and will be truncated."
        )
        instance_id = instance_id[:max_len]

    return instance_id
