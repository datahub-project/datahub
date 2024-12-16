import logging
from typing import Any, Dict

import boto3
import botocore
from botocore.client import BaseClient, Config
from kombu.transport.SQS import Channel

from datahub_executor.common.client.config.resolver import ExecutorConfigResolver
from datahub_executor.common.monitoring.metrics import (
    STATS_CREDENTIALS_REFRESH_ERRORS,
    STATS_CREDENTIALS_REFRESH_REQUESTS,
)
from datahub_executor.config import DATAHUB_EXECUTOR_WORKER_ID

logger = logging.getLogger(__name__)


def refresh_external_credentials(queue_id: str) -> Dict[str, str]:
    executor_config_resolver = ExecutorConfigResolver()
    _, executor_configs = executor_config_resolver.refresh_executor_configs()
    for executor_config in executor_configs:
        if executor_config.executor_id == queue_id:
            STATS_CREDENTIALS_REFRESH_REQUESTS.labels(queue_id).inc()
            return {
                "region": executor_config.region,
                "access_key": executor_config.access_key,
                "secret_key": executor_config.secret_key,
                "token": executor_config.session_token,
                "expiry_time": (
                    executor_config.expiration.isoformat()
                    if executor_config.expiration
                    else ""
                ),
            }

    STATS_CREDENTIALS_REFRESH_ERRORS.labels("NoQueue", queue_id).inc()

    logger.error(f"Failed to find credentials for queue_id = {queue_id}")
    return {}


def patched_handle_sts_session(
    self: Channel, queue: str, q: Dict[str, Any]
) -> BaseClient:
    if queue in self._predefined_queue_clients:
        return self._predefined_queue_clients[queue]

    queue_id = queue if queue is not None else DATAHUB_EXECUTOR_WORKER_ID
    metadata = refresh_external_credentials(queue_id)

    credentials = botocore.credentials.RefreshableCredentials.create_from_metadata(
        metadata=metadata,
        refresh_using=lambda: refresh_external_credentials(queue_id),
        method="sts-assume-role",
    )

    botocore_session = botocore.session.get_session()
    botocore_session._credentials = credentials
    botocore_session.set_config_variable("region", metadata.get("region"))
    session = boto3.session.Session(botocore_session=botocore_session)

    is_secure = self.is_secure if self.is_secure is not None else True
    client_kwargs = {"use_ssl": is_secure}
    if self.endpoint_url is not None:
        client_kwargs["endpoint_url"] = self.endpoint_url
    client_config = self.transport_options.get("client-config") or {}
    config = Config(**client_config)

    client = session.client("sqs", config=config, **client_kwargs)
    self._predefined_queue_clients[queue] = client

    return client
