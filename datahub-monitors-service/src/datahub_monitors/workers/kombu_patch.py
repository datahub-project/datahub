from typing import Dict, Optional

import boto3
import botocore
from botocore.client import BaseClient, Config
from kombu.transport.SQS import Channel

from datahub_monitors.config import EXECUTOR_ID
from datahub_monitors.workers.resolvers.executor_config_resolver import (
    ExecutorConfigResolver,
)


def refresh_external_credentials() -> Dict[str, str]:
    executor_config_resolver = ExecutorConfigResolver()
    _, executor_configs = executor_config_resolver.refresh_executor_configs()
    for executor_config in executor_configs:
        if executor_config.executor_id == EXECUTOR_ID:
            return {
                "access_key": executor_config.access_key,
                "secret_key": executor_config.secret_key,
                "token": executor_config.session_token,
                "expiry_time": executor_config.expiration.isoformat()
                if executor_config.expiration
                else "",
            }
    return {}


def patched_new_sqs_client(
    self: Channel,
    region: str,
    access_key_id: str,
    secret_access_key: str,
    session_token: Optional[str] = None,
) -> BaseClient:
    credentials = botocore.credentials.RefreshableCredentials.create_from_metadata(
        metadata=refresh_external_credentials(),
        refresh_using=refresh_external_credentials,
        method="sts-assume-role",
    )

    botocore_session = botocore.session.get_session()
    botocore_session._credentials = credentials
    botocore_session.set_config_variable("region", region)
    session = boto3.session.Session(botocore_session=botocore_session)

    is_secure = self.is_secure if self.is_secure is not None else True
    client_kwargs = {"use_ssl": is_secure}
    if self.endpoint_url is not None:
        client_kwargs["endpoint_url"] = self.endpoint_url
    client_config = self.transport_options.get("client-config") or {}
    config = Config(**client_config)
    return session.client("sqs", config=config, **client_kwargs)
