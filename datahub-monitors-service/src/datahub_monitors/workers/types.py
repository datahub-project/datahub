from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import Field

from datahub_monitors.common.config import PermissiveBaseModel


class CredentialResolverType(Enum):
    """Enumeration of credential resolver types."""

    LOCAL = "LOCAL"
    REMOTE = "REMOTE"


class ExecutorConfig(PermissiveBaseModel):
    region: str
    executor_id: str = Field(alias="executorId")
    queue_url: str = Field(alias="queueUrl")
    access_key: str = Field(alias="accessKeyId")
    secret_key: str = Field(alias="secretKeyId")
    session_token: str = Field(alias="sessionToken")
    expiration: Optional[datetime]
