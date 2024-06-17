from enum import Enum

import pydantic
from datahub.configuration.common import ConnectionModel
from datahub.ingestion.graph.client import DatahubClientConfig


class LineageDirection(str, Enum):
    UPSTREAM = "UPSTREAM"
    DOWNSTREAM = "DOWNSTREAM"
    BOTH = "BOTH"


class ShareConfig(ConnectionModel):
    connection: DatahubClientConfig

    # Eventually we might add "config" fields here too. For now, it's
    # just the GMS connection info.


class ExecuteShareResult(pydantic.BaseModel):
    status: str
    entities_shared: list[str]


class ExecuteUnshareResult(pydantic.BaseModel):
    status: str
    entities_unshared: list[str]
