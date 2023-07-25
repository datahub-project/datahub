import logging
from typing import Optional

import pydantic
from pydantic import Field

from datahub.configuration.source_common import DatasetSourceConfigMixin

logger = logging.getLogger(__name__)


class Constant:
    """
    keys used in airbyte plugin
    """

    WORKSPACE_LIST = "WORKSPACE_LIST"
    CONNECTION_LIST = "CONNECTION_LIST"
    SOURCE_GET = "SOURCE_GET"
    DESTINATION_GET = "DESTINATION_GET"
    JOBS_LIST = "JOBS_LIST"
    AUTHORIZATION = "Authorization"
    ASCII = "ascii"
    WORKSPACES = "workspaces"
    WORKSPACEID = "workspaceId"
    NAME = "name"
    CONNECTIONS = "connections"
    CONNECTIONID = "connectionId"
    SOURCEID = "sourceId"
    DESTINATIONID = "destinationId"
    STATUS = "status"
    NAMESPACEDEFINITION = "namespaceDefinition"
    NAMESPACEFORMAT = "namespaceFormat"
    PREFIX = "prefix"
    SOURCETYPE = "sourceType"
    SOURCENAME = "sourceName"
    DESTINATIONTYPE = "destinationType"
    DESTINATIONNAME = "destinationName"
    JOBS = "jobs"
    JOBID = "jobId"
    ID = "id"
    JOBTYPE = "jobType"
    CONFIGTYPE = "configType"
    STARTTIME = "startTime"
    CREATEDAT = "createdAt"
    ENDTIME = "endTime"
    ENDEDAT = "endedAt"
    DURATION = "duration"
    CONFIGID = "configId"
    LASTUPDATEDAT = "lastUpdatedAt"
    UPDATEDAT = "updatedAt"
    BYTESSYNCED = "bytesSynced"
    ROWSSYNCED = "rowsSynced"
    RECORDSSYNCED = "recordsSynced"
    ATTEMPTS = "attempts"
    DATA = "data"


class AirbyteConfig(DatasetSourceConfigMixin):
    connect_uri: str = Field(default="localhost:8000", description="Airbyte host URL.")
    username: Optional[str] = Field(default=None, description="Airbyte username.")
    password: Optional[pydantic.SecretStr] = Field(
        default=None, description="Airbyte password."
    )
    api_key: Optional[str] = Field(default=None, description="Airbyte Cloud API key.")
