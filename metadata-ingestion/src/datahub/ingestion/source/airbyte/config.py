import logging
from dataclasses import dataclass, field as dataclass_field
from typing import List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

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
    JOB = "job"
    JOBS = "jobs"
    JOBID = "jobId"
    ID = "id"
    JOBTYPE = "jobType"
    CONFIGTYPE = "configType"
    CONFIGTYPES = "configTypes"
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


@dataclass
class AirbyteSourceReport(StaleEntityRemovalSourceReport):
    workspaces_scanned: int = 0
    connections_scanned: int = 0
    filtered_workspaces: List[str] = dataclass_field(default_factory=list)
    filtered_connections: List[str] = dataclass_field(default_factory=list)

    def report_workspaces_scanned(self, count: int = 1) -> None:
        self.workspaces_scanned += count

    def report_connections_scanned(self, count: int = 1) -> None:
        self.connections_scanned += count

    def report_workspaces_dropped(self, model: str) -> None:
        self.filtered_workspaces.append(model)

    def report_connections_dropped(self, view: str) -> None:
        self.filtered_connections.append(view)


class AirbyteSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    cloud_deploy: bool = pydantic.Field(
        default=False,
        description="Whether to fetch metadata from Airbyte Cloud or Airbyte OSS. For Airbyte Cloud provide api_key and for Airbyte OSS provide username/password",
    )
    api_key: Optional[str] = Field(default=None, description="Airbyte Cloud API key.")
    username: Optional[str] = Field(default=None, description="Airbyte username.")
    password: Optional[pydantic.SecretStr] = Field(
        default=None, description="Airbyte password."
    )
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Airbyte Stateful Ingestion Config."
    )
