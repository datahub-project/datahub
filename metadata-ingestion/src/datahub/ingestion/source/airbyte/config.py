import logging
from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from typing import Dict, List, Optional
from urllib import parse

import pydantic
from pydantic import Field
from pydantic.class_validators import root_validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import DEFAULT_ENV, DatasetSourceConfigMixin
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

    ORCHESTRATOR = "airbyte"
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
    CONNECTIONCONFIGURATION = "connectionConfiguration"
    CONFIGURATION = "configuration"
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

    # Source Config constants
    API_URL = "api_url"
    CLOUD_DEPLOY = "cloud_deploy"
    API_KEY = "api_key"
    USERNAME = "username"
    PASSWORD = "password"
    DEFAULT_CLOUD_DEPLOY = False
    DEFAULT_API_URL = "http://localhost:8000/api"

    # Airbyte data classes attribute constants
    NAMESPACE_DEFINITION = "namespace_definition"
    NAMESPACE_FORMAT = "namespace_format"
    JOB_TYPE = "job_type"
    LAST_UPDATED_AT = "last_updated_at"
    BYTES_SYNCED = "bytes_synced"
    ROWS_SYNCED = "rows_synced"

    # Job status constants
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class DataPlatformPair:
    airbyte_data_platform_name: str
    datahub_data_platform_name: str


class SupportedDataPlatform(Enum):
    POSTGRES_SQL = DataPlatformPair(
        airbyte_data_platform_name="postgres", datahub_data_platform_name="postgres"
    )


@dataclass
class AirbyteSourceReport(StaleEntityRemovalSourceReport):
    workspaces_scanned: int = 0
    connections_scanned: int = 0
    jobs_scanned: int = 0
    filtered_workspaces: List[str] = dataclass_field(default_factory=list)
    filtered_connections: List[str] = dataclass_field(default_factory=list)
    filtered_jobs: List[str] = dataclass_field(default_factory=list)

    def report_workspaces_scanned(self, count: int = 1) -> None:
        self.workspaces_scanned += count

    def report_connections_scanned(self, count: int = 1) -> None:
        self.connections_scanned += count

    def report_jobs_scanned(self, count: int = 1) -> None:
        self.jobs_scanned += count

    def report_workspaces_dropped(self, model: str) -> None:
        self.filtered_workspaces.append(model)

    def report_connections_dropped(self, view: str) -> None:
        self.filtered_connections.append(view)

    def report_jobs_dropped(self, view: str) -> None:
        self.filtered_jobs.append(view)


class PlatformDetail(ConfigModel):
    platform_instance: Optional[str] = pydantic.Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )
    env: str = pydantic.Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )


class AirbyteSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    cloud_deploy: bool = pydantic.Field(
        default=Constant.DEFAULT_CLOUD_DEPLOY,
        description="Enabled to fetch metadata from Airbyte Cloud."
        "If it is enabled then provide api_key in the recipe."
        "Username & password is required for Airbyte OSS only.",
    )
    api_url: str = Field(
        default=Constant.DEFAULT_API_URL, description="Airbyte API hosted URL."
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
    # Airbyte source/destination connector's server to platform instance mapping
    server_to_platform_instance: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of Airbyte source/destination connectors server i.e host or url to Data platform instance."
        "Ex: Airbyte Postgres connector will have host as server.",
    )

    @root_validator(pre=True)
    def validate_api_url(cls, values: Dict) -> Dict:
        api_url = (
            values.get(Constant.API_URL)
            if Constant.API_URL in values
            else Constant.DEFAULT_API_URL
        )
        url_parsed = parse.urlparse(api_url)
        if url_parsed.scheme not in ("http", "https") or not url_parsed.netloc:
            raise ValueError("Please provide valid API hosted URL.")
        cloud_deploy = (
            values.get(Constant.CLOUD_DEPLOY)
            if Constant.CLOUD_DEPLOY in values
            else Constant.DEFAULT_CLOUD_DEPLOY
        )
        if cloud_deploy and url_parsed.netloc != "api.airbyte.com":
            raise ValueError(
                "To fetch metadata from Airbyte Cloud, user must set api_url as 'https://api.airbyte.com' in the recipe."
            )
        elif not cloud_deploy and (not url_parsed.port or url_parsed.path != "/api"):
            raise ValueError(
                "To fetch metadata from Airbyte OSS, user must set api_url as 'http://<host>:<port>/api' in the recipe."
            )
        return values

    @root_validator(pre=True)
    def validate_auth_configs(cls, values: Dict) -> Dict:
        cloud_deploy = (
            values.get(Constant.CLOUD_DEPLOY)
            if Constant.CLOUD_DEPLOY in values
            else Constant.DEFAULT_CLOUD_DEPLOY
        )
        if cloud_deploy and values.get(Constant.API_KEY) is None:
            raise ValueError(
                "To fetch metadata from Airbyte Cloud, user must provide api_key in the recipe."
            )
        elif not cloud_deploy and (
            values.get(Constant.USERNAME) is None
            or values.get(Constant.PASSWORD) is None
        ):
            raise ValueError(
                "To fetch metadata from Airbyte OSS, user must provide username and password in the recipe."
            )
        return values
