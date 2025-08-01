import logging
import os
from typing import Any, Dict, Optional

from google.cloud import bigquery, datacatalog_v1, resourcemanager_v3
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from pydantic import Field, PrivateAttr

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential

logger = logging.getLogger(__name__)


class BigQueryConnectionConfig(ConfigModel):
    credential: Optional[GCPCredential] = Field(
        default=None, description="BigQuery credential informations"
    )

    _credentials_path: Optional[str] = PrivateAttr(None)

    extra_client_options: Dict[str, Any] = Field(
        default={},
        description="Additional options to pass to google.cloud.logging_v2.client.Client.",
    )

    project_on_behalf: Optional[str] = Field(
        default=None,
        description="[Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account.",
    )

    def __init__(self, **data: Any):
        super().__init__(**data)

        if self.credential:
            self._credentials_path = self.credential.create_credential_temp_file()
            logger.debug(
                f"Creating temporary credential file at {self._credentials_path}"
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path

    def get_bigquery_client(self) -> bigquery.Client:
        client_options = self.extra_client_options
        return bigquery.Client(self.project_on_behalf, **client_options)

    def get_projects_client(self) -> resourcemanager_v3.ProjectsClient:
        return resourcemanager_v3.ProjectsClient()

    def get_policy_tag_manager_client(self) -> datacatalog_v1.PolicyTagManagerClient:
        return datacatalog_v1.PolicyTagManagerClient()

    def make_gcp_logging_client(
        self, project_id: Optional[str] = None
    ) -> GCPLoggingClient:
        # See https://github.com/googleapis/google-cloud-python/issues/2674 for
        # why we disable gRPC here.
        client_options = self.extra_client_options.copy()
        client_options["_use_grpc"] = False
        if project_id is not None:
            return GCPLoggingClient(**client_options, project=project_id)
        else:
            return GCPLoggingClient(**client_options)

    def get_sql_alchemy_url(self) -> str:
        if self.project_on_behalf:
            return f"bigquery://{self.project_on_behalf}"
        # When project_id is not set, we will attempt to detect the project ID
        # based on the credentials or environment variables.
        # See https://github.com/mxmzdlv/pybigquery#authentication.
        return "bigquery://"
