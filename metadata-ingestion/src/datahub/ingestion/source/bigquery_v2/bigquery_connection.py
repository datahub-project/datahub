import logging
from typing import Any, Dict, Optional

from google.api_core.client_info import ClientInfo
from google.cloud import bigquery, datacatalog_v1, resourcemanager_v3
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from pydantic import Field

from datahub._version import __version__
from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential

logger = logging.getLogger(__name__)


def _get_bigquery_client_info() -> ClientInfo:
    """Get ClientInfo with DataHub user-agent for GCP API identification.

    Follows Google's recommended format:
    "<prod_name>/ver (GPN:<company name>; <other comments>)"
    """
    return ClientInfo(user_agent=f"DataHub/{__version__} (GPN:DataHub)")


class BigQueryConnectionConfig(ConfigModel):
    credential: Optional[GCPCredential] = Field(
        default=None, description="BigQuery credential informations"
    )

    extra_client_options: Dict[str, Any] = Field(
        default={},
        description="Additional options to pass to google.cloud.logging_v2.client.Client.",
    )

    project_on_behalf: Optional[str] = Field(
        default=None,
        description="[Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account.",
    )

    def get_credentials_dict(self) -> Optional[Dict[str, Any]]:
        if self.credential:
            return self.credential.to_dict()
        return None

    def get_bigquery_client(self) -> bigquery.Client:
        client_options = self.extra_client_options
        return bigquery.Client(
            self.project_on_behalf,
            client_info=_get_bigquery_client_info(),
            **client_options,
        )

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
            return GCPLoggingClient(
                **client_options,
                project=project_id,
                client_info=_get_bigquery_client_info(),
            )
        else:
            return GCPLoggingClient(
                **client_options, client_info=_get_bigquery_client_info()
            )

    def get_sql_alchemy_url(self) -> str:
        if self.project_on_behalf:
            return f"bigquery://{self.project_on_behalf}"
        # When project_id is not set, we will attempt to detect the project ID
        # based on the credentials or environment variables.
        # See https://github.com/mxmzdlv/pybigquery#authentication.
        return "bigquery://"
