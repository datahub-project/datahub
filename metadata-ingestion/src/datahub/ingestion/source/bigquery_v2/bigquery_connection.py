import json
import logging
import os
import tempfile
from typing import Any, Dict, Optional

from google.api_core.client_info import ClientInfo
from google.auth.credentials import Credentials
from google.cloud import bigquery, datacatalog_v1, resourcemanager_v3
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from google.oauth2 import service_account
from pydantic import Field, PrivateAttr, model_validator

from datahub._version import __version__
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.common.gcp_wif_config import (
    GCPWIFConfig,
    load_wif_credentials,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


def _get_bigquery_client_info() -> ClientInfo:
    """Get ClientInfo with DataHub user-agent for GCP API identification.

    Follows Google's recommended format:
    "<prod_name>/ver (GPN:<company name>; <other comments>)"
    """
    return ClientInfo(user_agent=f"DataHub/{__version__} (GPN:DataHub)")


class BigQueryAuthType(StrEnum):
    SERVICE_ACCOUNT = "service_account"
    WORKLOAD_IDENTITY_FEDERATION = "workload_identity_federation"


class BigQueryConnectionConfig(GCPWIFConfig):
    auth_type: BigQueryAuthType = Field(
        default=BigQueryAuthType.SERVICE_ACCOUNT,
        description=(
            "Authentication type to use. Defaults to 'service_account'. "
            "Set to 'workload_identity_federation' to authenticate via "
            "[Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation), "
            "which avoids long-lived service account keys."
        ),
    )

    credential: Optional[GCPCredential] = Field(
        default=None,
        description=(
            "BigQuery service account credential. Required when auth_type is "
            "'service_account' unless Application Default Credentials are "
            "available (e.g. running on GCE/GKE)."
        ),
    )

    _credentials_path: Optional[str] = PrivateAttr(None)
    _credentials: Optional[Credentials] = PrivateAttr(None)

    extra_client_options: Dict[str, Any] = Field(
        default={},
        description="Additional keyword arguments passed to GCP client constructors (bigquery.Client, GCPLoggingClient, etc.).",
    )

    project_on_behalf: Optional[str] = Field(
        default=None,
        description="[Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account.",
    )

    @model_validator(mode="after")
    def _validate_and_setup_auth(self) -> "BigQueryConnectionConfig":
        # Pydantic re-runs mode="after" validators on model_copy/model_validate.
        # Without this guard, each rerun would write a new temp file (leaking
        # the previous one), re-stomp GOOGLE_APPLICATION_CREDENTIALS, and call
        # credentials.refresh() again (network round-trip).
        if self._credentials is not None:
            return self

        if self.auth_type == BigQueryAuthType.WORKLOAD_IDENTITY_FEDERATION:
            if self.wif_config_source() is None:
                raise ValueError(
                    "One of gcp_wif_configuration (file path), "
                    "gcp_wif_configuration_json (JSON content), or "
                    "gcp_wif_configuration_json_string (JSON string) is required "
                    "when auth_type is 'workload_identity_federation'"
                )
            if self.credential is not None:
                raise ValueError(
                    "credential must not be set when auth_type is "
                    "'workload_identity_federation'. Use the gcp_wif_configuration* "
                    "options instead."
                )
            self._setup_wif_credentials()
        elif self.credential is not None:
            self._setup_service_account_credentials()
        # else: no credential and no WIF — fall back to Application Default
        # Credentials (ADC), which GCP client libraries pick up automatically
        # from the environment (e.g. GCE/GKE metadata server).

        return self

    def _setup_service_account_credentials(self) -> None:
        assert self.credential is not None
        self._credentials_path = self.credential.create_credential_temp_file()
        logger.debug(f"Creating temporary credential file at {self._credentials_path}")
        # Keep env var for backward compatibility (e.g. SQLAlchemy BigQuery dialect).
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path
        # Build explicit credentials for thread-safe client creation.
        # In Observe/assertions, multiple BigQuery connections with different
        # service accounts run concurrently in the same process. Without
        # explicit credentials, concurrent configs overwrite the shared env
        # var, causing clients to authenticate with the wrong service account.
        self._credentials = service_account.Credentials.from_service_account_info(
            self.credential.to_dict()
        )

    def _setup_wif_credentials(self) -> None:
        # project_id from the WIF config is intentionally ignored — users must
        # set project_on_behalf explicitly when targeting a specific project.
        self._credentials, _ = load_wif_credentials(self)

        # Persist the WIF JSON to a temp file so callers that rely on the
        # GOOGLE_APPLICATION_CREDENTIALS env var (e.g. the SQLAlchemy BigQuery
        # dialect) can pick it up. google-auth auto-detects external_account
        # credentials from the JSON file.
        #
        # Caveat: like the service-account path, this env var is process-wide.
        # Concurrent BigQuery configs with different WIF identities will
        # trample each other for SQLAlchemy callers (the explicit `_credentials`
        # object handles this for direct GCP client calls). This temp file is also
        # never explicitly deleted (same issue as the SA path). Track at:
        # https://linear.app/acryl-data/issue/ING-2376
        wif_dict = self.to_wif_dict()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fp:
            json.dump(wif_dict, fp)
            self._credentials_path = fp.name
        logger.debug(
            f"Wrote WIF configuration to temporary file at {self._credentials_path}"
        )
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path

    def get_bigquery_client(self) -> bigquery.Client:
        client_options = self.extra_client_options
        return bigquery.Client(
            self.project_on_behalf,
            credentials=self._credentials,
            client_info=_get_bigquery_client_info(),
            **client_options,
        )

    def get_projects_client(self) -> resourcemanager_v3.ProjectsClient:
        return resourcemanager_v3.ProjectsClient(credentials=self._credentials)

    def get_policy_tag_manager_client(self) -> datacatalog_v1.PolicyTagManagerClient:
        return datacatalog_v1.PolicyTagManagerClient(credentials=self._credentials)

    def make_gcp_logging_client(
        self, project_id: Optional[str] = None
    ) -> GCPLoggingClient:
        # See https://github.com/googleapis/google-cloud-python/issues/2674 for
        # why we disable gRPC here.
        client_options = {**self.extra_client_options, "_use_grpc": False}
        if project_id is not None:
            client_options["project"] = project_id
        return GCPLoggingClient(
            **client_options,
            credentials=self._credentials,
            client_info=_get_bigquery_client_info(),
        )

    def get_sql_alchemy_url(self) -> str:
        if self.project_on_behalf:
            return f"bigquery://{self.project_on_behalf}"
        # When project_id is not set, we will attempt to detect the project ID
        # based on the credentials or environment variables.
        # See https://github.com/mxmzdlv/pybigquery#authentication.
        return "bigquery://"
