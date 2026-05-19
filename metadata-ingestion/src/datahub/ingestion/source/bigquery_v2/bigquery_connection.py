import logging
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
    build_credentials_from_wif_dict,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


def _get_bigquery_client_info() -> ClientInfo:
    # Format: "<prod_name>/ver (GPN:<company name>)" per Google's recommended convention.
    return ClientInfo(user_agent=f"DataHub/{__version__} (GPN:DataHub)")


class BigQueryAuthType(StrEnum):
    SERVICE_ACCOUNT = "service_account"
    WORKLOAD_IDENTITY_FEDERATION = "workload_identity_federation"


class BigQueryConnectionConfig(GCPWIFConfig):
    """Connection configuration for BigQuery.

    Supports three authentication modes:
    - **Application Default Credentials (ADC)**: No credential fields needed; GCP client
      libraries pick up credentials from the environment (e.g. GCE/GKE metadata server).
    - **Service account** (``auth_type: service_account``): Supply ``credential`` with the
      service account JSON key, or set ``GOOGLE_APPLICATION_CREDENTIALS`` in the environment.
    - **Workload Identity Federation** (``auth_type: workload_identity_federation``): Supply
      one of the ``gcp_wif_configuration*`` fields; no long-lived key required.
    """

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

    _credentials: Optional[Credentials] = PrivateAttr(None)

    extra_client_options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional keyword arguments passed to GCP client constructors (bigquery.Client, GCPLoggingClient, etc.).",
    )

    project_on_behalf: Optional[str] = Field(
        default=None,
        description="[Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account.",
    )

    @model_validator(mode="after")
    def _validate_and_setup_auth(self) -> "BigQueryConnectionConfig":
        # Guard against double-initialization on model_copy() (which does not
        # re-run validators).  Note: model_validate(config.model_dump()) DOES
        # re-run this validator because PrivateAttrs are not serialized.
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
            # project_id from the WIF config is intentionally ignored; users must
            # set project_on_behalf explicitly when targeting a specific project.
            self._credentials, project_id = build_credentials_from_wif_dict(
                self.to_wif_dict(), self.wif_config_source()
            )
            if project_id is not None and self.project_on_behalf is None:
                logger.info(
                    "WIF credential includes project_id=%s but project_on_behalf is "
                    "not set; the WIF project_id is not used automatically. Set "
                    "project_on_behalf to target a specific project.",
                    project_id,
                )
        elif self.credential is not None:
            logger.debug("Setting up service account credentials from credential field")
            try:
                self._credentials = (
                    service_account.Credentials.from_service_account_info(
                        self.credential.to_dict()
                    )
                )
            except Exception as e:
                raise ValueError(
                    f"Failed to load service account credentials "
                    f"(error_type={type(e).__name__}): {e}"
                ) from e
        else:
            logger.info(
                "No credential provided for auth_type=%s; falling back to "
                "Application Default Credentials (ADC).",
                self.auth_type,
            )

        return self

    def has_explicit_credentials(self) -> bool:
        """Return True if an explicit credential object has been loaded.

        When False, GCP client libraries fall back to Application Default
        Credentials (ADC) from the environment.
        """
        return self._credentials is not None

    def get_bigquery_client(self) -> bigquery.Client:
        client_options = {**self.extra_client_options}
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
