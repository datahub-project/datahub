import json
import logging
import os
import tempfile

# Google Cloud Dataform client is imported conditionally to avoid import errors
from typing import TYPE_CHECKING, Any, Dict, Optional

from google.api_core.gapic_v1.client_info import ClientInfo

if TYPE_CHECKING:
    from google.cloud import dataform_v1beta1  # type: ignore
else:
    from google.cloud import dataform_v1beta1

from google.oauth2 import service_account
from pydantic import Field, PrivateAttr

from datahub._version import __version__
from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.dataform.dataform_config import DataformCloudConfig

logger = logging.getLogger(__name__)


def _get_dataform_client_info() -> ClientInfo:
    """Get ClientInfo with DataHub user-agent for GCP API identification.

    Follows Google's recommended format:
    "<prod_name>/ver (GPN:<company name>; <other comments>)"
    """
    return ClientInfo(user_agent=f"DataHub/{__version__} (GPN:DataHub)")


class DataformConnectionConfig(ConfigModel):
    """Configuration for connecting to Google Cloud Dataform."""

    cloud_config: DataformCloudConfig = Field(
        description="Google Cloud Dataform configuration"
    )

    extra_client_options: Dict[str, Any] = Field(
        default={},
        description="Additional options to pass to the Dataform client.",
    )

    _credentials_path: Optional[str] = PrivateAttr(None)

    def __init__(self, **data: Any):
        super().__init__(**data)

        # Handle authentication setup
        if self.cloud_config.credential:
            # Use the new GCPCredential approach
            self._credentials_path = (
                self.cloud_config.credential.create_credential_temp_file(
                    project_id=self.cloud_config.project_id
                )
            )
            logger.debug(
                f"Creating temporary credential file at {self._credentials_path}"
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path
        elif self.cloud_config.service_account_key_file:
            # Legacy approach - set environment variable
            logger.warning(
                "Using deprecated 'service_account_key_file'. Consider migrating to 'credential' configuration."
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
                self.cloud_config.service_account_key_file
            )
        elif self.cloud_config.credentials_json:
            # Legacy approach - create temp file from JSON

            logger.warning(
                "Using deprecated 'credentials_json'. Consider migrating to 'credential' configuration."
            )

            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, suffix=".json"
            ) as f:
                json.dump(json.loads(self.cloud_config.credentials_json), f)
                self._credentials_path = f.name
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path

    def get_dataform_client(self) -> "dataform_v1beta1.DataformClient":
        """Create and return a Dataform client with proper authentication."""
        try:
            credentials = None

            if self.cloud_config.credential:
                # Use the structured credential configuration
                credentials_info = self.cloud_config.credential.to_dict(
                    project_id=self.cloud_config.project_id
                )
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info
                )
            elif self.cloud_config.service_account_key_file:
                # Legacy: Load from key file
                credentials = service_account.Credentials.from_service_account_file(
                    self.cloud_config.service_account_key_file
                )
            elif self.cloud_config.credentials_json:
                # Legacy: Load from JSON string

                credentials_info = json.loads(self.cloud_config.credentials_json)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info
                )
            # If no credentials provided, use default authentication (e.g., from environment)

            client_options = self.extra_client_options.copy()

            return dataform_v1beta1.DataformClient(
                credentials=credentials,
                client_info=_get_dataform_client_info(),
                **client_options,
            )

        except Exception as e:
            logger.error(f"Failed to create Dataform client: {e}")
            raise

    def get_repository_path(self) -> str:
        """Get the full repository path for Dataform API calls."""
        return f"projects/{self.cloud_config.project_id}/locations/{self.cloud_config.region}/repositories/{self.cloud_config.repository_id}"

    def get_workspace_path(self) -> str:
        """Get the full workspace path for Dataform API calls."""
        workspace_id = self.cloud_config.workspace_id or "default"
        return f"{self.get_repository_path()}/workspaces/{workspace_id}"

    def get_compilation_result_path(self, compilation_result_id: str) -> str:
        """Get the full compilation result path for Dataform API calls."""
        return (
            f"{self.get_repository_path()}/compilationResults/{compilation_result_id}"
        )

    def cleanup(self) -> None:
        """Clean up temporary credential files."""
        if self._credentials_path and os.path.exists(self._credentials_path):
            try:
                os.unlink(self._credentials_path)
                logger.debug(
                    f"Cleaned up temporary credential file: {self._credentials_path}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to clean up credential file {self._credentials_path}: {e}"
                )


def create_dataform_connection(
    cloud_config: DataformCloudConfig,
) -> DataformConnectionConfig:
    """Factory function to create a DataformConnectionConfig from cloud config."""
    return DataformConnectionConfig(cloud_config=cloud_config)
