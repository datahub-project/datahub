import json
import logging
import os
import tempfile
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from pydantic import Field, SecretStr, model_validator

from datahub.configuration import ConfigModel
from datahub.configuration.validate_multiline_string import pydantic_multiline_string

logger = logging.getLogger(__name__)


@contextmanager
def temporary_credentials_file(credentials_dict: Dict[str, Any]) -> Iterator[str]:
    """Create a temporary credentials file that is automatically cleaned up."""
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        suffix=".json",
        prefix="gcp_creds_",
    )

    try:
        json.dump(credentials_dict, temp_file)
        temp_file.flush()
        temp_file.close()

        os.chmod(temp_file.name, 0o600)

        logger.debug("Created temporary credentials file: %s", temp_file.name)
        yield temp_file.name
    finally:
        try:
            os.unlink(temp_file.name)
            logger.debug("Cleaned up temporary credentials file: %s", temp_file.name)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.error(
                "SECURITY: Failed to cleanup credentials file %s - credentials may be leaked on disk! Error: %s",
                temp_file.name,
                e,
                exc_info=True,
            )


@contextmanager
def with_temporary_credentials(credentials_path: str) -> Iterator[None]:
    """Set GOOGLE_APPLICATION_CREDENTIALS temporarily, restoring on exit."""
    original = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        yield
    finally:
        if original is not None:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original
        else:
            os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)


@contextmanager
def gcp_credentials_context(
    credentials_dict: Optional[Dict[str, Any]],
) -> Iterator[None]:
    """
    Context manager for GCP credential setup/teardown.

    - If credentials_dict is None: yields immediately (uses ADC)
    - If credentials_dict provided: creates temp file, sets env var, cleans up

    Example:
        with gcp_credentials_context(config.get_credentials_dict()):
            yield from super().get_workunits()
    """
    if credentials_dict is None:
        yield
    else:
        with (
            temporary_credentials_file(credentials_dict) as cred_path,
            with_temporary_credentials(cred_path),
        ):
            yield


class GCPCredential(ConfigModel):
    project_id: Optional[str] = Field(
        None, description="Project id to set the credentials"
    )
    private_key_id: str = Field(description="Private key id")
    private_key: SecretStr = Field(
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n'"
    )
    client_email: str = Field(description="Client email")
    client_id: str = Field(description="Client Id")
    auth_uri: str = Field(
        default="https://accounts.google.com/o/oauth2/auth",
        description="Authentication uri",
    )
    token_uri: str = Field(
        default="https://oauth2.googleapis.com/token", description="Token uri"
    )
    auth_provider_x509_cert_url: str = Field(
        default="https://www.googleapis.com/oauth2/v1/certs",
        description="Auth provider x509 certificate url",
    )
    type: str = Field(default="service_account", description="Authentication type")
    client_x509_cert_url: Optional[str] = Field(
        default=None,
        description="If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
    )

    _fix_private_key_newlines = pydantic_multiline_string("private_key")

    @model_validator(mode="after")
    def validate_config(self) -> "GCPCredential":
        if self.client_x509_cert_url is None:
            self.client_x509_cert_url = (
                f"https://www.googleapis.com/robot/v1/metadata/x509/{self.client_email}"
            )
        return self

    def _get_serializable_config(
        self, project_id: Optional[str] = None
    ) -> Dict[str, str]:
        """Get config dict with SecretStr values converted to strings."""
        configs = self.model_dump()
        if hasattr(self.private_key, "get_secret_value"):
            configs["private_key"] = self.private_key.get_secret_value()
        if project_id:
            configs["project_id"] = project_id
        return configs

    # TODO: To be deprecated in favor of temporary_credentials_file
    def create_credential_temp_file(self, project_id: Optional[str] = None) -> str:
        configs = self._get_serializable_config(project_id)
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            cred_json = json.dumps(configs, indent=4, separators=(",", ": "))
            fp.write(cred_json.encode())
            return fp.name

    def to_dict(self, project_id: Optional[str] = None) -> Dict[str, str]:
        return self._get_serializable_config(project_id)

    @contextmanager
    def as_context(self, project_id: Optional[str] = None) -> Iterator[None]:
        """Context manager that sets up these credentials for GCP API calls.

        Creates a temporary credentials file, sets GOOGLE_APPLICATION_CREDENTIALS,
        and cleans up on exit.

        Example:
            with config.credential.as_context():
                client = bigquery.Client()
        """
        creds_dict = self.to_dict(project_id)
        with (
            temporary_credentials_file(creds_dict) as cred_path,
            with_temporary_credentials(cred_path),
        ):
            yield
