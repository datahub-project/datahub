import json
import logging
from typing import Any, Dict, Optional, Tuple, Union

from google.auth import load_credentials_from_dict
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from pydantic import Field, model_validator

from datahub.configuration.common import ConfigModel

logger = logging.getLogger(__name__)


class GCPWIFConfig(ConfigModel):
    """
    Mixin config for GCP Workload Identity Federation (WIF) authentication.

    Provides three mutually-exclusive ways to supply the WIF JSON configuration.
    Sources that support WIF inherit from this class and call `load_wif_credentials`
    to obtain a `google.auth.credentials.Credentials` object.

    BigQuery, Dataplex, VertexAI, and other GCP sources can adopt this mixin when
    they need WIF support — no changes required to this module.
    """

    gcp_wif_configuration: Optional[str] = Field(
        default=None,
        description=(
            "Path to the GCP Workload Identity Federation configuration JSON file. "
            "Mutually exclusive with gcp_wif_configuration_json and "
            "gcp_wif_configuration_json_string."
        ),
    )

    gcp_wif_configuration_json: Optional[Union[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "GCP Workload Identity Federation configuration as a JSON string or dict. "
            "Mutually exclusive with gcp_wif_configuration and "
            "gcp_wif_configuration_json_string."
        ),
    )

    gcp_wif_configuration_json_string: Optional[str] = Field(
        default=None,
        description=(
            "GCP Workload Identity Federation configuration as a JSON string "
            "(contents of the configuration file). Useful for injecting configuration "
            "from secrets managers. Mutually exclusive with gcp_wif_configuration and "
            "gcp_wif_configuration_json."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _validate_wif_json_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that the JSON-typed WIF options contain valid JSON."""
        if not isinstance(values, dict):
            return values

        gcp_wif_configuration_json = values.get("gcp_wif_configuration_json")
        gcp_wif_configuration_json_string = values.get(
            "gcp_wif_configuration_json_string"
        )

        if gcp_wif_configuration_json:
            if isinstance(gcp_wif_configuration_json, str):
                try:
                    json.loads(gcp_wif_configuration_json)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"gcp_wif_configuration_json must be valid JSON: {e}"
                    ) from e
            elif not isinstance(gcp_wif_configuration_json, dict):
                raise ValueError(
                    "gcp_wif_configuration_json must be either a JSON string or a dictionary"
                )

        if gcp_wif_configuration_json_string:
            try:
                json.loads(gcp_wif_configuration_json_string)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"gcp_wif_configuration_json_string must be valid JSON: {e}"
                ) from e

        return values

    @model_validator(mode="after")
    def _validate_wif_mutual_exclusion(self) -> "GCPWIFConfig":
        """Validate that at most one WIF configuration option is set."""
        provided = [
            opt
            for opt in [
                self.gcp_wif_configuration,
                self.gcp_wif_configuration_json,
                self.gcp_wif_configuration_json_string,
            ]
            if opt is not None
        ]
        if len(provided) > 1:
            raise ValueError(
                "Cannot specify multiple WIF configuration options. Use only one of: "
                "gcp_wif_configuration, gcp_wif_configuration_json, or gcp_wif_configuration_json_string."
            )
        return self


def load_wif_credentials(
    wif_config: GCPWIFConfig,
) -> Tuple[Credentials, Optional[str]]:
    """
    Load GCP Workload Identity Federation credentials from a GCPWIFConfig.

    Resolves whichever config option is set to a dict, then calls
    `google.auth.load_credentials_from_dict`. Applies the cloud-platform scope
    (required for service account impersonation via WIF) and attempts an initial
    token refresh to validate the credentials.

    Returns:
        A tuple of (credentials, project_id). project_id may be None if the WIF
        configuration does not specify one.

    Raises:
        ValueError: If no WIF configuration is provided or if credential loading fails.
    """
    if not any(
        [
            wif_config.gcp_wif_configuration,
            wif_config.gcp_wif_configuration_json,
            wif_config.gcp_wif_configuration_json_string,
        ]
    ):
        raise ValueError("No valid WIF configuration provided")

    try:
        if wif_config.gcp_wif_configuration:
            with open(wif_config.gcp_wif_configuration) as f:
                wif_config_dict: Dict[str, Any] = json.load(f)
            logger.info(
                "Using Workload Identity Federation configuration from file: %s",
                wif_config.gcp_wif_configuration,
            )
        elif wif_config.gcp_wif_configuration_json:
            if isinstance(wif_config.gcp_wif_configuration_json, dict):
                wif_config_dict = wif_config.gcp_wif_configuration_json
            else:
                wif_config_dict = json.loads(wif_config.gcp_wif_configuration_json)
            logger.info(
                "Using Workload Identity Federation configuration from JSON content"
            )
        else:
            wif_config_dict = json.loads(wif_config.gcp_wif_configuration_json_string)  # type: ignore[arg-type]
            logger.info(
                "Using Workload Identity Federation configuration from JSON string"
            )

        credentials, project_id = load_credentials_from_dict(wif_config_dict)
        # Impersonation (WIF → SA) requires scopes; otherwise IAM returns 400 "Scope required."
        credentials = credentials.with_scopes(
            ["https://www.googleapis.com/auth/cloud-platform"]
        )

        # Try to refresh credentials to validate they work.
        # If refresh fails, log a warning but continue — the caller will refresh
        # automatically on the first actual API call.
        try:
            credentials.refresh(Request())
            logger.debug("Successfully refreshed WIF credentials")
        except Exception as refresh_error:
            logger.warning(
                "Failed to refresh WIF credentials during setup (this may be expected): %s",
                refresh_error,
            )

        logger.info("Successfully loaded Workload Identity Federation credentials")
        return credentials, project_id

    except Exception as e:
        raise ValueError(
            f"Failed to load Workload Identity Federation credentials: {e}"
        ) from e
