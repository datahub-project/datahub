import copy
import json
import logging
from typing import Any, Dict, Optional, Tuple

from google.auth import load_credentials_from_dict
from google.auth.credentials import Credentials
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

    gcp_wif_configuration_json: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "GCP Workload Identity Federation configuration as a dict or a JSON string. "
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
            "gcp_wif_configuration_json. "
            "Note: WIF configuration typically contains public endpoint URLs rather "
            "than private keys, so SecretStr masking is not applied. If your WIF "
            "config contains sensitive material, ensure it is not logged at DEBUG level."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _validate_wif_json_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(values, dict):
            return values

        gcp_wif_configuration_json = values.get("gcp_wif_configuration_json")
        gcp_wif_configuration_json_string = values.get(
            "gcp_wif_configuration_json_string"
        )

        if gcp_wif_configuration_json is not None and not isinstance(
            gcp_wif_configuration_json, dict
        ):
            if isinstance(gcp_wif_configuration_json, str):
                # Backward-compat: auto-parse JSON strings to dicts so that
                # existing recipes using gcp_wif_configuration_json: "..." keep working.
                try:
                    parsed = json.loads(gcp_wif_configuration_json)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"gcp_wif_configuration_json must be valid JSON: {e}"
                    ) from e
                if not isinstance(parsed, dict):
                    raise ValueError(
                        "gcp_wif_configuration_json must be a JSON object, "
                        f"not a {type(parsed).__name__}"
                    )
                values["gcp_wif_configuration_json"] = parsed
            else:
                raise ValueError(
                    "gcp_wif_configuration_json must be a dict or a JSON string."
                )

        if gcp_wif_configuration_json_string is not None:
            try:
                json.loads(gcp_wif_configuration_json_string)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"gcp_wif_configuration_json_string must be valid JSON: {e}"
                ) from e

        return values

    @model_validator(mode="after")
    def _validate_wif_mutual_exclusion(self) -> "GCPWIFConfig":
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

    def wif_config_source(self) -> Optional[str]:
        """Return a short label for which WIF option was used, for log provenance."""
        if self.gcp_wif_configuration:
            return f"file:{self.gcp_wif_configuration}"
        if self.gcp_wif_configuration_json is not None:
            return "inline_json"
        if self.gcp_wif_configuration_json_string is not None:
            return "json_string"
        return None

    def to_wif_dict(self) -> Dict[str, Any]:
        if self.gcp_wif_configuration:
            try:
                with open(self.gcp_wif_configuration) as f:
                    loaded = json.load(f)
            except FileNotFoundError:
                raise ValueError(
                    f"WIF configuration file not found: {self.gcp_wif_configuration}"
                ) from None
            except OSError as e:
                raise ValueError(
                    f"WIF configuration file could not be read "
                    f"(path={self.gcp_wif_configuration}): {e}"
                ) from e
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"WIF configuration file is not valid JSON "
                    f"(path={self.gcp_wif_configuration}): {e}"
                ) from e
            if not isinstance(loaded, dict):
                raise ValueError(
                    f"WIF configuration must be a JSON object, not a {type(loaded).__name__}"
                )
            return loaded
        elif self.gcp_wif_configuration_json is not None:
            # Deep-copy to protect nested dicts (e.g. credential_source) from mutation.
            return copy.deepcopy(self.gcp_wif_configuration_json)
        elif self.gcp_wif_configuration_json_string is not None:
            loaded = json.loads(self.gcp_wif_configuration_json_string)
            if not isinstance(loaded, dict):
                raise ValueError(
                    f"WIF configuration must be a JSON object, not a {type(loaded).__name__}"
                )
            return loaded
        else:
            raise ValueError("No valid WIF configuration provided")


def build_credentials_from_wif_dict(
    wif_dict: Dict[str, Any],
    source_label: Optional[str],
) -> Tuple[Credentials, Optional[str]]:
    """Build GCP credentials from an already-resolved WIF configuration dict.

    Separated from load_wif_credentials so callers that have already resolved
    the dict (e.g. to reuse it for writing a temp file) don't need to read it
    twice.
    """
    try:
        logger.info(
            "Loading Workload Identity Federation credentials (source=%s)",
            source_label,
        )
        credentials, project_id = load_credentials_from_dict(wif_dict)
        # For WIF → SA impersonation, scopes must be set; otherwise IAM returns
        # 400 "Scope required."  Guard with requires_scopes so credential types
        # that manage access via audience (not scopes) are not broken.
        if getattr(credentials, "requires_scopes", False):
            credentials = credentials.with_scopes(
                ["https://www.googleapis.com/auth/cloud-platform"]
            )
        logger.info("Successfully loaded Workload Identity Federation credentials")
        return credentials, project_id
    except Exception as e:
        raise ValueError(
            f"Failed to load Workload Identity Federation credentials "
            f"(source={source_label}, error_type={type(e).__name__}): {e}"
        ) from e


def load_wif_credentials(
    wif_config: GCPWIFConfig,
) -> Tuple[Credentials, Optional[str]]:
    """Load GCP Workload Identity Federation credentials from a GCPWIFConfig.

    Resolves whichever config option is set to a dict, then calls
    `google.auth.load_credentials_from_dict`. Applies the cloud-platform scope
    (required for service account impersonation via WIF). Token refresh happens
    lazily on the first API call.

    Returns:
        A tuple of (credentials, project_id). project_id may be None if the WIF
        configuration does not specify one.

    Raises:
        ValueError: If no WIF configuration is provided or if credential loading fails.
    """
    wif_dict = wif_config.to_wif_dict()
    return build_credentials_from_wif_dict(wif_dict, wif_config.wif_config_source())
