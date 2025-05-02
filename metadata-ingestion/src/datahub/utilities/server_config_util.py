import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Union,
)

import requests

from datahub.configuration.common import (
    ConfigurationError,
)
from datahub.telemetry.telemetry import suppress_telemetry

logger = logging.getLogger(__name__)

# Only to be written to for logging server related information
global_debug: Dict[str, Any] = {}


def get_gms_config() -> Dict:
    return global_debug.get("gms_config", {})


class ServiceFeature(Enum):
    """
    Enum representing supported features in the REST service.
    """

    OPEN_API_SDK = "openapi_sdk"
    API_TRACING = "api_tracing"
    NO_CODE = "no_code"
    STATEFUL_INGESTION = "stateful_ingestion"
    IMPACT_ANALYSIS = "impact_analysis"
    PATCH_CAPABLE = "patch_capable"
    CLI_TELEMETRY = "cli_telemetry"
    DATAHUB_CLOUD = "datahub_cloud"
    # Add more features as needed


_REQUIRED_VERSION_OPENAPI_TRACING = {
    "cloud": (0, 3, 11, 0),
    "core": (1, 0, 1, 0),
}


@dataclass
class RestServiceConfig:
    """
    A class to represent REST service configuration with semantic version parsing capabilities.
    """

    session: Optional[requests.Session] = None
    url: Optional[str] = None
    raw_config: Dict[str, Any] = field(default_factory=dict)
    _version_cache: Optional[Tuple[int, int, int, int]] = None

    def fetch_config(self) -> Dict[str, Any]:
        """
        Fetch configuration from the server if not already loaded.

        Returns:
            The configuration dictionary

        Raises:
            ConfigurationError: If there's an error fetching or validating the configuration
        """
        if not self.raw_config:
            if self.session is None or self.url is None:
                raise ConfigurationError(
                    "Session and URL are required to load configuration"
                )

            response = self.session.get(self.url)

            if response.status_code == 200:
                config = response.json()

                # Validate that we're connected to the correct service
                if config.get("noCode") == "true":
                    self.raw_config = config
                else:
                    raise ConfigurationError(
                        "You seem to have connected to the frontend service instead of the GMS endpoint. "
                        "The rest emitter should connect to DataHub GMS (usually <datahub-gms-host>:8080) or Frontend GMS API (usually <frontend>:9002/api/gms). "
                        "For Acryl users, the endpoint should be https://<name>.acryl.io/gms"
                    )
            else:
                logger.debug(
                    f"Unable to connect to {self.url} with status_code: {response.status_code}. Response: {response.text}"
                )

                if response.status_code == 401:
                    message = f"Unable to connect to {self.url} - got an authentication error: {response.text}."
                else:
                    message = f"Unable to connect to {self.url} with status_code: {response.status_code}."

                message += "\nPlease check your configuration and make sure you are talking to the DataHub GMS (usually <datahub-gms-host>:8080) or Frontend GMS API (usually <frontend>:9002/api/gms)."
                raise ConfigurationError(message)

        return self.raw_config

    @property
    def config(self) -> Dict[str, Any]:
        """
        Get the full configuration dictionary, loading it if necessary.

        Returns:
            The configuration dictionary
        """
        return self.fetch_config()

    @property
    def commit_hash(self) -> Optional[str]:
        """
        Get the commit hash for the current version.

        Returns:
            The commit hash or None if not found
        """
        versions = self.config.get("versions") or {}
        datahub_info = versions.get("acryldata/datahub") or {}
        return datahub_info.get("commit")

    @property
    def server_type(self) -> str:
        """
        Get the server type.

        Returns:
            The server type or "unknown" if not found
        """
        datahub = self.config.get("datahub") or {}
        return datahub.get("serverType", "unknown")

    @property
    def service_version(self) -> Optional[str]:
        """
        Get the raw service version string.

        Returns:
            The version string or None if not found
        """
        config = self.fetch_config()
        versions = config.get("versions") or {}
        datahub_info = versions.get("acryldata/datahub") or {}
        return datahub_info.get("version")

    def _parse_version(
        self, version_str: Optional[str] = None
    ) -> Tuple[int, int, int, int]:
        """
        Parse a semantic version string into its components, ignoring rc and suffixes.
        Supports standard three-part versions (1.0.0) and four-part versions (1.0.0.1).

        Args:
            version_str: Version string to parse. If None, uses the service version.

        Returns:
            Tuple of (major, minor, patch, build) version numbers where build is 0 for three-part versions

        Raises:
            ValueError: If the version string cannot be parsed
        """
        if version_str is None:
            version_str = self.service_version

        if not version_str:
            return (0, 0, 0, 0)

        # Remove 'v' prefix if present
        if version_str.startswith("v"):
            version_str = version_str[1:]

        # Extract the semantic version part (before any rc or suffix)
        # This pattern will match both three-part (1.0.0) and four-part (1.0.0.1) versions
        match = re.match(r"(\d+)\.(\d+)\.(\d+)(?:\.(\d+))?(?:rc\d+|-.*)?", version_str)
        if not match:
            raise ValueError(f"Invalid version format: {version_str}")

        major = int(match.group(1))
        minor = int(match.group(2))
        patch = int(match.group(3))
        build = (
            int(match.group(4)) if match.group(4) else 0
        )  # Default to 0 if not present

        return (major, minor, patch, build)

    @property
    def parsed_version(self) -> Optional[Tuple[int, int, int, int]]:
        """
        Get the parsed semantic version of the service.
        Uses caching for efficiency.

        Returns:
            Tuple of (major, minor, patch) version numbers
        """
        if self._version_cache is None:
            self._version_cache = self._parse_version()
        return self._version_cache

    def is_version_at_least(
        self, major: int, minor: int = 0, patch: int = 0, build: int = 0
    ) -> bool:
        """
        Check if the service version is at least the specified version.

        Args:
            major: Major version to check against
            minor: Minor version to check against
            patch: Patch version to check against
            build: Build version to check against (for four-part versions)

        Returns:
            True if the service version is at least the specified version
        """
        current_version = self.parsed_version or (0, 0, 0, 0)
        requested_version = (major, minor, patch, build)

        return current_version >= requested_version

    @property
    def is_no_code_enabled(self) -> bool:
        """
        Check if noCode is enabled.

        Returns:
            True if noCode is set to "true"
        """
        return self.config.get("noCode") == "true"

    @property
    def is_managed_ingestion_enabled(self) -> bool:
        """
        Check if managedIngestion is enabled.

        Returns:
            True if managedIngestion.enabled is True
        """
        managed_ingestion = self.config.get("managedIngestion") or {}
        return managed_ingestion.get("enabled", False)

    @property
    def is_datahub_cloud(self) -> bool:
        """
        Check if DataHub Cloud is enabled.

        Returns:
            True if the server environment is not 'core'
        """
        datahub_config = self.config.get("datahub") or {}
        server_env = datahub_config.get("serverEnv")

        # Return False if serverEnv is None or empty string
        if not server_env:
            return False

        return server_env != "core"

    def supports_feature(self, feature: ServiceFeature) -> bool:
        """
        Determines whether a specific feature is supported based on service version
        and whether this is a cloud deployment or not.

        Args:
            feature: Feature enum value to check

        Returns:
            Boolean indicating whether the feature is supported
        """
        # Special handling for features that rely on config flags
        config_based_features = {
            ServiceFeature.NO_CODE: lambda: self.is_no_code_enabled,
            ServiceFeature.STATEFUL_INGESTION: lambda: self.config.get(
                "statefulIngestionCapable", False
            )
            is True,
            ServiceFeature.IMPACT_ANALYSIS: lambda: self.config.get(
                "supportsImpactAnalysis", False
            )
            is True,
            ServiceFeature.PATCH_CAPABLE: lambda: self.config.get("patchCapable", False)
            is True,
            ServiceFeature.CLI_TELEMETRY: lambda: (
                self.config.get("telemetry") or {}
            ).get("enabledCli", None),
            ServiceFeature.DATAHUB_CLOUD: lambda: self.is_datahub_cloud,
        }

        # Check if this is a config-based feature
        if feature in config_based_features:
            return config_based_features[feature]()

        # For environment-based features, determine requirements based on cloud vs. non-cloud
        deployment_type = "cloud" if self.is_datahub_cloud else "core"

        # Define feature requirements
        feature_requirements = {
            ServiceFeature.OPEN_API_SDK: _REQUIRED_VERSION_OPENAPI_TRACING,
            ServiceFeature.API_TRACING: _REQUIRED_VERSION_OPENAPI_TRACING,
            # Additional features can be defined here
        }

        # Check if the feature exists in our requirements dictionary
        if feature not in feature_requirements:
            # Unknown feature, assume not supported
            return False

        # Get version requirements for this feature and deployment type
        feature_reqs = feature_requirements[feature]
        requirements = feature_reqs.get(deployment_type)

        if not requirements:
            # If no specific requirements defined for this deployment type,
            # assume feature is not supported
            return False

        # Check if the current version meets the requirements
        req_major, req_minor, req_patch, req_build = requirements
        return self.is_version_at_least(req_major, req_minor, req_patch, req_build)

    def __str__(self) -> str:
        """
        Return a string representation of the configuration as JSON.

        Returns:
            A string representation of the configuration dictionary
        """
        return str(self.config)

    def __repr__(self) -> str:
        """
        Return a representation of the object that can be used to recreate it.

        Returns:
            A string representation that can be used with pprint
        """
        return str(self.config)


def set_gms_config(config: Union[Dict[str, Any], RestServiceConfig]) -> None:
    global_debug["gms_config"] = config

    config_obj = (
        config
        if isinstance(config, RestServiceConfig)
        else RestServiceConfig(raw_config=config)
    )

    cli_telemetry_enabled = is_cli_telemetry_enabled(config_obj)
    if cli_telemetry_enabled is not None and not cli_telemetry_enabled:
        # server requires telemetry to be disabled on client
        suppress_telemetry()


def is_cli_telemetry_enabled(config: RestServiceConfig) -> bool:
    return config.supports_feature(ServiceFeature.CLI_TELEMETRY)
