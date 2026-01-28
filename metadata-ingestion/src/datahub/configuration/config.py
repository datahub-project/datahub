"""Configuration models for DataHub CLI profile management."""

import os
import re
from typing import Dict, Optional

from pydantic import Field, field_validator

from datahub.ingestion.graph.config import DatahubClientConfig


def interpolate_env_vars(value: str) -> str:
    """
    Interpolate environment variables in a string.

    Supports the following formats:
    - ${VAR_NAME} - Simple variable substitution
    - ${VAR_NAME:-default} - Variable with default value if not set
    - ${VAR_NAME:?error_message} - Variable with error if not set

    Args:
        value: String that may contain environment variable references

    Returns:
        String with environment variables interpolated

    Raises:
        ValueError: If a required variable (using :?) is not set
    """
    if not isinstance(value, str):
        return value

    def replace_var(match: re.Match) -> str:
        var_expr = match.group(1)

        # Handle ${VAR:?error} format
        if ":?" in var_expr:
            var_name, error_msg = var_expr.split(":?", 1)
            env_value = os.getenv(var_name)
            if env_value is None:
                raise ValueError(f"Required environment variable not set: {error_msg}")
            return env_value

        # Handle ${VAR:-default} format
        if ":-" in var_expr:
            var_name, default = var_expr.split(":-", 1)
            return os.getenv(var_name, default)

        # Handle simple ${VAR} format
        return os.getenv(var_expr, "")

    # Pattern to match ${...} with various formats
    pattern = r"\$\{([^}]+)\}"
    return re.sub(pattern, replace_var, value)


class ProfileConfig(DatahubClientConfig):
    """Configuration for a single DataHub connection profile."""

    description: Optional[str] = Field(
        default=None, description="Human-readable description of this profile"
    )

    require_confirmation: bool = Field(
        default=False,
        description="Require explicit confirmation for destructive operations (recommended for production)",
    )

    @field_validator("server", "token", mode="before")
    @classmethod
    def interpolate_string_fields(cls, v: Optional[str]) -> Optional[str]:
        """Interpolate environment variables in string fields."""
        if v is None:
            return v
        return interpolate_env_vars(v)


class DataHubConfig(DatahubClientConfig):
    """
    Root configuration model for DataHub CLI.

    This represents the structure of ~/.datahub/config.yaml:

    version: "1.0"
    current_profile: dev

    profiles:
      dev:
        server: http://localhost:8080
        token: ${DATAHUB_DEV_TOKEN}
        description: "Local development"

      prod:
        server: https://datahub.company.com
        token: ${DATAHUB_PROD_TOKEN}
        require_confirmation: true
        description: "Production - USE WITH CAUTION"
    """

    version: str = Field(default="1.0", description="Configuration file version")

    current_profile: Optional[str] = Field(
        default=None, description="The currently active profile name"
    )

    profiles: Dict[str, ProfileConfig] = Field(
        default_factory=dict, description="Map of profile name to profile configuration"
    )

    # Override fields from DatahubClientConfig since they're not used at root level
    server: Optional[str] = None  # type: ignore

    @field_validator("profiles", mode="before")
    @classmethod
    def validate_profiles_dict(cls, v: Dict) -> Dict:
        """Ensure profiles is a dictionary and convert nested dicts to ProfileConfig."""
        if not isinstance(v, dict):
            raise ValueError("profiles must be a dictionary")

        # Convert plain dicts to ProfileConfig instances
        result = {}
        for name, config in v.items():
            if isinstance(config, ProfileConfig):
                result[name] = config
            elif isinstance(config, dict):
                result[name] = ProfileConfig(**config)
            else:
                raise ValueError(f"Invalid profile config for '{name}'")

        return result

    def get_profile(self, profile_name: str) -> ProfileConfig:
        """
        Get a specific profile by name.

        Args:
            profile_name: Name of the profile to retrieve

        Returns:
            ProfileConfig for the requested profile

        Raises:
            KeyError: If profile doesn't exist
        """
        if profile_name not in self.profiles:
            available = ", ".join(self.profiles.keys()) if self.profiles else "none"
            raise KeyError(
                f"Profile '{profile_name}' not found. Available profiles: {available}"
            )
        return self.profiles[profile_name]

    def add_profile(self, profile_name: str, config: ProfileConfig) -> None:
        """Add or update a profile."""
        self.profiles[profile_name] = config

    def remove_profile(self, profile_name: str) -> None:
        """
        Remove a profile.

        Args:
            profile_name: Name of the profile to remove

        Raises:
            KeyError: If profile doesn't exist
        """
        if profile_name not in self.profiles:
            raise KeyError(f"Profile '{profile_name}' not found")
        del self.profiles[profile_name]

    def list_profiles(self) -> list[str]:
        """List all profile names."""
        return list(self.profiles.keys())
