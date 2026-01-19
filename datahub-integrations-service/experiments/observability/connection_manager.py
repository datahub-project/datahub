"""
Connection Manager - Reusable module for managing DataHub service connections.

This module provides connection profile management for DataHub services:
- Save/load connection profiles
- Manage local, remote, and custom connection modes
- Persist profiles to ~/.datahub/chat_admin/connection_profiles.json
- Support for integrations service and GMS configuration

Can be used by any tool that needs to connect to DataHub services.
"""

import json
import os
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from loguru import logger


class ConnectionMode(str, Enum):
    """Connection mode enum."""

    QUICKSTART = "quickstart"  # Quickstart: Local GMS + Local integrations (simplest local setup)
    EMBEDDED = "embedded"  # Run agent directly in process (no HTTP)
    LOCAL_SERVICE = "local_service"  # Spawn local integrations service with GMS credentials
    REMOTE = "remote"  # kubectl port-forward to remote integrations service
    LOCAL = "local"  # Local GMS + Local integrations (all local development)
    CUSTOM = "custom"  # Custom URLs
    GRAPHQL_DIRECT = "graphql_direct"  # Use GraphQL endpoint directly (future)


@dataclass
class ConnectionConfig:
    """Configuration for connecting to DataHub services."""

    # Connection mode
    mode: ConnectionMode = ConnectionMode.QUICKSTART

    # Integrations Service configuration
    integrations_url: str = "http://localhost:9003"

    # GMS configuration
    gms_url: str = "http://localhost:8080"
    gms_token: Optional[str] = None

    # Kubernetes configuration (for remote mode)
    kube_namespace: Optional[str] = None
    kube_context: Optional[str] = None
    pod_name: Optional[str] = None
    pod_label_selector: str = "app=datahub-integrations"
    local_port: int = 9003
    remote_port: int = 9003

    # AWS configuration (for Bedrock)
    aws_region: str = "us-west-2"
    aws_profile: Optional[str] = None

    # Local service configuration (for LOCAL_SERVICE mode)
    use_local_service: bool = False
    local_service_port: int = 9003

    # Metadata
    name: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        data = asdict(self)
        return {k: v for k, v in data.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectionConfig":
        """Create from dictionary."""
        # Convert mode string to enum
        if "mode" in data and isinstance(data["mode"], str):
            data["mode"] = ConnectionMode(data["mode"])
        return cls(**data)

    def validate(self) -> List[str]:
        """
        Validate the configuration.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        if self.mode == ConnectionMode.REMOTE:
            if not self.kube_namespace:
                errors.append("Kubernetes namespace is required for remote mode")
            if not self.pod_name and not self.pod_label_selector:
                errors.append("Either pod_name or pod_label_selector is required for remote mode")

        if self.mode == ConnectionMode.LOCAL_SERVICE:
            if not self.gms_url:
                errors.append("GMS URL is required for local service mode")
            if not self.gms_token:
                errors.append("GMS token is required for local service mode")

        if not self.integrations_url:
            errors.append("Integrations service URL is required")

        if not self.gms_url:
            errors.append("GMS URL is required")

        return errors


@dataclass
class ConnectionProfile:
    """A saved connection profile."""

    name: str
    config: ConnectionConfig
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "config": self.config.to_dict(),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectionProfile":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            config=ConnectionConfig.from_dict(data["config"]),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )


class ConnectionManager:
    """Manager for DataHub service connections and profiles."""

    DEFAULT_PROFILE_PATH = Path.home() / ".datahub" / "chat_admin" / "connection_profiles.json"

    def __init__(self, profile_path: Optional[Path] = None):
        """
        Initialize connection manager.

        Args:
            profile_path: Path to profiles JSON file (default: ~/.datahub_connection_profiles.json)
        """
        self.profile_path = profile_path or self.DEFAULT_PROFILE_PATH
        self._ensure_profile_file()

    def _ensure_profile_file(self) -> None:
        """Ensure the profile file exists with proper structure."""
        if not self.profile_path.exists():
            # Create parent directory if it doesn't exist
            self.profile_path.parent.mkdir(parents=True, exist_ok=True)
            self._save_data({"profiles": {}, "active_profile": None, "connection_mode": "embedded"})
            # Set restrictive permissions (owner read/write only)
            self.profile_path.chmod(0o600)
            logger.info(f"Created new profile file: {self.profile_path}")

    def _load_data(self) -> Dict[str, Any]:
        """Load raw data from profile file."""
        try:
            with open(self.profile_path, "r") as f:
                data = json.load(f)
                # Ensure connection_mode exists for backwards compatibility
                if "connection_mode" not in data:
                    data["connection_mode"] = "embedded"
                return data
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Failed to load profiles: {e}")
            return {"profiles": {}, "active_profile": None, "connection_mode": "embedded"}

    def _save_data(self, data: Dict[str, Any]) -> None:
        """Save raw data to profile file."""
        try:
            with open(self.profile_path, "w") as f:
                json.dump(data, f, indent=2)
            # Ensure restrictive permissions
            self.profile_path.chmod(0o600)
            logger.debug(f"Saved profiles to {self.profile_path}")
        except Exception as e:
            logger.error(f"Failed to save profiles: {e}")
            raise

    def _load_datahubenv_profile(self) -> Optional[ConnectionProfile]:
        """
        Load profile from ~/.datahubenv file if it exists.

        Returns:
            ConnectionProfile or None if file doesn't exist or is invalid
        """
        datahubenv_path = Path.home() / ".datahubenv"

        if not datahubenv_path.exists():
            return None

        try:
            with open(datahubenv_path, 'r') as f:
                config_data = yaml.safe_load(f)

            # Extract gms config
            gms_config = config_data.get('gms', {})
            gms_url = gms_config.get('server')
            gms_token = gms_config.get('token')

            if not gms_url:
                logger.warning("~/.datahubenv exists but has no gms.server configured")
                return None

            logger.debug(f"Loaded profile from ~/.datahubenv: {gms_url}")

            config = ConnectionConfig(
                mode=ConnectionMode.EMBEDDED,
                integrations_url="embedded://local",
                gms_url=gms_url,
                gms_token=gms_token,
                description="From ~/.datahubenv file",
            )

            return ConnectionProfile(
                name="datahubenv",
                config=config,
            )

        except Exception as e:
            logger.error(f"Failed to read ~/.datahubenv: {e}")
            return None

    def list_profiles(self) -> List[str]:
        """
        Get list of all profile names.

        Returns:
            List of profile names
        """
        data = self._load_data()
        return list(data.get("profiles", {}).keys())

    def get_profile(self, name: str) -> Optional[ConnectionProfile]:
        """
        Get a profile by name.

        Special handling for "datahubenv" - loads from ~/.datahubenv file.

        Args:
            name: Profile name

        Returns:
            ConnectionProfile or None if not found
        """
        # Special case: load from ~/.datahubenv file
        if name == "datahubenv":
            return self._load_datahubenv_profile()

        # Regular profile from storage
        data = self._load_data()
        profile_data = data.get("profiles", {}).get(name)

        if profile_data:
            try:
                return ConnectionProfile.from_dict(profile_data)
            except Exception as e:
                logger.error(f"Failed to parse profile '{name}': {e}")
                return None

        return None

    def save_profile(self, profile: ConnectionProfile) -> bool:
        """
        Save a connection profile.

        Args:
            profile: ConnectionProfile to save

        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # Validate config before saving
            errors = profile.config.validate()
            if errors:
                logger.error(f"Invalid profile configuration: {errors}")
                return False

            # IMPORTANT: Always reload from disk to get latest state
            # This prevents race conditions when multiple backend processes are running
            data = self._load_data()

            # Update timestamps
            from datetime import datetime

            now = datetime.utcnow().isoformat()
            if profile.name not in data.get("profiles", {}):
                profile.created_at = now
            profile.updated_at = now

            # Save profile
            if "profiles" not in data:
                data["profiles"] = {}
            data["profiles"][profile.name] = profile.to_dict()

            self._save_data(data)
            logger.info(f"Saved profile: {profile.name}")
            return True

        except Exception as e:
            logger.error(f"Failed to save profile '{profile.name}': {e}")
            return False

    def delete_profile(self, name: str) -> bool:
        """
        Delete a profile by name.

        Args:
            name: Profile name to delete

        Returns:
            True if deleted, False if not found or error
        """
        try:
            data = self._load_data()

            if name not in data.get("profiles", {}):
                logger.warning(f"Profile '{name}' not found")
                return False

            del data["profiles"][name]

            # Clear active profile if it was deleted
            if data.get("active_profile") == name:
                data["active_profile"] = None

            self._save_data(data)
            logger.info(f"Deleted profile: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete profile '{name}': {e}")
            return False

    def get_active_profile(self) -> Optional[str]:
        """
        Get the name of the currently active profile.

        Returns:
            Active profile name or None
        """
        data = self._load_data()
        return data.get("active_profile")

    def set_active_profile(self, name: Optional[str]) -> bool:
        """
        Set the active profile.

        Special handling for "datahubenv" - allowed even though not in saved profiles.

        Args:
            name: Profile name to activate, or None to clear

        Returns:
            True if set successfully, False otherwise
        """
        try:
            data = self._load_data()

            # Check if profile exists (special case for datahubenv)
            if name is not None and name != "datahubenv" and name not in data.get("profiles", {}):
                logger.error(f"Profile '{name}' not found")
                return False

            # For datahubenv, verify the file exists
            if name == "datahubenv":
                datahubenv_profile = self._load_datahubenv_profile()
                if not datahubenv_profile:
                    logger.error("Cannot activate datahubenv profile - ~/.datahubenv file not found or invalid")
                    return False

            data["active_profile"] = name
            self._save_data(data)
            logger.info(f"Set active profile: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to set active profile: {e}")
            return False

    def get_connection_mode(self) -> str:
        """
        Get the global connection mode setting (separate from profiles).

        Returns:
            Connection mode string (e.g., "embedded", "quickstart")
        """
        data = self._load_data()
        return data.get("connection_mode", "embedded")

    def set_connection_mode(self, mode: str) -> bool:
        """
        Set the global connection mode setting (separate from profiles).

        Args:
            mode: Connection mode string

        Returns:
            True if successful
        """
        try:
            data = self._load_data()
            data["connection_mode"] = mode
            self._save_data(data)
            logger.info(f"Set global connection mode: {mode}")
            return True
        except Exception as e:
            logger.error(f"Failed to set connection mode: {e}")
            return False

    def load_active_config(self) -> ConnectionConfig:
        """
        Load configuration from the active profile, or return default.
        Combines profile data (WHERE) with global connection mode (HOW).

        Returns:
            ConnectionConfig (either from active profile or default)
        """
        # Get global connection mode (HOW to connect)
        global_mode = self.get_connection_mode()
        mode = ConnectionMode(global_mode) if global_mode else ConnectionMode.EMBEDDED

        active_name = self.get_active_profile()

        if active_name:
            profile = self.get_profile(active_name)
            if profile:
                logger.info(f"Loaded active profile: {active_name}")
                # Merge profile data (WHERE) with global mode (HOW)
                config = profile.config
                config.mode = mode
                # Clear integrations_url if embedded mode
                if mode == ConnectionMode.EMBEDDED:
                    config.integrations_url = None
                return config

        # Return default config with global mode
        logger.debug("No active profile, using default config")
        return ConnectionConfig(mode=mode)

    def create_default_profiles(self) -> None:
        """Create default profiles (local, remote templates)."""
        # Local profile
        local_profile = ConnectionProfile(
            name="local",
            config=ConnectionConfig(
                mode=ConnectionMode.LOCAL,
                integrations_url="http://localhost:9003",
                gms_url="http://localhost:8080",
                gms_token=None,
                name="Local Development",
                description="Connect to locally running services",
            ),
        )
        self.save_profile(local_profile)

        # Remote template
        remote_profile = ConnectionProfile(
            name="remote-template",
            config=ConnectionConfig(
                mode=ConnectionMode.REMOTE,
                integrations_url="http://localhost:9003",
                gms_url="https://your-instance.acryl.io/api/gms",
                gms_token=None,
                kube_namespace="datahub",
                kube_context=None,
                pod_label_selector="app=datahub-integrations",
                name="Remote Template",
                description="Template for remote kubectl connections",
            ),
        )
        self.save_profile(remote_profile)

        logger.info("Created default profiles")

    @staticmethod
    def load_from_env() -> ConnectionConfig:
        """
        Load configuration from environment variables.

        This provides backwards compatibility with existing environment-based config.

        Returns:
            ConnectionConfig populated from environment variables
        """
        return ConnectionConfig(
            mode=ConnectionMode.CUSTOM,
            integrations_url=os.environ.get("INTEGRATIONS_SERVICE_URL", "http://localhost:9003"),
            gms_url=os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080"),
            gms_token=os.environ.get("DATAHUB_GMS_API_TOKEN"),
            kube_namespace=os.environ.get("KUBE_NAMESPACE"),
            kube_context=os.environ.get("KUBE_CONTEXT"),
            aws_region=os.environ.get("AWS_REGION", "us-west-2"),
            aws_profile=os.environ.get("AWS_PROFILE"),
            name="Environment Variables",
            description="Configuration loaded from environment variables",
        )
