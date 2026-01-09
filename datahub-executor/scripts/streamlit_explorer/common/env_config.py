# ruff: noqa: INP001
"""
Environment configuration loader for Time Series Explorer.

This module provides utilities for loading DataHub connection configuration
from ~/.datahubenv files or custom env files, supporting multiple endpoints.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import yaml

# Default config file path
DEFAULT_DATAHUB_CONFIG_PATH = "~/.datahubenv"


@dataclass
class DataHubEnvConfig:
    """DataHub environment configuration."""

    server: str
    token: Optional[str] = None
    source_file: Optional[str] = None

    @property
    def hostname(self) -> str:
        """Extract hostname from server URL for use as endpoint identifier."""
        parsed = urlparse(self.server)
        return parsed.netloc or parsed.path

    @property
    def display_name(self) -> str:
        """Human-readable name for the endpoint."""
        return self.hostname

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "server": self.server,
            "token": self.token,
            "source_file": self.source_file,
            "hostname": self.hostname,
        }


def load_env_config(
    config_path: Optional[str] = None,
    allow_env_override: bool = True,
) -> Optional[DataHubEnvConfig]:
    """
    Load DataHub configuration from env file or environment variables.

    Priority order:
    1. Environment variables (if allow_env_override=True)
    2. Specified config_path
    3. Default ~/.datahubenv

    Args:
        config_path: Path to custom env file. If None, uses default.
        allow_env_override: If True, environment variables take precedence.

    Returns:
        DataHubEnvConfig if configuration found, None otherwise.
    """
    # Check environment variables first
    if allow_env_override:
        env_url = os.environ.get("DATAHUB_GMS_URL")
        if not env_url:
            # Legacy support: construct from host/port/protocol
            host = os.environ.get("DATAHUB_GMS_HOST")
            port = os.environ.get("DATAHUB_GMS_PORT")
            protocol = os.environ.get("DATAHUB_GMS_PROTOCOL", "http")
            if host:
                if port:
                    env_url = f"{protocol}://{host}:{port}"
                else:
                    env_url = host  # Assume it's a full URL

        if env_url:
            return DataHubEnvConfig(
                server=env_url,
                token=os.environ.get("DATAHUB_GMS_TOKEN"),
                source_file="environment variables",
            )

    # Determine config file path
    if config_path:
        file_path = Path(config_path).expanduser()
    else:
        file_path = Path(DEFAULT_DATAHUB_CONFIG_PATH).expanduser()

    # Load from file
    if file_path.exists():
        return load_env_config_from_file(str(file_path))

    return None


def load_env_config_from_file(file_path: str) -> Optional[DataHubEnvConfig]:
    """
    Load DataHub configuration from a specific file.

    Args:
        file_path: Path to the env file (YAML format).

    Returns:
        DataHubEnvConfig if valid configuration found, None otherwise.
    """
    path = Path(file_path).expanduser()

    if not path.exists():
        return None

    try:
        with open(path) as f:
            config = yaml.safe_load(f)

        if not config:
            return None

        # Handle nested gms structure
        gms_config = config.get("gms", config)

        server = gms_config.get("server")
        if not server:
            return None

        return DataHubEnvConfig(
            server=server,
            token=gms_config.get("token"),
            source_file=str(path),
        )

    except (yaml.YAMLError, OSError) as e:
        print(f"Warning: Failed to load config from {file_path}: {e}")
        return None


def list_env_files(
    search_dirs: Optional[list[str]] = None,
) -> list[tuple[str, Optional[DataHubEnvConfig]]]:
    """
    Discover available datahubenv files.

    Args:
        search_dirs: Additional directories to search. Default searches:
            - ~/.datahubenv*  (all files matching this pattern)
            - ~/.datahub/*.env
            - ~/.datahub/*.yaml

    Returns:
        List of (file_path, config) tuples. config is None if file is invalid.
    """
    found_files: list[tuple[str, Optional[DataHubEnvConfig]]] = []
    seen_paths: set[str] = set()

    def add_file(path: Path) -> None:
        """Add a file if not already seen."""
        path_str = str(path)
        if path_str not in seen_paths and path.is_file():
            seen_paths.add(path_str)
            config = load_env_config_from_file(path_str)
            found_files.append((path_str, config))

    # Check home directory for ~/.datahubenv* pattern files
    home_dir = Path.home()
    for env_file in home_dir.glob(".datahubenv*"):
        add_file(env_file)

    # Check ~/.datahub/ for additional env files
    datahub_dir = home_dir / ".datahub"
    if datahub_dir.is_dir():
        for env_file in datahub_dir.glob("*.env"):
            add_file(env_file)
        for env_file in datahub_dir.glob("*.yaml"):
            add_file(env_file)

    # Check additional directories
    if search_dirs:
        for dir_path in search_dirs:
            dir_path_obj = Path(dir_path).expanduser()
            if dir_path_obj.is_dir():
                for pattern in ["*.env", "*.yaml", "*datahubenv*"]:
                    for env_file in dir_path_obj.glob(pattern):
                        add_file(env_file)

    # Sort by filename for consistent ordering
    found_files.sort(key=lambda x: x[0])

    return found_files


def get_env_config_summary(config: DataHubEnvConfig) -> dict:
    """Get a summary of the configuration for display."""
    return {
        "Server": config.server,
        "Hostname": config.hostname,
        "Token": "***" + config.token[-4:] if config.token else "Not set",
        "Source": config.source_file or "Unknown",
    }
