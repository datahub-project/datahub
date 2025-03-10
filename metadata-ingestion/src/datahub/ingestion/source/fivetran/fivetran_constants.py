from enum import Enum


class FivetranMode(str, Enum):
    """Mode of operation for Fivetran connector."""

    ENTERPRISE = "enterprise"  # Using log tables (enterprise)
    STANDARD = "standard"  # Using REST API (standard/free)
    AUTO = "auto"  # Auto-detect based on provided configs
