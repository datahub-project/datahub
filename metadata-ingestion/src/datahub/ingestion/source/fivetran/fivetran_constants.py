from enum import Enum


class FivetranMode(str, Enum):
    """Mode of operation for Fivetran connector."""

    ENTERPRISE = "enterprise"  # Using log tables (enterprise)
    STANDARD = "standard"  # Using REST API (standard/free)
    AUTO = "auto"  # Auto-detect based on provided configs


class DataJobMode(str, Enum):
    """Mode for creating DataJobs."""

    CONSOLIDATED = "consolidated"  # One DataJob per connector (default)
    PER_TABLE = "per_table"  # One DataJob per table
