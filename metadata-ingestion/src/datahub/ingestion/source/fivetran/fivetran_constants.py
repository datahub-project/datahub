from datahub.utilities.str_enum import StrEnum


class FivetranMode(StrEnum):
    """Mode of operation for Fivetran connector."""

    ENTERPRISE = "enterprise"  # Using log tables (enterprise)
    STANDARD = "standard"  # Using REST API (standard/free)
    AUTO = "auto"  # Auto-detect based on provided configs


class DataJobMode(StrEnum):
    """Mode for creating DataJobs."""

    CONSOLIDATED = "consolidated"  # One DataJob per connector (default)
    PER_TABLE = "per_table"  # One DataJob per table
