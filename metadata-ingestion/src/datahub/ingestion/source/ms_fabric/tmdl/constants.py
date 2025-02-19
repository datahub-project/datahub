"""Constants used throughout the TMDL parser."""

from typing import FrozenSet

# Valid data types for columns
VALID_DATA_TYPES: FrozenSet[str] = frozenset(
    ["string", "int64", "double", "datetime", "boolean", "decimal", "binary", "variant"]
)

# Default compatibility level for TMDL models
DEFAULT_COMPATIBILITY_LEVEL: int = 1600

# Maximum lengths for fields
MAX_NAME_LENGTH: int = 128
MAX_DESCRIPTION_LENGTH: int = 4000
MAX_EXPRESSION_LENGTH: int = 32768

# Version information
CURRENT_VERSION: str = "1.0"
MIN_SUPPORTED_VERSION: str = "1.0"
MAX_SUPPORTED_VERSION: str = "1.0"
