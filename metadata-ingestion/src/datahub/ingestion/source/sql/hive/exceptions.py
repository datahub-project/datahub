"""Common exceptions for Hive sources"""


class HiveSourceError(Exception):
    """Base exception for Hive source errors"""

    pass


class InvalidDatasetIdentifierError(HiveSourceError, ValueError):
    """Raised when a dataset identifier cannot be parsed into database/schema"""

    pass
