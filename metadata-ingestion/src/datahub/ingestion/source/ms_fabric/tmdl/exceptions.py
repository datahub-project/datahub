"""TMDL parsing exceptions."""


class TMDLError(Exception):
    """Base class for all TMDL errors."""

    pass


class ValidationError(TMDLError):
    """Raised when TMDL validation fails."""

    pass


class ParsingError(TMDLError):
    """Raised when TMDL parsing fails."""

    pass


class LineageError(TMDLError):
    """Raised when lineage analysis fails."""

    pass
