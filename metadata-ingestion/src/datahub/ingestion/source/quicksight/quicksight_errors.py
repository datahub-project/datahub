"""Shared error-classification helpers for the QuickSight connector."""

from typing import Optional

from botocore.exceptions import ClientError

# QuickSight returns these error codes when a feature (namespaces, folders,
# identity APIs) is only available on Enterprise edition or the caller lacks the
# relevant permission. Callers degrade gracefully on these rather than failing
# the whole run.
GRACEFUL_DEGRADE_CODES = frozenset(
    {
        "UnsupportedUserEditionException",
        "AccessDeniedException",
        "ResourceNotFoundException",
    }
)


def graceful_code(error: Exception) -> Optional[str]:
    """Return the AWS error code if ``error`` is a gracefully-degradable
    ``ClientError`` (unsupported edition / missing permission), else ``None``."""
    if isinstance(error, ClientError):
        code = error.response.get("Error", {}).get("Code")
        if code in GRACEFUL_DEGRADE_CODES:
            return code
    return None
