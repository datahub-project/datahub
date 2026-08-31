import re

# Prefix must only contain characters valid in both AWS and GCP secret names.
_VALID_PREFIX_PATTERN = re.compile(r"^[a-zA-Z0-9_-]*$")


def validate_prefix(prefix: str) -> str:
    """Validate prefix contains only characters safe for both AWS and GCP secret names."""
    if not prefix:
        return ""
    if not _VALID_PREFIX_PATTERN.match(prefix):
        raise ValueError(
            f"Invalid secret prefix '{prefix}'. "
            f"Only letters, digits, hyphens, and underscores are allowed (must work for both AWS and GCP)."
        )
    return prefix
