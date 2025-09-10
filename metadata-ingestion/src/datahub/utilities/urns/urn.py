from typing import Optional

from datahub.metadata.urns import (
    Urn,
)

__all__ = ["Urn", "guess_entity_type", "get_platform_v1"]


def guess_entity_type(urn: str) -> str:
    assert urn.startswith("urn:li:"), "urns must start with urn:li:"
    return urn.split(":")[2]


def get_platform_v1(urn: str) -> Optional[str]:
    """Extract platform from URN using a mapping dictionary."""
    urn_obj = Urn.from_string(urn)

    try:
        return urn_obj.platform
    except Exception:
        # Not every platform has a platform attribute
        return None
