from typing import Optional

from datahub.metadata.urns import (
    DataPlatformUrn,
    Urn,
)

__all__ = ["Urn", "guess_entity_type", "guess_platform_name"]


def guess_entity_type(urn: str) -> str:
    assert urn.startswith("urn:li:"), "urns must start with urn:li:"
    return urn.split(":")[2]


def guess_platform_name(urn: str) -> Optional[str]:
    """Extract platform from URN using a mapping dictionary."""
    urn_obj = Urn.from_string(urn)

    try:
        platform = None
        try:
            platform = urn_obj.platform  # type: ignore[attr-defined]
            platform_name = DataPlatformUrn.from_string(
                platform
            ).get_entity_id_as_string()
            return platform_name
        except AttributeError:
            pass
        try:
            return urn_obj.orchestrator  # type: ignore[attr-defined]
        except AttributeError:
            pass
        try:
            return urn_obj.dashboard_tool  # type: ignore[attr-defined]
        except AttributeError:
            pass
        try:
            return urn_obj.ml_model_tool  # type: ignore[attr-defined]
        except AttributeError:
            pass

        if platform is None:
            return None
    except AttributeError:
        pass
    return None
