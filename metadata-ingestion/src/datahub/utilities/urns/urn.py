from datahub.metadata.urns import Urn

__all__ = ["Urn", "guess_entity_type"]


def guess_entity_type(urn: str) -> str:
    assert urn.startswith("urn:li:"), "urns must start with urn:li:"
    return urn.split(":")[2]
