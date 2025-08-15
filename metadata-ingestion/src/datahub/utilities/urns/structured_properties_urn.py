from datahub.metadata.urns import StructuredPropertyUrn

__all__ = ["StructuredPropertyUrn", "make_structured_property_urn"]


def make_structured_property_urn(structured_property_id: str) -> str:
    return str(StructuredPropertyUrn.from_string(structured_property_id))
