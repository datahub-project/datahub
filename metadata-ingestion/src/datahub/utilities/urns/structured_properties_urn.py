from datahub.metadata.urns import StructuredPropertyUrn

__all__ = ["StructuredPropertyUrn", "make_structured_property_urn"]


def make_structured_property_urn(structured_property_id: str) -> str:
    if structured_property_id.startswith("urn:li:"):
        return str(StructuredPropertyUrn.from_string(structured_property_id))
    return StructuredPropertyUrn(structured_property_id).urn()
