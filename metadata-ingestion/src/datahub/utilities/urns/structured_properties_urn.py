from datahub.metadata.urns import StructuredPropertyUrn  # noqa: F401


def make_structured_property_urn(structured_property_id: str) -> str:
    return str(StructuredPropertyUrn(structured_property_id))
