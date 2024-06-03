from datahub.metadata.urns import StructuredPropertyUrn  # noqa: F401


def make_structured_property_urn(structured_property_id: str) -> str:
    return str(StructuredPropertyUrn.create_from_string(structured_property_id))
