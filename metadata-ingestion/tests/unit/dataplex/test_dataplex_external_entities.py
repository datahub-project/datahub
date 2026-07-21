from datahub.ingestion.source.dataplex.dataplex_external_entities import (
    DATAPLEX_ASPECT_RESOURCE_TYPE,
    DATAPLEX_PLATFORM,
    GLOSSARY_TERMS_ASPECT_KEY,
    DataplexAspectId,
    DataplexAspectPlatformResource,
)


def test_platform_resource_key_matches_syncback_shape():
    aspect_id = DataplexAspectId(
        aspect_key=GLOSSARY_TERMS_ASPECT_KEY,
        entry_name="projects/p/locations/l/entryGroups/@bigquery/entries/e",
        field_key="urn:li:glossaryTerm:pii",
    )
    key = aspect_id.to_platform_resource_key()
    assert key.platform == DATAPLEX_PLATFORM
    assert key.resource_type == DATAPLEX_ASPECT_RESOURCE_TYPE
    assert key.primary_key == (
        "projects/p/locations/l/entryGroups/@bigquery/entries/e/"
        "datahub-glossary-terms/urn:li:glossaryTerm:pii"
    )


def test_entity_exposes_linked_resource_and_managed_flag():
    entity = DataplexAspectPlatformResource(
        datahub_urn="urn:li:glossaryTerm:pii",
        managed_by_datahub=True,
        aspect_key=GLOSSARY_TERMS_ASPECT_KEY,
        entry_name="projects/p/locations/l/entryGroups/@bigquery/entries/e",
        field_key="urn:li:glossaryTerm:pii",
    )
    assert entity.is_managed_by_datahub()
    assert entity.datahub_linked_resources().urns == ["urn:li:glossaryTerm:pii"]
    assert isinstance(entity.get_id(), DataplexAspectId)
    # Round-trips through PlatformResource with the term URN as a secondary key.
    pr = entity.as_platform_resource()
    assert pr.resource_info.secondary_keys == ["urn:li:glossaryTerm:pii"]
