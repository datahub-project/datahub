from typing import Optional

import datahub.metadata.urns as urns
from datahub.utilities.urns.urn import guess_entity_type

_url_prefixes = {
    # Atypical mappings.
    urns.DataJobUrn.ENTITY_TYPE: "tasks",
    urns.DataFlowUrn.ENTITY_TYPE: "pipelines",
    urns.CorpUserUrn.ENTITY_TYPE: "user",
    urns.CorpGroupUrn.ENTITY_TYPE: "group",
    # Normal mappings - matches the entity type.
    urns.ChartUrn.ENTITY_TYPE: "chart",
    urns.ContainerUrn.ENTITY_TYPE: "container",
    urns.DataProductUrn.ENTITY_TYPE: "dataProduct",
    urns.DatasetUrn.ENTITY_TYPE: "dataset",
    urns.DashboardUrn.ENTITY_TYPE: "dashboard",
    urns.DomainUrn.ENTITY_TYPE: "domain",
    urns.GlossaryNodeUrn.ENTITY_TYPE: "glossaryNode",
    urns.GlossaryTermUrn.ENTITY_TYPE: "glossaryTerm",
    urns.TagUrn.ENTITY_TYPE: "tag",
}


def make_url_for_urn(
    frontend_base_url: str,
    entity_urn: str,
    *,
    tab: Optional[str] = None,
) -> str:
    """Build the public-facing URL for an entity urn.

    Args:
        frontend_url: The public-facing base url of the frontend.
        entity_urn: The urn of the entity to get the url for.
        tab: The tab to deep link into. If not provided, the default tab for the entity will be shown.

    Returns:
        The public-facing url for the entity.

    Examples:
        >>> make_url_for_urn("https://demo.datahub.com", "urn:li:container:b41c14bc5cb3ccfbb0433c8cbdef2992", tab="Contents")
        'https://demo.datahub.com/container/urn:li:container:b41c14bc5cb3ccfbb0433c8cbdef2992/Contents'
        >>> make_url_for_urn("https://demo.datahub.com", "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.actuating,PROD)")
        'https://demo.datahub.com/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.actuating,PROD)/'
    """
    entity_type = guess_entity_type(entity_urn)

    url_prefix = _url_prefixes.get(entity_type, entity_type)
    url = f"{frontend_base_url}/{url_prefix}/{entity_urn}/"
    if tab:
        url += f"{tab}"
    return url
