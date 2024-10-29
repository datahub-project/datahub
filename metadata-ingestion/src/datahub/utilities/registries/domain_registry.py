import logging
from typing import List, Optional

from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


class DomainRegistry:
    """A class that makes it easy to resolve domains using DataHub"""

    def __init__(
        self,
        cached_domains: Optional[List[str]] = None,
        graph: Optional[DataHubGraph] = None,
    ):
        self.domain_registry = {}
        if cached_domains:
            # isolate the domains that don't seem fully specified
            domains_needing_resolution = [
                d
                for d in cached_domains
                if (not d.startswith("urn:li:domain") and d.count("-") != 4)
            ]
            if domains_needing_resolution and not graph:
                raise ValueError(
                    f"Following domains need server-side resolution {domains_needing_resolution} but a DataHub server wasn't provided. Either use fully qualified domain ids (e.g. urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba) or provide a datahub_api config in your recipe."
                )
            for domain_identifier in domains_needing_resolution:
                assert graph
                # first try to check if this domain exists by urn
                maybe_domain_urn = f"urn:li:domain:{domain_identifier}"
                maybe_domain_properties = graph.get_domain_properties(maybe_domain_urn)
                if maybe_domain_properties:
                    self.domain_registry[domain_identifier] = maybe_domain_urn
                else:
                    # try to get this domain by name
                    domain_urn = graph.get_domain_urn_by_name(domain_identifier)
                    if domain_urn:
                        self.domain_registry[domain_identifier] = domain_urn
                    else:
                        logger.error(
                            f"Failed to retrieve domain id for domain {domain_identifier}"
                        )
                        raise ValueError(
                            f"domain {domain_identifier} doesn't seem to be provisioned on DataHub. Either provision it first and re-run ingestion, or provide a fully qualified domain id (e.g. urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba) to skip this check."
                        )

    def get_domain_urn(self, domain_identifier: str) -> str:
        return self.domain_registry.get(domain_identifier) or domain_identifier
