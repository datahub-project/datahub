"""
Domain Builder

Builds domain hierarchy from glossary terms.
Domains are derived from IRI path segments, not extracted directly from RDF.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
from datahub.ingestion.source.rdf.entities.domain.urn_generator import (
    DomainUrnGenerator,
)

# Forward references to avoid circular imports
if TYPE_CHECKING:
    from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
        DataHubGlossaryTerm,
    )

logger = logging.getLogger(__name__)


class DomainBuilder:
    """
    Builds domain hierarchy from entities.

    Domains are constructed from the path_segments of glossary terms.
    The hierarchy is created automatically.

    Domains with glossary terms in their hierarchy are created.
    """

    def __init__(self, urn_generator: Optional[DomainUrnGenerator] = None):
        """
        Initialize the builder.

        Args:
            urn_generator: URN generator for creating domain URNs
        """
        self.urn_generator = urn_generator or DomainUrnGenerator()

    def build_domains(
        self,
        glossary_terms: List["DataHubGlossaryTerm"],
        context: Optional[Dict[str, Any]] = None,
    ) -> List[DataHubDomain]:
        """
        Build domain hierarchy from glossary terms.

        Args:
            glossary_terms: List of DataHub glossary terms
            context: Optional context

        Returns:
            List of DataHub domains with hierarchy
        """
        # Collect all unique path prefixes
        path_to_domain: Dict[
            Tuple[str, ...], DataHubDomain
        ] = {}  # path_tuple -> DataHubDomain
        path_to_terms: Dict[
            Tuple[str, ...], List[DataHubGlossaryTerm]
        ] = {}  # path_tuple -> [terms]

        # Process glossary terms
        for term in glossary_terms:
            if term.path_segments:
                path = tuple(term.path_segments)
                # Exclude the term itself (last segment is the term name)
                for i in range(1, len(path)):
                    parent_path = path[:i]
                    if parent_path not in path_to_domain:
                        path_to_domain[parent_path] = self._create_domain(parent_path)
                        path_to_terms[parent_path] = []

                    # Add term to its immediate parent domain
                    if i == len(path) - 1:
                        path_to_terms[parent_path].append(term)

        # Build domain hierarchy
        domains = []
        for path, domain in path_to_domain.items():
            # Set parent
            if len(path) > 1:
                parent_path = path[:-1]
                if parent_path in path_to_domain:
                    domain.parent_domain_urn = path_to_domain[parent_path].urn

            # Add terms
            domain.glossary_terms = path_to_terms.get(path, [])

            # Add subdomains
            domain.subdomains = [
                d
                for p, d in path_to_domain.items()
                if len(p) == len(path) + 1 and p[: len(path)] == path
            ]

            domains.append(domain)

        # Filter out empty domains (no glossary terms)
        domains = self._filter_empty_domains(domains)

        logger.info(f"Built {len(domains)} domains")
        return domains

    def _create_domain(self, path: Tuple[str, ...]) -> DataHubDomain:
        """Create a domain from a path tuple."""
        domain_urn_str = self.urn_generator.generate_domain_urn(path)
        from datahub.utilities.urns.domain_urn import DomainUrn

        domain_urn = DomainUrn.from_string(domain_urn_str)

        return DataHubDomain(
            urn=domain_urn,
            name=path[-1] if path else "",
            path_segments=list(path),
            parent_domain_urn=None,
            glossary_terms=[],
            subdomains=[],
        )

    def _filter_empty_domains(
        self, domains: List[DataHubDomain]
    ) -> List[DataHubDomain]:
        """Filter to only include domains with content (glossary terms)."""
        # Build lookup by URN
        domains_by_urn = {str(d.urn): d for d in domains}

        # Mark domains that have content
        has_content = set()

        for domain in domains:
            if self._domain_has_content(domain, domains_by_urn):
                has_content.add(str(domain.urn))

        # Filter
        filtered = [d for d in domains if str(d.urn) in has_content]

        if len(filtered) < len(domains):
            logger.info(f"Filtered out {len(domains) - len(filtered)} empty domains")

        return filtered

    def _domain_has_content(
        self, domain: DataHubDomain, domains_by_urn: Dict[str, DataHubDomain]
    ) -> bool:
        """Check if domain or any subdomain has content (glossary terms)."""
        # Direct content
        if domain.glossary_terms:
            return True

        # Check subdomains recursively
        for subdomain in domain.subdomains:
            if self._domain_has_content(subdomain, domains_by_urn):
                return True

        return False
