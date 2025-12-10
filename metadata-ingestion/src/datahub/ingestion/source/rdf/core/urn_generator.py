#!/usr/bin/env python3
"""
URN Generator Base

This module provides the base class for URN generators with shared functionality.
Entity-specific URN generators are distributed to their respective entity modules
and inherit from UrnGeneratorBase.
"""

import logging
from typing import List, Optional
from urllib.parse import ParseResult, urlparse

from rdflib import Graph, URIRef

from datahub.utilities.urn_encoder import UrnEncoder

logger = logging.getLogger(__name__)


class UrnGeneratorBase:
    """
    Base class for URN generators with shared functionality.

    Entity-specific URN generators should inherit from this class
    and implement entity-specific methods.
    """

    def __init__(self):
        """Initialize the URN generator base class."""
        # Note: Using module-level logger instead of instance logger for consistency

    def _normalize_platform(self, platform: Optional[str]) -> str:
        """
        Normalize platform value, defaulting to "logical" if None.

        This is the centralized function for platform defaulting.
        Any dataset missing a platform will default to "logical".

        Args:
            platform: Platform URN (e.g., "urn:li:dataPlatform:mysql"),
                     platform name (e.g., "mysql"), or None

        Returns:
            Platform name (e.g., "logical", "mysql") - always returns a string

        Raises:
            ValueError: If platform is an empty string
        """
        if platform is None:
            return "logical"

        if not isinstance(platform, str):
            raise ValueError(f"Platform must be a string, got {type(platform)}")

        if not platform.strip():
            raise ValueError("Platform cannot be an empty string")

        # If it's already a URN, extract the platform name
        if platform.startswith("urn:li:dataPlatform:"):
            platform_name = platform.replace("urn:li:dataPlatform:", "")
            if not platform_name.strip():
                raise ValueError(f"Invalid platform URN: {platform}")
            return platform_name

        # Otherwise, return as-is (assumed to be a platform name)
        return platform

    def derive_path_from_iri(self, iri: str, include_last: bool = True) -> List[str]:
        """
        Derive hierarchical path segments from an IRI.

        Args:
            iri: The RDF IRI
            include_last: Whether to include the last segment (entity name)

        Returns:
            List of path segments for domain hierarchy creation

        Raises:
            ValueError: If IRI is invalid or cannot be parsed

        Example:
            >>> generator = UrnGeneratorBase()
            >>> generator.derive_path_from_iri("http://example.com/path/to/term")
            ['example.com', 'path', 'to', 'term']
        """
        if not iri or not isinstance(iri, str):
            raise ValueError(f"IRI must be a non-empty string, got: {iri}")

        # Parse the IRI
        parsed = urlparse(iri)

        # Extract path segments from parsed URL
        path_segments = []

        # Use parsed.path if available (standard HTTP/HTTPS URLs)
        if parsed.path:
            # Include netloc (domain) as first segment for hierarchical structure
            if parsed.netloc:
                # Remove www. prefix if present
                domain = parsed.netloc.split(":")[0]  # Remove port if present
                if domain.startswith("www."):
                    domain = domain[4:]
                path_segments.append(domain)
            # Add path segments
            path_segments.extend([s for s in parsed.path.split("/") if s.strip()])
        elif parsed.netloc:
            # Handle cases like "scheme:netloc" without path
            domain = parsed.netloc.split(":")[0]
            if domain.startswith("www."):
                domain = domain[4:]
            path_segments.append(domain)
        else:
            # Fallback: handle non-standard schemes like "trading:term/Customer_Name"
            original_iri = parsed.geturl()
            if "://" in original_iri:
                path_without_scheme = original_iri.split("://", 1)[1]
                path_segments = [s for s in path_without_scheme.split("/") if s.strip()]
            elif ":" in original_iri:
                path_without_scheme = original_iri.split(":", 1)[1]
                path_segments = [s for s in path_without_scheme.split("/") if s.strip()]
            else:
                raise ValueError(
                    f"IRI must have a valid scheme (http://, https://, or custom:): {original_iri}"
                )

        if not path_segments:
            raise ValueError(f"Cannot extract path segments from IRI: {iri}")

        # Clean segments (already done in list comprehension above, but ensure)
        clean_segments = [
            segment.strip() for segment in path_segments if segment.strip()
        ]

        # Exclude the last segment (entity name) if requested
        if not include_last and len(clean_segments) > 0:
            clean_segments = clean_segments[:-1]

        return clean_segments

    def parse_iri_path(self, iri: str) -> List[str]:
        """
        Parse IRI into path segments array. Consistent across glossary and domains.

        Args:
            iri: The IRI to parse

        Returns:
            List of path segments in hierarchical order
        """
        return self.derive_path_from_iri(iri, include_last=True)

    def _preserve_iri_structure(self, parsed: ParseResult) -> str:
        """
        Extract the path portion from an IRI, removing the scheme.
        This preserves the original IRI structure exactly as it was.

        Args:
            parsed: Parsed URL object

        Returns:
            IRI path without scheme, exactly as it was

        Raises:
            ValueError: If IRI cannot be parsed
        """
        # Reconstruct the original IRI to extract path
        original_iri = parsed.geturl()

        if not original_iri:
            raise ValueError("Cannot extract path from empty IRI")

        # Handle standard schemes (http://, https://, ftp://)
        for scheme in ["https://", "http://", "ftp://"]:
            if original_iri.startswith(scheme):
                return original_iri[len(scheme) :]

        # Handle other schemes with ://
        if "://" in original_iri:
            return original_iri.split("://", 1)[1]

        # Handle non-HTTP schemes like "trading:term/Customer_Name"
        if ":" in original_iri:
            return original_iri.split(":", 1)[1]

        raise ValueError(f"IRI must have a valid scheme: {original_iri}")

    def _derive_platform_from_iri(self, parsed: ParseResult) -> str:
        """
        Derive platform name from IRI structure.

        Args:
            parsed: Parsed URL object

        Returns:
            Platform name

        Raises:
            ValueError: If platform cannot be derived from IRI
        """
        # Use domain as platform if available
        if parsed.netloc:
            domain = parsed.netloc.split(":")[0]
            if not domain:
                raise ValueError(
                    f"Cannot derive platform from IRI with empty netloc: {parsed}"
                )
            if domain.startswith("www."):
                domain = domain[4:]
            return domain

        # Use scheme as platform
        if parsed.scheme:
            return parsed.scheme

        # No fallback - raise error for invalid IRIs
        raise ValueError(f"Cannot derive platform from IRI: {parsed}")

    def generate_data_platform_urn(self, platform_name: str) -> str:
        """
        Generate a DataPlatform URN from platform name.

        Args:
            platform_name: The platform name (postgres, mysql, snowflake, etc.)

        Returns:
            DataHub DataPlatform URN

        Raises:
            ValueError: If platform_name is invalid

        Example:
            >>> generator = UrnGeneratorBase()
            >>> generator.generate_data_platform_urn("postgres")
            'urn:li:dataPlatform:postgres'
        """
        if not platform_name or not isinstance(platform_name, str):
            raise ValueError(
                f"Platform name must be a non-empty string, got: {platform_name}"
            )

        platform_name = platform_name.strip()
        if not platform_name:
            raise ValueError("Platform name cannot be empty")

        # Encode reserved characters in platform name
        encoded_name = UrnEncoder.encode_string(platform_name)

        return f"urn:li:dataPlatform:{encoded_name}"

    def generate_corpgroup_urn_from_owner_iri(self, owner_iri: str) -> str:
        """
        Generate a DataHub corpGroup URN from an owner IRI with unique identifier.

        Args:
            owner_iri: The owner IRI (e.g., "http://example.com/FINANCE/Business_Owners")

        Returns:
            DataHub corpGroup URN with unique identifier

        Raises:
            ValueError: If owner_iri is invalid

        Example:
            >>> generator = UrnGeneratorBase()
            >>> generator.generate_corpgroup_urn_from_owner_iri("http://example.com/FINANCE/Business_Owners")
            'urn:li:corpGroup:finance_business_owners'
        """
        if not owner_iri or not isinstance(owner_iri, str):
            raise ValueError(f"Owner IRI must be a non-empty string, got: {owner_iri}")

        # Extract domain and owner type from IRI for unique URN
        # Format: http://example.com/FINANCE/Business_Owners -> finance_business_owners
        if "/" in owner_iri:
            parts = owner_iri.split("/")
            if len(parts) < 2:
                raise ValueError(f"Invalid owner IRI format: {owner_iri}")
            domain = parts[-2].lower() if len(parts) >= 2 else ""
            owner_type = parts[-1].lower() if parts[-1] else ""
            if not domain or not owner_type:
                raise ValueError(
                    f"Cannot extract domain and owner type from IRI: {owner_iri}"
                )
            group_name = f"{domain}_{owner_type}"
        else:
            group_name = owner_iri.lower().replace(" ", "_")

        # Encode reserved characters
        group_name = UrnEncoder.encode_string(group_name)

        return f"urn:li:corpGroup:{group_name}"

    def generate_group_name_from_owner_iri(self, owner_iri: str) -> str:
        """
        Generate a group name from an owner IRI for URN generation.

        This method is used for URN generation, not display names.
        Display names come from rdfs:label in the RDF.

        Args:
            owner_iri: The owner IRI (e.g., "http://example.com/FINANCE/Business_Owners")

        Returns:
            Group name for URN generation (e.g., "finance_business_owners")

        Raises:
            ValueError: If owner_iri is invalid

        Note:
            This method duplicates logic from generate_corpgroup_urn_from_owner_iri()
            but returns only the group name without the URN prefix. Consider consolidating.
        """
        if not owner_iri or not isinstance(owner_iri, str):
            raise ValueError(f"Owner IRI must be a non-empty string, got: {owner_iri}")

        if "/" in owner_iri:
            parts = owner_iri.split("/")
            if len(parts) < 2:
                raise ValueError(f"Invalid owner IRI format: {owner_iri}")
            domain = parts[-2].lower() if len(parts) >= 2 else ""
            owner_type = parts[-1].lower() if parts[-1] else ""
            if not domain or not owner_type:
                raise ValueError(
                    f"Cannot extract domain and owner type from IRI: {owner_iri}"
                )
            group_name = f"{domain}_{owner_type}"
        else:
            group_name = owner_iri.lower().replace(" ", "_")

        return group_name


def extract_name_from_label(graph: Graph, uri: URIRef) -> Optional[str]:
    """
    Extract name from RDF labels (separate from URN generation).

    This function handles name extraction from various label properties,
    keeping it separate from URN generation which uses IRI structure.

    Args:
        graph: RDFLib Graph
        uri: URI to extract label from

    Returns:
        Extracted name or None if no valid label found

    Example:
        >>> from rdflib import Graph, URIRef
        >>> graph = Graph()
        >>> graph.add((URIRef("http://example.com/term"), RDFS.label, "My Term"))
        >>> extract_name_from_label(graph, URIRef("http://example.com/term"))
        'My Term'
    """
    if not isinstance(graph, Graph):
        raise ValueError(f"graph must be an RDFLib Graph, got: {type(graph)}")
    if not isinstance(uri, URIRef):
        raise ValueError(f"uri must be an RDFLib URIRef, got: {type(uri)}")

    from rdflib import Namespace
    from rdflib.namespace import DCTERMS, RDFS, SKOS

    # Use Namespace objects for proper matching
    SCHEMA = Namespace("http://schema.org/")
    DCAT = Namespace("http://www.w3.org/ns/dcat#")

    # Priority order for label extraction using Namespace objects
    label_properties = [
        SKOS.prefLabel,  # skos:prefLabel
        RDFS.label,  # rdfs:label
        DCTERMS.title,  # dcterms:title
        SCHEMA.name,  # schema:name
        DCAT.title,  # dcat:title
    ]

    for prop in label_properties:
        for label in graph.objects(uri, prop):
            label_str: str
            if hasattr(label, "value"):
                label_str = str(label.value).strip()
            elif isinstance(label, str):
                label_str = label.strip()
            else:
                label_str = str(label).strip()

            # Require minimum length for valid labels
            if len(label_str) >= 3:
                return label_str

    # No fallback - return None if no proper RDF label found
    return None
