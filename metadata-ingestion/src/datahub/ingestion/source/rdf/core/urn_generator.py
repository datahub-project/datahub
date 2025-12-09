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

logger = logging.getLogger(__name__)


class UrnGeneratorBase:
    """
    Base class for URN generators with shared functionality.

    Entity-specific URN generators should inherit from this class
    and implement entity-specific methods.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

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
        """
        if platform is None:
            return "logical"

        # If it's already a URN, extract the platform name
        if platform.startswith("urn:li:dataPlatform:"):
            return platform.replace("urn:li:dataPlatform:", "")

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
        """
        # Parse the IRI
        parsed = urlparse(iri)

        # Extract path segments
        path_segments = []

        # Handle standard schemes (http://, https://, ftp://)
        original_iri = parsed.geturl()
        for scheme in ["https://", "http://", "ftp://"]:
            if original_iri.startswith(scheme):
                path_without_scheme = original_iri[len(scheme) :]
                path_segments = path_without_scheme.split("/")
                break

        # Handle other schemes with ://
        if not path_segments and "://" in original_iri:
            path_without_scheme = original_iri.split("://", 1)[1]
            path_segments = path_without_scheme.split("/")

        # Handle non-HTTP schemes like "trading:term/Customer_Name"
        if not path_segments and ":" in original_iri:
            path_without_scheme = original_iri.split(":", 1)[1]
            path_segments = path_without_scheme.split("/")

        if not path_segments:
            raise ValueError(f"IRI must have a valid scheme: {original_iri}")

        # Filter out empty segments and clean them
        clean_segments = []
        for segment in path_segments:
            if segment.strip():  # Skip empty segments
                clean_segments.append(segment.strip())

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
        """
        # Reconstruct the original IRI to extract path
        original_iri = parsed.geturl()

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
        """
        # Use domain as platform if available
        if parsed.netloc:
            domain = parsed.netloc.split(":")[0]
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
        """
        return f"urn:li:dataPlatform:{platform_name}"

    def generate_corpgroup_urn_from_owner_iri(self, owner_iri: str) -> str:
        """
        Generate a DataHub corpGroup URN from an owner IRI with unique identifier.

        Args:
            owner_iri: The owner IRI (e.g., "http://example.com/FINANCE/Business_Owners")

        Returns:
            DataHub corpGroup URN with unique identifier
        """
        # Extract domain and owner type from IRI for unique URN
        # Format: http://example.com/FINANCE/Business_Owners -> finance_business_owners
        if "/" in owner_iri:
            parts = owner_iri.split("/")
            domain = parts[-2].lower()  # FINANCE -> finance
            owner_type = (
                parts[-1].lower().replace("_", "_")
            )  # Business_Owners -> business_owners
            group_name = f"{domain}_{owner_type}"
        else:
            group_name = owner_iri.lower().replace(" ", "_").replace("_", "_")

        return f"urn:li:corpGroup:{group_name}"

    def generate_group_name_from_owner_iri(self, owner_iri: str) -> str:
        """
        Generate a group name from an owner IRI for URN generation.

        Args:
            owner_iri: The owner IRI (e.g., "http://example.com/FINANCE/Business_Owners")

        Returns:
            Group name for URN generation (e.g., "finance_business_owners")
        """
        # This method is used for URN generation, not display names
        # Display names come from rdfs:label in the RDF
        if "/" in owner_iri:
            parts = owner_iri.split("/")
            domain = parts[-2].lower()  # FINANCE -> finance
            owner_type = (
                parts[-1].lower().replace("_", "_")
            )  # Business_Owners -> business_owners
            group_name = f"{domain}_{owner_type}"
        else:
            group_name = owner_iri.lower().replace(" ", "_").replace("_", "_")
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
        Extracted name or None
    """
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
            if hasattr(label, "value") and len(str(label.value).strip()) >= 3:
                return str(label.value).strip()
            elif isinstance(label, str) and len(label.strip()) >= 3:
                return label.strip()

    # No fallback - return None if no proper RDF label found
    return None
