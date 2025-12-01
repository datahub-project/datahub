"""
Data Product Extractor

Extracts data products from RDF graphs.
"""

import logging
from typing import Any, Dict, List, Optional

from rdflib import RDF, RDFS, Graph, Literal, Namespace, URIRef

from datahub.ingestion.source.rdf.entities.base import EntityExtractor
from datahub.ingestion.source.rdf.entities.data_product.ast import (
    RDFDataProduct,
    RDFDataProductAsset,
)

logger = logging.getLogger(__name__)

# Namespaces (per old implementation)
DPROD = Namespace("https://ekgf.github.io/dprod/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
DCTERMS = Namespace("http://purl.org/dc/terms/")


class DataProductExtractor(EntityExtractor[RDFDataProduct]):
    """
    Extracts data products from RDF graphs.

    Identifies entities as data products if they have type dprod:DataProduct.
    """

    @property
    def entity_type(self) -> str:
        return "data_product"

    def can_extract(self, graph: Graph, uri: URIRef) -> bool:
        """Check if this URI represents a data product."""
        # Explicit check for dprod:DataProduct (per old implementation)
        return (uri, RDF.type, DPROD.DataProduct) in graph

    def extract(
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] = None
    ) -> Optional[RDFDataProduct]:
        """Extract a single data product from the RDF graph."""
        try:
            name = self._extract_name(graph, uri)
            if not name:
                return None

            description = self._extract_description(graph, uri)
            domain = self._extract_domain(graph, uri)
            owner = self._extract_owner(graph, uri)
            # owner_type extracted but not currently used
            # self._extract_owner_type(graph, owner) if owner else None
            assets = self._extract_assets(graph, uri)

            properties = {}
            properties["rdf:originalIRI"] = str(uri)

            return RDFDataProduct(
                uri=str(uri),
                name=name,
                description=description,
                domain=domain,
                owner=owner,
                assets=assets,
                properties=properties,
            )

        except Exception as e:
            logger.warning(f"Error extracting data product from {uri}: {e}")
            return None

    def extract_all(
        self, graph: Graph, context: Dict[str, Any] = None
    ) -> List[RDFDataProduct]:
        """Extract all data products from the RDF graph."""
        products = []
        seen_uris = set()

        # Find dprod:DataProduct (per old implementation - explicit type check)
        for subject in graph.subjects(RDF.type, DPROD.DataProduct):
            if isinstance(subject, URIRef) and str(subject) not in seen_uris:
                product = self.extract(graph, subject, context)
                if product:
                    products.append(product)
                    seen_uris.add(str(subject))

        logger.info(f"Extracted {len(products)} data products")
        return products

    def _extract_name(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract name from label properties."""
        for prop in [RDFS.label, DCTERMS.title]:
            for obj in graph.objects(uri, prop):
                if isinstance(obj, Literal):
                    return str(obj).strip()

        return None

    def _extract_description(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract description."""
        for prop in [RDFS.comment, DCTERMS.description]:
            for obj in graph.objects(uri, prop):
                if isinstance(obj, Literal):
                    return str(obj).strip()
        return None

    def _extract_domain(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract domain reference using dprod:hasDomain.

        Only dprod:hasDomain is supported. No fallback to dprod:domain.
        """
        for obj in graph.objects(uri, DPROD.hasDomain):
            if obj:
                return str(obj)
        return None

    def _extract_owner(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract owner reference."""
        for obj in graph.objects(uri, DPROD.dataOwner):
            if obj:
                return str(obj)
        return None

    def _extract_owner_type(
        self, graph: Graph, owner_iri: Optional[str]
    ) -> Optional[str]:
        """Extract owner type from owner IRI.

        Returns the owner type as a string (supports custom owner types defined in DataHub UI).
        Primary source: dh:hasOwnerType property (can be any custom type string).
        Fallback: Map standard RDF types to their string equivalents.
        """
        if not owner_iri:
            return None

        try:
            from rdflib import RDF, URIRef
            from rdflib.namespace import Namespace

            DH = Namespace("http://datahub.com/ontology/")
            owner_uri = URIRef(owner_iri)

            # Primary: Check for explicit owner type property (supports custom types)
            owner_type_literal = graph.value(owner_uri, DH.hasOwnerType)
            if owner_type_literal:
                # Return the string value directly - supports any custom owner type
                return str(owner_type_literal).strip()

            # Fallback: Map standard RDF types to their string equivalents
            if (owner_uri, RDF.type, DH.BusinessOwner) in graph:
                return "BUSINESS_OWNER"
            elif (owner_uri, RDF.type, DH.DataSteward) in graph:
                return "DATA_STEWARD"
            elif (owner_uri, RDF.type, DH.TechnicalOwner) in graph:
                return "TECHNICAL_OWNER"

            return None
        except Exception as e:
            logger.warning(f"Error extracting owner type for {owner_iri}: {e}")
            return None

    def _extract_assets(self, graph: Graph, uri: URIRef) -> List[RDFDataProductAsset]:
        """Extract asset references with platform information."""
        assets = []
        for obj in graph.objects(uri, DPROD.asset):
            if isinstance(obj, URIRef):
                # Extract platform for this asset
                platform = self._extract_platform(graph, obj)
                assets.append(RDFDataProductAsset(uri=str(obj), platform=platform))
        return assets

    def _extract_platform(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract platform from dcat:accessService.

        Requires dcat:accessService pointing to a service with dcterms:title.
        Returns None if platform cannot be determined - no fallback to URI parsing.
        """
        for service in graph.objects(uri, DCAT.accessService):
            # Get the title of the service
            for title in graph.objects(service, DCTERMS.title):
                if isinstance(title, Literal):
                    return str(title).strip()
        return None
