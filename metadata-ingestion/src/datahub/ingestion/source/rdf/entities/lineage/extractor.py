"""
Lineage Extractor

Extracts lineage relationships and activities from RDF graphs using PROV-O patterns.
"""

import logging
from typing import Any, Dict, List, Optional

from rdflib import RDF, RDFS, Graph, Literal, Namespace, URIRef

from datahub.ingestion.source.rdf.entities.base import EntityExtractor
from datahub.ingestion.source.rdf.entities.lineage.ast import (
    LineageType,
    RDFLineageActivity,
    RDFLineageRelationship,
)

logger = logging.getLogger(__name__)

# Namespaces
PROV = Namespace("http://www.w3.org/ns/prov#")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
DCTERMS = Namespace("http://purl.org/dc/terms/")


class LineageExtractor(EntityExtractor[RDFLineageRelationship]):
    """
    Extracts lineage relationships from RDF graphs.

    Supports PROV-O patterns (per old implementation):
    - prov:wasDerivedFrom - direct derivation
    - prov:wasInfluencedBy - indirect influence
    - prov:used - activity input
    - prov:wasGeneratedBy - activity output
    - prov:generated - activity output (inverse)
    """

    @property
    def entity_type(self) -> str:
        return "lineage"

    def can_extract(self, graph: Graph, uri: URIRef) -> bool:
        """Check if this URI has lineage relationships."""
        # Check for prov:wasDerivedFrom
        for _ in graph.objects(uri, PROV.wasDerivedFrom):
            return True
        # Check for prov:wasGeneratedBy
        for _ in graph.objects(uri, PROV.wasGeneratedBy):
            return True
        return False

    def extract(
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] = None
    ) -> Optional[RDFLineageRelationship]:
        """Extract a single lineage relationship."""
        return None  # Lineage is extracted in bulk

    def extract_all(
        self, graph: Graph, context: Dict[str, Any] = None
    ) -> List[RDFLineageRelationship]:
        """Extract all lineage relationships from the RDF graph."""
        relationships = []
        seen = set()

        # Extract prov:wasDerivedFrom (direct derivation)
        for subject, _, obj in graph.triples((None, PROV.wasDerivedFrom, None)):
            if isinstance(subject, URIRef) and isinstance(obj, URIRef):
                rel_key = (str(subject), str(obj), "was_derived_from")
                if rel_key not in seen:
                    # Get platforms from entities
                    target_platform = self._extract_platform(graph, subject)
                    source_platform = self._extract_platform(graph, obj)

                    relationships.append(
                        RDFLineageRelationship(
                            source_uri=str(obj),  # Upstream
                            target_uri=str(subject),  # Downstream
                            lineage_type=LineageType.WAS_DERIVED_FROM,
                            source_platform=source_platform,
                            target_platform=target_platform,
                        )
                    )
                    seen.add(rel_key)

        # Extract prov:wasInfluencedBy (indirect influence) - per old implementation
        for subject, _, obj in graph.triples((None, PROV.wasInfluencedBy, None)):
            if isinstance(subject, URIRef) and isinstance(obj, URIRef):
                rel_key = (str(subject), str(obj), "was_influenced_by")
                if rel_key not in seen:
                    target_platform = self._extract_platform(graph, subject)
                    source_platform = self._extract_platform(graph, obj)

                    relationships.append(
                        RDFLineageRelationship(
                            source_uri=str(obj),  # Upstream
                            target_uri=str(subject),  # Downstream
                            lineage_type=LineageType.WAS_INFLUENCED_BY,
                            source_platform=source_platform,
                            target_platform=target_platform,
                        )
                    )
                    seen.add(rel_key)

        # Extract activity-based lineage
        relationships.extend(self._extract_activity_lineage(graph, seen))

        logger.info(f"Extracted {len(relationships)} lineage relationships")
        return relationships

    def extract_activities(
        self, graph: Graph, context: Dict[str, Any] = None
    ) -> List[RDFLineageActivity]:
        """Extract lineage activities from the graph."""
        activities = []
        seen_activities = set()

        # Find prov:Activity entities (direct type)
        for activity_uri in graph.subjects(RDF.type, PROV.Activity):
            if (
                isinstance(activity_uri, URIRef)
                and str(activity_uri) not in seen_activities
            ):
                activity = self._create_activity(graph, activity_uri)
                if activity:
                    activities.append(activity)
                    seen_activities.add(str(activity_uri))

        # Find subclasses of prov:Activity and their instances
        activity_subclasses = [
            PROV.ETLActivity,
            PROV.AnalyticsActivity,
            PROV.RegulatoryActivity,
            PROV.DataFlowActivity,
        ]

        # Also find any classes that are declared as subClassOf prov:Activity
        for subclass in graph.subjects(RDFS.subClassOf, PROV.Activity):
            if isinstance(subclass, URIRef):
                activity_subclasses.append(subclass)

        # Find instances of activity subclasses
        for activity_class in activity_subclasses:
            for activity_uri in graph.subjects(RDF.type, activity_class):
                if (
                    isinstance(activity_uri, URIRef)
                    and str(activity_uri) not in seen_activities
                ):
                    activity = self._create_activity(graph, activity_uri)
                    if activity:
                        activities.append(activity)
                        seen_activities.add(str(activity_uri))

        logger.info(f"Extracted {len(activities)} lineage activities")
        return activities

    def _extract_activity_lineage(
        self, graph: Graph, seen: set
    ) -> List[RDFLineageRelationship]:
        """Extract lineage from prov:Activity patterns."""
        relationships = []

        # Get all activity URIs (including subclasses)
        activity_uris = set()

        # Find prov:Activity entities (direct type)
        for activity_uri in graph.subjects(RDF.type, PROV.Activity):
            if isinstance(activity_uri, URIRef):
                activity_uris.add(activity_uri)

        # Find subclasses of prov:Activity and their instances
        activity_subclasses = [
            PROV.ETLActivity,
            PROV.AnalyticsActivity,
            PROV.RegulatoryActivity,
            PROV.DataFlowActivity,
        ]

        # Also find any classes that are declared as subClassOf prov:Activity
        for subclass in graph.subjects(RDFS.subClassOf, PROV.Activity):
            if isinstance(subclass, URIRef):
                activity_subclasses.append(subclass)

        # Find instances of activity subclasses
        for activity_class in activity_subclasses:
            for activity_uri in graph.subjects(RDF.type, activity_class):
                if isinstance(activity_uri, URIRef):
                    activity_uris.add(activity_uri)

        # Process activities for lineage
        for activity_uri in activity_uris:
            # Get used entities (inputs)
            used_entities = []
            for used in graph.objects(activity_uri, PROV.used):
                if isinstance(used, URIRef):
                    used_entities.append(str(used))

            # Get generated entities (outputs) - both prov:wasGeneratedBy and prov:generated
            generated_entities = set()
            for generated in graph.subjects(PROV.wasGeneratedBy, activity_uri):
                if isinstance(generated, URIRef):
                    generated_entities.add(generated)

            for generated in graph.objects(activity_uri, PROV.generated):
                if isinstance(generated, URIRef):
                    generated_entities.add(generated)

            # Create relationships from each input to each output
            for generated in generated_entities:
                for used_uri in used_entities:
                    rel_key = (used_uri, str(generated), "activity")
                    if rel_key not in seen:
                        source_platform = self._extract_platform(
                            graph, URIRef(used_uri)
                        )
                        target_platform = self._extract_platform(graph, generated)

                        # Always look up platform from connected datasets (target first, then source)
                        # Only use activity's own platform if no connected datasets have platforms
                        activity_platform = target_platform or source_platform
                        if not activity_platform:
                            activity_platform = self._extract_platform(
                                graph, activity_uri
                            )

                        relationships.append(
                            RDFLineageRelationship(
                                source_uri=used_uri,
                                target_uri=str(generated),
                                lineage_type=LineageType.USED,
                                activity_uri=str(activity_uri),
                                source_platform=source_platform,
                                target_platform=target_platform,
                                activity_platform=activity_platform,
                            )
                        )
                        seen.add(rel_key)

        return relationships

    def _create_activity(
        self, graph: Graph, uri: URIRef
    ) -> Optional[RDFLineageActivity]:
        """Create a lineage activity from a URI."""
        try:
            # Extract name
            name = None
            for label in graph.objects(uri, RDFS.label):
                if isinstance(label, Literal):
                    name = str(label)
                    break

            if not name:
                name = str(uri).split("/")[-1].split("#")[-1]

            # Extract description
            description = None
            for desc in graph.objects(uri, RDFS.comment):
                if isinstance(desc, Literal):
                    description = str(desc)
                    break

            # Always look up platform from connected datasets first
            # Get generated entities (outputs) - these are the target datasets
            generated_entities = []
            for generated in graph.subjects(PROV.wasGeneratedBy, uri):
                if isinstance(generated, URIRef):
                    generated_entities.append(generated)
            for generated in graph.objects(uri, PROV.generated):
                if isinstance(generated, URIRef):
                    generated_entities.append(generated)

            # Get used entities (inputs) - these are the source datasets
            used_entities = []
            for used in graph.objects(uri, PROV.used):
                if isinstance(used, URIRef):
                    used_entities.append(used)

            # Always try to get platform from generated (target) datasets first
            platform = None
            for generated in generated_entities:
                platform = self._extract_platform(graph, generated)
                if platform:
                    break

            # Fallback to used (source) datasets
            if not platform:
                for used in used_entities:
                    platform = self._extract_platform(graph, used)
                    if platform:
                        break

            # Only use activity's own platform if no connected datasets have platforms
            if not platform:
                platform = self._extract_platform(graph, uri)

            # Skip activities without platforms - platform is required for DataJob URNs
            if not platform:
                logger.debug(
                    f"Skipping lineage activity '{name}' ({uri}): no platform found. "
                    f"Activity has no platform and no connected datasets with platforms."
                )
                return None

            return RDFLineageActivity(
                uri=str(uri),
                name=name,
                description=description,
                platform=platform,
                properties={},
            )

        except Exception as e:
            logger.warning(f"Error creating activity from {uri}: {e}")
            return None

    def _extract_platform(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract platform from dcat:accessService."""
        for service in graph.objects(uri, DCAT.accessService):
            for title in graph.objects(service, DCTERMS.title):
                if isinstance(title, Literal):
                    return str(title).strip()
            if isinstance(service, URIRef):
                return str(service).split("/")[-1].split("#")[-1].lower()
        return None
