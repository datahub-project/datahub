"""
DataHub Client - Handles all DataHub operations including glossary creation and deletion.
"""

import logging
from typing import Any, Dict, List, Optional, Set

import requests
from rdflib import RDF, Graph
from rdflib.namespace import Namespace

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.source.rdf.core.urn_generator import UrnGeneratorBase
from datahub.ingestion.source.rdf.entities.domain.urn_generator import (
    DomainUrnGenerator,
)
from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
    GlossaryTermUrnGenerator,
)
from datahub.metadata.schema_classes import (
    GlossaryNodeInfoClass,
    GlossaryRelatedTermsClass,
    GlossaryTermInfoClass,
)

logger = logging.getLogger(__name__)


class DataHubClient:
    """Client for DataHub operations including glossary management."""

    def __init__(self, datahub_gms: str, api_token: str = None):
        """Initialize DataHub client."""
        self.datahub_gms = datahub_gms
        self.api_token = api_token
        self.is_validation_only = datahub_gms is None

        if self.is_validation_only:
            # Validation-only mode - no actual connections
            self.graphql_endpoint = None
            self.emitter = None
        else:
            # Live mode - set up real connections
            self.graphql_endpoint = f"{self.datahub_gms}/api/graphql"

            # Initialize emitter
            if api_token:
                self.emitter = DatahubRestEmitter(self.datahub_gms, token=api_token)
            else:
                self.emitter = DatahubRestEmitter(self.datahub_gms)

        # Track processed items
        self.processed_terms: Set[str] = set()
        self.processed_domains: Set[str] = set()
        self.registered_properties: Dict[str, Dict[str, Any]] = {}
        self.processed_nodes: Set[str] = set()
        self.processed_property_values: Set[str] = (
            set()
        )  # Track applied property values to prevent duplicates
        # Use entity-specific generators
        self.glossary_urn_generator = GlossaryTermUrnGenerator()
        self.domain_urn_generator = DomainUrnGenerator()
        # Base generator for shared methods
        self._base_generator = UrnGeneratorBase()

    def _get_emitter(self) -> DatahubRestEmitter:
        """Get configured DataHub emitter."""
        if self.is_validation_only:
            raise RuntimeError("Cannot get emitter in validation-only mode")
        return self.emitter

    def _emit_mcp(self, event: MetadataChangeProposalWrapper) -> None:
        """Emit MCP event using configured emitter."""
        if self.is_validation_only:
            logger.debug("Validation-only mode: skipping MCP emission")
            return

        logger.debug(f"🔍 DEBUG: _emit_mcp called for entity: {event.entityUrn}")
        logger.debug(f"🔍 DEBUG: Aspect type: {type(event.aspect).__name__}")

        emitter = self._get_emitter()
        try:
            emitter.emit_mcp(event)
            logger.debug(
                f"✅ SUCCESS: MCP event emitted successfully for {event.entityUrn}"
            )
        except Exception as e:
            logger.error(f"❌ FAILED: MCP emission failed for {event.entityUrn}: {e}")
            import traceback

            logger.error(f"💥 TRACEBACK: {traceback.format_exc()}")
            raise

    def _execute_graphql(self, query: str, variables: Dict = None) -> Dict:
        """Execute a GraphQL query."""
        headers = {"Content-Type": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        payload = {"query": query, "variables": variables or {}}

        try:
            response = requests.post(
                self.graphql_endpoint, headers=headers, json=payload, timeout=30
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"GraphQL query failed: {e}")
            if "connection" in str(e).lower() or "timeout" in str(e).lower():
                raise ConnectionError(
                    f"DataHub connection failed during GraphQL query: {e}"
                ) from e
            else:
                raise RuntimeError(
                    f"DataHub API error during GraphQL query: {e}"
                ) from e

    def create_glossary_node(
        self, node_name: str, parent_urn: str = None, description: str = None
    ) -> str:
        """Create or get a glossary node in DataHub."""
        description = description or f"Glossary node: {node_name}"

        # Use centralized URN generation (preserves case)
        node_urn = self.glossary_urn_generator.generate_glossary_node_urn_from_name(
            node_name, parent_urn
        )

        if node_urn in self.processed_nodes:
            return node_urn

        try:
            node_info = GlossaryNodeInfoClass(
                name=node_name, definition=description, parentNode=parent_urn
            )

            # Use MetadataChangeProposalWrapper instead of MCE
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=node_urn,
                aspect=node_info,
            )

            self._emit_mcp(event)

            self.processed_nodes.add(node_urn)
            logger.debug(f"Created glossary node: {node_name}")
            return node_urn

        except Exception as e:
            logger.error(f"Failed to create glossary node {node_name}: {e}")
            raise RuntimeError(
                f"Failed to create glossary node '{node_name}': {e}"
            ) from e

    def create_glossary_term(
        self,
        term_name: str,
        parent_node_urn: Optional[str],
        definition: str = None,
        custom_properties: Dict = None,
        source_ref: str = None,
        term_urn: str = None,
    ) -> str:
        """Create a glossary term in DataHub."""
        if not term_urn:
            raise ValueError(f"No URN provided for term: {term_name}")

        # Extract term ID for deduplication
        term_id = (
            term_urn[20:] if term_urn.startswith("urn:li:glossaryTerm:") else term_urn
        )

        if term_id in self.processed_terms:
            logger.debug(f"Skipping already processed term: {term_id}")
            return term_urn

        try:
            term_info = GlossaryTermInfoClass(
                name=term_name,
                definition=definition or f"Glossary term: {term_name}",
                termSource="EXTERNAL",
                parentNode=parent_node_urn,
                sourceRef=source_ref,
                sourceUrl=source_ref,
                customProperties=custom_properties or {},
            )

            # Use MetadataChangeProposalWrapper instead of MCE
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=term_urn,
                aspect=term_info,
            )

            # Use the centralized emitter method
            self._emit_mcp(event)

            self.processed_terms.add(term_id)
            logger.debug(f"Created glossary term: {term_name}  {term_urn}")
            if source_ref:
                logger.debug(f"✅ Saved original IRI to DataHub: {source_ref}")
            return term_urn

        except Exception as e:
            logger.error(f"Failed to create glossary term {term_name}: {e}")
            raise RuntimeError(
                f"Failed to create glossary term '{term_name}': {e}"
            ) from e

    def add_term_relationships(
        self,
        term_urn: str,
        related_terms: List[str] = None,
        synonyms: List[str] = None,
        broader_terms: List[str] = None,
    ) -> bool:
        """Add relationships to an existing glossary term."""
        if not any([related_terms, synonyms, broader_terms]):
            return True

        try:
            # Filter DataHub URNs only
            datahub_related = [
                t
                for t in (related_terms if related_terms else [])
                if t.startswith("urn:li:glossaryTerm:")
            ]
            datahub_broader = [
                t
                for t in (broader_terms if broader_terms else [])
                if t.startswith("urn:li:glossaryTerm:")
            ]

            related_terms_aspect = GlossaryRelatedTermsClass(
                relatedTerms=datahub_related,
                isRelatedTerms=datahub_broader,
                values=synonyms if synonyms else [],
            )

            # Use MetadataChangeProposalWrapper instead of MCE
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=term_urn,
                aspect=related_terms_aspect,
            )

            # Use the centralized emitter method
            self._emit_mcp(event)

            logger.debug(
                f"Added relationships to term: {len(datahub_related)} related, "
                f"{len(synonyms if synonyms else [])} synonyms, {len(datahub_broader)} broader"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to add relationships to term {term_urn}: {e}")
            return False

    def term_exists(self, term_urn: str) -> bool:
        """Check if a glossary term already exists in DataHub."""
        if term_urn in self.processed_terms:
            return True

        try:
            from datahub.sdk import DataHubClient, GlossaryTermUrn

            client = (
                DataHubClient(server=self.datahub_gms, token=self.api_token)
                if self.api_token
                else DataHubClient(server=self.datahub_gms)
            )
            term_urn_obj = GlossaryTermUrn(term_urn)
            term = client.entities.get(term_urn_obj)
            return term is not None

        except Exception as e:
            logger.debug(f"Error checking term existence for {term_urn}: {e}")
            return False

    def clear_processed_tracking(self):
        """Clear the processed items tracking."""
        self.processed_terms.clear()
        self.processed_domains.clear()
        self.processed_nodes.clear()
        self.processed_property_values.clear()
        logger.info("Cleared processed items tracking")

    def get_processed_stats(self) -> Dict[str, int]:
        """Get statistics about processed items."""
        return {
            "processed_terms": len(self.processed_terms),
            "processed_domains": len(self.processed_domains),
            "processed_nodes": len(self.processed_nodes),
            "processed_property_values": len(self.processed_property_values),
        }

    def search_glossary_items(
        self, parent_urn: str = None, recursive: bool = True
    ) -> Dict:
        """Search for glossary items (terms and nodes) in DataHub with full functionality."""
        if self.is_validation_only:
            logger.debug("Validation-only mode: returning empty search results")
            return {"terms": [], "nodes": []}

        query = """
        query searchGlossaryItems($type: EntityType!, $query: String!, $start: Int!, $count: Int!) {
            search(input: {type: $type, query: $query, start: $start, count: $count}) {
                searchResults {
                    entity {
                        urn
                        type
                        ... on GlossaryTerm {
                            glossaryTermInfo {
                                name
                            }
                            parentNodes {
                                nodes {
                                    urn
                                }
                            }
                        }
                        ... on GlossaryNode {
                            properties {
                                name
                            }
                            parentNodes {
                                nodes {
                                    urn
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        # Search for terms and nodes
        terms_result = self._execute_graphql(
            query, {"type": "GLOSSARY_TERM", "query": "*", "start": 0, "count": 1000}
        )

        nodes_result = self._execute_graphql(
            query, {"type": "GLOSSARY_NODE", "query": "*", "start": 0, "count": 1000}
        )

        # Parse results
        all_terms = []
        all_nodes = []

        # Handle terms results
        terms_data = terms_result.get("data", {})
        if terms_data:
            search_results = terms_data.get("search", {})
            if search_results:
                for result in search_results.get("searchResults", []):
                    entity = result.get("entity", {})
                    if entity.get("type") == "GLOSSARY_TERM":
                        term_info = entity.get("glossaryTermInfo", {})
                        name = term_info.get("name")
                        if name is None:
                            raise ValueError(
                                f"Glossary term URN {entity.get('urn')} has no name"
                            )
                        all_terms.append(
                            {
                                "urn": entity.get("urn"),
                                "name": name,
                                "parentNodes": entity.get("parentNodes", {}),
                            }
                        )

        # Handle nodes results
        nodes_data = nodes_result.get("data", {})
        if nodes_data:
            search_results = nodes_data.get("search", {})
            if search_results:
                for result in search_results.get("searchResults", []):
                    entity = result.get("entity", {})
                    if entity.get("type") == "GLOSSARY_NODE":
                        properties = entity.get("properties", {})
                        name = properties.get("name")
                        if name is None:
                            raise ValueError(
                                f"Glossary node URN {entity.get('urn')} has no name"
                            )
                        all_nodes.append(
                            {
                                "urn": entity.get("urn"),
                                "name": name,
                                "parentNodes": entity.get("parentNodes", {}),
                            }
                        )

        # Filter by parent if specified
        if parent_urn:
            # Include items that have the parent_urn as their parent
            terms = [t for t in all_terms if self._has_parent(t, parent_urn)]
            nodes = [n for n in all_nodes if self._has_parent(n, parent_urn)]

            # Also include the root node itself if it matches parent_urn
            root_node = next((n for n in all_nodes if n["urn"] == parent_urn), None)
            if root_node and root_node not in nodes:
                nodes.append(root_node)
        else:
            terms = all_terms
            nodes = all_nodes

        return {"terms": terms, "nodes": nodes}

    def _has_parent(self, item: Dict, parent_urn: str) -> bool:
        """Check if an item has the specified parent."""
        parent_nodes = item.get("parentNodes", {}).get("nodes", [])
        return any(p.get("urn") == parent_urn for p in parent_nodes)

    def get_term_info(self, term_urn: str) -> Optional[Dict]:
        """Get basic information about a glossary term."""
        if self.is_validation_only:
            logger.debug("Validation-only mode: returning empty term info")
            return None

        try:
            query = f"""
            query {{
                glossaryTerm(urn: "{term_urn}") {{
                    urn
                    glossaryTermInfo {{
                        name
                        description
                    }}
                }}
            }}
            """

            result = self._execute_graphql(query)
            term_data = result.get("data", {}).get("glossaryTerm")

            if not term_data:
                return None

            term_info = term_data.get("glossaryTermInfo", {})
            return {
                "urn": term_urn,
                "name": term_info.get("name"),
                "description": term_info.get("description"),
            }

        except Exception as e:
            logger.error(f"Failed to get term info for {term_urn}: {e}")
            return None

    def get_term_relationships(self, term_urn: str) -> Dict[str, List[str]]:
        """Get relationships for a glossary term using SDK."""
        if self.is_validation_only:
            logger.debug("Validation-only mode: returning empty relationships")
            return {}

        try:
            graph_client = self.emitter.to_graph()
            entity = graph_client.get_entities("glossaryTerm", [term_urn])

            if not entity or len(entity) == 0:
                return {}

            entity_data = entity[term_urn]
            relationships = {}

            if "glossaryRelatedTerms" in entity_data:
                rel_aspect_obj, _ = entity_data["glossaryRelatedTerms"]
                relationships = {
                    "broader": getattr(rel_aspect_obj, "isRelatedTerms", [])
                    if getattr(rel_aspect_obj, "isRelatedTerms", None)
                    else [],
                    "related": getattr(rel_aspect_obj, "relatedTerms", [])
                    if getattr(rel_aspect_obj, "relatedTerms", None)
                    else [],
                    "synonyms": getattr(rel_aspect_obj, "values", [])
                    if getattr(rel_aspect_obj, "values", None)
                    else [],
                    "has_related": getattr(rel_aspect_obj, "hasRelatedTerms", [])
                    if getattr(rel_aspect_obj, "hasRelatedTerms", None)
                    else [],
                }

            return relationships

        except Exception as e:
            logger.error(f"Error getting term relationships for {term_urn}: {e}")
            return {}

    def list_glossary_items(self, parent_urn: str = None) -> List[Dict]:
        """List glossary items (terms and nodes) optionally filtered by parent."""
        try:
            search_results = self.search_glossary_items(parent_urn, recursive=True)

            if not search_results:
                return []

            items = []
            # Add terms
            for term in search_results.get("terms", []):
                items.append({"urn": term["urn"], "name": term["name"], "type": "term"})

            # Add nodes
            for node in search_results.get("nodes", []):
                items.append({"urn": node["urn"], "name": node["name"], "type": "node"})

            return items

        except Exception as e:
            logger.error(f"Failed to list glossary items: {e}")
            return []

    def link_glossary_terms(
        self, term_urn: str, broader_term_urn: str, relationship_type: str
    ) -> bool:
        """Link glossary terms using MCP with GlossaryRelatedTermsClass."""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import GlossaryRelatedTermsClass

            # Create the relationship using GlossaryRelatedTermsClass
            if relationship_type == "broader":
                # For broader relationships, use isRelatedTerms
                relationship_aspect = GlossaryRelatedTermsClass(
                    isRelatedTerms=[broader_term_urn]
                )
            else:
                # For related relationships, use relatedTerms
                relationship_aspect = GlossaryRelatedTermsClass(
                    relatedTerms=[broader_term_urn]
                )

            # Use MetadataChangeProposalWrapper
            mcp = MetadataChangeProposalWrapper(
                entityUrn=term_urn,
                aspect=relationship_aspect,
            )

            # Emit the MCP
            self.emitter.emit_mcp(mcp)

            logger.debug(
                f"Linked glossary term {term_urn} to {broader_term_urn} ({relationship_type})"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to link glossary terms {term_urn} to {broader_term_urn}: {e}"
            )
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Exception details: {str(e)}")
            return False

    def create_domain(
        self, domain_name: str, description: str = None, parent_domain_urn: str = None
    ) -> str:
        """Create a domain in DataHub."""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import DomainPropertiesClass

            # Use centralized URN generation (preserves case)
            domain_urn = self.domain_urn_generator.generate_domain_urn_from_name(
                domain_name, parent_domain_urn
            )

            # Check for deduplication
            if domain_urn in self.processed_domains:
                logger.debug(f"Skipping already processed domain: {domain_urn}")
                return domain_urn

            # Create domain properties aspect
            domain_properties_aspect = DomainPropertiesClass(
                name=domain_name,
                description=description or f"Domain for {domain_name}",
                parentDomain=parent_domain_urn,
            )

            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=domain_urn,
                aspect=domain_properties_aspect,
            )

            self._emit_mcp(event)

            # Track processed domain
            self.processed_domains.add(domain_urn)

            logger.debug(f"Created domain: {domain_name}")
            return domain_urn

        except Exception as e:
            logger.error(f"Failed to create domain {domain_name}: {e}")
            raise RuntimeError(
                f"Domain creation failed for '{domain_name}': {e}"
            ) from e

    def assign_glossary_term_to_domain(
        self, glossary_term_urn: str, domain_urn: str
    ) -> bool:
        """Assign a glossary term to a domain."""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import DomainsClass

            domains_aspect = DomainsClass(domains=[domain_urn])

            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=glossary_term_urn,
                aspect=domains_aspect,
            )

            self._emit_mcp(event)

            logger.info(
                f"Assigned glossary term {glossary_term_urn} to domain {domain_urn}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to assign glossary term {glossary_term_urn} to domain {domain_urn}: {e}"
            )
            return False

    def create_group(
        self,
        group_name: str,
        group_description: str = None,
        group_email: str = None,
        display_name: str = None,
    ) -> bool:
        """Create a DataHub Group (corpGroup)."""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import CorpGroupInfoClass

            group_urn = f"urn:li:corpGroup:{group_name}"

            # Create group info
            group_info = CorpGroupInfoClass(
                displayName=display_name or group_name,
                description=group_description,
                email=group_email,
                admins=[],
                members=[],
                groups=[],
            )

            # Emit MCP with corpGroupInfo aspect for the corpGroup entity
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=group_urn,
                aspect=group_info,
            )

            self._emit_mcp(event)
            logger.info(f"Created DataHub group: {group_urn}")
            return True

        except Exception as e:
            logger.error(f"Failed to create group {group_name}: {e}")
            return False

    def assign_domain_owners(
        self, domain_urn: str, owner_iris: List[str], rdf_graph=None
    ) -> bool:
        """Assign owners to a domain using owner IRIs."""
        try:
            from rdflib.namespace import Namespace

            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import (
                OwnerClass,
                OwnershipClass,
            )

            if not owner_iris:
                logger.debug(f"No owners to assign to domain {domain_urn}")
                return True

            # Convert owner IRIs to DataHub owner objects
            owners = []

            # Owner types must be determined from RDF graph
            if not rdf_graph:
                raise ValueError(
                    f"Cannot determine owner types for domain {domain_urn} without RDF graph. "
                    f"Owners must be defined in RDF with explicit types (dh:BusinessOwner, dh:DataSteward, dh:TechnicalOwner)."
                )

            DH = Namespace("http://datahub.com/ontology/")
            for owner_iri in owner_iris:
                owner_type = self._determine_owner_type_from_rdf(
                    rdf_graph, owner_iri, DH
                )
                if not owner_type:
                    raise ValueError(
                        f"Cannot determine owner type for {owner_iri}. "
                        f"Owner must have dh:hasOwnerType property in RDF (supports custom owner types)."
                    )
                owner_urn = self._base_generator.generate_corpgroup_urn_from_owner_iri(
                    owner_iri
                )

                owners.append(OwnerClass(owner=owner_urn, type=owner_type))

            if not owners:
                logger.debug(f"No owners to assign to domain {domain_urn}")
                return True

            # Create ownership aspect
            ownership_aspect = OwnershipClass(owners=owners)

            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=domain_urn,
                aspect=ownership_aspect,
            )

            self._emit_mcp(event)

            logger.info(f"Assigned {len(owners)} owners to domain {domain_urn}")
            return True

        except Exception as e:
            logger.error(f"Failed to assign owners to domain {domain_urn}: {e}")
            return False

    def _determine_owner_type_from_rdf(
        self, graph: Graph, owner_iri: str, DH: Namespace
    ) -> Optional[str]:
        """Determine the owner type from RDF graph.

        Returns the owner type as a string (supports custom owner types defined in DataHub UI).
        Primary source: dh:hasOwnerType property (can be any custom type string).
        Fallback: Map standard RDF types to their string equivalents.

        Returns None if owner type cannot be determined - no fallback defaults.
        """
        try:
            from rdflib import URIRef

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

            # No fallback - return None if type cannot be determined
            return None

        except Exception as e:
            logger.error(f"Error determining owner type for {owner_iri}: {e}")
            return None

    def delete_entity(self, entity_urn: str) -> bool:
        """
        Delete a DataHub entity by URN.

        Args:
            entity_urn: The URN of the entity to delete

        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            # Create a delete MCP
            mcp = MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=None,  # Delete the entire entity
                changeType="DELETE",
            )
            self._emit_mcp(mcp)
            logger.info(f"Successfully deleted entity: {entity_urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete entity {entity_urn}: {e}")
            return False
