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
from datahub.ingestion.source.rdf.entities.lineage.urn_generator import (
    LineageUrnGenerator,
)
from datahub.metadata.schema_classes import (
    DataHubSearchConfigClass,
    GlossaryNodeInfoClass,
    GlossaryRelatedTermsClass,
    GlossaryTermInfoClass,
    PropertyCardinalityClass,
    PropertyValueClass,
    SearchFieldTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn

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
        self.lineage_urn_generator = LineageUrnGenerator()
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

    def create_dataset(self, dataset_urn: str, dataset_properties: Dict) -> bool:
        """Create a dataset in DataHub."""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import DatasetPropertiesClass

            # Create dataset properties aspect
            if "name" not in dataset_properties:
                raise ValueError("Dataset name is required")
            if "description" not in dataset_properties:
                raise ValueError(
                    f"Dataset description is required for: {dataset_properties['name']}"
                )

            properties_aspect = DatasetPropertiesClass(
                name=dataset_properties["name"],
                description=dataset_properties["description"],
                customProperties=dataset_properties.get("custom_properties") or {},
            )

            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=properties_aspect,
            )

            self._emit_mcp(event)

            # Create schema if schema fields are provided
            schema_fields = dataset_properties.get("schema_fields") or []
            if schema_fields:
                from datahub.metadata.schema_classes import (
                    SchemalessClass,
                    SchemaMetadataClass,
                )

                # Schema fields are already SchemaFieldClass objects from the AST
                fields = schema_fields

                # Create SchemaMetadata aspect
                # Platform defaults to "logical" if not specified (via URN generator normalization)
                platform = dataset_properties.get("platform")

                # Normalize platform using URN generator's centralized function
                platform_name = self._base_generator._normalize_platform(platform)
                platform_urn = self._base_generator.generate_data_platform_urn(
                    platform_name
                )

                schema_metadata = SchemaMetadataClass(
                    schemaName=dataset_properties["name"].replace(" ", "_"),
                    platform=platform_urn,
                    version=0,
                    hash="",  # Empty hash is valid for schemaless datasets
                    platformSchema=SchemalessClass(),
                    fields=fields,
                )

                self.create_dataset_schema(dataset_urn, schema_metadata)

            logger.debug(f"Created dataset: {dataset_properties['name']}")
            return True

        except Exception as e:
            logger.error(f"Failed to create dataset {dataset_properties['name']}: {e}")
            return False

    def create_dataset_schema(self, dataset_urn: str, schema_metadata) -> bool:
        """Create dataset schema in DataHub."""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper

            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=schema_metadata,
            )

            self._emit_mcp(event)

            logger.debug(f"Created schema for dataset: {dataset_urn}")
            return True

        except Exception as e:
            logger.error(f"Failed to create schema for dataset {dataset_urn}: {e}")
            return False

    def link_field_glossary_term(
        self, dataset_urn: DatasetUrn, field_name: str, glossary_term_urn: str
    ) -> bool:
        """Link a schema field to a glossary term using the DataHub SDK."""
        try:
            from datahub.sdk import DataHubClient, GlossaryTermUrn

            # Create DataHub client with proper configuration
            client = DataHubClient(server=self.datahub_gms, token=self.api_token)

            # Get the dataset entity
            dataset = client.entities.get(dataset_urn)

            # Add the glossary term to the field
            dataset[field_name].add_term(GlossaryTermUrn(glossary_term_urn))

            # Update the dataset
            client.entities.update(dataset)

            logger.debug(
                f"Linked field {field_name} to glossary term {glossary_term_urn}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to link field {field_name} to glossary term {glossary_term_urn}: {e}"
            )
            return False

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

    def apply_structured_property(
        self, dataset_urn: str, property_urn: str, property_value: Any
    ) -> bool:
        """Apply a structured property to a dataset."""
        try:
            # Validate property value - skip null/empty values
            if property_value is None or str(property_value).strip() == "":
                logger.warning(
                    f"Skipping null/empty structured property value: {property_urn} on {dataset_urn}"
                )
                return True

            # Create a unique key for this property value assignment
            property_key = f"{dataset_urn}|{property_urn}|{str(property_value)}"

            # Check for deduplication
            if property_key in self.processed_property_values:
                logger.debug(
                    f"Skipping already processed property value: {property_urn} on {dataset_urn}"
                )
                return True

            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import (
                StructuredPropertiesClass,
                StructuredPropertyValueAssignmentClass,
            )

            # Create structured property value assignment
            property_value_assignment = StructuredPropertyValueAssignmentClass(
                propertyUrn=property_urn, values=[str(property_value)]
            )

            # Create structured properties aspect
            # CORRECT: properties should be an array, not a dict
            structured_properties = StructuredPropertiesClass(
                properties=[property_value_assignment]
            )

            # Create metadata change proposal
            event = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=structured_properties
            )

            # Emit the event
            self._emit_mcp(event)

            # Track this property value as processed
            self.processed_property_values.add(property_key)

            logger.info(
                f"Applied structured property {property_urn} to dataset {dataset_urn}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to apply structured property {property_urn} to dataset {dataset_urn}: {e}"
            )
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

    def assign_dataset_to_domain(self, dataset_urn: str, domain_urn: str) -> bool:
        """Assign a dataset to a domain."""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import DomainsClass

            domains_aspect = DomainsClass(domains=[domain_urn])

            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=domains_aspect,
            )

            self._emit_mcp(event)

            logger.info(f"Assigned dataset {dataset_urn} to domain {domain_urn}")
            return True

        except Exception as e:
            logger.error(
                f"Failed to assign dataset {dataset_urn} to domain {domain_urn}: {e}"
            )
            raise RuntimeError(f"Dataset assignment failed: {e}") from e

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

    def register_structured_property(self, property_definition: Dict[str, Any]) -> bool:
        """
        Register a structured property definition in DataHub.

        Args:
            property_definition: Property definition dictionary

        Returns:
            True if successful, False otherwise
        """
        try:
            property_name = property_definition.get("name") or property_definition.get(
                "property_name"
            )
            if not property_name:
                logger.error(
                    "Property definition missing 'name' or 'property_name' field"
                )
                return False

            # Convert allowed values to proper format (only if specified)
            allowed_values = None
            allowed_values_list = property_definition.get("allowed_values")
            if allowed_values_list:
                allowed_values = []
                for value in allowed_values_list:
                    allowed_values.append(PropertyValueClass(value=value))

            # Extract qualified name from URN
            property_urn = property_definition["property_urn"]
            if hasattr(property_urn, "entity_ids") and property_urn.entity_ids:
                qualified_name = property_urn.entity_ids[0]
            elif hasattr(property_urn, "get_entity_id"):
                # Fallback for older DataHub SDK versions (returns list)
                entity_id_result = property_urn.get_entity_id()
                qualified_name = (
                    entity_id_result[0]
                    if isinstance(entity_id_result, list) and entity_id_result
                    else str(entity_id_result)
                )
            else:
                # Fallback for string URNs
                qualified_name = str(property_urn).replace(
                    "urn:li:structuredProperty:", ""
                )

            # Normalize qualified name (DataHub doesn't allow spaces in qualified names)
            qualified_name = qualified_name.replace(" ", "_")

            # Validate required fields
            if "description" not in property_definition:
                raise ValueError(
                    f"Description required for structured property: {property_name}"
                )
            if "value_type" not in property_definition:
                raise ValueError(
                    f"Value type required for structured property: {property_name}"
                )
            if "entity_types" not in property_definition:
                raise ValueError(
                    f"Entity types required for structured property: {property_name}"
                )

            # Create search configuration for searchable properties
            search_config = DataHubSearchConfigClass(
                enableAutocomplete=True,
                addToFilters=True,
                queryByDefault=True,
                fieldType=SearchFieldTypeClass.TEXT,
            )

            # Create DataHub definition with sidebar and search configuration
            datahub_definition = StructuredPropertyDefinitionClass(
                qualifiedName=qualified_name,
                displayName=property_name,  # Use the original name with spaces as display name
                description=property_definition["description"],
                valueType=property_definition["value_type"],
                cardinality=PropertyCardinalityClass.SINGLE,
                entityTypes=property_definition["entity_types"],
                allowedValues=allowed_values,  # None means no restrictions
                searchConfiguration=search_config,
            )

            # Create MCP for property definition
            mcp = MetadataChangeProposalWrapper(
                entityUrn=property_definition["property_urn"], aspect=datahub_definition
            )

            # Emit to DataHub
            self._emit_mcp(mcp)

            # Store locally
            self.registered_properties[property_name] = property_definition

            logger.info(f"✅ Registered structured property: {property_name}")
            return True

        except Exception as e:
            logger.error(
                f"❌ Failed to register structured property {property_name}: {e}"
            )
            return False

    def apply_structured_properties(self, dataset_export: Dict[str, Any]) -> bool:
        """
        Apply structured properties to a DataHub dataset.

        Args:
            dataset_export: Dataset export object with properties

        Returns:
            True if successful, False otherwise
        """
        try:
            dataset_urn = dataset_export["dataset_urn"]
            properties_to_apply = dataset_export[
                "properties"
            ]  # List of StructuredPropertyValueAssignmentClass

            if not properties_to_apply:
                logger.debug(f"No structured properties to apply for {dataset_urn}")
                return True

            # Create structured properties aspect
            structured_properties_aspect = StructuredPropertiesClass(
                properties=properties_to_apply
            )

            # Create MCP
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=structured_properties_aspect
            )

            # Emit to DataHub
            self._emit_mcp(mcp)

            logger.info(f"✅ Applied structured properties to dataset: {dataset_urn}")
            return True

        except Exception as e:
            logger.error(
                f"❌ Failed to apply structured properties to dataset {dataset_urn}: {e}"
            )
            return False

    def create_data_job(
        self,
        job_name: str,
        job_description: str,
        job_type: str,
        platform: str,
        environment: str,
        input_datasets: List[str] = None,
        output_datasets: List[str] = None,
        custom_properties: Dict = None,
    ) -> bool:
        """
        Create a DataJob in DataHub with input/output datasets for lineage.

        Args:
            job_name: Name of the job
            job_description: Description of the job
            job_type: Type of job (BATCH, STREAMING, AD_HOC)
            platform: Platform name (dbt, spark, airflow, etc.)
            environment: Environment (PROD, DEV, etc.)
            input_datasets: List of input dataset URNs
            output_datasets: List of output dataset URNs
            custom_properties: Additional custom properties

        Returns:
            True if creation was successful, False otherwise
        """
        try:
            from datahub.metadata.schema_classes import (
                DataJobInfoClass,
                DataJobInputOutputClass,
                DataJobKeyClass,
            )

            # Use URN generator for DataJob URN
            job_urn = self.lineage_urn_generator.generate_data_job_urn(
                platform, job_name, environment
            )

            # Use URN generator for DataFlow URN
            flow_urn = self.lineage_urn_generator.generate_data_flow_urn(
                job_name, platform, environment
            )

            # Create data job key (not used but required for DataJobKeyClass structure)
            DataJobKeyClass(flow=flow_urn, jobId=job_name)

            # Create data job info
            job_info = DataJobInfoClass(
                name=job_name,
                description=job_description,
                type=job_type,
                customProperties=custom_properties or {},
            )

            # Create metadata change proposal for job info
            event = MetadataChangeProposalWrapper(entityUrn=job_urn, aspect=job_info)

            self._emit_mcp(event)

            # Create input/output datasets aspect if provided
            if input_datasets or output_datasets:
                input_output = DataJobInputOutputClass(
                    inputDatasets=input_datasets or [],
                    outputDatasets=output_datasets or [],
                )

                # Create metadata change proposal for input/output
                io_event = MetadataChangeProposalWrapper(
                    entityUrn=job_urn, aspect=input_output
                )

                self._emit_mcp(io_event)
                logger.info(
                    f"Created DataJob with I/O datasets: {job_name} (URN: {job_urn})"
                )
            else:
                logger.info(f"Created DataJob: {job_name} (URN: {job_urn})")

            return True

        except Exception as e:
            logger.error(f"Failed to create DataJob {job_name}: {e}")
            return False

    def create_upstream_lineage(
        self,
        target_dataset_urn: str,
        source_dataset_urn: str,
        lineage_type: str = "TRANSFORMED",
    ) -> bool:
        """
        Create upstream lineage between datasets.

        Args:
            target_dataset_urn: URN of the target dataset
            source_dataset_urn: URN of the source dataset
            lineage_type: Type of lineage (TRANSFORMED, COPY, etc.)

        Returns:
            True if creation was successful, False otherwise
        """
        return self.create_upstream_lineage_multiple(
            target_dataset_urn, [source_dataset_urn], lineage_type
        )

    def create_upstream_lineage_multiple(
        self,
        target_dataset_urn: str,
        source_dataset_urns: List[str],
        lineage_type: str = "TRANSFORMED",
    ) -> bool:
        """
        Create upstream lineage between datasets with multiple sources.

        Args:
            target_dataset_urn: URN of the target dataset
            source_dataset_urns: List of URNs of the source datasets
            lineage_type: Type of lineage (TRANSFORMED, COPY, etc.)

        Returns:
            True if creation was successful, False otherwise
        """
        try:
            logger.debug("🔍 DEBUG: create_upstream_lineage_multiple called:")
            logger.debug(f"   Target Dataset URN: {target_dataset_urn}")
            logger.debug(f"   Source Dataset URNs: {source_dataset_urns}")
            logger.debug(f"   Lineage Type: {lineage_type}")

            from datahub.metadata.schema_classes import (
                DatasetLineageTypeClass,
                UpstreamClass,
                UpstreamLineageClass,
            )

            # Create upstream datasets
            upstream_datasets = []
            for source_dataset_urn in source_dataset_urns:
                upstream_dataset = UpstreamClass(
                    dataset=source_dataset_urn,
                    type=getattr(
                        DatasetLineageTypeClass,
                        lineage_type,
                        DatasetLineageTypeClass.TRANSFORMED,
                    ),
                )
                upstream_datasets.append(upstream_dataset)

            # Create upstream lineage with all sources
            upstream_lineage = UpstreamLineageClass(upstreams=upstream_datasets)

            # Create metadata change proposal
            event = MetadataChangeProposalWrapper(
                entityUrn=target_dataset_urn, aspect=upstream_lineage
            )

            logger.debug("🔍 DEBUG: About to emit MCP event for lineage")
            self._emit_mcp(event)
            logger.debug(
                f"✅ SUCCESS: Created upstream lineage: {source_dataset_urns} -> {target_dataset_urn}"
            )
            return True

        except Exception as e:
            logger.error(
                f"❌ FAILED: Failed to create upstream lineage {source_dataset_urns} -> {target_dataset_urn}: {e}"
            )
            import traceback

            logger.error(f"💥 TRACEBACK: {traceback.format_exc()}")
            return False

    def create_field_lineage(
        self,
        target_dataset_urn: str,
        source_dataset_urn: str,
        target_field: str,
        source_field: str,
        lineage_type: str = "TRANSFORMED",
    ) -> bool:
        """
        Create field-level lineage between datasets.

        Args:
            target_dataset_urn: URN of the target dataset
            source_dataset_urn: URN of the source dataset
            target_field: Name of the target field
            source_field: Name of the source field
            lineage_type: Type of lineage (TRANSFORMED, COPY, etc.)

        Returns:
            True if creation was successful, False otherwise
        """
        try:
            from datahub.metadata.schema_classes import (
                DatasetLineageTypeClass,
                FineGrainedLineageClass,
                UpstreamClass,
                UpstreamLineageClass,
            )

            # Create fine-grained lineage for field-level mapping
            fine_grained_lineage = FineGrainedLineageClass(
                upstreamType="FIELD_SET",
                downstreamType="FIELD_SET",
                upstreams=[f"{source_dataset_urn}#{source_field}"],
                downstreams=[f"{target_dataset_urn}#{target_field}"],
            )

            # Create upstream dataset with fine-grained lineage
            upstream_dataset = UpstreamClass(
                dataset=source_dataset_urn,
                type=getattr(
                    DatasetLineageTypeClass,
                    lineage_type,
                    DatasetLineageTypeClass.TRANSFORMED,
                ),
            )

            # Create upstream lineage with fine-grained information
            upstream_lineage = UpstreamLineageClass(
                upstreams=[upstream_dataset], fineGrainedLineages=[fine_grained_lineage]
            )

            # Create metadata change proposal
            event = MetadataChangeProposalWrapper(
                entityUrn=target_dataset_urn, aspect=upstream_lineage
            )

            self._emit_mcp(event)
            logger.info(
                f"Created field-level lineage: {source_dataset_urn}#{source_field} -> {target_dataset_urn}#{target_field}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to create field-level lineage {source_dataset_urn}#{source_field} -> {target_dataset_urn}#{target_field}: {e}"
            )
            return False

    def create_field_lineage_modern(
        self,
        upstream_dataset_urn: str,
        downstream_dataset_urn: str,
        column_lineage=None,
    ) -> bool:
        """
        Create field-level lineage using the modern DataHub SDK approach.

        Args:
            upstream_dataset_urn: URN of the upstream dataset
            downstream_dataset_urn: URN of the downstream dataset
            column_lineage: Column lineage configuration:
                - True: Fuzzy matching
                - "auto_strict": Strict matching
                - dict: Custom mapping {downstream_field: [upstream_fields]}

        Returns:
            True if creation was successful, False otherwise
        """
        try:
            from datahub.metadata.urns import DatasetUrn
            from datahub.sdk import DataHubClient

            # Create modern DataHub client with explicit configuration
            modern_client = DataHubClient(server=self.datahub_gms, token=self.api_token)

            # Parse URNs to extract platform and name
            upstream_platform, upstream_name = self._parse_dataset_urn(
                upstream_dataset_urn
            )
            downstream_platform, downstream_name = self._parse_dataset_urn(
                downstream_dataset_urn
            )

            # Create DatasetUrn objects
            upstream_urn = DatasetUrn(platform=upstream_platform, name=upstream_name)
            downstream_urn = DatasetUrn(
                platform=downstream_platform, name=downstream_name
            )

            # Create lineage with column-level mapping using official SDK approach
            # Note: The SDK returns None but actually creates the lineage
            result = modern_client.lineage.add_lineage(
                upstream=upstream_urn,
                downstream=downstream_urn,
                column_lineage=column_lineage,
            )

            # The SDK returns None even on success, so we assume success if no exception was raised
            logger.info(
                f"✅ SUCCESS: Created modern field-level lineage: {upstream_dataset_urn} -> {downstream_dataset_urn}"
            )
            logger.debug(f"   Column lineage config: {column_lineage}")
            logger.debug(f"   SDK result: {result} (None is expected)")

            return True

        except Exception as e:
            logger.error(
                f"❌ FAILED: Failed to create modern field-level lineage {upstream_dataset_urn} -> {downstream_dataset_urn}: {e}"
            )
            import traceback

            logger.error(f"💥 TRACEBACK: {traceback.format_exc()}")
            return False

    def _parse_dataset_urn(self, dataset_urn: str) -> tuple[str, str]:
        """Parse DataHub dataset URN to extract platform and name."""
        try:
            # Format: urn:li:dataset:(urn:li:dataPlatform:platform,name,environment)
            if dataset_urn.startswith("urn:li:dataset:"):
                # Extract the content inside the parentheses
                content = dataset_urn.split("(", 1)[1].rstrip(")")
                parts = content.split(",")

                # Platform is in format: urn:li:dataPlatform:platform
                platform_part = parts[0]
                platform = platform_part.split(":")[-1]

                # Name is the second part
                name = parts[1]

                return platform, name
            else:
                raise ValueError(f"Invalid dataset URN format: {dataset_urn}")

        except Exception as e:
            logger.error(f"❌ Failed to parse dataset URN {dataset_urn}: {e}")
            raise

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
