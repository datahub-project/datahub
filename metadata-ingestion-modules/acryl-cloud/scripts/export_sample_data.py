#!/usr/bin/env python3
"""
Export sample data from a live DataHub instance to MCP JSON format.
Filters out entities that don't support soft delete and sensitive data.
"""

import argparse
import json
import logging
from typing import Any, Dict, List, Set

import requests

# Entity types to EXCLUDE from export
# Includes: entities without soft delete support + sensitive entity types
EXCLUDED_ENTITIES = {
    # Entities without soft delete support (no 'status' aspect)
    "actionRequest",
    "actionWorkflow",
    "constraint",
    "dataHubAccessToken",
    "dataHubAction",
    "dataHubAiConversation",
    "dataHubConnection",
    "dataHubExecutionRequest",
    "dataHubIngestionSource",
    "dataHubMetricCube",
    "dataHubOpenAPISchema",
    "dataHubPageModule",
    "dataHubPageTemplate",
    "dataHubPersona",
    "dataHubPolicy",
    "dataHubRemoteExecutor",
    "dataHubRemoteExecutorGlobalConfig",
    "dataHubRemoteExecutorPool",
    "dataHubRetention",
    "dataHubRole",
    "dataHubSecret",
    "dataHubStepState",
    "dataHubUpgrade",
    "dataHubView",
    "dataPlatform",
    "form",
    "globalSettings",
    "incident",
    "inviteToken",
    "linkPreview",
    "monitorSuite",
    "recommendationModule",
    "role",
    "structuredProperty",  # Excluded to avoid entity type race condition
    "telemetry",
    "versionSet",
    # Sensitive entity types (even though they support soft delete)
    "application",  # May contain deployment configs, credentials, connection strings
}

# Aspect names to exclude (sensitive data or references to excluded entities)
EXCLUDED_ASPECTS = {
    # References to excluded entities
    "incidentsSummary",  # References to incidents
    "structuredProperties",  # References to structured properties (excluded to avoid race condition)
    "applications",  # References to applications (excluded as sensitive)
    # Sensitive authentication/authorization data
    "corpUserCredentials",  # Password hashes, salts, reset tokens
    "dataHubAccessTokenInfo",  # Access token details
    "accessTokenMetadata",  # Token information
    "inviteToken",  # Invitation tokens
    # Sensitive connection/configuration data
    "dataHubConnectionDetails",  # Connection strings, credentials
    "dataHubSecretValue",  # Secret values
    "dataHubIngestionSourceInfo",  # May contain credentials
    "dataHubExecutionRequestInput",  # May contain sensitive execution params
    "dataHubExecutionRequestResult",  # May contain sensitive execution results
    # Internal/system data that shouldn't be in sample data
    "telemetryClientId",  # Internal telemetry IDs
    "corpUserSettings",  # User-specific settings/preferences
    "corpGroupSettings",  # Group-specific settings
}

# Glossary terms/nodes to exclude from sample data exports
# - Compliance-related terms from fieldeng that shouldn't be in public samples
EXCLUDED_GLOSSARY_TERMS = {
    "CMMC Ready",
    "DOD 5015.2 Records Management",
    "OMB Data Policy Mandate",
    "Risk Management Framework (RMF)",
}

EXCLUDED_GLOSSARY_NODES = {
    "Compliance Audit Evidence",
}



class DataHubExporter:
    def __init__(self, server: str, token: str):
        self.server = server.rstrip("/")
        self.token = token
        self.graphql_url = f"{self.server}/api/graphql"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    def execute_graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a GraphQL query."""
        response = self.session.post(
            self.graphql_url,
            json={"query": query, "variables": variables},
            timeout=60,
        )
        response.raise_for_status()
        result = response.json()
        if "errors" in result:
            raise Exception(f"GraphQL errors: {result['errors']}")
        return result["data"]

    def get_lineage(
        self,
        seed_urn: str,
        direction: str,  # "UPSTREAM" or "DOWNSTREAM"
        max_hops: int = 10,
    ) -> Set[str]:
        """
        Traverse lineage graph from seed entity.
        Returns set of entity URNs in the lineage.
        """
        query = """
        query scrollAcrossLineage($input: ScrollAcrossLineageInput!) {
            scrollAcrossLineage(input: $input) {
                searchResults {
                    degree
                    entity {
                        urn
                        type
                    }
                }
                nextScrollId
            }
        }
        """

        urns = set()
        scroll_id = None
        first_iter = True

        # Determine hop values - use "3+" for max_hops > 2
        # as per DataHub SDK lineage_client.py implementation
        max_hop_values = (
            [str(hop) for hop in range(1, max_hops + 1)]
            if max_hops <= 2
            else ["1", "2", "3+"]
        )

        variables = {
            "input": {
                "query": "*",
                "urn": seed_urn,
                "count": 1000,
                "direction": direction,
                "scrollId": None,
                "orFilters": [
                    {
                        "and": [
                            {
                                "condition": "EQUAL",
                                "field": "degree",
                                "values": max_hop_values,
                            }
                        ]
                    }
                ],
            }
        }

        while first_iter or scroll_id:
            first_iter = False
            variables["input"]["scrollId"] = scroll_id

            data = self.execute_graphql(query, variables)
            scroll_data = data["scrollAcrossLineage"]
            scroll_id = scroll_data.get("nextScrollId")

            for result in scroll_data["searchResults"]:
                entity = result["entity"]
                urns.add(entity["urn"])

            logging.info(f"Fetched {len(urns)} {direction} lineage URNs...")

        return urns

    def traverse_full_lineage(
        self,
        seed_urn: str,
        max_hops: int = 10,
    ) -> Set[str]:
        """
        Get full lineage graph (both upstream and downstream).
        Returns all entity URNs in the connected graph.
        """
        logging.info(f"Starting lineage traversal from {seed_urn}")

        # Include seed entity
        all_urns = {seed_urn}

        # Get upstream lineage
        logging.info(f"Fetching upstream lineage (max {max_hops} hops)...")
        upstream_urns = self.get_lineage(seed_urn, "UPSTREAM", max_hops)
        all_urns.update(upstream_urns)

        # Get downstream lineage
        logging.info(f"Fetching downstream lineage (max {max_hops} hops)...")
        downstream_urns = self.get_lineage(seed_urn, "DOWNSTREAM", max_hops)
        all_urns.update(downstream_urns)

        logging.info(f"Total lineage graph: {len(all_urns)} entities")
        return all_urns

    def _extract_urns_from_aspect(
        self, aspects: Dict[str, Any], aspect_name: str, urn_path: str
    ) -> Set[str]:
        """
        Extract URNs from an aspect.
        urn_path can be:
        - Simple key: "container" extracts aspect["container"]
        - List key: "domains[]" extracts aspect["domains"] as list of strings
        - List key: "domains[].urn" extracts [item["urn"] for item in aspect["domains"]]
        - List key: "owners[].owner" extracts [item["owner"] for item in aspect["owners"]]
        """
        urns = set()
        if aspect_name not in aspects:
            return urns

        aspect = aspects[aspect_name]
        aspect_data = aspect.get("value") or aspect.get("json", {})

        if "[]" in urn_path:
            # List extraction
            if urn_path.endswith("[]"):
                # Extract list of strings directly (e.g., "domains[]")
                list_key = urn_path[:-2]  # Remove "[]"
                items = aspect_data.get(list_key, [])
                for item in items:
                    if isinstance(item, str):
                        urns.add(item)
            else:
                # Extract from list of objects (e.g., "owners[].owner")
                list_key, item_key = urn_path.split("[].")
                items = aspect_data.get(list_key, [])
                for item in items:
                    if isinstance(item, dict):
                        urn = item.get(item_key)
                        if urn:
                            urns.add(urn)
                    elif isinstance(item, str):
                        # Handle case where item is already a string URN
                        urns.add(item)
        else:
            # Simple extraction
            urn = aspect_data.get(urn_path)
            if urn:
                urns.add(urn)

        return urns

    def get_related_entities(self, entity_urns: Set[str]) -> Set[str]:
        """
        Get related entities (domains, containers, owners, tags, glossary terms, data products).
        Only includes entities that support soft delete.
        Also recursively fetches parent containers to complete the container hierarchy.
        """
        related_urns = set()

        # Group by type for batch fetching
        urns_by_type: Dict[str, List[str]] = {}
        for urn in entity_urns:
            try:
                entity_type = self.get_entity_type_from_urn(urn)
                urns_by_type.setdefault(entity_type, []).append(urn)
            except Exception as e:
                logging.warning(f"Error extracting entity type from {urn}: {e}")
                continue

        # Define what to extract from each aspect
        extraction_config = [
            ("domains", "domains[]"),  # domains is a list of string URNs
            ("container", "container"),
            ("ownership", "owners[].owner"),
            ("dataProduct", "dataProduct"),
            ("globalTags", "tags[].tag"),
            ("glossaryTerms", "terms[].urn"),
            ("structuredProperties", "properties[].propertyUrn"),
        ]

        # Batch fetch and extract related entities
        for entity_type, type_urns in urns_by_type.items():
            try:
                entities = self.batch_get_entities(entity_type, type_urns)

                for _urn, entity_data in entities.items():
                    aspects = entity_data.get("aspects", {})

                    for aspect_name, urn_path in extraction_config:
                        extracted_urns = self._extract_urns_from_aspect(
                            aspects, aspect_name, urn_path
                        )
                        related_urns.update(extracted_urns)

            except Exception as e:
                logging.warning(
                    f"Error extracting related entities from {entity_type}: {e}"
                )
                continue

        logging.info(f"Found {len(related_urns)} related entities")

        # Recursively fetch parent containers to complete hierarchy
        container_urns = {urn for urn in related_urns if "urn:li:container:" in urn}
        if container_urns:
            parent_containers = self._fetch_parent_containers(container_urns)
            if parent_containers:
                logging.info(
                    f"Found {len(parent_containers)} parent containers in hierarchy"
                )
                related_urns.update(parent_containers)

        # Recursively fetch parent glossary nodes to complete hierarchy
        glossary_term_urns = {
            urn for urn in related_urns if "urn:li:glossaryTerm:" in urn
        }
        if glossary_term_urns:
            parent_nodes = self._fetch_parent_glossary_nodes(glossary_term_urns)
            if parent_nodes:
                logging.info(
                    f"Found {len(parent_nodes)} parent glossary nodes in hierarchy"
                )
                related_urns.update(parent_nodes)

        return related_urns

    def extract_schema_field_urns(self, mcps: List[Dict[str, Any]]) -> Set[str]:
        """
        Extract all schemaField URNs from fine-grained lineage in upstreamLineage aspects.
        These entities need to be explicitly fetched for column-level lineage to work in UI.
        """
        schema_field_urns = set()

        for mcp in mcps:
            aspect_name = mcp.get("aspectName")
            if aspect_name != "upstreamLineage":
                continue

            aspect_json = mcp.get("aspect", {}).get("json", {})
            fine_grained_lineages = aspect_json.get("fineGrainedLineages", [])

            for fg in fine_grained_lineages:
                # Extract from upstreams
                upstreams = fg.get("upstreams", [])
                for urn in upstreams:
                    if urn.startswith("urn:li:schemaField:("):
                        schema_field_urns.add(urn)

                # Extract from downstreams
                downstreams = fg.get("downstreams", [])
                for urn in downstreams:
                    if urn.startswith("urn:li:schemaField:("):
                        schema_field_urns.add(urn)

        return schema_field_urns

    def _fetch_parent_containers(self, container_urns: Set[str]) -> Set[str]:
        """
        Recursively fetch parent containers to complete the container hierarchy.
        Returns set of parent container URNs.
        """
        parent_urns = set()
        to_fetch = container_urns.copy()
        fetched = set()

        # Limit recursion depth to prevent infinite loops
        max_depth = 10
        depth = 0

        while to_fetch and depth < max_depth:
            depth += 1
            current_batch = to_fetch - fetched

            if not current_batch:
                break

            try:
                # Fetch containers
                containers = self.batch_get_entities("container", list(current_batch))
                fetched.update(current_batch)

                # Extract parent container references
                for _urn, entity_data in containers.items():
                    aspects = entity_data.get("aspects", {})
                    if "container" in aspects:
                        container_aspect = aspects["container"]
                        container_data = container_aspect.get(
                            "value"
                        ) or container_aspect.get("json", {})
                        parent_urn = container_data.get("container")
                        if parent_urn and parent_urn not in fetched:
                            parent_urns.add(parent_urn)
                            to_fetch.add(parent_urn)

            except Exception as e:
                logging.warning(f"Error fetching parent containers: {e}")
                break

        return parent_urns

    def _fetch_parent_glossary_nodes(self, glossary_term_urns: Set[str]) -> Set[str]:
        """
        Recursively fetch parent glossary nodes to complete the glossary hierarchy.
        Returns set of parent glossary node URNs.
        """
        parent_urns = set()
        to_fetch = glossary_term_urns.copy()
        fetched_terms = set()
        fetched_nodes = set()

        # Limit recursion depth to prevent infinite loops
        max_depth = 10
        depth = 0

        while to_fetch and depth < max_depth:
            depth += 1

            # Separate terms and nodes to fetch
            terms_to_fetch = {urn for urn in to_fetch if "glossaryTerm" in urn}
            nodes_to_fetch = {urn for urn in to_fetch if "glossaryNode" in urn}

            to_fetch = set()

            # Fetch glossary terms to get their parent nodes
            if terms_to_fetch:
                terms_to_fetch = terms_to_fetch - fetched_terms
                if terms_to_fetch:
                    try:
                        terms = self.batch_get_entities(
                            "glossaryTerm", list(terms_to_fetch)
                        )
                        fetched_terms.update(terms_to_fetch)

                        for _urn, entity_data in terms.items():
                            aspects = entity_data.get("aspects", {})
                            if "glossaryTermInfo" in aspects:
                                term_info_aspect = aspects["glossaryTermInfo"]
                                term_info_data = term_info_aspect.get(
                                    "value"
                                ) or term_info_aspect.get("json", {})

                                # Skip excluded glossary terms
                                term_name = term_info_data.get("name", "")
                                if term_name in EXCLUDED_GLOSSARY_TERMS:
                                    logging.info(
                                        f"Skipping excluded glossary term: {term_name}"
                                    )
                                    continue

                                parent_node = term_info_data.get("parentNode")
                                if parent_node and parent_node not in fetched_nodes:
                                    parent_urns.add(parent_node)
                                    to_fetch.add(parent_node)
                    except Exception as e:
                        logging.warning(f"Error fetching glossary terms: {e}")

            # Fetch glossary nodes to get their parent nodes
            if nodes_to_fetch:
                nodes_to_fetch = nodes_to_fetch - fetched_nodes
                if nodes_to_fetch:
                    try:
                        nodes = self.batch_get_entities(
                            "glossaryNode", list(nodes_to_fetch)
                        )
                        fetched_nodes.update(nodes_to_fetch)

                        for _urn, entity_data in nodes.items():
                            aspects = entity_data.get("aspects", {})
                            if "glossaryNodeInfo" in aspects:
                                node_info_aspect = aspects["glossaryNodeInfo"]
                                node_info_data = node_info_aspect.get(
                                    "value"
                                ) or node_info_aspect.get("json", {})

                                # Skip excluded glossary nodes
                                node_name = node_info_data.get("name", "")
                                if node_name in EXCLUDED_GLOSSARY_NODES:
                                    logging.info(
                                        f"Skipping excluded glossary node: {node_name}"
                                    )
                                    continue

                                parent_node = node_info_data.get("parentNode")
                                if parent_node and parent_node not in fetched_nodes:
                                    parent_urns.add(parent_node)
                                    to_fetch.add(parent_node)
                    except Exception as e:
                        logging.warning(f"Error fetching glossary nodes: {e}")

        return parent_urns

    def get_entity_type_from_urn(self, urn: str) -> str:
        """Extract entity type from URN."""
        # URN format: urn:li:<entity_type>:...
        parts = urn.split(":")
        if len(parts) >= 3:
            return parts[2]
        raise ValueError(f"Invalid URN format: {urn}")

    def batch_get_entities(
        self,
        entity_name: str,
        urns: List[str],
        batch_size: int = 100,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Batch fetch entities using OpenAPI v2 batch endpoint.
        Returns dict mapping URN to entity data with aspects.
        """
        all_entities = {}

        # Process in batches
        for i in range(0, len(urns), batch_size):
            batch_urns = urns[i : i + batch_size]

            payload = {
                "urns": batch_urns,
                "aspectNames": [],  # Empty = fetch all aspects
                "withSystemMetadata": False,
            }

            url = f"{self.server}/openapi/v2/entity/batch/{entity_name}"
            response = self.session.post(
                url,
                json=payload,
                timeout=60,
            )
            response.raise_for_status()

            json_resp = response.json()
            entities = json_resp.get("entities", [])

            for entity in entities:
                entity_urn = entity.get("urn")
                if entity_urn:
                    all_entities[entity_urn] = entity

            logging.info(
                f"Fetched batch {i // batch_size + 1}: {len(batch_urns)} {entity_name} entities"
            )

        return all_entities

    def sort_mcps_by_dependency(
        self, mcps: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Sort MCPs to ensure dependencies are ingested before entities that reference them.

        Ingestion order:
        1. Structured properties (referenced by structuredProperties aspect)
        2. Tags (referenced by globalTags aspect)
        3. Glossary terms (referenced by glossaryTerms aspect)
        4. Domains (referenced by domains aspect)
        5. Data products (referenced by dataProduct aspect)
        6. Everything else
        """
        # Define priority order (lower number = higher priority)
        priority_map = {
            "structuredProperty": 0,
            "tag": 1,
            "glossaryTerm": 2,
            "glossaryNode": 2,  # Parent of glossary terms
            "domain": 3,
            "dataProduct": 4,
            "corpuser": 5,  # Owners reference users
            "corpGroup": 5,  # Owners reference groups
        }

        def get_priority(mcp: Dict[str, Any]) -> int:
            entity_type = mcp.get("entityType", "")
            return priority_map.get(entity_type, 100)  # Default low priority

        return sorted(mcps, key=get_priority)

    def anonymize_sensitive_data(
        self, mcps: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Anonymize sensitive data in MCPs: emails, URLs, and personal names.

        Converts real data to generic sample data while maintaining consistency.
        """
        import re
        from collections import defaultdict

        # Convert MCPs to JSON string for efficient replacement
        content = json.dumps(mcps)

        # 1. Anonymize email addresses
        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        emails = set(re.findall(email_pattern, content))

        email_counter = defaultdict(int)
        email_map = {}

        for email in sorted(emails):
            if "@example.com" in email:
                continue

            local_part = email.split("@")[0]
            name = local_part.split(".")[0].lower()

            if email_counter[name] == 0:
                sample_email = f"{name}@example.com"
            else:
                sample_email = f"{name}{email_counter[name]}@example.com"

            email_counter[name] += 1
            email_map[email] = sample_email

        if email_map:
            logging.info(f"Anonymizing {len(email_map)} email addresses")
            for original, anonymized in email_map.items():
                content = content.replace(original, anonymized)

        # 2. Anonymize URLs - replace account/tenant IDs and domains
        url_replacements = {
            # Snowflake accounts
            r"xaa48144": "demo123",
            r"us-west-2/xaa48144": "us-west-2/demo123",
            # dbt Cloud accounts
            r"cloud\.getdbt\.com/explore/107298": "cloud.getdbt.com/explore/123456",
            r"projects/424662": "projects/789012",
            # Tableau sites
            r"acryldatatableaupartnershipportal": "demo-tableau-site",
            # PowerBI workspace IDs
            r"f77fe42d-ea5f-477a-8cf2-acb4031d9849": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            r"09466991-0804-4269-adcb-758e92818af6": "11111111-2222-3333-4444-555555555555",
            r"ca7386ef-a1ec-4d43-9df3-7a8864519652": "66666666-7777-8888-9999-000000000000",
            # AWS S3 buckets
            r"acryl-fieldeng": "demo-data-bucket",
            # GitHub repos
            r"github\.com/acryldata/order-entry-looker": "github.com/demo-org/demo-looker-project",
            # Looker instances
            r"acryl\.cloud\.looker\.com": "demo.cloud.looker.com",
            # Remove Google profile pictures
            r"https://lh3\.googleusercontent\.com/[^\s\"']+": "",
        }

        logging.info("Anonymizing URLs and account identifiers")
        for pattern, replacement in url_replacements.items():
            content = re.sub(pattern, replacement, content)

        # 3. Anonymize personal names in corpUserInfo
        mcps_parsed = json.loads(content)

        # Common generic names for sample data
        generic_names = [
            "Alice Johnson",
            "Bob Smith",
            "Charlie Brown",
            "Diana Prince",
            "Ethan Hunt",
            "Fiona Green",
            "George Wilson",
            "Hannah Lee",
            "Ian Chen",
            "Julia Martinez",
            "Kevin Anderson",
            "Laura Thompson",
            "Mike Davis",
            "Nina Patel",
            "Oscar Rodriguez",
            "Paula White",
            "Quinn Taylor",
            "Rachel Moore",
            "Steve Jackson",
            "Tina Miller",
            "Uma Sharma",
            "Victor Kim",
        ]

        name_counter = 0
        name_map = {}

        for mcp in mcps_parsed:
            if mcp["aspectName"] == "corpUserInfo":
                info = mcp["aspect"]["json"]
                display_name = info.get("displayName", "")
                full_name = info.get("fullName", "")

                # Only anonymize if it looks like a real name
                if display_name and display_name not in name_map:
                    if name_counter < len(generic_names):
                        name_map[display_name] = generic_names[name_counter]
                        name_counter += 1
                    else:
                        # Fallback to numbered names if we run out
                        name_map[display_name] = (
                            f"User {name_counter - len(generic_names) + 1}"
                        )
                        name_counter += 1

                # Replace names
                if display_name in name_map:
                    info["displayName"] = name_map[display_name]
                if full_name in name_map:
                    info["fullName"] = name_map[full_name]

        logging.info(f"Anonymized {len(name_map)} personal names")

        # 4. Anonymize names in descriptions (domains, etc.)
        for mcp in mcps_parsed:
            if mcp["aspectName"] in ["domainProperties", "containerProperties"]:
                props = mcp["aspect"]["json"]
                description = props.get("description", "")
                if description:
                    # Replace any names found in descriptions
                    for real_name, generic_name in name_map.items():
                        description = description.replace(real_name, generic_name)
                    props["description"] = description

        return mcps_parsed

    def add_sample_prefix_to_urns(
        self, mcps: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Add 'sample_data_' prefix to all entity names in URNs to prevent collisions.

        Examples:
        - urn:li:dataset:(urn:li:dataPlatform:snowflake,order_entry_db.analytics.order_details,PROD)
          → urn:li:dataset:(urn:li:dataPlatform:snowflake,sample_data_order_entry_db.analytics.sample_data_order_details,PROD)

        - urn:li:dashboard:(looker,order_entry::order_dashboard)
          → urn:li:dashboard:(looker,order_entry::sample_data_order_dashboard)

        - urn:li:corpuser:john.doe
          → urn:li:corpuser:sample_data_john.doe

        - urn:li:tag:PII
          → urn:li:tag:sample_data_PII

        This applies to:
        1. Entity URNs (entityUrn field)
        2. References within aspects (ownership.owners, container.container, etc.)
        3. Lineage relationships (upstreamLineage, downstreamLineage)
        """
        import re

        # Build URN mapping (old → new)
        urn_map = {}

        # First pass: Create mapping for all entity URNs
        for mcp in mcps:
            old_urn = mcp.get("entityUrn", "")
            if old_urn:
                new_urn = self._add_prefix_to_urn(old_urn)
                if new_urn != old_urn:
                    urn_map[old_urn] = new_urn

        logging.info(f"Generated {len(urn_map)} URN transformations")

        # Second pass: Apply transformations to all URNs (entity URNs + references)
        for mcp in mcps:
            # Transform entity URN
            if "entityUrn" in mcp:
                mcp["entityUrn"] = urn_map.get(mcp["entityUrn"], mcp["entityUrn"])

            # Transform references within aspects
            aspect_json = mcp.get("aspect", {}).get("json", {})
            if aspect_json:
                self._transform_aspect_urns(aspect_json, urn_map)
                # Also transform display names in certain aspects
                self._transform_display_names(aspect_json, mcp.get("aspectName"))
                # Special handling for browsePathsV2 - transform URNs even if not in urn_map
                if mcp.get("aspectName") == "browsePathsV2":
                    self._transform_browse_paths_v2(aspect_json, urn_map)
                # Transform URNs embedded in text fields (documentation, descriptions, etc.)
                self._transform_embedded_urns_in_text(aspect_json, urn_map)

        return mcps

    def _add_prefix_to_urn(self, urn: str) -> str:
        """Add sample_data_ prefix to entity name within URN."""
        if not urn or not urn.startswith("urn:li:"):
            return urn

        # Skip if already prefixed to avoid double-prefixing
        if "sample_data_" in urn:
            return urn

        try:
            # Parse URN: urn:li:<entityType>:<key>
            parts = urn.split(":", 3)  # Split into ['urn', 'li', 'entityType', 'key']
            if len(parts) < 4:
                return urn

            entity_type = parts[2]
            key = parts[3]

            # Different transformation strategies based on entity type
            if entity_type == "schemaField":
                # SchemaField: (urn:li:dataset:(...),field) → transform embedded dataset URN
                # Extract and transform the dataset URN within the schemaField URN
                new_key = self._prefix_schema_field_key(key)
            elif entity_type == "dataset":
                # Dataset: (platform,name,env) → (platform,sample_data_name,env)
                new_key = self._prefix_dataset_key(key)
            elif entity_type in ["dashboard", "chart"]:
                # Dashboard/Chart: (platform,name) → (platform,sample_data_name)
                new_key = self._prefix_platform_entity_key(key)
            elif entity_type == "dataFlow":
                # DataFlow: (platform,flowName,env) → (platform,sample_data_flowName,sample_data_env)
                new_key = self._prefix_dataflow_key(key)
            elif entity_type == "dataJob":
                # DataJob: Complex nested structure with embedded dataFlow URN
                new_key = self._prefix_data_job_key(key)
            elif entity_type in ["corpuser", "corpGroup"]:
                # Users/Groups: username → sample_data_username
                new_key = f"sample_data_{key}"
            elif entity_type in ["tag", "glossaryTerm", "glossaryNode", "domain"]:
                # Tags/Terms: name → sample_data_name
                new_key = f"sample_data_{key}"
            elif entity_type == "container":
                # Container: guid → keep as-is (GUIDs are unique)
                return urn
            else:
                # Default: prefix the key
                new_key = f"sample_data_{key}"

            return f"urn:li:{entity_type}:{new_key}"

        except Exception as e:
            logging.warning(f"Could not transform URN {urn}: {e}")
            return urn

    def _prefix_dataset_key(self, key: str) -> str:
        """Transform dataset key: (platform,db.schema.table,env) → (platform,sample_data_db.schema.sample_data_table,env)"""
        if not key.startswith("(") or not key.endswith(")"):
            return f"sample_data_{key}"

        # Remove parentheses and split
        inner = key[1:-1]
        parts = inner.split(",")

        if len(parts) == 3:
            platform, name, env = parts
            # Add prefix to database and table parts
            # order_entry_db.analytics.order_details → sample_data_order_entry_db.analytics.sample_data_order_details
            name_parts = name.split(".")
            if len(name_parts) >= 2:
                # Prefix first (database) and last (table) parts
                name_parts[0] = f"sample_data_{name_parts[0]}"
                name_parts[-1] = f"sample_data_{name_parts[-1]}"
                new_name = ".".join(name_parts)
            else:
                new_name = f"sample_data_{name}"

            return f"({platform},{new_name},{env})"

        return f"sample_data_{key}"

    def _prefix_platform_entity_key(self, key: str) -> str:
        """Transform platform entity key: (platform,name) → (platform,sample_data_name)"""
        if not key.startswith("(") or not key.endswith(")"):
            return f"sample_data_{key}"

        inner = key[1:-1]
        parts = inner.split(",", 1)  # Split only on first comma

        if len(parts) == 2:
            platform, name = parts
            # Handle :: separators in Looker/Mode dashboards
            if "::" in name:
                name_parts = name.split("::")
                name_parts[-1] = f"sample_data_{name_parts[-1]}"
                new_name = "::".join(name_parts)
            else:
                new_name = f"sample_data_{name}"

            return f"({platform},{new_name})"

        return f"sample_data_{key}"

    def _prefix_dataflow_key(self, key: str) -> str:
        """
        Transform dataFlow key: (platform,flowName,env) → (platform,sample_data_flowName,sample_data_env).

        Example: (spark,export_table_orders_to_s3,default) → (spark,sample_data_export_table_orders_to_s3,sample_data_default)
        """
        if not key.startswith("(") or not key.endswith(")"):
            return f"sample_data_{key}"

        # Remove parentheses and split
        inner = key[1:-1]
        parts = inner.split(",")

        if len(parts) == 3:
            platform, flow_name, env = parts
            # Add prefix to flow name and environment
            new_flow_name = f"sample_data_{flow_name}"
            new_env = f"sample_data_{env}"
            return f"({platform},{new_flow_name},{new_env})"

        return f"sample_data_{key}"

    def _prefix_data_job_key(self, key: str) -> str:
        """Transform dataJob key which contains embedded dataFlow URN."""
        # DataJob format: (urn:li:dataFlow:(platform,name,env),jobName)
        # Need to recursively transform the embedded dataFlow URN
        if "urn:li:dataFlow:" in key:
            # Regex to extract and replace dataFlow URN
            import re

            def replace_flow(match):
                flow_urn = match.group(0)
                return self._add_prefix_to_urn(flow_urn)

            key = re.sub(r"urn:li:dataFlow:\([^)]+\)", replace_flow, key)

        # Also prefix the job name at the end
        if key.endswith(")") and "," in key:
            parts = key.rsplit(",", 1)
            if len(parts) == 2:
                base, job_name = parts
                job_name = job_name.rstrip(")")
                key = f"{base},sample_data_{job_name})"

        return key

    def _prefix_schema_field_key(self, key: str) -> str:
        """
        Transform schemaField key by transforming the embedded dataset URN.
        Format: (urn:li:dataset:(...),fieldPath)
        Example: (urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),field_name)
        """
        if not key.startswith("(") or not key.endswith(")"):
            return key

        try:
            # Remove outer parentheses
            content = key[1:-1]

            # Find the dataset URN (it ends with ),fieldPath)
            dataset_urn_end = content.rfind("),")
            if dataset_urn_end == -1:
                return key

            dataset_urn = content[:dataset_urn_end + 1]
            field_path = content[dataset_urn_end + 2:]

            # Transform the dataset URN
            transformed_dataset_urn = self._add_prefix_to_urn(dataset_urn)

            # Reconstruct the schemaField key
            return f"({transformed_dataset_urn},{field_path})"
        except Exception as e:
            logging.warning(f"Could not transform schemaField key {key}: {e}")
            return key

    def _transform_aspect_urns(self, obj: Any, urn_map: Dict[str, str]) -> None:
        """Recursively transform URN references within aspect JSON."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str) and value.startswith("urn:li:"):
                    # Handle schemaField URNs specially - they contain embedded dataset URNs
                    if value.startswith("urn:li:schemaField:("):
                        obj[key] = self._transform_schema_field_urn(value, urn_map)
                    else:
                        # Regular entity URN - try urn_map first, fall back to _add_prefix_to_urn
                        # This ensures URNs referencing non-exported entities also get prefixed
                        obj[key] = urn_map.get(value, self._add_prefix_to_urn(value))
                elif isinstance(value, (dict, list)):
                    # Recurse into nested structures
                    self._transform_aspect_urns(value, urn_map)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, str) and item.startswith("urn:li:"):
                    # Handle schemaField URNs specially
                    if item.startswith("urn:li:schemaField:("):
                        obj[i] = self._transform_schema_field_urn(item, urn_map)
                    else:
                        # Regular entity URN - try urn_map first, fall back to _add_prefix_to_urn
                        obj[i] = urn_map.get(item, self._add_prefix_to_urn(item))
                elif isinstance(item, (dict, list)):
                    self._transform_aspect_urns(item, urn_map)

    def _transform_schema_field_urn(self, schema_field_urn: str, urn_map: Dict[str, str]) -> str:
        """
        Transform a schemaField URN by transforming its embedded dataset URN.

        Format: urn:li:schemaField:(urn:li:dataset:(...),fieldPath)
        """
        try:
            # Extract the dataset URN and field path
            # Format: urn:li:schemaField:(dataset_urn,field_path)
            if not schema_field_urn.startswith("urn:li:schemaField:("):
                return schema_field_urn

            # Remove prefix and trailing )
            content = schema_field_urn[len("urn:li:schemaField:("):-1]

            # Find the dataset URN (it ends with ,PROD) or similar)
            # Look for the last occurrence of a closing parenthesis before the comma
            dataset_urn_end = content.rfind("),")
            if dataset_urn_end == -1:
                return schema_field_urn

            dataset_urn = content[:dataset_urn_end + 1]
            field_path = content[dataset_urn_end + 2:]

            # Transform the dataset URN if it exists in the map
            transformed_dataset_urn = urn_map.get(dataset_urn, dataset_urn)

            # Reconstruct the schemaField URN
            return f"urn:li:schemaField:({transformed_dataset_urn},{field_path})"
        except Exception as e:
            logging.warning(f"Could not transform schemaField URN {schema_field_urn}: {e}")
            return schema_field_urn

    def _transform_display_names(self, aspect_json: Dict[str, Any], aspect_name: str) -> None:
        """Transform display names in aspects to add sample_data_ prefix."""
        if not aspect_json or not aspect_name:
            return

        # datasetProperties: prefix 'name' and 'qualifiedName'
        if aspect_name == 'datasetProperties':
            if 'name' in aspect_json:
                name = aspect_json['name']
                # For qualified names like ORDER_DETAILS or db.schema.table
                if '.' in name:
                    # Format: db.schema.table → sample_data_db.schema.sample_data_table
                    parts = name.split('.')
                    aspect_json['name'] = f"sample_data_{parts[0]}.{'.'.join(parts[1:-1]) + '.' if len(parts) > 2 else ''}{parts[-1] if len(parts) > 1 else parts[0]}"
                else:
                    # Simple name: TABLE → sample_data_TABLE
                    aspect_json['name'] = f"sample_data_{name}"

            if 'qualifiedName' in aspect_json:
                qualified = aspect_json['qualifiedName']
                # Format: DB.SCHEMA.TABLE → sample_data_DB.SCHEMA.sample_data_TABLE
                if '.' in qualified:
                    parts = qualified.split('.')
                    # Prefix database and table name
                    prefixed_parts = [f"sample_data_{parts[0]}"] + parts[1:-1] + [f"sample_data_{parts[-1]}"]
                    aspect_json['qualifiedName'] = '.'.join(prefixed_parts)
                else:
                    aspect_json['qualifiedName'] = f"sample_data_{qualified}"

        # dataFlowInfo, dataJobInfo: prefix 'name'
        elif aspect_name in ['dataFlowInfo', 'dataJobInfo']:
            if 'name' in aspect_json:
                name = aspect_json['name']
                # Only prefix if not already prefixed
                if not name.startswith('sample_data_'):
                    aspect_json['name'] = f"sample_data_{name}"

        # dashboardInfo, chartInfo: prefix 'title' and 'customProperties'
        elif aspect_name in ['dashboardInfo', 'chartInfo']:
            if 'title' in aspect_json and aspect_json['title']:
                title = aspect_json['title']
                if not title.startswith('sample_data_'):
                    aspect_json['title'] = f"sample_data_{title}"

        # glossaryTermInfo, glossaryNodeInfo: prefix 'name'
        elif aspect_name in ['glossaryTermInfo', 'glossaryNodeInfo']:
            if 'name' in aspect_json:
                name = aspect_json['name']
                if not name.startswith('sample_data_'):
                    aspect_json['name'] = f"sample_data_{name}"

        # tagProperties: prefix 'name'
        elif aspect_name == 'tagProperties':
            if 'name' in aspect_json:
                name = aspect_json['name']
                if not name.startswith('sample_data_'):
                    aspect_json['name'] = f"sample_data_{name}"

        # containerProperties: prefix 'name' and 'qualifiedName' to prevent conflicts with user data
        elif aspect_name == 'containerProperties':
            if 'name' in aspect_json:
                name = aspect_json['name']
                if not name.startswith('sample_data_'):
                    aspect_json['name'] = f"sample_data_{name}"

            if 'qualifiedName' in aspect_json:
                qualified = aspect_json['qualifiedName']
                if qualified and not qualified.startswith('sample_data_'):
                    aspect_json['qualifiedName'] = f"sample_data_{qualified}"

    def _transform_browse_paths_v2(self, aspect_json: Dict[str, Any], urn_map: Dict[str, str]) -> None:
        """
        Special handling for browsePathsV2 aspects.

        The browsePathsV2 aspect contains URN references that may not be in urn_map
        because the source data (fieldeng) has unprefixed URNs in these aspects.
        We need to transform them using _add_prefix_to_urn to ensure consistency.
        """
        if not aspect_json or 'path' not in aspect_json:
            return

        path_entries = aspect_json.get('path', [])
        for entry in path_entries:
            if isinstance(entry, dict):
                # Transform both 'id' and 'urn' fields if they exist
                if 'id' in entry and isinstance(entry['id'], str) and entry['id'].startswith('urn:li:'):
                    # Try urn_map first, fall back to _add_prefix_to_urn
                    original_urn = entry['id']
                    entry['id'] = urn_map.get(original_urn, self._add_prefix_to_urn(original_urn))

                if 'urn' in entry and isinstance(entry['urn'], str) and entry['urn'].startswith('urn:li:'):
                    # Try urn_map first, fall back to _add_prefix_to_urn
                    original_urn = entry['urn']
                    entry['urn'] = urn_map.get(original_urn, self._add_prefix_to_urn(original_urn))

    def _transform_embedded_urns_in_text(self, obj: Any, urn_map: Dict[str, str]) -> None:
        """
        Find and transform URN references embedded within text strings.

        This handles cases like documentation/description fields that contain
        markdown text with URN references like `urn:li:corpuser:EMP006`.

        These URNs need to be transformed to use the sample_data_ prefix.
        """
        import re

        # Pattern to match URNs in text (including those wrapped in backticks or parentheses)
        urn_pattern = re.compile(r'(urn:li:[a-zA-Z]+:[^\s`"\'\)\]<>,]+)')

        def transform_text_field(text: str) -> str:
            """Transform all URN references in a text string."""
            def replace_urn(match):
                urn = match.group(1)
                # Try urn_map first, fall back to _add_prefix_to_urn
                return urn_map.get(urn, self._add_prefix_to_urn(urn))
            return urn_pattern.sub(replace_urn, text)

        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str):
                    # Check if this is a text field that might contain embedded URNs
                    # Skip if it's a standalone URN field (those are handled by _transform_aspect_urns)
                    if value.startswith("urn:li:") and not '\n' in value and len(value) < 200:
                        # This is likely a standalone URN, skip
                        continue
                    # Transform URN references in text fields
                    if 'urn:li:' in value:
                        obj[key] = transform_text_field(value)
                elif isinstance(value, (dict, list)):
                    self._transform_embedded_urns_in_text(value, urn_map)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, str):
                    if item.startswith("urn:li:") and not '\n' in item and len(item) < 200:
                        continue
                    if 'urn:li:' in item:
                        obj[i] = transform_text_field(item)
                elif isinstance(item, (dict, list)):
                    self._transform_embedded_urns_in_text(item, urn_map)

    def _clean_ownership(
        self, aspect_json: Dict[str, Any], existing_entities: set, removed_count: Dict, valid_users: set = None
    ) -> tuple[bool, bool]:
        """Clean ownership aspect. Returns (modified, should_skip)."""
        owners = aspect_json.get("owners", [])
        if not owners:
            return False, False

        original_count = len(owners)

        # Remove owners that don't exist OR are stub users (users without corpUserInfo)
        if valid_users is not None:
            cleaned_owners = [
                owner for owner in owners
                if owner.get("owner", "") in existing_entities and
                   (not owner.get("owner", "").startswith("urn:li:corpuser:") or owner.get("owner", "") in valid_users)
            ]
        else:
            cleaned_owners = [
                owner for owner in owners if owner.get("owner", "") in existing_entities
            ]

        removed_count["ownership_references"] += original_count - len(cleaned_owners)

        if len(cleaned_owners) != original_count:
            aspect_json["owners"] = cleaned_owners
            if not cleaned_owners:
                removed_count["empty_ownership_aspects"] += 1
                return True, True
            return True, False
        return False, False

    def _clean_group_membership(
        self,
        aspect_name: str,
        aspect_json: Dict[str, Any],
        existing_entities: set,
        removed_count: Dict,
    ) -> bool:
        """Clean group membership aspects."""
        groups = aspect_json.get("groups", [])
        if not groups:
            return False

        original_count = len(groups)
        cleaned_groups = [g for g in groups if g in existing_entities]

        if len(cleaned_groups) != original_count:
            aspect_json["groups"] = cleaned_groups
            removed_count[f"{aspect_name}_references"] += original_count - len(
                cleaned_groups
            )
            return True
        return False

    def _clean_assertions(
        self, aspect_json: Dict[str, Any], existing_entities: set, removed_count: Dict
    ) -> bool:
        """Clean assertionsSummary aspects."""
        modified = False
        for field in [
            "failingAssertions",
            "passingAssertions",
            "initializingAssertionDetails",
            "failingAssertionDetails",
            "passingAssertionDetails",
            "erroringAssertionDetails",
        ]:
            items = aspect_json.get(field, [])
            if not items:
                continue

            if isinstance(items[0], str):
                cleaned = [urn for urn in items if urn in existing_entities]
            else:
                cleaned = [
                    obj for obj in items if obj.get("urn", "") in existing_entities
                ]

            if len(cleaned) != len(items):
                aspect_json[field] = cleaned
                removed_count["assertion_references"] += len(items) - len(cleaned)
                modified = True
        return modified

    def _clean_usage_features(
        self, aspect_json: Dict[str, Any], existing_entities: set, removed_count: Dict
    ) -> bool:
        """Clean corpUserUsageFeatures aspects."""
        modified = False

        # Clean userTopDatasetsByUsage
        top_datasets = aspect_json.get("userTopDatasetsByUsage", {})
        if top_datasets:
            cleaned = {
                urn: count
                for urn, count in top_datasets.items()
                if urn in existing_entities
            }
            if len(cleaned) != len(top_datasets):
                aspect_json["userTopDatasetsByUsage"] = cleaned
                removed_count["usage_references"] += len(top_datasets) - len(cleaned)
                modified = True

        # Clean counts array
        counts = aspect_json.get("counts", [])
        if counts:
            cleaned = [c for c in counts if c.get("resource", "") in existing_entities]
            if len(cleaned) != len(counts):
                aspect_json["counts"] = cleaned
                removed_count["usage_references"] += len(counts) - len(cleaned)
                modified = True

        return modified

    def _clean_chart_dashboard_inputs(
        self, aspect_json: Dict[str, Any], existing_entities: set, removed_count: Dict
    ) -> bool:
        """Clean chartInfo/dashboardInfo input references."""
        inputs = aspect_json.get("inputs", [])
        if not inputs:
            return False

        cleaned_inputs = []
        for inp in inputs:
            urn = inp.get("string", "") if isinstance(inp, dict) else ""
            if urn and urn.strip():
                if urn in existing_entities:
                    cleaned_inputs.append(inp)
                else:
                    removed_count["dataset_references"] += 1

        if len(cleaned_inputs) != len(inputs):
            aspect_json["inputs"] = cleaned_inputs
            return True
        return False

    def _clean_domains(
        self, aspect_json: Dict[str, Any], existing_entities: set, removed_count: Dict
    ) -> bool:
        """Clean domain references."""
        domains = aspect_json.get("domains", [])
        if not domains:
            return False

        cleaned = [d for d in domains if d in existing_entities]
        if len(cleaned) != len(domains):
            aspect_json["domains"] = cleaned
            removed_count["domain_references"] += len(domains) - len(cleaned)
            return True
        return False

    def _clean_glossary_terms(
        self, aspect_json: Dict[str, Any], existing_entities: set, removed_count: Dict
    ) -> bool:
        """Clean entity-level glossary term references (not column-level)."""
        terms = aspect_json.get("terms", [])
        if not terms:
            return False

        cleaned = [t for t in terms if t.get("urn", "") in existing_entities]
        if len(cleaned) != len(terms):
            aspect_json["terms"] = cleaned
            removed_count["entity_glossary_term_references"] += len(terms) - len(cleaned)
            return True
        return False

    def _clean_schema_metadata(
        self, aspect_json: Dict[str, Any], existing_entities: set, removed_count: Dict
    ) -> bool:
        """Clean column-level tags and glossary terms."""
        modified = False
        fields = aspect_json.get("editableSchemaFieldInfo", [])

        for field in fields:
            # Clean glossary terms
            terms_obj = field.get("glossaryTerms", {})
            if terms_obj:
                terms = terms_obj.get("terms", [])
                if terms:
                    cleaned = [
                        t for t in terms if t.get("urn", "") in existing_entities
                    ]
                    if len(cleaned) != len(terms):
                        terms_obj["terms"] = cleaned
                        removed_count["column_term_references"] += len(terms) - len(
                            cleaned
                        )
                        modified = True

            # Clean tags
            tags_obj = field.get("globalTags", {})
            if tags_obj:
                tags = tags_obj.get("tags", [])
                if tags:
                    cleaned = [t for t in tags if t.get("tag", "") in existing_entities]
                    if len(cleaned) != len(tags):
                        tags_obj["tags"] = cleaned
                        removed_count["column_tag_references"] += len(tags) - len(
                            cleaned
                        )
                        modified = True

        return modified

    def cleanup_broken_references(
        self, mcps: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Remove broken references to entities that don't exist in the sample data.

        This prevents URNs from showing in the UI where entity names should appear.
        Also removes stub users (users without corpUserInfo) from ownership.
        """
        from collections import defaultdict

        # Build set of all entity URNs
        existing_entities = set()
        existing_by_type = defaultdict(set)

        for mcp in mcps:
            urn = mcp.get("entityUrn", "")
            existing_entities.add(urn)
            if "urn:li:" in urn:
                entity_type = urn.split("urn:li:")[1].split(":")[0]
                existing_by_type[entity_type].add(urn)

        # Build set of valid users (users with corpUserInfo, not just corpUserKey stubs)
        valid_users = set()
        for mcp in mcps:
            if mcp.get("aspectName") == "corpUserInfo":
                valid_users.add(mcp.get("entityUrn"))

        stub_users = existing_by_type["corpuser"] - valid_users

        logging.info(f"Found {len(existing_entities)} entities in sample data")
        logging.info(f"  - {len(existing_by_type['corpuser'])} users ({len(stub_users)} stub users will be cleaned)")
        logging.info(f"  - {len(existing_by_type['corpGroup'])} groups")
        logging.info(f"  - {len(existing_by_type['dataset'])} datasets")

        cleaned_mcps = []
        removed_count = defaultdict(int)

        for mcp in mcps:
            aspect_name = mcp.get("aspectName", "")
            aspect = mcp.get("aspect", {})
            aspect_json = aspect.get("json", {})
            modified = False

            # Skip stub user aspects (corpUserKey/status for users without corpUserInfo)
            if mcp.get("entityType") == "corpuser":
                entity_urn = mcp.get("entityUrn", "")
                if entity_urn not in valid_users:
                    # This is a stub user - only keep if it's being converted to a real user
                    if aspect_name in ["corpUserKey", "status", "roleMembership"]:
                        removed_count["stub_user_aspects"] += 1
                        continue

            # Dispatch to appropriate cleaner
            if aspect_name == "ownership":
                modified, should_skip = self._clean_ownership(
                    aspect_json, existing_entities, removed_count, valid_users
                )
                if should_skip:
                    continue
            elif aspect_name in ["groupMembership", "nativeGroupMembership"]:
                modified = self._clean_group_membership(
                    aspect_name, aspect_json, existing_entities, removed_count
                )
            elif aspect_name == "assertionsSummary":
                modified = self._clean_assertions(
                    aspect_json, existing_entities, removed_count
                )
            elif aspect_name == "corpUserUsageFeatures":
                modified = self._clean_usage_features(
                    aspect_json, existing_entities, removed_count
                )
            elif aspect_name in ["chartInfo", "dashboardInfo"]:
                modified = self._clean_chart_dashboard_inputs(
                    aspect_json, existing_entities, removed_count
                )
            elif aspect_name in ["forms", "formNotifications"]:
                removed_count["forms_aspects"] += 1
                continue
            elif aspect_name == "domains":
                modified = self._clean_domains(
                    aspect_json, existing_entities, removed_count
                )
            elif aspect_name == "glossaryTerms":
                modified = self._clean_glossary_terms(
                    aspect_json, existing_entities, removed_count
                )
            elif aspect_name == "editableSchemaMetadata":
                modified = self._clean_schema_metadata(
                    aspect_json, existing_entities, removed_count
                )

            if modified:
                aspect["json"] = aspect_json
                mcp["aspect"] = aspect

            cleaned_mcps.append(mcp)

        # Log summary
        logging.info("\n=== Reference Cleanup Summary ===")
        for ref_type, count in sorted(removed_count.items()):
            logging.info(f"  Removed {count} {ref_type}")
        logging.info(f"Total MCPs: {len(mcps)} → {len(cleaned_mcps)}")

        return cleaned_mcps

    def add_missing_status_aspects(
        self, mcps: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Add status aspect (removed=false) to entities that support it but don't have it.

        This ensures all entities can be soft-deleted when the toggle sample data
        feature is used. Without status aspects, entities remain visible even when
        sample data is toggled off, creating a confusing UX.

        Entity types that commonly need status added:
        - domain, tag, glossaryTerm, glossaryNode
        """
        from collections import defaultdict

        # Track which entities have status aspect
        entities_with_status = set()
        for mcp in mcps:
            if mcp.get("aspectName") == "status":
                entities_with_status.add(mcp.get("entityUrn"))

        # Track all entities by type
        all_entities = defaultdict(set)
        for mcp in mcps:
            urn = mcp.get("entityUrn", "")
            if urn and "urn:li:" in urn:
                entity_type = urn.split("urn:li:")[1].split(":")[0]
                all_entities[entity_type].add(urn)

        # Entity types that support status aspect (from entity-registry.yml)
        # We exclude corpuser, corpGroup, dataPlatform as they're preserved by SampleDataService
        # We exclude domain as it doesn't support the status aspect
        entity_types_with_status = {
            "chart",
            "container",
            "dashboard",
            "dataFlow",
            "dataJob",
            "dataProduct",
            "dataset",
            "glossaryNode",
            "glossaryTerm",
            "tag",
        }

        # Find entities that need status aspect added
        added_count = 0
        for entity_type in entity_types_with_status:
            if entity_type in all_entities:
                for urn in all_entities[entity_type]:
                    if urn not in entities_with_status:
                        # Add status aspect with removed=false
                        status_mcp = {
                            "entityType": entity_type,
                            "entityUrn": urn,
                            "changeType": "UPSERT",
                            "aspectName": "status",
                            "aspect": {"json": {"removed": False}},
                        }
                        mcps.append(status_mcp)
                        added_count += 1

        if added_count > 0:
            logging.info(f"Added {added_count} status aspects for toggle feature")
        else:
            logging.info("All entities already have status aspects")

        return mcps

    def add_missing_dataflow_info(
        self, mcps: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Add dataFlowInfo aspects for dataFlow entities that don't have them.
        This prevents them from showing as URNs in the UI.

        Spark dataFlow entities from fieldeng often lack dataFlowInfo aspects,
        causing them to render as raw URNs like 'urn:li:dataFlow:(spark,ex...'
        instead of proper names. This function synthesizes dataFlowInfo from
        the URN structure to ensure proper UI display.

        Additionally, if dataFlow entities are missing entirely (only dataJob
        entities were exported), this function will create them by extracting
        dataFlow URNs from dataJob URNs.
        """
        # Find all dataFlow URNs - both explicit entities and embedded in dataJob URNs
        dataflow_urns = set()
        dataflows_with_info = set()

        for mcp in mcps:
            # Find explicit dataFlow entities
            if mcp.get('entityType') == 'dataFlow':
                urn = mcp.get('entityUrn')
                if urn:
                    dataflow_urns.add(urn)
                    if mcp.get('aspectName') == 'dataFlowInfo':
                        dataflows_with_info.add(urn)

            # Extract dataFlow URNs from dataJob entities
            elif mcp.get('entityType') == 'dataJob':
                job_urn = mcp.get('entityUrn', '')
                # DataJob URN format: urn:li:dataJob:(urn:li:dataFlow:(platform,name,env),jobName)
                if ':dataFlow:(' in job_urn:
                    try:
                        # Extract the dataFlow URN
                        flow_start = job_urn.index(':dataFlow:(')
                        flow_end = job_urn.index('),', flow_start) + 1
                        dataflow_urn = job_urn[flow_start-6:flow_end]  # Include 'urn:li:' prefix
                        dataflow_urns.add(dataflow_urn)
                    except (ValueError, IndexError):
                        logging.debug(f"Could not extract dataFlow URN from: {job_urn}")

        # Add dataFlowInfo for flows missing it (and create dataFlow entities if needed)
        added_count = 0
        for urn in dataflow_urns:
            if urn not in dataflows_with_info:
                # Extract name from URN: urn:li:dataFlow:(platform,flowName,env)
                try:
                    # Parse URN structure
                    parts = urn.split(':(')[1].rstrip(')').split(',')
                    platform = parts[0]
                    flow_name = parts[1] if len(parts) > 1 else 'unknown'

                    dataflow_info_mcp = {
                        'entityType': 'dataFlow',
                        'entityUrn': urn,
                        'changeType': 'UPSERT',
                        'aspectName': 'dataFlowInfo',
                        'aspect': {
                            'json': {
                                'name': flow_name,
                                'description': f'{platform.title()} data pipeline',
                                'customProperties': {}
                            }
                        },
                        'systemMetadata': {'properties': {'sampleData': 'true'}},
                    }
                    mcps.append(dataflow_info_mcp)
                    added_count += 1
                    logging.info(f"Added dataFlowInfo for {urn}")
                except Exception as e:
                    logging.warning(f"Could not synthesize dataFlowInfo for {urn}: {e}")

        if added_count > 0:
            logging.info(f"Added {added_count} dataFlowInfo aspects for proper UI display")
        else:
            logging.info("All dataFlow entities already have dataFlowInfo")

        return mcps

    def convert_to_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert entity data to MCP format.
        Returns list of MCPs (one per aspect).
        """
        entity_urn = entity_data.get("urn")
        # Extract entity type from URN if not in response
        entity_type_info = entity_data.get("entityType")
        if not entity_type_info:
            entity_type_info = self.get_entity_type_from_urn(entity_urn)
        aspects = entity_data.get("aspects", {})

        mcps = []

        for aspect_name, aspect_data in aspects.items():
            # Skip excluded aspects
            if aspect_name in EXCLUDED_ASPECTS:
                logging.debug(
                    f"Skipping excluded aspect {aspect_name} for {entity_urn}"
                )
                continue

            # Convert aspect format from {"value": {...}} to {"json": {...}}
            # McpJsonUtils expects "json" field, but batch API returns "value"
            aspect_json = aspect_data
            if "value" in aspect_data:
                aspect_json = {"json": aspect_data["value"]}

            mcp = {
                "entityType": entity_type_info,
                "entityUrn": entity_urn,
                "changeType": "UPSERT",
                "aspectName": aspect_name,
                "aspect": aspect_json,
                "systemMetadata": {"properties": {"sampleData": "true"}},
            }
            mcps.append(mcp)

        return mcps

    def export_sample_data(
        self,
        output_file: str,
        seed_urn: str,
        max_hops: int = 10,
        include_related: bool = True,
        filter_non_soft_delete: bool = True,
    ):
        """
        Export sample data from DataHub instance to MCP JSON file.
        Uses lineage traversal starting from a seed entity.
        """
        logging.info("Starting lineage-based export from DataHub instance...")
        logging.info(f"Seed entity: {seed_urn}")
        logging.info(f"Max lineage hops: {max_hops}")

        # Step 1: Traverse lineage graph
        lineage_urns = self.traverse_full_lineage(seed_urn, max_hops)

        # Step 2: Get related entities (domains, containers, owners, tags, etc.)
        related_urns = set()
        if include_related:
            logging.info(
                "Fetching related entities (domains, containers, owners, tags, glossary terms, data products)..."
            )
            related_urns = self.get_related_entities(lineage_urns)

        # Step 3: Combine all URNs
        all_urns = lineage_urns.union(related_urns)
        logging.info(f"Total entities to export: {len(all_urns)}")

        # Step 4: Group URNs by entity type for batch fetching
        logging.info("Grouping URNs by entity type for batch fetching...")
        urns_by_type: Dict[str, List[str]] = {}

        for urn in all_urns:
            try:
                entity_type = self.get_entity_type_from_urn(urn)

                # Skip excluded entity types early
                if filter_non_soft_delete and entity_type in EXCLUDED_ENTITIES:
                    logging.debug(f"Skipping excluded entity type {entity_type}: {urn}")
                    continue

                urns_by_type.setdefault(entity_type, []).append(urn)
            except Exception as e:
                logging.warning(f"Error extracting entity type from {urn}: {e}")
                continue

        logging.info(f"Found {len(urns_by_type)} entity types to fetch")
        for entity_type, type_urns in urns_by_type.items():
            logging.info(f"  {entity_type}: {len(type_urns)} entities")

        # Step 5: Batch fetch entities by type and convert to MCPs
        all_mcps = []
        filtered_aspect_count = 0

        for entity_type, type_urns in urns_by_type.items():
            logging.info(f"Batch fetching {len(type_urns)} {entity_type} entities...")

            try:
                entities = self.batch_get_entities(entity_type, type_urns)

                for urn, entity_data in entities.items():
                    try:
                        # Filter excluded glossary terms
                        if entity_type == "glossaryTerm":
                            aspects = entity_data.get("aspects", {})
                            if "glossaryTermInfo" in aspects:
                                term_info = aspects["glossaryTermInfo"]
                                term_data = term_info.get("value") or term_info.get("json", {})
                                term_name = term_data.get("name", "")
                                if term_name in EXCLUDED_GLOSSARY_TERMS:
                                    logging.info(
                                        f"Filtering out excluded glossary term: {term_name}"
                                    )
                                    continue

                        # Filter excluded glossary nodes
                        if entity_type == "glossaryNode":
                            aspects = entity_data.get("aspects", {})
                            if "glossaryNodeInfo" in aspects:
                                node_info = aspects["glossaryNodeInfo"]
                                node_data = node_info.get("value") or node_info.get("json", {})
                                node_name = node_data.get("name", "")
                                if node_name in EXCLUDED_GLOSSARY_NODES:
                                    logging.info(
                                        f"Filtering out excluded glossary node: {node_name}"
                                    )
                                    continue

                        mcps = self.convert_to_mcps(entity_data)

                        # Count filtered aspects
                        original_aspect_count = len(entity_data.get("aspects", {}))
                        filtered_aspect_count += original_aspect_count - len(mcps)

                        all_mcps.extend(mcps)

                    except Exception as e:
                        logging.error(f"Error converting {urn} to MCPs: {e}")
                        continue

            except Exception as e:
                logging.error(f"Error batch fetching {entity_type} entities: {e}")
                continue

        # Step 6: Extract and fetch schemaField entities from fine-grained lineage
        logging.info("Extracting schemaField entities from fine-grained lineage...")
        schema_field_urns = self.extract_schema_field_urns(all_mcps)
        if schema_field_urns:
            logging.info(f"Found {len(schema_field_urns)} schemaField URNs in lineage")
            try:
                schema_field_entities = self.batch_get_entities("schemaField", list(schema_field_urns))
                for urn, entity_data in schema_field_entities.items():
                    try:
                        mcps = self.convert_to_mcps(entity_data)
                        all_mcps.extend(mcps)
                    except Exception as e:
                        logging.warning(f"Error converting schemaField {urn} to MCPs: {e}")
            except Exception as e:
                logging.error(f"Error batch fetching schemaField entities: {e}")

        # Step 7: Sort MCPs to ensure dependencies come first
        logging.info("Sorting MCPs to ensure proper ingestion order...")
        sorted_mcps = self.sort_mcps_by_dependency(all_mcps)

        # Step 8: Anonymize sensitive data (emails, URLs, names)
        logging.info("Anonymizing sensitive data (emails, URLs, names)...")
        anonymized_mcps = self.anonymize_sensitive_data(sorted_mcps)

        # Step 9: Add sample_data_ prefix to all URNs to prevent collisions
        logging.info("Adding sample_data_ prefix to URNs to prevent collisions...")
        prefixed_mcps = self.add_sample_prefix_to_urns(anonymized_mcps)

        # Step 10: Clean up broken references to missing entities
        # This removes references that would show as URNs in the UI:
        # - Missing users/groups in ownership and group membership
        # - Missing datasets in user activity
        # - Missing assertions, domains, tags, glossary terms
        # - Forms aspects (removed entirely for demo simplicity)
        logging.info("Cleaning up broken references to missing entities...")
        cleaned_mcps = self.cleanup_broken_references(prefixed_mcps)

        # Step 11: Add missing status aspects for toggle feature
        # This ensures all entities can be soft-deleted when sample data is toggled off
        logging.info("Adding missing status aspects for toggle feature...")
        final_mcps = self.add_missing_status_aspects(cleaned_mcps)

        # Step 12: Add missing dataFlowInfo to prevent URN display
        # This ensures dataFlow entities show proper names instead of URNs in the UI
        logging.info("Ensuring all dataFlows have display names...")
        final_mcps = self.add_missing_dataflow_info(final_mcps)

        # Step 13: Write to output file
        logging.info(f"Writing {len(final_mcps)} MCPs to {output_file}...")
        with open(output_file, "w") as f:
            json.dump(final_mcps, f, indent=2)

        # Step 8: Summary
        unique_entities = len(set(mcp["entityUrn"] for mcp in all_mcps))
        total_filtered = len(all_urns) - sum(
            len(urns) for urns in urns_by_type.values()
        )
        logging.info(
            f"""
Export complete!
  Lineage entities: {len(lineage_urns)}
  Related entities: {len(related_urns)}
  Total MCPs: {len(all_mcps)}
  Unique entities exported: {unique_entities}
  Filtered entities (non-soft-deletable): {total_filtered}
  Filtered aspects: {filtered_aspect_count}
  Output: {output_file}
        """
        )


def main():
    parser = argparse.ArgumentParser(
        description="Export sample data from DataHub instance via lineage traversal"
    )
    parser.add_argument(
        "--server",
        required=True,
        help="DataHub server URL (e.g., https://fieldeng.acryl.io)",
    )
    parser.add_argument(
        "--token",
        required=True,
        help="DataHub access token for authentication",
    )
    parser.add_argument(
        "--seed-urn",
        required=True,
        help="Seed entity URN to start lineage traversal (e.g., urn:li:dataset:...)",
    )
    parser.add_argument(
        "--max-hops",
        type=int,
        default=10,
        help="Maximum lineage hops to traverse (default: 10)",
    )
    parser.add_argument(
        "--output",
        default="sample_data_mcp.json",
        help="Output file path (default: sample_data_mcp.json)",
    )
    parser.add_argument(
        "--no-related",
        action="store_true",
        help="Don't include related entities (domains, tags, glossary terms)",
    )
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="Don't filter out entities that don't support soft delete",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    exporter = DataHubExporter(args.server, args.token)
    exporter.export_sample_data(
        output_file=args.output,
        seed_urn=args.seed_urn,
        max_hops=args.max_hops,
        include_related=not args.no_related,
        filter_non_soft_delete=not args.no_filter,
    )


if __name__ == "__main__":
    main()
