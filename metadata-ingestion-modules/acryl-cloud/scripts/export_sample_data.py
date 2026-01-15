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

    def _clean_ownership(
        self, aspect_json: Dict[str, Any], existing_entities: set, removed_count: Dict
    ) -> tuple[bool, bool]:
        """Clean ownership aspect. Returns (modified, should_skip)."""
        owners = aspect_json.get("owners", [])
        if not owners:
            return False, False

        original_count = len(owners)
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

        logging.info(f"Found {len(existing_entities)} entities in sample data")
        logging.info(f"  - {len(existing_by_type['corpuser'])} users")
        logging.info(f"  - {len(existing_by_type['corpGroup'])} groups")
        logging.info(f"  - {len(existing_by_type['dataset'])} datasets")

        cleaned_mcps = []
        removed_count = defaultdict(int)

        for mcp in mcps:
            aspect_name = mcp.get("aspectName", "")
            aspect = mcp.get("aspect", {})
            aspect_json = aspect.get("json", {})
            modified = False

            # Dispatch to appropriate cleaner
            if aspect_name == "ownership":
                modified, should_skip = self._clean_ownership(
                    aspect_json, existing_entities, removed_count
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

        # Step 6: Sort MCPs to ensure dependencies come first
        logging.info("Sorting MCPs to ensure proper ingestion order...")
        sorted_mcps = self.sort_mcps_by_dependency(all_mcps)

        # Step 7: Anonymize sensitive data (emails, URLs, names)
        logging.info("Anonymizing sensitive data (emails, URLs, names)...")
        anonymized_mcps = self.anonymize_sensitive_data(sorted_mcps)

        # Step 8: Clean up broken references to missing entities
        # This removes references that would show as URNs in the UI:
        # - Missing users/groups in ownership and group membership
        # - Missing datasets in user activity
        # - Missing assertions, domains, tags, glossary terms
        # - Forms aspects (removed entirely for demo simplicity)
        logging.info("Cleaning up broken references to missing entities...")
        cleaned_mcps = self.cleanup_broken_references(anonymized_mcps)

        # Step 9: Add missing status aspects for toggle feature
        # This ensures all entities can be soft-deleted when sample data is toggled off
        logging.info("Adding missing status aspects for toggle feature...")
        final_mcps = self.add_missing_status_aspects(cleaned_mcps)

        # Step 10: Add missing dataFlowInfo to prevent URN display
        # This ensures dataFlow entities show proper names instead of URNs in the UI
        logging.info("Ensuring all dataFlows have display names...")
        final_mcps = self.add_missing_dataflow_info(final_mcps)

        # Step 11: Write to output file
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
