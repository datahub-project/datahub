import re
from typing import Dict, List, Optional

from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

from datahub_integrations.app import DATAHUB_FRONTEND_URL
from datahub_integrations.graphql.slack import SLACK_GET_ENTITY_QUERY


def get_type_url(entity_type: Optional[str], urn: Optional[str]) -> Optional[str]:
    """
    Generate a DataHub frontend URL for an entity.

    Args:
        entity_type: The type of entity (e.g., 'DATASET', 'CHART')
        urn: The entity URN

    Returns:
        The frontend URL for the entity
    """

    if not entity_type or not urn:
        return None

    # Map entity types to frontend paths
    type_to_path = {
        "DATASET": "dataset",
        "CHART": "chart",
        "DASHBOARD": "dashboard",
        "DATA_JOB": "dataJob",
        "DATA_FLOW": "dataFlow",
        "CONTAINER": "container",
        "DOMAIN": "domain",
        "DATA_PRODUCT": "dataProduct",
        "GLOSSARY_TERM": "glossaryTerm",
        "GLOSSARY_NODE": "glossaryNode",
        "TAG": "tag",
        "CORP_USER": "user",
        "CORP_GROUP": "group",
    }

    path = type_to_path.get(entity_type)
    if not path:
        return None

    return f"{DATAHUB_FRONTEND_URL}/{path}/{urn}?is_lineage_mode=false"


class ExtractedEntity:
    """Wrapper for entity data extracted from GraphQL responses."""

    def __init__(self, entity_data: dict):
        self.entity_data = entity_data

    @property
    def urn(self) -> Optional[str]:
        return self.entity_data.get("urn")

    @property
    def type(self) -> Optional[str]:
        return self.entity_data.get("type")

    @property
    def name(self) -> Optional[str]:
        properties = self.entity_data.get("properties", {})
        return properties.get("name")

    @property
    def description(self) -> Optional[str]:
        properties = self.entity_data.get("properties", {})
        return properties.get("description")

    @property
    def url(self) -> Optional[str]:
        entity_type = self.type
        urn = self.urn
        if entity_type is not None and urn is not None:
            return get_type_url(entity_type, urn)
        return None


def extract_entities_from_response(response_text: str) -> List[dict]:
    """
    Extract entity references from AI response text.

    This function looks for DataHub entity URLs in the response text and extracts
    entity information that can be used to create a sources card.

    Args:
        response_text: The AI response text containing entity references

    Returns:
        List of dictionaries containing entity information (name, type, url, etc.)
    """
    logger.info(f"Extracting entities from response text: {response_text[:200]}...")
    entities = []

    # Pattern to match DataHub entity URLs in markdown format
    # Matches: [Entity Name](https://domain.com/entity_type/urn)
    url_pattern = r"\[([^\]]+)\]\((https?://[^/]+/(?:dataset|chart|dashboard|dataJob|dataFlow|container|domain|dataProduct|glossaryTerm|glossaryNode|tag|user|group)/[^)]+)\)"

    # Pattern to match URN-based entity references in markdown format
    # Matches: [Entity Name](urn:li:dataset:(...))
    urn_pattern = r"\[([^\]]+)\]\((urn:li:(?:dataset|chart|dashboard|dataJob|dataFlow|container|domain|dataProduct|glossaryTerm|glossaryNode|tag|corpuser|corpGroup):[^)]*\))\)"

    url_matches = re.findall(url_pattern, response_text)
    urn_matches = re.findall(urn_pattern, response_text)

    logger.info(f"Found {len(url_matches)} URL matches: {url_matches}")
    logger.info(f"Found {len(urn_matches)} URN matches: {urn_matches}")

    # Process URL-based matches
    for name, url in url_matches:
        # Extract entity type from URL path
        entity_type = None
        if "/dataset/" in url:
            entity_type = "Dataset"
        elif "/chart/" in url:
            entity_type = "Chart"
        elif "/dashboard/" in url:
            entity_type = "Dashboard"
        elif "/dataJob/" in url:
            entity_type = "Data Job"
        elif "/dataFlow/" in url:
            entity_type = "Data Flow"
        elif "/container/" in url:
            entity_type = "Container"
        elif "/domain/" in url:
            entity_type = "Domain"
        elif "/dataProduct/" in url:
            entity_type = "Data Product"
        elif "/glossaryTerm/" in url:
            entity_type = "Glossary Term"
        elif "/glossaryNode/" in url:
            entity_type = "Glossary Node"
        elif "/tag/" in url:
            entity_type = "Tag"
        elif "/user/" in url:
            entity_type = "User"
        elif "/group/" in url:
            entity_type = "Group"

        if entity_type:
            entity_data = {
                "name": name,
                "type": entity_type,
                "url": url,
                "description": "",  # Could be enhanced to extract descriptions if needed
            }
            entities.append(entity_data)
            logger.info(f"Added entity: {entity_data}")

    # Process URN-based matches
    for name, urn in urn_matches:
        # Extract entity type from URN
        entity_type = None
        if ":dataset:" in urn:
            entity_type = "Dataset"
        elif ":chart:" in urn:
            entity_type = "Chart"
        elif ":dashboard:" in urn:
            entity_type = "Dashboard"
        elif ":dataJob:" in urn:
            entity_type = "Data Job"
        elif ":dataFlow:" in urn:
            entity_type = "Data Flow"
        elif ":container:" in urn:
            entity_type = "Container"
        elif ":domain:" in urn:
            entity_type = "Domain"
        elif ":dataProduct:" in urn:
            entity_type = "Data Product"
        elif ":glossaryTerm:" in urn:
            entity_type = "Glossary Term"
        elif ":glossaryNode:" in urn:
            entity_type = "Glossary Node"
        elif ":tag:" in urn:
            entity_type = "Tag"
        elif ":corpuser:" in urn:
            entity_type = "User"
        elif ":corpGroup:" in urn:
            entity_type = "Group"

        if entity_type:
            # Generate URL from URN for consistency with URL-based entities
            from datahub_integrations.teams.utils.entity_extract import get_type_url

            # Map display names back to internal type names for URL generation
            type_mapping = {
                "Dataset": "DATASET",
                "Chart": "CHART",
                "Dashboard": "DASHBOARD",
                "Data Job": "DATA_JOB",
                "Data Flow": "DATA_FLOW",
                "Container": "CONTAINER",
                "Domain": "DOMAIN",
                "Data Product": "DATA_PRODUCT",
                "Glossary Term": "GLOSSARY_TERM",
                "Glossary Node": "GLOSSARY_NODE",
                "Tag": "TAG",
                "User": "CORP_USER",
                "Group": "CORP_GROUP",
            }

            internal_type = type_mapping.get(entity_type)
            url = get_type_url(internal_type, urn) if internal_type else None

            entity_data = {
                "name": name,
                "type": entity_type,
                "url": url,  # Generated URL from URN
                "description": "",  # Could be enhanced to extract descriptions if needed
                "urn": urn,  # Store the original URN for reference
            }
            entities.append(entity_data)
            logger.info(f"Added URN-based entity: {entity_data}")

    # Remove duplicates based on URL or URN
    seen_identifiers = set()
    unique_entities = []
    for entity in entities:
        # Use URN if available, otherwise use URL for deduplication
        identifier = entity.get("urn") or entity.get("url")
        if identifier and identifier not in seen_identifiers:
            seen_identifiers.add(identifier)
            unique_entities.append(entity)

    logger.info(f"Returning {len(unique_entities)} unique entities: {unique_entities}")
    return unique_entities


async def extract_entities_with_metadata(
    response_text: str, graph: DataHubGraph
) -> List[dict]:
    """
    Extract entity references from AI response text and fetch their metadata.

    This function extracts entities from the response text and enriches them with
    metadata from DataHub for creating rich mini cards.

    Args:
        response_text: The AI response text containing entity references
        graph: DataHub graph client for fetching metadata

    Returns:
        List of dictionaries containing enriched entity information
    """
    logger.info(
        f"Starting entity metadata extraction for response: {response_text[:200]}..."
    )

    # First extract basic entity info from URLs
    basic_entities = extract_entities_from_response(response_text)
    logger.info(f"Extracted {len(basic_entities)} basic entities: {basic_entities}")

    enriched_entities = []

    for i, entity in enumerate(basic_entities):
        logger.info(f"Processing entity {i + 1}/{len(basic_entities)}: {entity}")
        try:
            # Check if entity already has a URN (from URN-based extraction)
            if "urn" in entity and entity["urn"]:
                urn = entity["urn"]
                logger.info(f"Using existing URN from entity: {urn}")
            else:
                # Extract URN from URL for URL-based entities
                url = entity["url"]
                if not url:
                    logger.warning(f"Entity has no URL or URN: {entity}")
                    enriched_entities.append(entity)
                    continue

                # DataHub URLs have structure: https://domain.com/entityType/encodedUrn/
                # Extract the URN by splitting the URL and taking the last non-empty part
                url_parts = url.rstrip("/").split("/")
                if len(url_parts) < 2:
                    # If we can't extract URN, keep basic info
                    logger.warning(f"Could not extract URN from URL: {url}")
                    enriched_entities.append(entity)
                    continue

                encoded_urn = url_parts[-1]
                logger.info(f"Extracted encoded URN: {encoded_urn}")
                urn = encoded_urn
                # URL decode the URN
                import urllib.parse

                urn = urllib.parse.unquote(urn)
                logger.info(f"Extracted URN: {urn}")

            # Fetch entity metadata from DataHub
            variables = {"urn": urn}
            logger.info(f"Fetching metadata with GraphQL query for URN: {urn}")
            data = graph.execute_graphql(SLACK_GET_ENTITY_QUERY, variables=variables)
            logger.info(f"GraphQL response: {data}")

            raw_entity = data.get("entity")
            if not raw_entity:
                # If entity not found, keep basic info
                logger.warning(f"Entity not found in GraphQL response for URN: {urn}")
                enriched_entities.append(entity)
                continue

            # Extract key metadata for mini card
            logger.info(f"Extracting mini card data from raw entity: {raw_entity}")
            enriched_entity = _extract_mini_card_data(raw_entity, entity["url"])
            logger.info(f"Enriched entity: {enriched_entity}")
            enriched_entities.append(enriched_entity)

        except Exception as e:
            # If anything fails, keep basic info
            logger.error(f"Error processing entity {entity}: {e}", exc_info=True)
            enriched_entities.append(entity)
            continue

    logger.info(
        f"Returning {len(enriched_entities)} enriched entities: {enriched_entities}"
    )
    return enriched_entities


def _extract_entity_name(raw_entity: Dict) -> str:
    """Extract entity name using the same logic as Slack implementation."""
    if (raw_entity.get("editableProperties") or {}).get("displayName") is not None:
        return raw_entity["editableProperties"]["displayName"]
    if (raw_entity.get("properties") or {}).get("displayName") is not None:
        return raw_entity["properties"]["displayName"]
    if (raw_entity.get("properties") or {}).get("name") is not None:
        return raw_entity["properties"]["name"]
    if (raw_entity.get("info") or {}).get("name") is not None:
        return raw_entity["info"]["name"]
    if raw_entity.get("name") is not None:
        return raw_entity["name"]
    logger.warning(
        f"Could not extract name from entity: {raw_entity.get('urn', 'unknown')}. Returning 'Unknown'"
    )
    return "Unknown"


def _extract_mini_card_data(raw_entity: Dict, url: str) -> Dict:
    """Extract key metadata for mini card display."""
    try:
        logger.info(
            f"Extracting mini card data for entity: {raw_entity.get('urn', 'unknown')}"
        )
        properties = raw_entity.get("properties", {})

        # Basic info - use the same logic as Slack implementation
        logger.info("Extracting name...")
        name = _extract_entity_name(raw_entity)
        logger.info(f"Name extracted: {name}")

        logger.info("Extracting description...")
        description = properties.get("description") or ""
        logger.info(f"Description extracted: {description[:50]}...")

        logger.info("Extracting entity type...")
        entity_type = raw_entity.get("type", "Unknown")
        logger.info(f"Entity type extracted: {entity_type}")

        # Enhanced info
        logger.info("Extracting subtype...")
        subtype = _get_entity_subtype(raw_entity)
        logger.info(f"Subtype extracted: {subtype}")

        logger.info("Extracting platform...")
        platform = _get_platform_name(raw_entity)
        logger.info(f"Platform extracted: {platform}")

        logger.info("Extracting owner...")
        owner = _get_primary_owner(raw_entity)
        logger.info(f"Owner extracted: {owner}")

        logger.info("Extracting certification...")
        certification = _get_certification_status(raw_entity)
        logger.info(f"Certification extracted: {certification}")

        logger.info("Extracting stats...")
        stats = _get_key_stats(raw_entity)
        logger.info(f"Stats extracted: {stats}")

        # Create display type (prefer subtype over entity type)
        display_type = subtype if subtype else entity_type.replace("_", " ").title()
        logger.info(f"Display type: {display_type}")

        result = {
            "name": name,
            "type": display_type,
            "url": url,
            "description": description[:100] + ("..." if len(description) > 100 else "")
            if description
            else "",
            "platform": platform,
            "owner": owner,
            "certified": certification,
            "stats": stats,
        }
        logger.info(f"Mini card data extracted successfully: {result}")
        return result

    except Exception as e:
        logger.error(f"Error in _extract_mini_card_data: {e}", exc_info=True)
        raise


def _get_entity_subtype(raw_entity: Dict) -> Optional[str]:
    """Get entity subtype for more specific type display."""
    subtypes = raw_entity.get("subTypes", {})
    if isinstance(subtypes, dict):
        type_names = subtypes.get("typeNames", [])
        if isinstance(type_names, list) and type_names:
            return type_names[0].replace("_", " ").capitalize()
    return None


def _get_platform_name(raw_entity: Dict) -> Optional[str]:
    """Get platform name for display."""
    platform = raw_entity.get("platform", {})
    if isinstance(platform, dict):
        properties = platform.get("properties", {})
        if isinstance(properties, dict):
            return properties.get("displayName") or platform.get("name")
    return None


def _get_primary_owner(raw_entity: Dict) -> Optional[str]:
    """Get primary owner for display."""
    ownership = raw_entity.get("ownership", {})
    if isinstance(ownership, dict):
        owners = ownership.get("owners", [])
        if isinstance(owners, list) and owners:
            owner_entry = owners[0]  # Get first owner
            if isinstance(owner_entry, dict):
                owner = owner_entry.get("owner", {})
                if isinstance(owner, dict):
                    properties = owner.get("properties", {})
                    info = owner.get("info", {})

                    # Try different name fields
                    if isinstance(properties, dict):
                        name = properties.get("displayName")
                        if name:
                            return name

                    if isinstance(info, dict):
                        name = info.get("displayName")
                        if name:
                            return name

                    # Fallback to username
                    return owner.get("username") or owner.get("name")
    return None


def _get_certification_status(raw_entity: Dict) -> bool:
    """Check if entity has certification indicators."""
    # Check for deprecation (inverse of certification)
    deprecation = raw_entity.get("deprecation")
    if isinstance(deprecation, dict) and deprecation.get("deprecated"):
        return False

    # Check for certification tags
    global_tags = raw_entity.get("globalTags", {})
    if isinstance(global_tags, dict):
        tags_list = global_tags.get("tags", [])
        if isinstance(tags_list, list):
            for tag_entry in tags_list:
                if isinstance(tag_entry, dict):
                    tag = tag_entry.get("tag", {})
                    if isinstance(tag, dict):
                        tag_props = tag.get("properties", {})
                        tag_name = ""
                        if isinstance(tag_props, dict):
                            tag_name = tag_props.get("name", "")
                        if not tag_name:
                            tag_name = tag.get("name", "")

                        tag_name = tag_name.lower()
                        if any(
                            cert_keyword in tag_name
                            for cert_keyword in [
                                "certified",
                                "verified",
                                "approved",
                                "production",
                            ]
                        ):
                            return True

    return False


def _get_key_stats(raw_entity: Dict) -> Optional[str]:
    """Get key statistics for display using popularity indicators instead of raw counts."""
    usage_level = _get_usage_level(raw_entity)
    if usage_level:
        # Use fire emoji for high popularity like in search results
        if usage_level == "High":
            return "🔥 High usage"
        elif usage_level == "Med":
            return "📈 Med usage"
        elif usage_level == "Low":
            return "📊 Low usage"

    # Fallback to raw counts if no percentile data is available
    stats_summary = raw_entity.get("statsSummary", {})
    if isinstance(stats_summary, dict):
        # For datasets, prioritize query count, then view count
        if raw_entity.get("type") == "DATASET":
            query_count = stats_summary.get("queryCountLast30Days")
            if query_count is not None and query_count > 0:
                return f"📊 {query_count} queries/30d"

        # Fallback to view count for all entities
        view_count = stats_summary.get("viewCountLast30Days")
        if view_count is not None and view_count > 0:
            return f"📊 {view_count} views/30d"

    return None


def _get_usage_level(raw_entity: Dict) -> Optional[str]:
    """Get usage level based on percentiles (High/Med/Low) like in Slack implementation."""
    stats_summary = raw_entity.get("statsSummary", {})
    if not isinstance(stats_summary, dict):
        return None

    queries_percentile: Optional[float] = stats_summary.get(
        "queryCountPercentileLast30Days"
    )
    views_percentile: Optional[float] = stats_summary.get(
        "viewCountPercentileLast30Days"
    )
    users_percentile: Optional[float] = stats_summary.get(
        "uniqueUserPercentileLast30Days"
    )

    # Datasets - use query and user percentiles
    if (
        raw_entity.get("type") == "DATASET"
        and queries_percentile is not None
        and users_percentile is not None
    ):
        if queries_percentile > 80 or users_percentile > 80:
            return "High"
        if queries_percentile > 30 or users_percentile > 30:
            return "Med"
        if queries_percentile > 0 or users_percentile > 0:
            return "Low"

    # Dashboards - use view and user percentiles
    elif (
        raw_entity.get("type") == "DASHBOARD"
        and views_percentile is not None
        and users_percentile is not None
    ):
        if views_percentile > 80 or users_percentile > 80:
            return "High"
        if views_percentile > 30 or users_percentile > 30:
            return "Med"
        if views_percentile > 0 or users_percentile > 0:
            return "Low"

    # Other entities - use any available percentile
    elif (
        queries_percentile is not None
        or views_percentile is not None
        or users_percentile is not None
    ):
        max_percentile = max(
            p
            for p in [queries_percentile, views_percentile, users_percentile]
            if p is not None
        )
        if max_percentile > 80:
            return "High"
        if max_percentile > 30:
            return "Med"
        if max_percentile > 0:
            return "Low"

    return None
