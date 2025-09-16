import logging
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

# Import url_utils directly to avoid circular import through utils/__init__.py
import datahub_integrations.teams.url_utils as url_utils
from datahub_integrations.app import DATAHUB_FRONTEND_URL

logger = logging.getLogger(__name__)


class EntityCardRenderField(Enum):
    DESCRIPTION = "description"
    TAG = "tag"
    TERM = "term"
    OWNERSHIP = "ownership"
    DEPRECATED = "deprecated"
    UNHEALTHY = "unhealthy"
    HEALTH_INDICATORS = "health_indicators"
    DOMAIN = "domain"
    USAGE_STATS = "usage_stats"
    LINEAGE_COUNTS = "lineage_counts"

    def __str__(self) -> str:
        return self.value


def _extract_name_from_urn(urn: str) -> Optional[str]:
    """Extract entity name from URN when properties.name is missing."""
    if not urn or not isinstance(urn, str):
        return None

    try:
        # Example URN: urn:li:dataset:(urn:li:dataPlatform:mssql,hr.compensation,PROD)
        # We want to extract "hr.compensation" (the dataset identifier)

        if urn.startswith("urn:li:dataset:"):
            # Remove the prefix and get the dataset part
            dataset_part = urn[len("urn:li:dataset:") :]

            # Handle both formats:
            # Format 1: (urn:li:dataPlatform:platform,dataset_name,env)
            # Format 2: urn:li:dataPlatform:platform,dataset_name,env
            if dataset_part.startswith("(") and dataset_part.endswith(")"):
                dataset_part = dataset_part[1:-1]  # Remove parentheses

            # Split by commas and get the middle part (dataset name)
            parts = dataset_part.split(",")
            if len(parts) >= 2:
                # The dataset name is typically the second part
                dataset_name = parts[1].strip()
                if dataset_name:
                    # Clean up the name - remove any remaining URN prefixes
                    if dataset_name.startswith("urn:li:dataPlatform:"):
                        # This shouldn't happen, but just in case
                        return None
                    return dataset_name

        # For other entity types, try to extract the last meaningful part
        elif ":" in urn:
            parts = urn.split(":")
            # Get the last non-empty part that might be the name
            for part in reversed(parts):
                if part and not part.startswith("urn") and not part.startswith("li"):
                    # Remove any trailing parentheses or environment indicators
                    clean_part = part.strip("()")
                    if clean_part and clean_part not in ["PROD", "DEV", "STAGING"]:
                        return clean_part

    except Exception as e:
        logger.warning(f"Error extracting name from URN {urn}: {e}")

    return None


def _get_entity_type_icon_url(entity_type: str, subtype: Optional[str] = None) -> str:
    """Get entity type icon URL following DataHub frontend patterns."""
    # Map subtypes to their SVG icons (using common icon URLs that work in Teams)
    subtype_icons = {
        "database": "https://cdn-icons-png.flaticon.com/24/2906/2906274.png",  # database icon
        "schema": "https://cdn-icons-png.flaticon.com/24/3334/3334886.png",  # schema icon
        "table": "https://cdn-icons-png.flaticon.com/24/3179/3179068.png",  # table icon
        "view": "https://cdn-icons-png.flaticon.com/24/2354/2354573.png",  # view icon
        "model": "https://cdn-icons-png.flaticon.com/24/3094/3094837.png",  # model icon
        "project": "https://cdn-icons-png.flaticon.com/24/3767/3767084.png",  # project icon
    }

    # Entity type fallback icons
    entity_type_icons = {
        "DATASET": "https://cdn-icons-png.flaticon.com/24/3179/3179068.png",  # table icon
        "CONTAINER": "https://cdn-icons-png.flaticon.com/24/3334/3334886.png",  # folder icon
        "DOMAIN": "https://cdn-icons-png.flaticon.com/24/3767/3767084.png",  # domain icon
        "DASHBOARD": "https://cdn-icons-png.flaticon.com/24/2041/2041389.png",  # dashboard icon
        "CHART": "https://cdn-icons-png.flaticon.com/24/3179/3179068.png",  # chart icon
    }

    # Try subtype first, then entity type
    if subtype:
        subtype_lower = subtype.lower()
        if subtype_lower in subtype_icons:
            return subtype_icons[subtype_lower]

    return entity_type_icons.get(entity_type, "")


def _build_breadcrumb_text(raw_entity: Dict[str, Any]) -> str:
    """Build breadcrumb text from parent containers, following DataHub frontend logic."""
    if not raw_entity or not isinstance(raw_entity, dict):
        return ""

    breadcrumb_parts = []

    # Build breadcrumb from parent containers (similar to frontend ContextPath logic)
    parent_containers = raw_entity.get("parentContainers", {})
    if isinstance(parent_containers, dict):
        containers = parent_containers.get("containers", [])
        if isinstance(containers, list):
            for container in containers:
                if isinstance(container, dict):
                    # Try to get the container name from various possible locations
                    container_name = None

                    # Check properties first (most common location)
                    container_props = container.get("properties", {})
                    if isinstance(container_props, dict):
                        container_name = container_props.get("name")

                    # If no name in properties, try other locations
                    if not container_name:
                        container_name = container.get("name")

                    if container_name:
                        breadcrumb_parts.append(container_name)

    return " > ".join(reversed(breadcrumb_parts)) if breadcrumb_parts else ""


def _build_breadcrumb_with_icons(raw_entity: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build breadcrumb items with icons from parent containers."""
    breadcrumb_items = []

    # Build breadcrumb from parent containers (similar to frontend ContextPath logic)
    parent_containers = raw_entity.get("parentContainers", {})
    if isinstance(parent_containers, dict):
        containers = parent_containers.get("containers", [])
        if isinstance(containers, list):
            for container in containers:
                if isinstance(container, dict):
                    # Try to get the container name from various possible locations
                    container_name = None

                    # Check properties first (most common location)
                    container_props = container.get("properties", {})
                    if isinstance(container_props, dict):
                        container_name = container_props.get("name")

                    # If no name in properties, try other locations
                    if not container_name:
                        container_name = container.get("name")

                    if container_name:
                        # Get container type and subtype for icon
                        container_type = container.get("type", "")
                        container_subtypes = container.get("subTypes", {})
                        container_subtype = None
                        if isinstance(container_subtypes, dict):
                            subtype_names = container_subtypes.get("typeNames", [])
                            if isinstance(subtype_names, list) and subtype_names:
                                container_subtype = subtype_names[0]

                        # Get icon URL
                        icon_url = _get_entity_type_icon_url(
                            container_type, container_subtype
                        )

                        breadcrumb_items.append(
                            {
                                "name": container_name,
                                "icon_url": icon_url,
                                "type": container_type,
                                "subtype": container_subtype,
                            }
                        )

    return list(reversed(breadcrumb_items)) if breadcrumb_items else []


def _get_platform_logo_url(raw_entity: Dict[str, Any]) -> str:
    """Get platform logo URL."""
    if not raw_entity or not isinstance(raw_entity, dict):
        return ""

    platform = raw_entity.get("platform")
    if not platform or not isinstance(platform, dict):
        return ""

    properties = platform.get("properties") or {}
    info = platform.get("info") or {}

    logo_url = ""
    if isinstance(properties, dict) and properties.get("logoUrl"):
        logo_url = properties.get("logoUrl", "")
    elif isinstance(info, dict) and info.get("logoUrl"):
        logo_url = info.get("logoUrl", "")

    if not logo_url:
        return ""

    # Convert relative URLs to absolute URLs using DataHub base URL
    if logo_url.startswith("/"):
        # Remove trailing slash from base URL if present, then add the relative path
        base_url = DATAHUB_FRONTEND_URL.rstrip("/")
        logo_url = f"{base_url}{logo_url}"
        logger.info(f"Converted relative logo URL to absolute: {logo_url}")

    # Teams requires absolute HTTPS URLs for images
    if _is_valid_teams_image_url(logo_url):
        logger.info(f"Using platform logo URL: {logo_url}")
        return logo_url
    else:
        logger.info(f"Rejected invalid logo URL: {logo_url}")

    return ""


def _get_platform_name(raw_entity: Dict[str, Any]) -> str:
    """Get platform name for display when logo is not available."""
    if not raw_entity or not isinstance(raw_entity, dict):
        return ""

    platform = raw_entity.get("platform")
    if not platform or not isinstance(platform, dict):
        return ""

    # Try to get platform name from properties or info
    properties = platform.get("properties") or {}
    info = platform.get("info") or {}

    # Check various name fields
    platform_name = None
    if isinstance(properties, dict):
        platform_name = properties.get("displayName") or properties.get("name")

    if not platform_name and isinstance(info, dict):
        platform_name = info.get("displayName") or info.get("name")

    # If still no name, try to extract from platform URN
    if not platform_name:
        platform_urn = platform.get("urn", "")
        if platform_urn and "dataPlatform:" in platform_urn:
            # Extract platform name from URN like "urn:li:dataPlatform:mssql"
            parts = platform_urn.split(":")
            if len(parts) >= 3:
                platform_name = parts[-1]  # Get the last part (platform identifier)

    # Clean up and format the platform name
    if platform_name:
        # Capitalize common platform names
        platform_map = {
            "mssql": "SQL Server",
            "mysql": "MySQL",
            "postgres": "PostgreSQL",
            "snowflake": "Snowflake",
            "bigquery": "BigQuery",
            "workday": "Workday",
            "databricks": "Databricks",
            "redshift": "Redshift",
            "tableau": "Tableau",
            "looker": "Looker",
        }
        return platform_map.get(platform_name.lower(), platform_name.title())

    return ""


def _is_valid_teams_image_url(url: str) -> bool:
    """Check if URL is valid for Teams adaptive cards."""
    if not url or not isinstance(url, str):
        return False

    # Teams requires absolute HTTPS URLs
    if not url.startswith("https://"):
        return False

    # Basic URL structure validation
    if len(url) < 10 or "." not in url:
        return False

    return True


def _get_entity_tags(raw_entity: Dict[str, Any]) -> List[str]:
    """Get entity tags and glossary terms for badges."""
    if not raw_entity or not isinstance(raw_entity, dict):
        return []

    tags = []

    # Add global tags
    global_tags = raw_entity.get("globalTags")
    if global_tags and isinstance(global_tags, dict):
        tags_list = global_tags.get("tags")
        if tags_list and isinstance(tags_list, list):
            for tag_entry in tags_list[:5]:  # Limit to 5 tags
                if tag_entry and isinstance(tag_entry, dict):
                    tag = tag_entry.get("tag")
                    if tag and isinstance(tag, dict):
                        tag_props = tag.get("properties") or {}
                        if isinstance(tag_props, dict):
                            tag_name = tag_props.get("name") or tag.get("name")
                            if tag_name:
                                tags.append(tag_name)

    # Add glossary terms with emoji prefix
    glossary_terms = raw_entity.get("glossaryTerms", {})
    if isinstance(glossary_terms, dict):
        terms_list = glossary_terms.get("terms", [])
        if isinstance(terms_list, list):
            for term_entry in terms_list[:5]:  # Limit to 5 terms
                if isinstance(term_entry, dict):
                    term = term_entry.get("term", {})
                    if isinstance(term, dict):
                        term_props = term.get("properties", {})
                        if isinstance(term_props, dict):
                            term_name = term_props.get("name") or term.get("name")
                            if term_name:
                                tags.append(f"📋 {term_name}")

    return tags


def _get_entity_terms(raw_entity: Dict[str, Any]) -> List[str]:
    """Get entity glossary terms for badges."""
    if not raw_entity:
        return []

    terms = []

    # Add glossary terms
    glossary_terms = raw_entity.get("glossaryTerms", {})
    if isinstance(glossary_terms, dict):
        terms_list = glossary_terms.get("terms", [])
        if isinstance(terms_list, list):
            for term_entry in terms_list[:5]:  # Limit to 5 terms
                if isinstance(term_entry, dict):
                    term = term_entry.get("term", {})
                    if isinstance(term, dict):
                        term_props = term.get("properties", {})
                        if isinstance(term_props, dict):
                            term_name = term_props.get("name") or term.get("name")
                            if term_name:
                                terms.append(f"📋 {term_name}")

    return terms


def _get_ownership_text(raw_entity: Dict[str, Any]) -> str:
    """Get ownership information."""
    if not raw_entity or not isinstance(raw_entity, dict):
        return ""

    ownership = raw_entity.get("ownership")
    if not ownership or not isinstance(ownership, dict):
        return ""

    owners = ownership.get("owners")
    if not owners or not isinstance(owners, list):
        return ""

    owner_names = []
    for owner_entry in owners[:2]:  # Limit to 2 owners
        if isinstance(owner_entry, dict):
            owner = owner_entry.get("owner", {})
            if isinstance(owner, dict):
                properties = owner.get("properties", {})
                info = owner.get("info", {})

                owner_name = None
                if isinstance(properties, dict):
                    owner_name = properties.get("displayName")
                if not owner_name and isinstance(info, dict):
                    owner_name = info.get("displayName")
                if not owner_name:
                    owner_name = owner.get("username") or owner.get("name")

                if owner_name:
                    owner_names.append(owner_name)

    if owner_names:
        return f"👤 {', '.join(owner_names)}"

    return ""


def _is_deprecated(raw_entity: Dict[str, Any]) -> bool:
    """Check if entity is deprecated."""
    if not raw_entity:
        return False

    deprecation = raw_entity.get("deprecation")
    if isinstance(deprecation, dict) and deprecation.get("deprecated"):
        return True

    return False


def _is_unhealthy(raw_entity: Dict[str, Any]) -> bool:
    """Check if entity has health issues."""
    if not raw_entity:
        return False

    health = raw_entity.get("health")
    if isinstance(health, list):
        for health_item in health:
            if isinstance(health_item, dict) and health_item.get("status") == "FAIL":
                return True
    elif isinstance(health, dict):
        # Handle single health status as dict
        if health.get("status") == "FAIL":
            return True

    return False


def _get_health_indicators(raw_entity: Dict[str, Any]) -> List[str]:
    """Get health indicator emojis for entity status."""
    indicators = []

    if _is_unhealthy(raw_entity):
        indicators.append("⚠️")

    if _is_deprecated(raw_entity):
        indicators.append("❗")

    return indicators


def _get_domain_info(raw_entity: Dict[str, Any]) -> Optional[str]:
    """Get domain information."""
    if not raw_entity:
        return None

    domain = raw_entity.get("domain")
    if isinstance(domain, dict):
        # Check if it's a domain association (nested structure)
        domain_entity = domain.get("domain")
        if isinstance(domain_entity, dict):
            domain_props = domain_entity.get("properties", {})
            if isinstance(domain_props, dict):
                domain_name = domain_props.get("name") or domain_props.get(
                    "displayName"
                )
                if domain_name:
                    return domain_name

        # Fallback: direct domain properties
        domain_props = domain.get("properties", {})
        if isinstance(domain_props, dict):
            domain_name = domain_props.get("name") or domain_props.get("displayName")
            if domain_name:
                return domain_name

    return None


def _get_usage_stats(raw_entity: Dict[str, Any]) -> Dict[str, Any]:
    """Get usage statistics (row count, column count, query count, etc)."""
    stats = {}

    # Row and column counts from profile
    if raw_entity.get("lastProfile") and len(raw_entity["lastProfile"]) > 0:
        profile = raw_entity["lastProfile"][0]
        if profile.get("rowCount") is not None:
            stats["rows"] = profile["rowCount"]
        if profile.get("columnCount") is not None:
            stats["columns"] = profile["columnCount"]

    # Usage statistics from statsSummary
    stats_summary = raw_entity.get("statsSummary", {})
    if isinstance(stats_summary, dict):
        if stats_summary.get("queryCountLast30Days") is not None:
            stats["queries_30d"] = stats_summary["queryCountLast30Days"]
        if stats_summary.get("viewCountLast30Days") is not None:
            stats["views_30d"] = stats_summary["viewCountLast30Days"]
        if stats_summary.get("uniqueUserCountLast30Days") is not None:
            stats["users_30d"] = stats_summary["uniqueUserCountLast30Days"]

    return stats


def _get_lineage_counts(raw_entity: Dict[str, Any]) -> Dict[str, Any]:
    """Get lineage information (upstream/downstream counts)."""
    lineage = {}

    upstream = raw_entity.get("upstream", {})
    if isinstance(upstream, dict) and upstream.get("total") is not None:
        lineage["upstreams"] = upstream["total"]

    downstream = raw_entity.get("downstream", {})
    if isinstance(downstream, dict) and downstream.get("total") is not None:
        lineage["downstreams"] = downstream["total"]

    return lineage


def _get_external_url(raw_entity: Dict[str, Any]) -> Optional[str]:
    """Get external URL for the entity."""
    properties = raw_entity.get("properties", {})
    info = raw_entity.get("info", {})

    # Try various external URL fields
    for source in [properties, info]:
        if isinstance(source, dict):
            for url_field in ["externalUrl", "dashboardUrl", "chartUrl"]:
                url = source.get(url_field)
                if url:
                    return url

    return None


def _format_number(num: int) -> str:
    """Format numbers with appropriate abbreviations."""
    if num >= 1_000_000:
        return f"{num / 1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num / 1_000:.1f}K"
    else:
        return str(num)


def _should_include_field(
    field: EntityCardRenderField,
    fields: Optional[List[EntityCardRenderField]],
    field_filtering: bool,
) -> bool:
    """Check if a field should be included based on field filtering logic."""
    return not field_filtering or (fields is not None and field in fields)


def _get_field_value_with_error_handling(
    field_name: str,
    field_func: Callable[[], Any],
    raw_entity: Dict[str, Any],
    urn: str,
    default_value: Any = None,
) -> Any:
    """Get field value with consistent error handling."""
    try:
        return field_func()
    except Exception as e:
        logger.warning(f"Error getting {field_name} for {urn}: {e}")
        return default_value


def _extract_entity_fields(
    raw_entity: Dict[str, Any], fields: Optional[List[EntityCardRenderField]], urn: str
) -> Dict[str, Any]:
    """Extract all entity fields with proper filtering and error handling."""
    field_filtering = fields is not None

    # Field extraction functions mapped to their field types
    field_extractors = {
        EntityCardRenderField.DESCRIPTION: lambda: raw_entity.get("properties", {}).get(
            "description"
        )
        or "No description available",
        EntityCardRenderField.TAG: lambda: _get_entity_tags(raw_entity),
        EntityCardRenderField.TERM: lambda: _get_entity_terms(raw_entity),
        EntityCardRenderField.OWNERSHIP: lambda: _get_ownership_text(raw_entity),
        EntityCardRenderField.DEPRECATED: lambda: _is_deprecated(raw_entity),
        EntityCardRenderField.UNHEALTHY: lambda: _is_unhealthy(raw_entity),
        EntityCardRenderField.HEALTH_INDICATORS: lambda: _get_health_indicators(
            raw_entity
        ),
        EntityCardRenderField.DOMAIN: lambda: _get_domain_info(raw_entity),
        EntityCardRenderField.USAGE_STATS: lambda: _get_usage_stats(raw_entity),
        EntityCardRenderField.LINEAGE_COUNTS: lambda: _get_lineage_counts(raw_entity),
    }

    # Default values for each field type
    default_values: Dict[EntityCardRenderField, Any] = {
        EntityCardRenderField.DESCRIPTION: None,
        EntityCardRenderField.TAG: [],
        EntityCardRenderField.TERM: [],
        EntityCardRenderField.OWNERSHIP: "",
        EntityCardRenderField.DEPRECATED: False,
        EntityCardRenderField.UNHEALTHY: False,
        EntityCardRenderField.HEALTH_INDICATORS: [],
        EntityCardRenderField.DOMAIN: None,
        EntityCardRenderField.USAGE_STATS: {},
        EntityCardRenderField.LINEAGE_COUNTS: {},
    }

    extracted_fields = {}

    # Extract each field if it should be included
    for field, extractor_func in field_extractors.items():
        if _should_include_field(field, fields, field_filtering):
            field_name = field.value
            default_value = default_values[field]
            extracted_fields[field_name] = _get_field_value_with_error_handling(
                field_name, extractor_func, raw_entity, urn, default_value
            )

    return extracted_fields


def render_entity_card(
    raw_entity: Optional[Dict[str, Any]],
    fields: Optional[List[EntityCardRenderField]] = None,
) -> Dict[str, Any]:
    """Render an entity as a Teams adaptive card with rich information.
    Args:
        raw_entity: The raw entity data.
        fields: The fields to render. If empty list, minimal card will be rendered. If None, all fields will be rendered.
    Returns:
        The Teams adaptive card.
    """

    if not raw_entity:
        return {}

    entity_type = raw_entity.get("type")
    urn = raw_entity.get("urn")
    properties = raw_entity.get("properties") or {}

    # Get basic entity information
    name = properties.get("name")
    if not name:
        # Extract name from URN if properties.name is missing
        name = _extract_name_from_urn(str(urn) if urn else "") or "Unknown"

    # Get the entity URL
    entity_url = (
        url_utils.get_type_url(entity_type, urn) if entity_type and urn else None
    )

    # Get enhanced information with error handling
    try:
        platform_logo_url = _get_platform_logo_url(raw_entity)
    except Exception as e:
        logger.warning(f"Error getting platform logo for {urn}: {e}")
        platform_logo_url = ""

    try:
        breadcrumb_text = _build_breadcrumb_text(raw_entity)
    except Exception as e:
        logger.warning(f"Error building breadcrumb for {urn}: {e}")
        breadcrumb_text = ""

    # Extract all fields using the helper function
    extracted_fields = _extract_entity_fields(raw_entity, fields, urn or "")

    # Assign extracted fields to variables for backward compatibility
    description = extracted_fields.get("description")
    if description and len(description) > 150:
        description = description[:147] + "..."

    tags = extracted_fields.get("tag", [])
    terms = extracted_fields.get("term", [])
    ownership_text = extracted_fields.get("ownership", "")
    health_indicators = extracted_fields.get("health_indicators", [])
    domain_info = extracted_fields.get("domain")
    usage_stats = extracted_fields.get("usage_stats", {})
    lineage_counts = extracted_fields.get("lineage_counts", {})

    try:
        external_url = _get_external_url(raw_entity)
    except Exception as e:
        logger.warning(f"Error getting external URL for {urn}: {e}")
        external_url = None
    # Build the header items
    header_items: List[Dict[str, Any]] = []

    # Add platform logo or platform name as fallback
    if platform_logo_url and _is_valid_teams_image_url(platform_logo_url):
        header_items.append(
            {
                "type": "Image",
                "url": platform_logo_url,
                "size": "Small",
                "height": "20px",
                "spacing": "None",
            }
        )
    else:
        # Fallback: show platform name if logo is not available/valid
        platform_name = _get_platform_name(raw_entity)
        if platform_name:
            header_items.append(
                {
                    "type": "TextBlock",
                    "text": f"🗄️ {platform_name}",
                    "size": "Small",
                    "color": "Accent",
                    "spacing": "None",
                    "weight": "Bolder",
                }
            )

    # Add entity name with status indicators
    entity_name_text = name

    # Add health indicators
    if health_indicators:
        entity_name_text = f"{' '.join(health_indicators)} {name}"

    header_items.append(
        {
            "type": "TextBlock",
            "text": entity_name_text,
            "weight": "Bolder",
            "size": "Medium",
            "color": "Default",  # Dark text for light grey background
            "spacing": "Small" if platform_logo_url else "None",
        }
    )

    # Add entity type - prioritize subtype over main entity type
    display_type = "Entity"

    # Try to get the subtype first (most specific)
    subtype = raw_entity.get("subTypes", {})
    if isinstance(subtype, dict):
        subtype_names = subtype.get("typeNames", [])
        if isinstance(subtype_names, list) and subtype_names:
            # Use the first subtype name and capitalize first letter only
            display_type = subtype_names[0].replace("_", " ").capitalize()

    # Fallback to main entity type if no subtype
    if display_type == "Entity" and entity_type:
        display_type = entity_type.replace("_", " ").title()

    logger.info(
        f"Entity type display logic: subtype={raw_entity.get('subTypes')}, final={display_type}"
    )

    header_items.append(
        {
            "type": "TextBlock",
            "text": display_type,
            "size": "Small",
            "color": "Default",  # Dark text for light grey background
            "spacing": "None",
        }
    )

    # Build body items with dark grey header
    # Using Container with 'emphasis' style for dark grey background
    body_items: List[Dict[str, Any]] = [
        {
            "type": "Container",
            "style": "emphasis",  # Dark grey background
            "items": header_items,
            "padding": "Default",
        }
    ]

    # Add breadcrumb if available
    if breadcrumb_text:
        body_items.append(
            {
                "type": "TextBlock",
                "text": f"📁 {breadcrumb_text}",
                "size": "Small",
                "color": "Accent",
                "spacing": "Small",
                "wrap": True,
            }
        )

    # Add domain info if available
    if domain_info:
        body_items.append(
            {
                "type": "TextBlock",
                "text": f"🏢 **Domain:** {domain_info}",
                "size": "Small",
                "color": "Accent",
                "spacing": "Small",
                "wrap": True,
            }
        )

    if description:
        # Add description
        body_items.append(
            {
                "type": "TextBlock",
                "text": description,
                "wrap": True,
                "spacing": "Medium",
            }
        )

    # Add ownership info with section header
    if ownership_text:
        body_items.append(
            {
                "type": "TextBlock",
                "text": ownership_text,
                "size": "Small",
                "color": "Default",
                "spacing": "Medium",
            }
        )

    # Add glossary terms section with header
    if terms:
        body_items.append(
            {
                "type": "TextBlock",
                "text": "**Terms**",
                "size": "Small",
                "weight": "Bolder",
                "color": "Default",
                "spacing": "Medium",
            }
        )

        # Join terms in a single text block
        terms_text = ", ".join(terms[:5])  # Show up to 5 terms
        body_items.append(
            {
                "type": "TextBlock",
                "text": terms_text,
                "size": "Small",
                "color": "Default",
                "spacing": "None",
                "wrap": True,
            }
        )

    # Add tags section with header
    if tags:
        body_items.append(
            {
                "type": "TextBlock",
                "text": "**Tags**",
                "size": "Small",
                "weight": "Bolder",
                "color": "Default",
                "spacing": "Medium",
            }
        )

        # Join tags in a single text block
        tags_text = ", ".join(tags[:5])  # Show up to 5 tags
        body_items.append(
            {
                "type": "TextBlock",
                "text": tags_text,
                "size": "Small",
                "color": "Default",
                "spacing": "None",
                "wrap": True,
            }
        )

    # Add usage statistics section
    if usage_stats:
        stats_items = []

        if "rows" in usage_stats:
            stats_items.append(f"📊 **Rows:** {_format_number(usage_stats['rows'])}")
        if "columns" in usage_stats:
            stats_items.append(
                f"📋 **Columns:** {_format_number(usage_stats['columns'])}"
            )
        if "queries_30d" in usage_stats:
            stats_items.append(
                f"🔍 **Queries (30d):** {_format_number(usage_stats['queries_30d'])}"
            )
        if "views_30d" in usage_stats:
            stats_items.append(
                f"👁️ **Views (30d):** {_format_number(usage_stats['views_30d'])}"
            )
        if "users_30d" in usage_stats:
            stats_items.append(
                f"👥 **Users (30d):** {_format_number(usage_stats['users_30d'])}"
            )

        if stats_items:
            body_items.append(
                {
                    "type": "TextBlock",
                    "text": "**Statistics**",
                    "size": "Small",
                    "weight": "Bolder",
                    "color": "Default",
                    "spacing": "Medium",
                }
            )

            # Show stats in chunks to avoid crowding
            stats_text = " • ".join(stats_items[:3])  # Show up to 3 stats
            body_items.append(
                {
                    "type": "TextBlock",
                    "text": stats_text,
                    "size": "Small",
                    "color": "Default",
                    "spacing": "None",
                    "wrap": True,
                }
            )

    # Add lineage information section
    if lineage_counts:
        lineage_items = []

        if "upstreams" in lineage_counts:
            lineage_items.append(
                f"⬆️ **Upstreams:** {_format_number(lineage_counts['upstreams'])}"
            )
        if "downstreams" in lineage_counts:
            lineage_items.append(
                f"⬇️ **Downstreams:** {_format_number(lineage_counts['downstreams'])}"
            )

        if lineage_items:
            body_items.append(
                {
                    "type": "TextBlock",
                    "text": "**Lineage**",
                    "size": "Small",
                    "weight": "Bolder",
                    "color": "Default",
                    "spacing": "Medium",
                }
            )

            lineage_text = " • ".join(lineage_items)
            body_items.append(
                {
                    "type": "TextBlock",
                    "text": lineage_text,
                    "size": "Small",
                    "color": "Default",
                    "spacing": "None",
                    "wrap": True,
                }
            )

    # Filter out None values and ensure all body items are valid
    valid_body_items = []
    for item in body_items:
        if item and isinstance(item, dict) and item.get("type"):
            # Ensure text fields don't have None values
            if "text" in item and item["text"] is None:
                item["text"] = ""
            valid_body_items.append(item)
        else:
            logger.warning(f"Skipping invalid body item: {item}")

    # Ensure we have at least one body item
    if not valid_body_items:
        logger.error(f"No valid body items for entity {urn}")
        return {}

    # Create Teams adaptive card
    card: dict[str, Any] = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.2",
            "body": valid_body_items,
        },
    }

    # Add actions if we have valid URLs
    actions = []

    # Primary action: View in DataHub
    if entity_url and isinstance(entity_url, str) and entity_url.strip():
        actions.append(
            {"type": "Action.OpenUrl", "title": "View in DataHub", "url": entity_url}
        )

    # Secondary action: View in external platform (if available)
    if external_url and isinstance(external_url, str) and external_url.strip():
        platform_name = _get_platform_name(raw_entity) or "Platform"
        actions.append(
            {
                "type": "Action.OpenUrl",
                "title": f"View in {platform_name}",
                "url": external_url,
            }
        )

    if actions:
        card["content"]["actions"] = actions

    # Log the full card structure for debugging
    logger.info(f"Generated Teams adaptive card for entity {urn}: {card}")

    return card


def render_entity_preview(
    raw_entity: Optional[Dict[str, Any]], include_link: bool = False
) -> Dict[str, Any]:
    """Render an entity preview for Teams."""

    if not raw_entity:
        return {"type": "message", "text": "Entity not found"}

    card = render_entity_card(raw_entity)

    return {"type": "message", "attachments": [card]}
