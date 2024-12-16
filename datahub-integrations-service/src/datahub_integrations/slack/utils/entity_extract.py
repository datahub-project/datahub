import logging
from datetime import datetime, timezone
from typing import List, Optional

from datahub_integrations.app import DATAHUB_FRONTEND_URL

logger = logging.getLogger(__name__)


class ExtractedEntity:
    urn: str
    entity_type: str
    name: str
    type: str
    profile_url: Optional[str]
    platform: Optional[str]
    platform_url: Optional[str]
    path_entities: Optional[List[dict]]
    description: Optional[str]
    owners: List[str]
    tags: List[str]
    glossary_terms: List[str]
    domain: Optional[str]
    downstream_count: Optional[int]
    upstream_count: Optional[int]
    asset_count: Optional[int]
    query_count: Optional[int]
    view_count: Optional[int]
    unique_user_count: Optional[int]
    row_count: Optional[int]
    column_count: Optional[int]
    usage_level: Optional[str]
    last_updated_time: Optional[datetime]
    last_ingested_time: Optional[datetime]
    external_url: Optional[str]
    is_unhealthy: Optional[bool]
    is_passing_assertions: Optional[bool]
    health: Optional[List[dict]]
    is_deprecated: Optional[bool]

    def __init__(self, entity: dict):
        self.urn = entity["urn"]
        self.entity_type = entity["type"]
        self.name = extract_name(entity)
        self.type = extract_type(entity)
        self.profile_url = extract_profile_url(entity)
        self.platform = extract_platform(entity)
        self.platform_url = extract_platform_url(entity)
        self.path_entities = extract_path_entities(entity)
        self.description = extract_description(entity)
        self.owners = extract_owners(entity)
        self.tags = extract_tags(entity)
        self.glossary_terms = extract_terms(entity)
        self.domain = extract_domain(entity)
        self.downstream_count = extract_downstream_count(entity)
        self.upstream_count = extract_upstream_count(entity)
        self.asset_count = extract_asset_count(entity)
        self.query_count = extract_query_count(entity)
        self.view_count = extract_view_count(entity)
        self.unique_user_count = extract_unique_user_count(entity)
        self.row_count = extract_row_count(entity)
        self.column_count = extract_column_count(entity)
        self.usage_level = extract_usage_level(entity)
        self.last_updated_time = extract_last_modified(entity)
        self.last_ingested_time = extract_last_ingested(entity)
        self.external_url = extract_external_url(entity)
        self.health = extract_health(entity)
        self.is_unhealthy = extract_is_unhealthy(entity)
        self.is_passing_assertions = extract_is_passing_assertions(entity)
        self.is_deprecated = extract_deprecation(entity)


def extract_name(entity: dict) -> str:
    if (entity.get("editableProperties") or {}).get("displayName") is not None:
        return entity["editableProperties"]["displayName"]
    if (entity.get("properties") or {}).get("displayName") is not None:
        return entity["properties"]["displayName"]
    if (entity.get("properties") or {}).get("name") is not None:
        return entity["properties"]["name"]
    if (entity.get("info") or {}).get("name") is not None:
        return entity["info"]["name"]
    if entity.get("name") is not None:
        return entity["name"]
    logger.warning(f"Could not extract name from: {entity}. Returning 'unknown'")
    return "Unknown"


def extract_profile_url(entity: dict) -> Optional[str]:
    return get_type_url(entity["type"], entity["urn"])


def extract_type(entity: dict) -> str:
    sub_types = entity.get("subTypes")
    if sub_types and sub_types.get("typeNames"):
        type_names = sub_types.get("typeNames")
        if type_names and len(type_names) > 0:
            return type_names[0]
    return get_type_display_name(entity["type"])


def extract_platform_entity(entity: dict) -> Optional[dict]:
    # Case 1: Check the platform field.
    if entity.get("platform"):
        return entity["platform"]

    # Case 2: Check the data platform instance field.
    if (entity.get("dataPlatformInstance") or {}).get("platform"):
        return entity["dataPlatformInstance"]["platform"]

    # Case 3: Check the parent data pipeline platform field.
    if (entity.get("dataFlow") or {}).get("platform"):
        return entity["dataFlow"]["platform"]

    return None


def extract_platform(entity: dict) -> Optional[str]:
    platform_entity = extract_platform_entity(entity)
    if platform_entity:
        return extract_name(platform_entity)
    return None


def extract_platform_url(entity: dict) -> Optional[str]:
    platform_entity = extract_platform_entity(entity)
    if platform_entity:
        platform_properties = platform_entity.get("properties")
        if platform_properties is not None:
            url = platform_properties.get("logoUrl")
            if ("https://" not in url) and (url[0] == "/"):
                return f"{DATAHUB_FRONTEND_URL}{url}"
            else:
                return url
    return None


def extract_path_entities(entity: dict) -> Optional[List[dict]]:
    return list(reversed((entity.get("parentContainers") or {}).get("containers", [])))


def extract_description(entity: dict) -> Optional[str]:
    if (entity.get("editableProperties") or {}).get("description", None):
        return entity["editableProperties"]["description"]
    if (entity.get("editableInfo") or {}).get("description", None):
        return entity["editableInfo"]["description"]
    if (entity.get("properties") or {}).get("description", None):
        return entity["properties"]["description"]
    if (entity.get("info") or {}).get("description", None):
        return entity["info"]["description"]
    return None


def extract_owners(entity: dict) -> List[str]:
    # Extract the "owner names from the entity"
    if entity.get("ownership") is None:
        return []
    return [
        extract_actor_name(owner["owner"])
        for owner in (entity.get("ownership") or {}).get("owners", [])
    ]


def extract_tags(entity: dict) -> List[str]:
    if entity.get("globalTags") is None:
        return []
    return [
        extract_name(tag["tag"])
        for tag in (entity.get("globalTags") or {}).get("tags", [])
    ]


def extract_terms(entity: dict) -> List[str]:
    if entity.get("glossaryTerms") is None:
        return []
    return [
        extract_name(term["term"])
        for term in (entity.get("glossaryTerms") or {}).get("terms", [])
    ]


def extract_domain(entity: dict) -> Optional[str]:
    if entity.get("domain", None):
        domain_wrapper = entity["domain"]
        if domain_wrapper.get("domain"):
            return extract_name(domain_wrapper["domain"])
    return None


def extract_asset_count(entity: dict) -> Optional[int]:
    if (entity.get("entities") or {}).get("total") is not None:
        return entity["entities"]["total"]
    return None


def extract_deprecation(entity: dict) -> Optional[bool]:
    if (entity.get("deprecation") or {}).get("deprecated"):
        return entity["deprecation"]["deprecated"]
    return None


def extract_query_count(entity: dict) -> Optional[int]:
    if (entity.get("statsSummary") or {}).get("queryCountLast30Days") is not None:
        return entity["statsSummary"]["queryCountLast30Days"]
    return None


def extract_view_count(entity: dict) -> Optional[int]:
    if (entity.get("statsSummary") or {}).get("viewCountLast30Days") is not None:
        return entity["statsSummary"]["viewCountLast30Days"]
    return None


def extract_unique_user_count(entity: dict) -> Optional[int]:
    if (entity.get("statsSummary") or {}).get("uniqueUserCountLast30Days") is not None:
        return entity["statsSummary"]["uniqueUserCountLast30Days"]
    return None


def extract_row_count(entity: dict) -> Optional[int]:
    if (
        entity.get("lastProfile") is not None
        and len(entity["lastProfile"]) > 0
        and entity["lastProfile"][0].get("rowCount") is not None
    ):
        return entity["lastProfile"][0]["rowCount"]
    return None


def extract_column_count(entity: dict) -> Optional[int]:
    if (
        entity.get("lastProfile") is not None
        and len(entity["lastProfile"]) > 0
        and entity["lastProfile"][0].get("columnCount") is not None
    ):
        return entity["lastProfile"][0]["columnCount"]
    return None


def extract_last_modified(entity: dict) -> Optional[datetime]:
    ts: Optional[int] = None
    if (entity.get("properties") or {}).get("lastModified", {}).get("time", None):
        ts = entity["properties"]["lastModified"]["time"]
    if (entity.get("info") or {}).get("lastModified", {}).get("time", None):
        ts = entity["info"]["lastModified"]["time"]
    # Datasets only
    if (
        entity.get("lastOperation") is not None
        and len(entity["lastOperation"]) > 0
        and entity["lastOperation"][0].get("lastUpdatedTimestamp")
    ):
        ts = entity["lastOperation"][0]["lastUpdatedTimestamp"]

    if ts:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return None


def extract_last_ingested(entity: dict) -> Optional[datetime]:
    if entity.get("lastIngested"):
        return datetime.fromtimestamp(entity["lastIngested"] / 1000, tz=timezone.utc)
    return None


def extract_external_url(entity: dict) -> Optional[str]:
    if (entity.get("properties") or {}).get("externalUrl", None):
        return entity["properties"]["externalUrl"]
    if (entity.get("info") or {}).get("externalUrl", None):
        return entity["info"]["externalUrl"]

    # Dashboards and Charts Only
    if (entity.get("properties") or {}).get("dashboardUrl", None):
        return entity["properties"]["dashboardUrl"]
    if (entity.get("properties") or {}).get("chartUrl", None):
        return entity["properties"]["chartUrl"]
    return None


def extract_health(entity: dict) -> Optional[List[dict]]:
    return entity.get("health", None)


def extract_downstream_count(entity: dict) -> Optional[int]:
    if (entity.get("downstream") or {}).get("total", None):
        return entity["downstream"]["total"]
    return None


def extract_upstream_count(entity: dict) -> Optional[int]:
    if (entity.get("upstream") or {}).get("total", None):
        return entity["upstream"]["total"]
    return None


def extract_actor_name(actor: dict) -> str:
    if (actor.get("editableProperties") or {}).get("displayName"):
        return actor["editableProperties"]["displayName"]
    if (actor.get("properties") or {}).get("displayName"):
        return actor["properties"]["displayName"]
    if (actor.get("properties") or {}).get("firstName") and (
        actor.get("properties") or {}
    ).get("lastName"):
        return f"{actor['properties']['firstName']} {actor['properties']['lastName']}"
    if actor.get("username"):
        return actor["username"]
    if actor.get("name"):
        return actor["name"]
    logger.warning(f"Could not extract actor name from: {actor}. Returning 'unknown'")
    return "Unknown"


def get_type_display_name(entity_type: str) -> str:
    if entity_type == "DATASET":
        return "Dataset"
    elif entity_type == "CHART":
        return "Chart"
    elif entity_type == "DASHBOARD":
        return "Dashboard"
    elif entity_type == "DATA_JOB":
        return "Task"
    elif entity_type == "DATA_FLOW":
        return "Pipeline"
    elif entity_type == "CONTAINER":
        return "Container"
    elif entity_type == "DOMAIN":
        return "Domain"
    elif entity_type == "DATA_PRODUCT":
        return "Data Product"
    elif entity_type == "GLOSSARY_TERM":
        return "Glossary Term"
    else:
        return entity_type


def get_type_url(entity_type: str, entity_urn: str) -> Optional[str]:
    if entity_type == "DATASET":
        return f"{DATAHUB_FRONTEND_URL}/dataset/{entity_urn}/"
    elif entity_type == "CHART":
        return f"{DATAHUB_FRONTEND_URL}/chart/{entity_urn}/"
    elif entity_type == "DASHBOARD":
        return f"{DATAHUB_FRONTEND_URL}/dashboard/{entity_urn}/"
    elif entity_type == "DATA_JOB":
        return f"{DATAHUB_FRONTEND_URL}/tasks/{entity_urn}/"
    elif entity_type == "DATA_FLOW":
        return f"{DATAHUB_FRONTEND_URL}/pipelines/{entity_urn}/"
    elif entity_type == "CONTAINER":
        return f"{DATAHUB_FRONTEND_URL}/container/{entity_urn}/"
    elif entity_type == "DOMAIN":
        return f"{DATAHUB_FRONTEND_URL}/domain/{entity_urn}/"
    elif entity_type == "DATA_PRODUCT":
        return f"{DATAHUB_FRONTEND_URL}/dataProduct/{entity_urn}/"
    elif entity_type == "GLOSSARY_TERM":
        return f"{DATAHUB_FRONTEND_URL}/glossaryTerm/{entity_urn}/"
    else:
        logger.warning(f"Unsupported entity type: {entity_type}")
    return None


def extract_usage_level(entity: dict) -> Optional[str]:
    queries_percentile: Optional[float] = None
    views_percentile: Optional[float] = None
    users_percentile: Optional[float] = None

    if (entity.get("statsSummary") or {}).get(
        "queryCountPercentileLast30Days"
    ) is not None:
        queries_percentile = entity["statsSummary"]["queryCountPercentileLast30Days"]

    if (entity.get("statsSummary") or {}).get(
        "uniqueUserPercentileLast30Days"
    ) is not None:
        users_percentile = entity["statsSummary"]["uniqueUserPercentileLast30Days"]

    if (entity.get("statsSummary") or {}).get(
        "viewCountPercentileLast30Days"
    ) is not None:
        users_percentile = entity["statsSummary"]["viewCountPercentileLast30Days"]

    # Datasets
    if queries_percentile is not None and users_percentile is not None:
        if queries_percentile > 80 or users_percentile > 80:
            return "High"
        if queries_percentile > 30 or users_percentile > 30:
            return "Med"
        if queries_percentile > 0 or users_percentile > 0:
            return "Low"

    # Dashboards
    if views_percentile is not None and users_percentile is not None:
        if views_percentile > 80 or users_percentile > 80:
            return "High"
        if views_percentile > 30 or users_percentile > 30:
            return "Med"
        if views_percentile > 0 or users_percentile > 0:
            return "Low"

    return None


def extract_is_unhealthy(entity: dict) -> Optional[bool]:
    health = extract_health(entity)

    # If no health status, return undefined.
    if health is None or len(health) == 0:
        return None

    # If any health status object in array is FAIL, return unhealthy
    for status in health:
        if status.get("status") == "FAIL":
            return True

    # If all health statuses are OK, return healthy
    return False


def extract_is_passing_assertions(entity: dict) -> Optional[bool]:
    health = extract_health(entity)

    if health is None or len(health) == 0:
        return False

    for status in health:
        if status.get("status") == "PASS" and status.get("type") == "ASSERTIONS":
            return True

    return False
