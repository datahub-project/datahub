"""Minimal elements of /openapi/v1/registry/models/entity/specifications, same shape as GMS."""

from typing import Any, Dict, List, Optional, Tuple


def searchable(
    path: str,
    field_name: str,
    *,
    tier: Optional[int] = None,
    query_by_default: bool = True,
    entity_name: bool = False,
    rich_text: bool = False,
    schema: Any = "string",
) -> Tuple[str, Dict[str, Any]]:
    return path, {
        "pathComponents": path.strip("/").split("/"),
        "fieldType": "SearchableFieldSpec",
        "pegasusSchema": schema,
        "annotations": {
            "searchableAnnotation": {
                "fieldName": field_name,
                "searchTier": tier,
                "queryByDefault": query_by_default,
                "fieldNameAliases": ["_entityName"] if entity_name else [],
                "sanitizeRichText": rich_text,
            }
        },
    }


def aspect(
    name: str, *fields: Tuple[str, Dict[str, Any]], timeseries: bool = False
) -> Dict[str, Any]:
    return {
        "aspectAnnotation": {"name": name, "timeseries": timeseries},
        "searchableFieldSpec": dict(fields),
    }


def entity(
    name: str,
    key_aspect: Dict[str, Any],
    aspects: List[Dict[str, Any]],
    field_types: Dict[str, List[str]],
    search_group: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "name": name,
        "entityAnnotation": {"name": name, "searchGroup": search_group},
        "keyAspectName": key_aspect["aspectAnnotation"]["name"],
        "keyAspectSpec": key_aspect,
        "aspectSpecs": aspects,
        "searchableFieldTypes": field_types,
    }


DATASET = entity(
    "dataset",
    aspect(
        "datasetKey",
        searchable("/name", "id"),
        searchable("/platform", "platform"),
    ),
    [
        aspect(
            "datasetProperties",
            # Ordered as GMS serves them, which is arbitrary: qualifiedName before name.
            searchable("/qualifiedName", "qualifiedName", tier=1),
            searchable("/name", "name", tier=1, entity_name=True),
            searchable("/description", "description", tier=2, rich_text=True),
            searchable(
                "/customProperties",
                "customProperties",
                schema={"type": "map", "values": "string"},
            ),
            searchable("/externalUrl", "externalUrl", query_by_default=False),
        ),
        aspect(
            "editableDatasetProperties",
            searchable("/description", "editedDescription", tier=2),
        ),
        aspect(
            "schemaMetadata",
            searchable("/fields/*/fieldPath", "fieldPaths"),
            searchable("/fields/*/description", "fieldDescriptions", rich_text=True),
            searchable("/fields/*/globalTags/tags/*/tag", "fieldTags"),
        ),
        aspect(
            "editableSchemaMetadata",
            searchable(
                "/editableSchemaFieldInfo/*/description", "editedFieldDescriptions"
            ),
        ),
        aspect("globalTags", searchable("/tags/*/tag", "tags")),
        aspect("domains", searchable("/domains/*", "domains")),
        aspect("container", searchable("/container", "container")),
        aspect(
            "ownership",
            searchable("/owners/*/owner", "owners", query_by_default=False),
        ),
        aspect(
            "siblings", searchable("/siblings/*", "siblings", query_by_default=False)
        ),
        aspect("dataPlatformInstance"),
        aspect("datasetProfile", timeseries=True),
        aspect("semanticContent"),
    ],
    {
        "id": ["WORD_GRAM"],
        "platform": ["URN"],
        "name": ["WORD_GRAM"],
        "qualifiedName": ["WORD_GRAM"],
        "description": ["TEXT"],
        "editedDescription": ["TEXT"],
        "customProperties": ["TEXT"],
        "externalUrl": ["KEYWORD"],
        "fieldPaths": ["TEXT"],
        "fieldDescriptions": ["TEXT"],
        "fieldTags": ["URN"],
        "editedFieldDescriptions": ["TEXT"],
        "tags": ["URN"],
        "domains": ["URN"],
        "container": ["URN"],
        "owners": ["URN"],
        "siblings": ["URN"],
    },
)

TAG = entity(
    "tag",
    aspect("tagKey", searchable("/name", "id")),
    [
        aspect(
            "tagProperties",
            searchable("/name", "name", tier=1, entity_name=True),
            searchable("/description", "description"),
        )
    ],
    {"id": ["WORD_GRAM"], "name": ["WORD_GRAM"], "description": ["TEXT"]},
)

DOMAIN = entity(
    "domain",
    aspect("domainKey"),
    [
        aspect(
            "domainProperties", searchable("/name", "name", tier=1, entity_name=True)
        ),
        aspect("semanticContent"),
    ],
    {"name": ["WORD_GRAM"]},
)

CONTAINER = entity(
    "container",
    aspect("containerKey"),
    [
        aspect(
            "containerProperties",
            searchable("/name", "name", tier=1, entity_name=True),
        ),
        aspect("dataPlatformInstance"),
        aspect("semanticContent"),
    ],
    {"name": ["WORD_GRAM"]},
)

DOCUMENT = entity(
    "document",
    aspect("documentKey"),
    [
        aspect("documentInfo", searchable("/title", "title", tier=1, entity_name=True)),
        aspect("semanticContent"),
    ],
    {"title": ["WORD_GRAM"]},
)

DATA_PROCESS_INSTANCE = entity(
    "dataProcessInstance",
    aspect("dataProcessInstanceKey"),
    [
        aspect(
            "dataProcessInstanceProperties",
            searchable("/name", "name", tier=1, entity_name=True),
        ),
        aspect("semanticContent"),
    ],
    {"name": ["WORD_GRAM"]},
    search_group="timeseries",
)

CHART = entity(
    "chart",
    aspect("chartKey"),
    [aspect("chartInfo", searchable("/title", "title", tier=1, entity_name=True))],
    {"title": ["WORD_GRAM"]},
)

REGISTRY = [DATASET, TAG, DOMAIN, CONTAINER, DOCUMENT, DATA_PROCESS_INSTANCE, CHART]
