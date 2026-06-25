import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator

from datahub.emitter.mcp_builder import ContainerKey

MSTRDict = Dict[str, Any]


class MicroStrategyBaseModel(BaseModel):
    model_config = ConfigDict(
        coerce_numbers_to_str=True,
        populate_by_name=True,
        extra="allow",
    )


def _first_str(data: MSTRDict, keys: Iterable[str]) -> Optional[str]:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
        if value is not None and not isinstance(value, (dict, list)):
            return str(value)
    return None


class Project(MicroStrategyBaseModel):
    id: str
    name: str
    description: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def normalize(cls, data: Any) -> MSTRDict:
        if isinstance(data, dict):
            result = dict(data)
            result["id"] = _first_str(result, ["id", "projectId", "project_id"])
            result["name"] = _first_str(result, ["name", "projectName"]) or result["id"]
            return result
        return data


class MSTRObject(MicroStrategyBaseModel):
    id: str
    name: str
    type: Optional[str] = None
    subtype: Optional[str] = None
    description: Optional[str] = None
    owner: Optional[str] = None
    date_created: Optional[str] = Field(default=None, alias="dateCreated")
    date_modified: Optional[str] = Field(default=None, alias="dateModified")

    @model_validator(mode="before")
    @classmethod
    def normalize(cls, data: Any) -> MSTRDict:
        if isinstance(data, dict):
            result = dict(data)
            result["id"] = _first_str(result, ["id", "objectId"])
            result["name"] = _first_str(result, ["name", "title"]) or result["id"]
            owner = result.get("owner")
            if isinstance(owner, dict):
                result["owner"] = _first_str(owner, ["username", "name", "id"])
            return result
        return data


class DatasetObject(MicroStrategyBaseModel):
    id: str
    name: str
    description: Optional[str] = None
    available_objects: MSTRDict = Field(default_factory=dict, alias="availableObjects")
    object_ids: List[str] = Field(default_factory=list)
    source_warehouse: Optional["DatasourceReference"] = Field(
        default=None,
        alias="sourceWarehouse",
    )
    warehouse_upstream_urns: List[str] = Field(default_factory=list)
    raw: MSTRDict = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def normalize(cls, data: Any) -> MSTRDict:
        if isinstance(data, dict):
            result = dict(data)
            result["id"] = _first_str(result, ["id", "objectId", "datasetId"])
            result["name"] = _first_str(result, ["name", "title"]) or result["id"]
            result["availableObjects"] = _normalize_available_objects(
                result.get("availableObjects") or result.get("available_objects")
            )
            result["object_ids"] = _extract_object_ids(result)
            result["sourceWarehouse"] = _extract_datasource_reference(result)
            result["raw"] = data
            return result
        return data


class DatasourceReference(MicroStrategyBaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    datasource_type: Optional[str] = Field(default=None, alias="datasourceType")
    database_type: Optional[str] = Field(default=None, alias="databaseType")
    database_version: Optional[str] = Field(default=None, alias="databaseVersion")
    dbms_name: Optional[str] = Field(default=None, alias="dbmsName")
    connection_id: Optional[str] = Field(default=None, alias="connectionId")
    connection_name: Optional[str] = Field(default=None, alias="connectionName")
    connection_embedded: Optional[bool] = Field(
        default=None,
        alias="connectionEmbedded",
    )
    database_name: Optional[str] = Field(default=None, alias="databaseName")
    schema_name: Optional[str] = Field(default=None, alias="schemaName")

    @model_validator(mode="before")
    @classmethod
    def normalize(cls, data: Any) -> MSTRDict:
        if isinstance(data, dict):
            return _normalize_datasource_reference(data)
        return data


class Datasource(DatasourceReference):
    id: str
    name: str


class DatasourceConnection(MicroStrategyBaseModel):
    id: str
    name: str
    driver_type: Optional[str] = Field(default=None, alias="driverType")
    database_type: Optional[str] = Field(default=None, alias="databaseType")
    database_version: Optional[str] = Field(default=None, alias="databaseVersion")
    connection_string_present: bool = Field(
        default=False,
        alias="connectionStringPresent",
    )
    database_name: Optional[str] = Field(default=None, alias="databaseName")
    schema_name: Optional[str] = Field(default=None, alias="schemaName")

    @model_validator(mode="before")
    @classmethod
    def normalize(cls, data: Any) -> MSTRDict:
        if isinstance(data, dict):
            database = data.get("database")
            if not isinstance(database, dict):
                database = {}
            connection_string = str(data.get("connectionString") or "")
            result = dict(data)
            result["id"] = _first_str(result, ["id", "objectId"])
            result["name"] = _first_str(result, ["name", "title"]) or result["id"]
            result["driverType"] = _first_str(result, ["driverType", "driver"])
            result["databaseType"] = _first_str(database, ["type", "databaseType"])
            result["databaseVersion"] = _first_str(
                database, ["version", "databaseVersion"]
            )
            result["databaseName"] = _first_str(
                result, ["databaseName", "database", "catalog"]
            ) or _first_str(database, ["name", "databaseName", "catalog"])
            result["schemaName"] = _first_str(
                result, ["schemaName", "schema", "databaseSchema"]
            ) or _first_str(database, ["schema", "schemaName", "databaseSchema"])
            result["databaseName"] = result["databaseName"] or _connection_param(
                connection_string, "DATABASE", "databaseName", "db", "catalog"
            )
            result["schemaName"] = result["schemaName"] or _connection_param(
                connection_string,
                "schema",
                "currentSchema",
                "CURRENT_SCHEMA",
                "searchpath",
                "search_path",
            )
            result["connectionStringPresent"] = bool(connection_string)
            result.pop("connectionString", None)
            return result
        return data


class Visualization(MicroStrategyBaseModel):
    key: str
    name: str
    type: Optional[str] = None
    chapter_key: Optional[str] = Field(default=None, alias="chapterKey")
    page_key: Optional[str] = Field(default=None, alias="pageKey")
    datasets: List[str] = Field(default_factory=list)
    object_ids: List[str] = Field(default_factory=list)
    raw: MSTRDict = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def normalize(cls, data: Any) -> MSTRDict:
        if isinstance(data, dict):
            result = dict(data)
            key = _first_str(
                result,
                [
                    "key",
                    "id",
                    "objectId",
                    "visualizationKey",
                    "nodeKey",
                    "definitionKey",
                ],
            )
            if not key:
                seed = json.dumps(data, sort_keys=True, default=str)
                key = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
            result["key"] = key
            result["name"] = _first_str(
                result,
                ["name", "title", "displayName"],
            ) or f"Visualization {key}"
            result["type"] = _first_str(
                result,
                ["type", "visualizationType"],
            )
            result["datasets"] = _extract_dataset_ids(result)
            result["object_ids"] = _extract_object_ids(result)
            result["raw"] = data
            return result
        return data


class DashboardDefinition(MicroStrategyBaseModel):
    id: str
    name: str
    description: Optional[str] = None
    datasets: List[DatasetObject] = Field(default_factory=list)
    visualizations: List[Visualization] = Field(default_factory=list)
    dependencies: List[MSTRObject] = Field(default_factory=list)
    raw: MSTRDict = Field(default_factory=dict)

    @classmethod
    def from_api_response(
        cls,
        object_id: str,
        object_name: str,
        response: MSTRDict,
        description: Optional[str] = None,
    ) -> "DashboardDefinition":
        definition = _unwrap_definition(response)
        datasets = [
            DatasetObject.model_validate(dataset)
            for dataset in _extract_datasets(definition)
            if isinstance(dataset, dict)
        ]
        visualizations = [
            Visualization.model_validate(visualization)
            for visualization in _extract_visualizations(definition)
            if isinstance(visualization, dict)
        ]
        return cls(
            id=object_id,
            name=object_name,
            description=description,
            datasets=datasets,
            visualizations=visualizations,
            raw=definition,
        )


class ProjectKey(ContainerKey):
    project_id: str


class FolderKey(ProjectKey):
    folder_path: str


def _unwrap_definition(response: MSTRDict) -> MSTRDict:
    result = response.get("result", response)
    if isinstance(result, dict):
        definition = result.get("definition", result)
        if isinstance(definition, dict):
            return definition
    return response


def _extract_datasets(definition: MSTRDict) -> List[MSTRDict]:
    datasets = definition.get("datasets")
    if isinstance(datasets, list):
        return [dataset for dataset in datasets if isinstance(dataset, dict)]
    if isinstance(datasets, dict):
        values = datasets.get("datasets") or datasets.get("items") or datasets.values()
        if isinstance(values, list):
            return [dataset for dataset in values if isinstance(dataset, dict)]
        return [dataset for dataset in values if isinstance(dataset, dict)]
    return []


def _normalize_available_objects(value: Any) -> MSTRDict:
    if isinstance(value, dict):
        return value
    if not isinstance(value, list):
        return {}

    metrics: List[MSTRDict] = []
    attributes: List[MSTRDict] = []
    others: List[MSTRDict] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        object_type = str(
            item.get("type") or item.get("objectType") or item.get("subtype") or ""
        ).lower()
        if "metric" in object_type:
            metrics.append(item)
        elif "attribute" in object_type:
            attributes.append(item)
        else:
            others.append(item)

    normalized: MSTRDict = {}
    if metrics:
        normalized["metrics"] = metrics
    if attributes:
        normalized["attributes"] = attributes
    if others:
        normalized["objects"] = others
    return normalized


def _normalize_datasource_reference(data: MSTRDict) -> MSTRDict:
    database = data.get("database")
    if not isinstance(database, dict):
        database = {}
    dbms = data.get("dbms")
    if not isinstance(dbms, dict):
        dbms = {}
    connection = data.get("connection")
    if not isinstance(connection, dict):
        connection = database.get("connection")
    if not isinstance(connection, dict):
        connection = {}

    result = dict(data)
    result["id"] = _first_str(result, ["id", "objectId", "datasourceId"])
    result["name"] = _first_str(result, ["name", "title"]) or result.get("id")
    result["datasourceType"] = _first_str(
        result, ["datasourceType", "sourceType", "type"]
    )
    result["databaseType"] = _first_str(database, ["type", "databaseType"])
    result["databaseVersion"] = _first_str(database, ["version", "databaseVersion"])
    result["dbmsName"] = _first_str(dbms, ["name", "type"])
    result["connectionId"] = _first_str(connection, ["id", "objectId"])
    result["connectionName"] = _first_str(connection, ["name", "title"])
    result["databaseName"] = _first_str(
        result, ["databaseName", "database", "catalog"]
    ) or _first_str(database, ["name", "databaseName", "catalog"])
    result["schemaName"] = _first_str(
        result, ["schemaName", "schema", "databaseSchema"]
    ) or _first_str(database, ["schema", "schemaName", "databaseSchema"])
    embedded = connection.get("embedded")
    if embedded is None:
        embedded = connection.get("isEmbedded")
    if isinstance(embedded, bool):
        result["connectionEmbedded"] = embedded
    return result


def _connection_param(connection_string: str, *param_names: str) -> Optional[str]:
    if not connection_string:
        return None
    alternatives = "|".join(re.escape(name) for name in param_names)
    match = re.search(
        rf"(?:^|[;,\s])(?:{alternatives})\s*=\s*([^;&,\s}}]+)",
        connection_string,
        re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).strip().strip("'\"")


def _looks_like_datasource_reference(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    database = value.get("database")
    dbms = value.get("dbms")
    connection = value.get("connection")
    return any(
        [
            "datasourceType" in value,
            "sourceType" in value,
            isinstance(database, dict)
            and any(key in database for key in ("type", "databaseType", "connection")),
            isinstance(dbms, dict) and any(key in dbms for key in ("name", "type")),
            isinstance(connection, dict)
            and any(key in connection for key in ("id", "name", "embedded")),
        ]
    )


def _extract_datasource_reference(data: MSTRDict) -> Optional[MSTRDict]:
    for key in (
        "sourceWarehouse",
        "warehouse",
        "sourceDatasource",
        "sourceDataSource",
        "dataSource",
        "datasource",
    ):
        value = data.get(key)
        if _looks_like_datasource_reference(value):
            return value
    if _looks_like_datasource_reference(data):
        return data
    return None


def _extract_visualizations(definition: MSTRDict) -> List[MSTRDict]:
    found: List[MSTRDict] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            maybe_visualizations = value.get("visualizations")
            if isinstance(maybe_visualizations, list):
                found.extend(
                    item for item in maybe_visualizations if isinstance(item, dict)
                )
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    chapters = definition.get("chapters")
    if isinstance(chapters, list):
        for chapter in chapters:
            if not isinstance(chapter, dict):
                continue
            chapter_key = _first_str(chapter, ["key", "id"])
            pages = chapter.get("pages")
            if not isinstance(pages, list):
                continue
            for page in pages:
                if not isinstance(page, dict):
                    continue
                page_key = _first_str(page, ["key", "id"])
                visualizations = page.get("visualizations")
                if not isinstance(visualizations, list):
                    continue
                for visualization in visualizations:
                    if not isinstance(visualization, dict):
                        continue
                    annotated = dict(visualization)
                    if chapter_key:
                        annotated["chapterKey"] = chapter_key
                    if page_key:
                        annotated["pageKey"] = page_key
                    found.append(annotated)

    visit(definition.get("chapters", definition))

    unique: Dict[str, MSTRDict] = {}
    for item in found:
        parsed = Visualization.model_validate(item)
        unique.setdefault(parsed.key, item)
    return list(unique.values())


def _extract_dataset_ids(value: Any) -> List[str]:
    dataset_ids: List[str] = []

    def add(candidate: Any) -> None:
        if isinstance(candidate, str) and candidate:
            dataset_ids.append(candidate)
        elif isinstance(candidate, dict):
            candidate_id = _first_str(
                candidate, ["id", "objectId", "datasetId", "dataSetId"]
            )
            if candidate_id:
                dataset_ids.append(candidate_id)

    def visit(node: Any, parent_key: Optional[str] = None) -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                lowered = key.lower()
                if lowered in {
                    "dataset",
                    "datasets",
                    "datasetid",
                    "datasetids",
                    "datasetkey",
                    "datasources",
                    "datasource",
                }:
                    if isinstance(child, list):
                        for item in child:
                            add(item)
                    else:
                        add(child)
                else:
                    visit(child, key)
        elif isinstance(node, list):
            for child in node:
                visit(child, parent_key)
        elif parent_key and re.search("dataset", parent_key, re.IGNORECASE):
            add(node)

    visit(value)
    return sorted(set(dataset_ids))


def _extract_object_ids(value: Any) -> List[str]:
    object_ids: List[str] = []

    def visit(node: Any, parent_key: Optional[str] = None) -> None:
        if isinstance(node, dict):
            node_type = str(node.get("type") or node.get("objectType") or "").lower()
            parent = (parent_key or "").lower()
            if node_type in {"metric", "attribute", "templateMetrics".lower()}:
                object_id = _first_str(node, ["id", "objectId"])
                if object_id and object_id != "00000000000000000000000000000000":
                    object_ids.append(object_id)
            elif parent in {"metric", "metrics", "attribute", "attributes"}:
                object_id = _first_str(node, ["id", "objectId"])
                if object_id and object_id != "00000000000000000000000000000000":
                    object_ids.append(object_id)
            for child_key, child in node.items():
                visit(child, str(child_key))
        elif isinstance(node, list):
            for child in node:
                visit(child, parent_key)

    visit(value)
    return sorted(set(object_ids))


def extract_folder_parts(raw_object: MSTRDict) -> Tuple[Optional[str], List[str]]:
    folder = raw_object.get("folder") or raw_object.get("location")
    if isinstance(folder, dict):
        folder_id = _first_str(folder, ["id", "objectId"])
        path = _first_str(folder, ["path", "name"])
    else:
        folder_id = None
        path = str(folder) if folder else None
    parts = [part for part in (path or "").strip("/").split("/") if part]
    return folder_id, parts
