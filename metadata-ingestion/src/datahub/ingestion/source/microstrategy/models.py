import hashlib
import json
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
)

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_DATABASE_PARAM_RE,
    MSTR_DATASET_CONTAINER_KEYS,
    MSTR_DATASET_KEY_RE,
    MSTR_KEYS_DATABASE_NAME,
    MSTR_KEYS_DATABASE_NAME_NESTED,
    MSTR_KEYS_DATABASE_TYPE,
    MSTR_KEYS_DATABASE_VERSION,
    MSTR_KEYS_DATASET_ID,
    MSTR_KEYS_DATASET_OBJECT_ID,
    MSTR_KEYS_DATASOURCE_ID,
    MSTR_KEYS_DATASOURCE_REFERENCE,
    MSTR_KEYS_DATASOURCE_TYPE,
    MSTR_KEYS_DBMS_NAME,
    MSTR_KEYS_DISPLAY_NAME,
    MSTR_KEYS_DRIVER_TYPE,
    MSTR_KEYS_FOLDER_PATH,
    MSTR_KEYS_ID,
    MSTR_KEYS_KEY_ID,
    MSTR_KEYS_LIST_CONTAINERS,
    MSTR_KEYS_NAME,
    MSTR_KEYS_OWNER,
    MSTR_KEYS_PROJECT_ID,
    MSTR_KEYS_PROJECT_NAME,
    MSTR_KEYS_SCHEMA_NAME,
    MSTR_KEYS_SCHEMA_NAME_NESTED,
    MSTR_KEYS_SOURCE_ID,
    MSTR_KEYS_SOURCE_NAME,
    MSTR_KEYS_SOURCE_OBJECT,
    MSTR_KEYS_SOURCE_OBJECT_ID,
    MSTR_KEYS_VISUALIZATION_KEY,
    MSTR_KEYS_VISUALIZATION_TYPE,
    MSTR_NULL_OBJECT_ID,
    MSTR_OBJECT_ID_PARENT_KEYS,
    MSTR_OBJECT_TYPES,
    MSTR_SCHEMA_PARAM_RE,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.microstrategy.report import MicroStrategyReport

# Raw MicroStrategy REST payloads; shapes vary across server versions, so they
# stay untyped until normalized by the model validators below.
MicroStrategyDict = Dict[str, Any]

_ModelT = TypeVar("_ModelT", bound=BaseModel)


def _validate_items(
    model_cls: Type[_ModelT],
    items: Iterable[Any],
    context: str,
    report: Optional["MicroStrategyReport"],
) -> List[_ModelT]:
    """Validate embedded definition objects one at a time so a single malformed
    dataset or visualization degrades to a skipped item, not a skipped
    dashboard."""
    validated: List[_ModelT] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            validated.append(model_cls.model_validate(item))
        except ValidationError as error:
            if report is not None:
                report.report_malformed_object(f"{context}: {model_cls.__name__}")
                report.warning(
                    title="Skipped malformed MicroStrategy object",
                    message=(
                        "An embedded definition object did not match the "
                        "expected shape and was skipped."
                    ),
                    context=f"{context}, model={model_cls.__name__}",
                    exc=error,
                )
    return validated


class MicroStrategyBaseModel(BaseModel):
    model_config = ConfigDict(
        coerce_numbers_to_str=True,
        populate_by_name=True,
        extra="allow",
    )


def _first_str(data: MicroStrategyDict, keys: Iterable[str]) -> Optional[str]:
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
    def normalize(cls, data: Any) -> MicroStrategyDict:
        if isinstance(data, dict):
            result = dict(data)
            result["id"] = _first_str(result, MSTR_KEYS_PROJECT_ID)
            result["name"] = _first_str(result, MSTR_KEYS_PROJECT_NAME) or result["id"]
            return result
        return data


class MicroStrategyObject(MicroStrategyBaseModel):
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
    def normalize(cls, data: Any) -> MicroStrategyDict:
        if isinstance(data, dict):
            result = dict(data)
            result["id"] = _first_str(result, MSTR_KEYS_ID)
            result["name"] = _first_str(result, MSTR_KEYS_NAME) or result["id"]
            owner = result.get("owner")
            if isinstance(owner, dict):
                result["owner"] = _first_str(owner, MSTR_KEYS_OWNER)
            return result
        return data


class MetricEnrichment(MicroStrategyBaseModel):
    """Metric model details fetched separately from the modeling API and joined
    onto dataset metrics by metric ID."""

    expression_text: Optional[str] = None
    expression_tokens: Optional[str] = None
    fact_ids: List[str] = Field(default_factory=list)


class ModelTablesResponse(MicroStrategyBaseModel):
    """Envelope for the model-tables listing. Only the outer shape is stable;
    each table's nested modeling structure varies by server version and is
    parsed by the lineage helpers, so tables stay untyped payloads here."""

    # None (key absent) is kept distinct from an empty list so the pagination
    # loop can tell "unrecognized response shape" from "empty page".
    tables: Optional[List[MicroStrategyDict]] = None
    total: Optional[int] = None


class SqlView(MicroStrategyBaseModel):
    """SQL-view response for a report instance. The statement can arrive under
    any of several keys and older servers nest it under `result`, so the lookup
    checks the top level then recurses once into `result`."""

    sql_statement: Optional[str] = Field(default=None, alias="sqlStatement")
    sql: Optional[str] = None
    statement: Optional[str] = None
    result: Optional["SqlView"] = None

    def get_statement(self) -> str:
        for value in (self.sql_statement, self.sql, self.statement):
            if isinstance(value, str) and value:
                return value
        if self.result is not None:
            return self.result.get_statement()
        return ""


class DatasetObject(MicroStrategyBaseModel):
    id: str
    name: str
    description: Optional[str] = None
    available_objects: MicroStrategyDict = Field(
        default_factory=dict, alias="availableObjects"
    )
    object_ids: List[str] = Field(default_factory=list)
    source_warehouse: Optional["DatasourceReference"] = Field(
        default=None,
        alias="sourceWarehouse",
    )
    warehouse_upstream_urns: List[str] = Field(default_factory=list)
    field_warehouse_upstreams: Dict[str, List[str]] = Field(default_factory=dict)
    # Keyed by normalized (upper-cased) metric object ID.
    metric_enrichments: Dict[str, MetricEnrichment] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def normalize(cls, data: Any) -> MicroStrategyDict:
        if isinstance(data, dict):
            result = dict(data)
            result["id"] = _first_str(result, MSTR_KEYS_DATASET_OBJECT_ID)
            result["name"] = _first_str(result, MSTR_KEYS_NAME) or result["id"]
            result["availableObjects"] = _normalize_available_objects(
                result.get("availableObjects") or result.get("available_objects")
            )
            result["object_ids"] = _extract_object_ids(result)
            result["sourceWarehouse"] = _extract_datasource_reference(result)
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
    def normalize(cls, data: Any) -> MicroStrategyDict:
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
    def normalize(cls, data: Any) -> MicroStrategyDict:
        if isinstance(data, dict):
            database = data.get("database")
            if not isinstance(database, dict):
                database = {}
            connection_string = str(data.get("connectionString") or "")
            result = dict(data)
            result["id"] = _first_str(result, MSTR_KEYS_ID)
            result["name"] = _first_str(result, MSTR_KEYS_NAME) or result["id"]
            result["driverType"] = _first_str(result, MSTR_KEYS_DRIVER_TYPE)
            result["databaseType"] = _first_str(database, MSTR_KEYS_DATABASE_TYPE)
            result["databaseVersion"] = _first_str(database, MSTR_KEYS_DATABASE_VERSION)
            result["databaseName"] = _first_str(
                result, MSTR_KEYS_DATABASE_NAME
            ) or _first_str(database, MSTR_KEYS_DATABASE_NAME_NESTED)
            result["schemaName"] = _first_str(
                result, MSTR_KEYS_SCHEMA_NAME
            ) or _first_str(database, MSTR_KEYS_SCHEMA_NAME_NESTED)
            result["databaseName"] = result["databaseName"] or _connection_param(
                connection_string, MSTR_DATABASE_PARAM_RE
            )
            result["schemaName"] = result["schemaName"] or _connection_param(
                connection_string, MSTR_SCHEMA_PARAM_RE
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
    raw: MicroStrategyDict = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def normalize(cls, data: Any) -> MicroStrategyDict:
        if isinstance(data, dict):
            result = dict(data)
            key = _first_str(result, MSTR_KEYS_VISUALIZATION_KEY)
            if not key:
                seed = json.dumps(data, sort_keys=True, default=str)
                key = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
            result["key"] = key
            result["name"] = (
                _first_str(result, MSTR_KEYS_DISPLAY_NAME) or f"Visualization {key}"
            )
            result["type"] = _first_str(result, MSTR_KEYS_VISUALIZATION_TYPE)
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
    dependencies: List[MicroStrategyObject] = Field(default_factory=list)

    @classmethod
    def from_api_response(
        cls,
        object_id: str,
        object_name: str,
        response: MicroStrategyDict,
        description: Optional[str] = None,
        report: Optional["MicroStrategyReport"] = None,
    ) -> "DashboardDefinition":
        definition = _unwrap_definition(response)
        context = f"dashboard definition dashboard_id={object_id}"
        datasets = _validate_items(
            DatasetObject, _extract_datasets(definition), context, report
        )
        visualizations = _validate_items(
            Visualization, _extract_visualizations(definition), context, report
        )
        return cls(
            id=object_id,
            name=object_name,
            description=description,
            datasets=datasets,
            visualizations=visualizations,
        )


class ReportSource(MicroStrategyBaseModel):
    """The upstream object a report reads from (a dataset, cube, or data
    source). Both fields are optional: a report built directly on warehouse
    tables, or one whose source could not be resolved, has neither."""

    id: Optional[str] = None
    name: Optional[str] = None


class ReportDefinition(MicroStrategyBaseModel):
    id: str
    name: str
    description: Optional[str] = None
    source_id: Optional[str] = Field(default=None, alias="sourceId")
    source_name: Optional[str] = Field(default=None, alias="sourceName")
    available_objects: MicroStrategyDict = Field(
        default_factory=dict, alias="availableObjects"
    )
    object_ids: List[str] = Field(default_factory=list)
    prompt_count: int = Field(default=0, alias="promptCount")
    has_filter: bool = Field(default=False, alias="hasFilter")

    @classmethod
    def from_api_response(
        cls,
        object_id: str,
        object_name: str,
        response: MicroStrategyDict,
        description: Optional[str] = None,
    ) -> "ReportDefinition":
        result = response.get("result", response)
        root = result if isinstance(result, dict) else response
        definition = _unwrap_definition(response)
        source = _extract_report_source(definition, root)
        available_objects = _normalize_available_objects(
            definition.get("availableObjects") or root.get("availableObjects")
        )
        prompts = _list_items(definition.get("prompts") or root.get("prompts"))
        return cls(
            id=object_id,
            name=object_name,
            description=(
                description
                or _first_str(definition, ["description"])
                or _first_str(root, ["description"])
            ),
            source_id=source.id,
            source_name=source.name,
            available_objects=available_objects,
            object_ids=_extract_object_ids({"availableObjects": available_objects}),
            prompt_count=len(prompts),
            has_filter=bool(
                definition.get("filter")
                or definition.get("viewFilter")
                or definition.get("qualification")
                or root.get("filter")
                or root.get("viewFilter")
                or root.get("qualification")
            ),
        )

    @classmethod
    def from_search_result(
        cls, report_object: MicroStrategyObject
    ) -> "ReportDefinition":
        raw = report_object.model_dump(by_alias=True, exclude_none=True)
        source = _extract_report_source(raw)
        available_objects = _normalize_available_objects(raw.get("availableObjects"))
        return cls(
            id=report_object.id,
            name=report_object.name,
            description=report_object.description,
            source_id=source.id,
            source_name=source.name,
            available_objects=available_objects,
            object_ids=_extract_object_ids({"availableObjects": available_objects}),
        )


class ProjectKey(ContainerKey):
    project_id: str


class FolderKey(ProjectKey):
    folder_path: str


def _unwrap_definition(response: MicroStrategyDict) -> MicroStrategyDict:
    result = response.get("result", response)
    if isinstance(result, dict):
        definition = result.get("definition", result)
        if isinstance(definition, dict):
            return definition
    return response


def _extract_datasets(definition: MicroStrategyDict) -> List[MicroStrategyDict]:
    datasets = definition.get("datasets")
    if isinstance(datasets, list):
        return [dataset for dataset in datasets if isinstance(dataset, dict)]
    if isinstance(datasets, dict):
        values = datasets.get("datasets") or datasets.get("items") or datasets.values()
        return [dataset for dataset in values if isinstance(dataset, dict)]
    return []


def _normalize_available_objects(value: Any) -> MicroStrategyDict:
    if isinstance(value, dict):
        return value
    if not isinstance(value, list):
        return {}

    metrics: List[MicroStrategyDict] = []
    attributes: List[MicroStrategyDict] = []
    others: List[MicroStrategyDict] = []
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

    normalized: MicroStrategyDict = {}
    if metrics:
        normalized["metrics"] = metrics
    if attributes:
        normalized["attributes"] = attributes
    if others:
        normalized["objects"] = others
    return normalized


def _extract_report_source(*locations: MicroStrategyDict) -> ReportSource:
    for location in locations:
        for source_key in MSTR_KEYS_SOURCE_OBJECT:
            source = location.get(source_key)
            if isinstance(source, dict):
                source_id = _first_str(source, MSTR_KEYS_SOURCE_OBJECT_ID)
                if source_id:
                    return ReportSource(
                        id=source_id, name=_first_str(source, MSTR_KEYS_NAME)
                    )
            elif isinstance(source, str) and source:
                return ReportSource(id=source)

        source_id = _first_str(location, MSTR_KEYS_SOURCE_ID)
        if source_id:
            return ReportSource(
                id=source_id, name=_first_str(location, MSTR_KEYS_SOURCE_NAME)
            )
    return ReportSource()


def _normalize_datasource_reference(data: MicroStrategyDict) -> MicroStrategyDict:
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
    result["id"] = _first_str(result, MSTR_KEYS_DATASOURCE_ID)
    result["name"] = _first_str(result, MSTR_KEYS_NAME) or result.get("id")
    result["datasourceType"] = _first_str(result, MSTR_KEYS_DATASOURCE_TYPE)
    result["databaseType"] = _first_str(database, MSTR_KEYS_DATABASE_TYPE)
    result["databaseVersion"] = _first_str(database, MSTR_KEYS_DATABASE_VERSION)
    result["dbmsName"] = _first_str(dbms, MSTR_KEYS_DBMS_NAME)
    result["connectionId"] = _first_str(connection, MSTR_KEYS_ID)
    result["connectionName"] = _first_str(connection, MSTR_KEYS_NAME)
    result["databaseName"] = _first_str(result, MSTR_KEYS_DATABASE_NAME) or _first_str(
        database, MSTR_KEYS_DATABASE_NAME_NESTED
    )
    result["schemaName"] = _first_str(result, MSTR_KEYS_SCHEMA_NAME) or _first_str(
        database, MSTR_KEYS_SCHEMA_NAME_NESTED
    )
    embedded = connection.get("embedded")
    if embedded is None:
        embedded = connection.get("isEmbedded")
    if isinstance(embedded, bool):
        result["connectionEmbedded"] = embedded
    return result


def _list_items(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in MSTR_KEYS_LIST_CONTAINERS:
            nested = value.get(key)
            if isinstance(nested, list):
                return nested
    return []


def _connection_param(
    connection_string: str, pattern: re.Pattern[str]
) -> Optional[str]:
    if not connection_string:
        return None
    match = pattern.search(connection_string)
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


def _extract_datasource_reference(
    data: MicroStrategyDict,
) -> Optional[MicroStrategyDict]:
    for key in MSTR_KEYS_DATASOURCE_REFERENCE:
        value = data.get(key)
        if _looks_like_datasource_reference(value):
            return value
    if _looks_like_datasource_reference(data):
        return data
    return None


def _extract_visualizations(definition: MicroStrategyDict) -> List[MicroStrategyDict]:
    found: List[MicroStrategyDict] = []

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
            chapter_key = _first_str(chapter, MSTR_KEYS_KEY_ID)
            pages = chapter.get("pages")
            if not isinstance(pages, list):
                continue
            for page in pages:
                if not isinstance(page, dict):
                    continue
                page_key = _first_str(page, MSTR_KEYS_KEY_ID)
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

    unique: Dict[str, MicroStrategyDict] = {}
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
            candidate_id = _first_str(candidate, MSTR_KEYS_DATASET_ID)
            if candidate_id:
                dataset_ids.append(candidate_id)

    def visit(node: Any, parent_key: Optional[str] = None) -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                if key.lower() in MSTR_DATASET_CONTAINER_KEYS:
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
        elif parent_key and MSTR_DATASET_KEY_RE.search(parent_key):
            add(node)

    visit(value)
    return sorted(set(dataset_ids))


def _extract_object_ids(value: Any) -> List[str]:
    object_ids: List[str] = []

    def visit(node: Any, parent_key: Optional[str] = None) -> None:
        if isinstance(node, dict):
            node_type = str(node.get("type") or node.get("objectType") or "").lower()
            parent = (parent_key or "").lower()
            if node_type in MSTR_OBJECT_TYPES or parent in MSTR_OBJECT_ID_PARENT_KEYS:
                object_id = _first_str(node, MSTR_KEYS_ID)
                if object_id and object_id != MSTR_NULL_OBJECT_ID:
                    object_ids.append(object_id)
            for child_key, child in node.items():
                visit(child, str(child_key))
        elif isinstance(node, list):
            for child in node:
                visit(child, parent_key)

    visit(value)
    return sorted(set(object_ids))


def extract_folder_parts(raw_object: MicroStrategyDict) -> List[str]:
    # Quick-search results requested with getAncestors carry the folder path
    # as a top-down list of ancestor objects.
    ancestors = raw_object.get("ancestors")
    if isinstance(ancestors, list) and ancestors:
        parts = [
            _first_str(ancestor, MSTR_KEYS_NAME)
            for ancestor in ancestors
            if isinstance(ancestor, dict)
        ]
        # Only trust the ancestor path when every entry resolved to a name;
        # a nameless entry mid-list would silently collapse the hierarchy
        # (A/B/C -> A/C), so fall back to folder/location instead.
        if parts and len(parts) == len(ancestors) and all(parts):
            return [part for part in parts if part]
    folder = raw_object.get("folder") or raw_object.get("location")
    if isinstance(folder, dict):
        path = _first_str(folder, MSTR_KEYS_FOLDER_PATH)
    else:
        path = str(folder) if folder else None
    return [part for part in (path or "").strip("/").split("/") if part]
