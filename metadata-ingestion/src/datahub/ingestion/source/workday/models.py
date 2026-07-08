from typing import Dict, List, Optional, Sequence

from pydantic import BaseModel, Field, model_validator

from datahub.ingestion.source.workday.constants import (
    WORKDAY_KEYS_CATEGORY,
    WORKDAY_KEYS_CREATED,
    WORKDAY_KEYS_DESCRIPTION,
    WORKDAY_KEYS_FIELD_MAPPINGS,
    WORKDAY_KEYS_ID,
    WORKDAY_KEYS_NAME,
    WORKDAY_KEYS_OWNER,
    WORKDAY_KEYS_OWNER_GROUP,
    WORKDAY_KEYS_RELATED_OBJECTS,
    WORKDAY_KEYS_REPORT_FIELDS,
    WORKDAY_KEYS_ROW_COUNT,
    WORKDAY_KEYS_TAGS,
    WORKDAY_KEYS_TERMS,
    WORKDAY_KEYS_TRANSFORM_LOGIC,
    WORKDAY_KEYS_UPDATED,
)


def _first_int(values: Dict[str, object], keys: Sequence[str]) -> Optional[int]:
    """Return the first int-valued key among the aliases (bools are not ints)."""
    for key in keys:
        value = values.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip().lstrip("-").isdigit():
            return int(value)
    return None


def _first_str(values: Dict[str, object], keys: Sequence[str]) -> Optional[str]:
    """Return the first non-empty string among the given aliased keys.

    Prism carries the same logical field under different keys across API
    versions and object shapes; callers pass an ordered alias list.
    """
    for key in keys:
        value = values.get(key)
        if isinstance(value, str) and value.strip():
            return value
        # Some descriptors arrive as {"descriptor": "..."} or {"value": "..."}.
        if isinstance(value, dict):
            for nested_key in ("descriptor", "value", "name", "id"):
                nested = value.get(nested_key)
                if isinstance(nested, str) and nested.strip():
                    return nested
    return None


def _extract_str_list(values: Dict[str, object], keys: Sequence[str]) -> List[str]:
    """Pull display strings from aliased keys, from plain strings or descriptors."""
    for key in keys:
        value = values.get(key)
        if isinstance(value, str) and value.strip():
            return [value.strip()]
        if not isinstance(value, list):
            continue
        out: List[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                out.append(item.strip())
            elif isinstance(item, dict):
                nested = _first_str(item, ("descriptor", "name", "value", "label"))
                if nested is not None:
                    out.append(nested)
        if out:
            return out
    return []


class WorkdayObject(BaseModel):
    """Common identity fields shared by Prism tables, datasets, and data sources."""

    id: str
    name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    owner_group: Optional[str] = None
    # ISO-8601 creation / last-change timestamps, when the API supplies them.
    created: Optional[str] = None
    updated: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    glossary_terms: List[str] = Field(default_factory=list)
    category: Optional[str] = None

    model_config = {"extra": "ignore"}

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        resolved_id = _first_str(values, WORKDAY_KEYS_ID)
        if resolved_id is not None:
            normalized["id"] = resolved_id
        resolved_name = _first_str(values, WORKDAY_KEYS_NAME)
        if resolved_name is not None:
            normalized["name"] = resolved_name
        if normalized.get("description") is None:
            normalized["description"] = _first_str(values, WORKDAY_KEYS_DESCRIPTION)
        if normalized.get("owner") is None:
            normalized["owner"] = _first_str(values, WORKDAY_KEYS_OWNER)
        if normalized.get("owner_group") is None:
            normalized["owner_group"] = _first_str(values, WORKDAY_KEYS_OWNER_GROUP)
        if normalized.get("created") is None:
            normalized["created"] = _first_str(values, WORKDAY_KEYS_CREATED)
        if normalized.get("updated") is None:
            normalized["updated"] = _first_str(values, WORKDAY_KEYS_UPDATED)
        if not normalized.get("tags"):
            normalized["tags"] = _extract_str_list(values, WORKDAY_KEYS_TAGS)
        if not normalized.get("glossary_terms"):
            normalized["glossary_terms"] = _extract_str_list(values, WORKDAY_KEYS_TERMS)
        if normalized.get("category") is None:
            normalized["category"] = _first_str(values, WORKDAY_KEYS_CATEGORY)
        return normalized


class PrismField(BaseModel):
    """One column in a Prism table/dataset schema."""

    name: str
    description: Optional[str] = None
    ordinal: Optional[int] = None
    type_descriptor: Optional[str] = None
    is_primary_key: bool = False
    nullable: Optional[bool] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    length: Optional[int] = None
    tags: List[str] = Field(default_factory=list)

    model_config = {"extra": "ignore"}

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        name = _first_str(values, ("name", "displayName", "label"))
        if name is not None:
            normalized["name"] = name
        if normalized.get("description") is None:
            normalized["description"] = _first_str(values, ("description",))
        if normalized.get("ordinal") is None:
            normalized["ordinal"] = _first_int(values, ("ordinal", "position", "index"))
        # Prism nests the semantic type under `type` (a dict) or reports it as a
        # flat string; both resolve to a lowercase descriptor for lookup.
        if normalized.get("type_descriptor") is None:
            normalized["type_descriptor"] = _first_str(
                values, ("type", "dataType", "fieldType")
            )
        if normalized.get("is_primary_key") in (None, False):
            for key in ("isPrimaryKey", "primaryKey", "businessKey"):
                value = values.get(key)
                if isinstance(value, bool):
                    normalized["is_primary_key"] = value
                    if value:
                        break
        if normalized.get("nullable") is None:
            for key in ("nullable", "isNullable", "optional"):
                value = values.get(key)
                if isinstance(value, bool):
                    normalized["nullable"] = value
                    break
            else:
                # Some shapes express the inverse ("required"); invert it.
                required = values.get("required")
                if isinstance(required, bool):
                    normalized["nullable"] = not required
        if normalized.get("precision") is None:
            normalized["precision"] = _first_int(
                values, ("precision", "precisionValue")
            )
        if normalized.get("scale") is None:
            normalized["scale"] = _first_int(values, ("scale", "scaleValue"))
        if normalized.get("length") is None:
            normalized["length"] = _first_int(values, ("length", "maxLength", "size"))
        if not normalized.get("tags"):
            normalized["tags"] = _extract_str_list(values, WORKDAY_KEYS_TAGS)
        return normalized


class PrismTable(WorkdayObject):
    """A Prism Analytics data table (the physical, schema-bearing output)."""

    source_type: Optional[str] = None
    fields: List[PrismField] = Field(default_factory=list)
    # Ids of the dataset/data sources this table is built from, for lineage.
    dataset_id: Optional[str] = None
    data_source_ids: List[str] = Field(default_factory=list)
    # Row count from the table detail response, when present (for profiling).
    row_count: Optional[int] = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_table(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        if normalized.get("source_type") is None:
            normalized["source_type"] = _first_str(
                values, ("sourceType", "source", "origin")
            )
        # The schema field list appears under a few keys across API shapes.
        for key in ("fields", "schema", "columns"):
            value = values.get(key)
            if isinstance(value, dict):
                value = value.get("fields") or value.get("columns")
            if isinstance(value, list):
                normalized["fields"] = value
                break
        if normalized.get("dataset_id") is None:
            normalized["dataset_id"] = _first_str(
                values, ("datasetId", "dataset", "sourceDatasetId")
            )
        if not normalized.get("data_source_ids"):
            normalized["data_source_ids"] = _extract_id_list(
                values, ("dataSources", "dataSourceIds", "sources")
            )
        if normalized.get("row_count") is None:
            normalized["row_count"] = _first_int(values, WORKDAY_KEYS_ROW_COUNT)
        return normalized

    def has_full_detail(self) -> bool:
        """Whether this looks hydrated (list responses often omit fields)."""
        return bool(self.fields)


class PrismFieldMapping(BaseModel):
    """One output-field <- source-field edge from a Prism dataset pipeline."""

    downstream_field: str
    upstream_field: Optional[str] = None
    # Id of the upstream table/data source the source field comes from, when the
    # mapping names it; lets the edge resolve to a specific upstream URN.
    upstream_object_id: Optional[str] = None

    model_config = {"extra": "ignore"}

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        down = _first_str(
            values, ("outputField", "targetField", "name", "field", "output")
        )
        if down is not None:
            normalized["downstream_field"] = down
        if normalized.get("upstream_field") is None:
            normalized["upstream_field"] = _first_str(
                values, ("sourceField", "inputField", "source", "input", "from")
            )
        if normalized.get("upstream_object_id") is None:
            normalized["upstream_object_id"] = _first_str(
                values,
                ("sourceObjectId", "sourceTableId", "sourceId", "dataSourceId"),
            )
        return normalized


class PrismDataset(WorkdayObject):
    """A Prism dataset (pipeline definition) that produces a table."""

    source_type: Optional[str] = None
    # Output table id and upstream table/data-source ids, for lineage.
    output_table_id: Optional[str] = None
    source_table_ids: List[str] = Field(default_factory=list)
    data_source_ids: List[str] = Field(default_factory=list)
    # Transformation pipeline text (Data Prep Language) and per-field mappings.
    transform_logic: Optional[str] = None
    field_mappings: List[PrismFieldMapping] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize_dataset(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        if normalized.get("source_type") is None:
            normalized["source_type"] = _first_str(values, ("sourceType", "source"))
        if normalized.get("output_table_id") is None:
            normalized["output_table_id"] = _first_str(
                values, ("tableId", "table", "outputTableId", "targetTableId")
            )
        if not normalized.get("source_table_ids"):
            normalized["source_table_ids"] = _extract_id_list(
                values, ("sourceTables", "inputTables", "tables")
            )
        if not normalized.get("data_source_ids"):
            normalized["data_source_ids"] = _extract_id_list(
                values, ("dataSources", "dataSourceIds", "sources")
            )
        if normalized.get("transform_logic") is None:
            normalized["transform_logic"] = _first_str(
                values, WORKDAY_KEYS_TRANSFORM_LOGIC
            )
        if not normalized.get("field_mappings"):
            for key in WORKDAY_KEYS_FIELD_MAPPINGS:
                value = values.get(key)
                if isinstance(value, list) and value:
                    normalized["field_mappings"] = value
                    break
        return normalized


class PrismDataSource(WorkdayObject):
    """A Prism data source: a Workday report/business object or external input."""

    source_type: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_data_source(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        if normalized.get("source_type") is None:
            normalized["source_type"] = _first_str(
                values, ("sourceType", "source", "type")
            )
        return normalized


class PrismBucket(WorkdayObject):
    """A Prism bucket: the staging area an upload lands in before publishing.

    ``target_table_id`` names the table the bucket feeds, so the bucket can be
    linked as an upstream of that table.
    """

    target_table_id: Optional[str] = None
    state: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_bucket(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        if normalized.get("target_table_id") is None:
            normalized["target_table_id"] = _first_str(
                values, ("targetId", "targetTableId", "tableId", "table")
            )
        if normalized.get("state") is None:
            normalized["state"] = _first_str(values, ("state", "status", "bucketState"))
        return normalized


class WqlDataSource(WorkdayObject):
    """A WQL data source: a Workday business object exposed for querying.

    Separate from ``PrismDataSource`` (a Prism-pipeline input) — this is the
    tenant's business-object catalog surfaced by the WQL metadata API.
    """

    alias: Optional[str] = None
    fields: List[PrismField] = Field(default_factory=list)
    related_object_ids: List[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize_wql(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        if normalized.get("alias") is None:
            normalized["alias"] = _first_str(values, ("alias", "referenceId"))
        # WQL data sources carry their display name under `descriptor`; fall back
        # to it (then the alias) so the shared name resolver has a value.
        if _first_str(values, WORKDAY_KEYS_NAME) is None:
            normalized["name"] = _first_str(values, ("descriptor", "alias"))
        for key in ("fields", "columns"):
            value = values.get(key)
            if isinstance(value, list):
                normalized["fields"] = value
                break
        if not normalized.get("related_object_ids"):
            normalized["related_object_ids"] = _extract_id_list(
                values, WORKDAY_KEYS_RELATED_OBJECTS
            )
        return normalized


class CustomReport(WorkdayObject):
    """A Workday custom report (RaaS) definition enumerated via WQL.

    ``data_source`` is the business object the report reads from, used to link
    the report to the catalog entity it derives from.
    """

    data_source: Optional[str] = None
    enabled_as_web_service: Optional[bool] = None
    fields: List[PrismField] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize_report(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        if normalized.get("data_source") is None:
            normalized["data_source"] = _first_str(
                values, ("dataSource", "primaryDataSource", "businessObject")
            )
        if normalized.get("enabled_as_web_service") is None:
            for key in ("enabledAsWebService", "webServiceEnabled", "raasEnabled"):
                value = values.get(key)
                if isinstance(value, bool):
                    normalized["enabled_as_web_service"] = value
                    break
        if not normalized.get("fields"):
            for key in WORKDAY_KEYS_REPORT_FIELDS:
                value = values.get(key)
                if isinstance(value, list) and value:
                    normalized["fields"] = value
                    break
        return normalized


def _extract_id_list(values: Dict[str, object], keys: Sequence[str]) -> List[str]:
    """Pull a list of object ids from any of the aliased keys.

    Handles both lists of id strings and lists of objects carrying an id field.
    """
    for key in keys:
        value = values.get(key)
        if not isinstance(value, list):
            continue
        ids: List[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                ids.append(item)
            elif isinstance(item, dict):
                nested = _first_str(item, WORKDAY_KEYS_ID)
                if nested is not None:
                    ids.append(nested)
        if ids:
            return ids
    return []
