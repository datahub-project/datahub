from typing import Dict, List, Optional, Sequence

from pydantic import BaseModel, Field, model_validator

from datahub.ingestion.source.workday.constants import (
    WORKDAY_KEYS_DESCRIPTION,
    WORKDAY_KEYS_ID,
    WORKDAY_KEYS_NAME,
    WORKDAY_KEYS_OWNER,
)


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


class WorkdayObject(BaseModel):
    """Common identity fields shared by Prism tables, datasets, and data sources."""

    id: str
    name: str
    description: Optional[str] = None
    owner: Optional[str] = None

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
        return normalized


class PrismField(BaseModel):
    """One column in a Prism table/dataset schema."""

    name: str
    description: Optional[str] = None
    ordinal: Optional[int] = None
    type_descriptor: Optional[str] = None
    is_primary_key: bool = False

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
            for key in ("ordinal", "position", "index"):
                value = values.get(key)
                if isinstance(value, int):
                    normalized["ordinal"] = value
                    break
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
        return normalized


class PrismTable(WorkdayObject):
    """A Prism Analytics data table (the physical, schema-bearing output)."""

    source_type: Optional[str] = None
    fields: List[PrismField] = Field(default_factory=list)
    # Ids of the dataset/data sources this table is built from, for lineage.
    dataset_id: Optional[str] = None
    data_source_ids: List[str] = Field(default_factory=list)

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
        return normalized


class PrismDataset(WorkdayObject):
    """A Prism dataset (pipeline definition) that produces a table."""

    source_type: Optional[str] = None
    # Output table id and upstream table/data-source ids, for lineage.
    output_table_id: Optional[str] = None
    source_table_ids: List[str] = Field(default_factory=list)
    data_source_ids: List[str] = Field(default_factory=list)

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
