"""Lakehouse table shortcuts from the Fabric OneLake Shortcuts API.

Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/list-shortcuts
"""

from dataclasses import dataclass
from typing import Optional

from datahub.ingestion.source.fabric.onelake.constants import FABRIC_SQL_DEFAULT_SCHEMA

SHORTCUT_TAG = "shortcut"

# Custom property keys set on shortcut tables.
ORIGIN_NAME_PROPERTY = "shortcut_origin_name"
ORIGIN_PATH_PROPERTY = "shortcut_origin_path"
ORIGIN_WORKSPACE_ID_PROPERTY = "shortcut_origin_workspace_id"
ORIGIN_WORKSPACE_NAME_PROPERTY = "shortcut_origin_workspace_name"
ORIGIN_ITEM_ID_PROPERTY = "shortcut_origin_item_id"
ORIGIN_ITEM_NAME_PROPERTY = "shortcut_origin_item_name"

# Table shortcuts live under the lakehouse Tables folder. Files shortcuts are not tables.
_TABLES_ROOT = "tables"


@dataclass
class LakehouseTableShortcut:
    """A lakehouse table that is a shortcut.

    `schema_name` and `table_name` identify the table inside this lakehouse.
    `origin_name` is the original table name at the shortcut target and
    `origin_path` is the target location (OneLake path, or external
    location plus subpath). The `upstream_*` fields are set only when the
    target is another OneLake table.
    """

    schema_name: str
    table_name: str
    origin_name: str
    origin_path: str
    upstream_workspace_id: Optional[str] = None
    upstream_item_id: Optional[str] = None
    upstream_schema_name: Optional[str] = None
    upstream_table_name: Optional[str] = None

    def lookup_key(self) -> tuple[str, str]:
        return (self.schema_name.lower(), self.table_name.lower())


@dataclass
class _Origin:
    name: str
    path: str
    workspace_id: Optional[str] = None
    item_id: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None


def _segments(path: str) -> list[str]:
    return [part for part in path.strip("/").split("/") if part]


def _last_segment(path: str) -> str:
    parts = _segments(path)
    return parts[-1] if parts else ""


def _join_path(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part and part.strip("/"))


def parse_table_shortcut(raw: dict) -> Optional[LakehouseTableShortcut]:
    """Return a table shortcut, or None when the shortcut is not a lakehouse table.

    A table shortcut's `path` is the parent folder under `Tables` and `name` is
    the table name. `Tables` alone is the schemas-disabled layout (schema `dbo`).
    `Tables/<schema>` is the schemas-enabled layout.
    """
    name = str(raw.get("name") or "").strip()
    local = _local_table(str(raw.get("path") or ""), name)
    if local is None:
        return None
    schema_name, table_name = local
    origin = _parse_origin(raw.get("target") or {})
    return LakehouseTableShortcut(
        schema_name=schema_name,
        table_name=table_name,
        origin_name=origin.name,
        origin_path=origin.path,
        upstream_workspace_id=origin.workspace_id,
        upstream_item_id=origin.item_id,
        upstream_schema_name=origin.schema_name,
        upstream_table_name=origin.table_name,
    )


def _local_table(path: str, name: str) -> Optional[tuple[str, str]]:
    if not name:
        return None
    parts = _segments(path)
    if not parts or parts[0].lower() != _TABLES_ROOT:
        return None
    folders = parts[1:]
    schema_name = folders[-1] if folders else FABRIC_SQL_DEFAULT_SCHEMA
    return schema_name, name


def _parse_origin(target: dict) -> _Origin:
    target_type = str(target.get("type") or "")
    if target_type == "OneLake":
        one_lake = target.get("oneLake") or {}
        if not isinstance(one_lake, dict):
            return _Origin(name="", path="")
        path = str(one_lake.get("path") or "")
        workspace_id = str(one_lake.get("workspaceId") or "") or None
        item_id = str(one_lake.get("itemId") or "") or None
        located = _onelake_target_table(path)
        if located is None:
            return _Origin(
                name=_last_segment(path),
                path=path,
                workspace_id=workspace_id,
                item_id=item_id,
            )
        schema_name, table_name = located
        return _Origin(
            name=table_name,
            path=path,
            workspace_id=workspace_id,
            item_id=item_id,
            # Lineage needs both ids to build the upstream URN.
            schema_name=schema_name if workspace_id and item_id else None,
            table_name=table_name if workspace_id and item_id else None,
        )

    if target_type == "Dataverse":
        dataverse = target.get("dataverse") or {}
        if not isinstance(dataverse, dict):
            return _Origin(name="", path="")
        table_name = str(dataverse.get("tableName") or "")
        return _Origin(
            name=table_name,
            path=_join_path(
                str(dataverse.get("environmentDomain") or ""),
                str(dataverse.get("deltaLakeFolder") or ""),
                table_name,
            ),
        )

    for key in (
        "adlsGen2",
        "amazonS3",
        "s3Compatible",
        "googleCloudStorage",
        "azureBlobStorage",
    ):
        body = target.get(key)
        if isinstance(body, dict):
            subpath = str(body.get("subpath") or "")
            return _Origin(
                name=_last_segment(subpath),
                path=_join_path(str(body.get("location") or ""), subpath),
            )
    return _Origin(name="", path="")


def matching_column_pairs(
    downstream_fields: list[str], upstream_fields: list[str]
) -> list[tuple[str, str]]:
    """Pair shortcut columns to origin columns that share a name.

    Comparison is case-insensitive. Each side keeps the field path stored on
    its dataset, so the lineage URN matches schema metadata.
    """
    upstream_by_name: dict[str, str] = {}
    for field in upstream_fields:
        upstream_by_name.setdefault(field.lower(), field)
    return [
        (field, upstream_by_name[field.lower()])
        for field in downstream_fields
        if field.lower() in upstream_by_name
    ]


def _onelake_target_table(path: str) -> Optional[tuple[str, str]]:
    """Parse `Tables/<table>` or `Tables/<schema>/<table>` from a OneLake target path."""
    parts = _segments(path)
    if not parts or parts[0].lower() != _TABLES_ROOT:
        return None
    rest = parts[1:]
    if not rest:
        return None
    if len(rest) == 1:
        return FABRIC_SQL_DEFAULT_SCHEMA, rest[0]
    return rest[-2], rest[-1]
