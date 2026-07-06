# Databricks Lakehouse Federation Lineage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect Unity Catalog *foreign catalogs* (Lakehouse Federation) and link each federated table to the external source dataset (SQL Server, Postgres, MySQL, Snowflake, BigQuery, …) via siblings or lineage, plus mark the catalog with facetable structured properties.

**Architecture:** Reuse the already-emitted Databricks catalog *container* and the existing `gen_siblings_workunit` / `gen_lineage_workunit` helpers in `source.py`. Add a small pure-logic module (`federation.py`) that maps a Unity Catalog `ConnectionType` → DataHub platform and builds the external dataset URN name (handling two-tier vs three-tier and per-connector `options` keys). Carry `connection_name`/`options` onto the `Catalog` model, fetch connections via a new `Proxy.connections()`, and emit federation structured-property definitions + assignments modeled on the Snowplow `property_manager.py` pattern.

**Tech Stack:** Python 3.10+, pydantic v2, `databricks-sdk` (`databricks.sdk.service.catalog`), DataHub ingestion framework, pytest.

## Global Constraints

- Python: type annotations everywhere; no bare `Any`; ruff format (88 cols); pydantic for config. (from `metadata-ingestion/CLAUDE.md`)
- Verify Python with `../gradlew :metadata-ingestion:lintFix` (ruff) and `../gradlew :metadata-ingestion:lint` (mypy). Never run `ruff`/`mypy`/`py_compile` directly.
- Tests: pytest, plain `assert`, regular classes (no `unittest.TestCase`), files named `test_*.py` under `tests/`.
- Connector platform id is `"databricks"` (`source.py:328`). External platform ids are exactly: `mysql, postgres, mssql, snowflake, redshift, bigquery, glue, oracle, teradata, databricks, hive` (verified against `bootstrap_mcps/data-platforms.yaml`).
- Confidentiality (public repo): no customer names, real DB/schema/table names, or ticket IDs in code/tests/commits. Use generic placeholders (`my_db.my_schema.my_table`, connection `pg_conn`, etc.).
- Work in worktree `/Users/treff7es/shadow/datahub/.claude/worktrees/databricks-lake-federation` on branch `worktree-databricks-lake-federation`. All paths below are relative to `metadata-ingestion/` unless noted.
- Run pytest via the venv: `../gradlew :metadata-ingestion:installDev` once, then `source venv/bin/activate`.

---

## File Structure

| File | Responsibility | Create/Modify |
|---|---|---|
| `src/datahub/ingestion/source/unity/federation.py` | Pure logic: connection-type→platform map, `options`-key map, `resolve_federation_target`, external URN name, structured-property definitions | **Create** |
| `src/datahub/ingestion/source/unity/proxy_types.py` | Add `connection_name`/`options` + `is_foreign_catalog` to `Catalog` | Modify (135-139) |
| `src/datahub/ingestion/source/unity/proxy.py` | Populate the new `Catalog` fields; add `connections()` API call | Modify (19-29, 1368-1382; add method near 553) |
| `src/datahub/ingestion/source/unity/config.py` | `FederationLinkType`, `FederationConnectionDetail`, 4 new config fields | Modify (after 297) |
| `src/datahub/ingestion/source/unity/report.py` | Federation counters | Modify (after 71) |
| `src/datahub/ingestion/source/unity/source.py` | Emit property definitions once; assign props on foreign-catalog container; per-table siblings/lineage link | Modify (`get_workunits_internal`, `gen_catalog_containers`, `process_table`) |
| `tests/unit/test_unity_catalog_federation.py` | Unit tests for `federation.py` + model + proxy + config | **Create** |
| `tests/integration/unity/test_unity_catalog_ingest.py` | Foreign-catalog + connection mock + golden test | Modify (`register_mock_data` ~189, add test) |
| `tests/integration/unity/unity_catalog_federation_mces_golden.json` | Golden output | **Create** (generated) |

---

## Task 1: `Catalog` model carries federation fields

**Files:**
- Modify: `src/datahub/ingestion/source/unity/proxy_types.py:135-139`
- Test: `tests/unit/test_unity_catalog_federation.py`

**Interfaces:**
- Produces: `Catalog.connection_name: Optional[str]`, `Catalog.options: Optional[Dict[str, str]]`, `Catalog.is_foreign_catalog: bool` (property).

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_unity_catalog_federation.py`:

```python
from databricks.sdk.service.catalog import CatalogType

from datahub.ingestion.source.unity.proxy_types import Catalog, Metastore


def _metastore() -> Metastore:
    return Metastore(
        id="ms", name="ms", comment=None, global_metastore_id=None,
        metastore_id=None, owner=None, region=None, cloud=None,
    )


def test_catalog_carries_federation_fields():
    catalog = Catalog(
        id="c", name="c", metastore=_metastore(), comment=None, owner=None,
        type=CatalogType.FOREIGN_CATALOG,
        connection_name="pg_conn",
        options={"database": "my_db"},
    )
    assert catalog.connection_name == "pg_conn"
    assert catalog.options == {"database": "my_db"}
    assert catalog.is_foreign_catalog is True


def test_managed_catalog_is_not_foreign():
    catalog = Catalog(
        id="c", name="c", metastore=_metastore(), comment=None, owner=None,
        type=CatalogType.MANAGED_CATALOG,
    )
    assert catalog.is_foreign_catalog is False
    assert catalog.connection_name is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'connection_name'`.

- [ ] **Step 3: Modify the `Catalog` dataclass**

In `proxy_types.py`, replace lines 135-139:

```python
@dataclass
class Catalog(CommonProperty):
    metastore: Optional[Metastore]
    owner: Optional[str]
    type: Optional[Union[CatalogType, CustomCatalogType]]
    connection_name: Optional[str] = None
    options: Optional[Dict[str, str]] = None

    @property
    def is_foreign_catalog(self) -> bool:
        return self.type == CatalogType.FOREIGN_CATALOG
```

`CatalogType` is already imported (`proxy_types.py:11`); `Dict`/`Optional`/`Union` are already imported.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add metadata-ingestion/src/datahub/ingestion/source/unity/proxy_types.py metadata-ingestion/tests/unit/test_unity_catalog_federation.py
git commit -m "feat(ingest/unity): carry connection_name/options on Catalog model"
```

---

## Task 2: `Proxy.connections()` fetches Unity Catalog connections

**Files:**
- Modify: `src/datahub/ingestion/source/unity/proxy.py:19-29` (import), add method after `catalog()` (~line 553)
- Modify: `src/datahub/ingestion/source/unity/report.py` (counter — done in Task 6; here reference `self.report.num_federation_connections_list_failed` guarded by `getattr` is NOT allowed, so add the counter in this task's report edit)
- Test: `tests/unit/test_unity_catalog_federation.py`

**Interfaces:**
- Consumes: `Catalog.connection_name` (Task 1).
- Produces: `UnityCatalogApiProxy.connections() -> Dict[str, ConnectionInfo]` (cached; returns `{}` on error).

- [ ] **Step 1: Add the report counter first (needed by the method)**

In `report.py`, after line 71 (`num_tables_missing_name: int = 0`) add:

```python
    num_federation_connections_list_failed: int = 0
```

- [ ] **Step 2: Write the failing test**

Append to `tests/unit/test_unity_catalog_federation.py`:

```python
from unittest.mock import MagicMock

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionInfo, ConnectionType

from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.report import UnityCatalogReport


def _proxy(workspace_client: MagicMock) -> UnityCatalogApiProxy:
    workspace_client.config.warehouse_id = "wh"
    return UnityCatalogApiProxy(
        workspace_client=workspace_client, report=UnityCatalogReport()
    )


def test_connections_returns_dict_keyed_by_name():
    wc = MagicMock(spec=WorkspaceClient)
    wc.connections.list.return_value = [
        ConnectionInfo(name="pg_conn", connection_type=ConnectionType.POSTGRESQL),
        ConnectionInfo(name="ss_conn", connection_type=ConnectionType.SQLSERVER),
    ]
    proxy = _proxy(wc)
    result = proxy.connections()
    assert set(result) == {"pg_conn", "ss_conn"}
    assert result["ss_conn"].connection_type == ConnectionType.SQLSERVER
    # cached: second call does not re-list
    proxy.connections()
    wc.connections.list.assert_called_once()


def test_connections_returns_empty_on_error():
    wc = MagicMock(spec=WorkspaceClient)
    wc.connections.list.side_effect = PermissionError("no access")
    proxy = _proxy(wc)
    assert proxy.connections() == {}
    assert proxy.report.num_federation_connections_list_failed == 1
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k connections`
Expected: FAIL — `AttributeError: 'UnityCatalogApiProxy' object has no attribute 'connections'`.

- [ ] **Step 4: Implement**

In `proxy.py`, extend the import at lines 19-29 to add `ConnectionInfo`:

```python
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    ConnectionInfo,
    GetMetastoreSummaryResponse,
    MetastoreInfo,
    ModelVersionInfo,
    RegisteredModelInfo,
    SchemaInfo,
    TableConstraint,
    TableInfo,
)
```

In the `UnityCatalogApiProxy.__init__` (after line 318 `self.databricks_api_page_size = databricks_api_page_size`) add the cache field:

```python
        self._connections_cache: Optional[Dict[str, ConnectionInfo]] = None
```

Add the method immediately after the `catalog()` method (after line 553):

```python
    def connections(self) -> Dict[str, ConnectionInfo]:
        """Return Unity Catalog connections keyed by name (cached).

        Lakehouse Federation foreign catalogs reference a connection by name; the
        connection carries the external system's type. Listing connections requires
        metastore-admin or connection-ownership; on permission/other errors we return
        an empty map so the source can fall back to config overrides.
        """
        if self._connections_cache is not None:
            return self._connections_cache
        result: Dict[str, ConnectionInfo] = {}
        try:
            response = self._workspace_client.connections.list(
                max_results=self.databricks_api_page_size
            )
            for connection in response or []:
                if connection.name:
                    result[connection.name] = connection
        except Exception as e:
            self.report.num_federation_connections_list_failed += 1
            logger.warning(f"Failed to list Unity Catalog connections: {e}")
        self._connections_cache = result
        return result
```

`Dict`/`Optional` are already imported in `proxy.py`; `logger` exists at module level.

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k connections`
Expected: PASS (2 passed).

- [ ] **Step 6: Populate the fields in `_create_catalog`**

Replace `proxy.py:1375-1382` (the `return Catalog(...)`):

```python
        return Catalog(
            name=obj.name,
            id=f"{metastore.id}.{catalog_name}" if metastore else catalog_name,
            metastore=metastore,
            comment=obj.comment,
            owner=obj.owner,
            type=obj.catalog_type,
            connection_name=obj.connection_name,
            options=obj.options,
        )
```

- [ ] **Step 7: Run test to verify nothing broke**

Run: `pytest tests/unit/test_unity_catalog_federation.py tests/unit/test_unity_catalog_proxy.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add metadata-ingestion/src/datahub/ingestion/source/unity/proxy.py metadata-ingestion/src/datahub/ingestion/source/unity/report.py metadata-ingestion/tests/unit/test_unity_catalog_federation.py
git commit -m "feat(ingest/unity): add Proxy.connections() and populate catalog connection fields"
```

---

## Task 3: Config — `FederationLinkType`, `FederationConnectionDetail`, new fields

**Files:**
- Modify: `src/datahub/ingestion/source/unity/config.py` (top imports; after field at line 297)
- Test: `tests/unit/test_unity_catalog_federation.py`

**Interfaces:**
- Produces: `FederationLinkType` (Enum: `SIBLINGS`/`LINEAGE`/`NONE`), `FederationConnectionDetail` (ConfigModel), and config fields `federation_link_type`, `emit_federation_structured_properties`, `federation_structured_property_namespace`, `federation_connection_details`.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_unity_catalog_federation.py`:

```python
from datahub.ingestion.source.unity.config import (
    FederationLinkType,
    UnityCatalogSourceConfig,
)

_BASE = {"workspace_url": "https://x.cloud.databricks.com", "token": "t"}


def test_federation_config_defaults():
    cfg = UnityCatalogSourceConfig.model_validate(_BASE)
    assert cfg.federation_link_type == FederationLinkType.SIBLINGS
    assert cfg.emit_federation_structured_properties is True
    assert cfg.federation_structured_property_namespace == "databricks.federation"
    assert cfg.federation_connection_details == {}


def test_federation_connection_detail_override():
    cfg = UnityCatalogSourceConfig.model_validate(
        {
            **_BASE,
            "federation_link_type": "lineage",
            "federation_connection_details": {
                "pg_conn": {
                    "platform": "postgres",
                    "platform_instance": "prod-pg",
                    "env": "PROD",
                    "database": "my_db",
                }
            },
        }
    )
    assert cfg.federation_link_type == FederationLinkType.LINEAGE
    detail = cfg.federation_connection_details["pg_conn"]
    assert detail.platform == "postgres"
    assert detail.platform_instance == "prod-pg"
    assert detail.database == "my_db"
    assert detail.convert_urns_to_lowercase is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k federation_config`
Expected: FAIL — `ImportError: cannot import name 'FederationLinkType'`.

- [ ] **Step 3: Implement**

At the top of `config.py`, ensure these imports exist (add any missing):

```python
from enum import Enum
from typing import Dict, Optional

import pydantic

from datahub.configuration.common import ConfigModel
```

Add the enum and model just above the `class UnityCatalogSourceConfig` declaration (line 159):

```python
class FederationLinkType(Enum):
    SIBLINGS = "siblings"
    LINEAGE = "lineage"
    NONE = "none"


class FederationConnectionDetail(ConfigModel):
    platform: Optional[str] = pydantic.Field(
        default=None,
        description="Override the DataHub platform auto-detected from the Unity Catalog "
        "connection type (e.g. 'mssql', 'postgres').",
    )
    platform_instance: Optional[str] = pydantic.Field(
        default=None,
        description="platform_instance the external source was ingested under. Must match "
        "for the sibling/lineage link to resolve.",
    )
    env: Optional[str] = pydantic.Field(
        default=None, description="env of the external dataset (defaults to the source env)."
    )
    database: Optional[str] = pydantic.Field(
        default=None,
        description="Override the remote database name (falls back to the foreign catalog's "
        "connection options).",
    )
    convert_urns_to_lowercase: Optional[bool] = pydantic.Field(
        default=None,
        description="Override case-folding of the external URN (defaults to the source's "
        "convert_urns_to_lowercase). Must match how the external source was ingested.",
    )
```

Add the four fields inside `UnityCatalogSourceConfig`, immediately after the `include_column_lineage` field (after line 297):

```python
    federation_link_type: FederationLinkType = pydantic.Field(
        default=FederationLinkType.SIBLINGS,
        description="How to link a Lakehouse Federation foreign catalog's tables to the "
        "external source dataset: 'siblings' (merge into one logical dataset — correct for a "
        "read-only mirror), 'lineage' (an upstream edge), or 'none'. Never both.",
    )
    emit_federation_structured_properties: bool = pydantic.Field(
        default=True,
        description="Define and assign structured properties marking foreign catalogs as "
        "federated (facetable in the UI).",
    )
    federation_structured_property_namespace: str = pydantic.Field(
        default="databricks.federation",
        description="Qualified-name namespace for the federation structured properties "
        "(e.g. '<namespace>.platform').",
    )
    federation_connection_details: Dict[str, FederationConnectionDetail] = pydantic.Field(
        default_factory=dict,
        description="Per-connection overrides keyed by Unity Catalog connection name.",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k federation_config`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add metadata-ingestion/src/datahub/ingestion/source/unity/config.py metadata-ingestion/tests/unit/test_unity_catalog_federation.py
git commit -m "feat(ingest/unity): add federation config (link type, overrides, structured-property namespace)"
```

---

## Task 4: `federation.py` — platform + external URN resolution

**Files:**
- Create: `src/datahub/ingestion/source/unity/federation.py`
- Test: `tests/unit/test_unity_catalog_federation.py`

**Interfaces:**
- Produces:
  - `CONNECTION_TYPE_TO_PLATFORM: Dict[ConnectionType, str]`
  - `DATABASE_OPTION_KEY: Dict[ConnectionType, Optional[str]]`
  - `KNOWN_FEDERATION_PLATFORMS: List[str]`
  - `@dataclass FederationTarget` with `platform: str`, `remote_database: Optional[str]`
  - `resolve_federation_target(connection_type: Optional[ConnectionType], options: Optional[Dict[str, str]], override_platform: Optional[str], override_database: Optional[str]) -> Optional[FederationTarget]`
  - `external_dataset_name(target: FederationTarget, schema: str, table: str) -> str`

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_unity_catalog_federation.py`:

```python
from datahub.ingestion.source.unity import federation as fed


def test_resolve_three_tier_uses_database_option():
    target = fed.resolve_federation_target(
        ConnectionType.POSTGRESQL, {"database": "my_db"}, None, None
    )
    assert target is not None
    assert target.platform == "postgres"
    assert target.remote_database == "my_db"
    assert fed.external_dataset_name(target, "my_schema", "t") == "my_db.my_schema.t"


def test_resolve_two_tier_has_no_database():
    target = fed.resolve_federation_target(ConnectionType.MYSQL, None, None, None)
    assert target is not None
    assert target.platform == "mysql"
    assert target.remote_database is None
    assert fed.external_dataset_name(target, "my_schema", "t") == "my_schema.t"


def test_resolve_bigquery_uses_data_project_id():
    target = fed.resolve_federation_target(
        ConnectionType.BIGQUERY, {"dataProjectId": "proj"}, None, None
    )
    assert target.platform == "bigquery"
    assert fed.external_dataset_name(target, "ds", "t") == "proj.ds.t"


def test_resolve_databricks_to_databricks_uses_catalog():
    target = fed.resolve_federation_target(
        ConnectionType.DATABRICKS, {"catalog": "remote_cat"}, None, None
    )
    assert target.platform == "databricks"
    assert fed.external_dataset_name(target, "s", "t") == "remote_cat.s.t"


def test_override_wins_over_autodetect():
    target = fed.resolve_federation_target(
        ConnectionType.POSTGRESQL, {"database": "auto_db"}, "mssql", "override_db"
    )
    assert target.platform == "mssql"
    assert target.remote_database == "override_db"


def test_three_tier_missing_database_returns_none():
    # three-tier connector but options lack the key and no override -> cannot resolve
    assert (
        fed.resolve_federation_target(ConnectionType.POSTGRESQL, {}, None, None) is None
    )


def test_unmapped_connection_type_returns_none():
    assert (
        fed.resolve_federation_target(
            ConnectionType.UNKNOWN_CONNECTION_TYPE, None, None, None
        )
        is None
    )


def test_override_platform_without_connection_type():
    # connections API unavailable (connection_type None) but user supplied platform+db
    target = fed.resolve_federation_target(None, None, "mssql", "my_db")
    assert target.platform == "mssql"
    assert target.remote_database == "my_db"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k "resolve or override or tier or unmapped"`
Expected: FAIL — `ModuleNotFoundError: ... unity.federation`.

- [ ] **Step 3: Implement `federation.py`**

Create `src/datahub/ingestion/source/unity/federation.py`:

```python
"""Lakehouse Federation helpers: map Unity Catalog connections to DataHub
platforms and build external dataset URN names for foreign catalogs.

A foreign catalog is a read-only mirror of an external database; its schemas and
tables map 1:1 to the remote system, so only the database prefix differs by
connector. See docs: CREATE FOREIGN CATALOG OPTIONS (database / dataProjectId / catalog).
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

from databricks.sdk.service.catalog import ConnectionType

# Unity Catalog connection type -> DataHub platform id.
CONNECTION_TYPE_TO_PLATFORM: Dict[ConnectionType, str] = {
    ConnectionType.MYSQL: "mysql",
    ConnectionType.POSTGRESQL: "postgres",
    ConnectionType.SQLSERVER: "mssql",
    ConnectionType.SQLDW: "mssql",
    ConnectionType.SNOWFLAKE: "snowflake",
    ConnectionType.REDSHIFT: "redshift",
    ConnectionType.BIGQUERY: "bigquery",
    ConnectionType.GLUE: "glue",
    ConnectionType.ORACLE: "oracle",
    ConnectionType.TERADATA: "teradata",
    ConnectionType.DATABRICKS: "databricks",
    ConnectionType.HIVE_METASTORE: "hive",
}

# Key in the foreign catalog's `options` holding the remote database/catalog name.
# None => two-tier namespace (no database segment in the external URN).
DATABASE_OPTION_KEY: Dict[ConnectionType, Optional[str]] = {
    ConnectionType.MYSQL: None,
    ConnectionType.POSTGRESQL: "database",
    ConnectionType.SQLSERVER: "database",
    ConnectionType.SQLDW: "database",
    ConnectionType.SNOWFLAKE: "database",
    ConnectionType.REDSHIFT: "database",
    ConnectionType.ORACLE: "database",
    ConnectionType.TERADATA: "database",
    ConnectionType.BIGQUERY: "dataProjectId",
    ConnectionType.DATABRICKS: "catalog",
    ConnectionType.GLUE: None,
    ConnectionType.HIVE_METASTORE: None,
}

KNOWN_FEDERATION_PLATFORMS: List[str] = sorted(set(CONNECTION_TYPE_TO_PLATFORM.values()))


@dataclass
class FederationTarget:
    platform: str
    remote_database: Optional[str]  # None for two-tier platforms


def resolve_federation_target(
    connection_type: Optional[ConnectionType],
    options: Optional[Dict[str, str]],
    override_platform: Optional[str],
    override_database: Optional[str],
) -> Optional[FederationTarget]:
    """Resolve the external platform + remote database for a foreign catalog.

    Returns None when the platform cannot be determined, or when a three-tier
    connector's remote database is neither in `options` nor overridden (emitting a
    URN without it would dangle).
    """
    platform = override_platform
    if not platform and connection_type is not None:
        platform = CONNECTION_TYPE_TO_PLATFORM.get(connection_type)
    if not platform:
        return None

    if override_database:
        return FederationTarget(platform=platform, remote_database=override_database)

    # No connection type known (e.g. connections API unavailable) and no db override:
    # emit a two-tier URN rather than nothing.
    if connection_type is None:
        return FederationTarget(platform=platform, remote_database=None)

    db_key = DATABASE_OPTION_KEY.get(connection_type)
    if db_key is None:
        return FederationTarget(platform=platform, remote_database=None)

    remote_database = (options or {}).get(db_key)
    if not remote_database:
        return None
    return FederationTarget(platform=platform, remote_database=remote_database)


def external_dataset_name(target: FederationTarget, schema: str, table: str) -> str:
    if target.remote_database:
        return f"{target.remote_database}.{schema}.{table}"
    return f"{schema}.{table}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k "resolve or override or tier or unmapped"`
Expected: PASS (8 passed).

- [ ] **Step 5: Commit**

```bash
git add metadata-ingestion/src/datahub/ingestion/source/unity/federation.py metadata-ingestion/tests/unit/test_unity_catalog_federation.py
git commit -m "feat(ingest/unity): add federation platform + external URN resolution"
```

---

## Task 5: `federation.py` — structured-property definitions

**Files:**
- Modify: `src/datahub/ingestion/source/unity/federation.py`
- Test: `tests/unit/test_unity_catalog_federation.py`

**Interfaces:**
- Consumes: `KNOWN_FEDERATION_PLATFORMS` (Task 4).
- Produces:
  - `FEDERATION_PROPERTY_SUFFIXES: List[str]` = `["catalog_type", "platform", "connection", "remote_database"]`
  - `structured_property_urns(namespace: str) -> Dict[str, str]` (suffix → `urn:li:structuredProperty:<namespace>.<suffix>`)
  - `federation_property_definition_mcps(namespace: str) -> List[MetadataChangeProposalWrapper]`

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_unity_catalog_federation.py`:

```python
from datahub.metadata.schema_classes import StructuredPropertyDefinitionClass


def test_structured_property_urns():
    urns = fed.structured_property_urns("databricks.federation")
    assert urns["platform"] == "urn:li:structuredProperty:databricks.federation.platform"
    assert set(urns) == {"catalog_type", "platform", "connection", "remote_database"}


def test_property_definition_mcps_target_container_and_platform_allowed_values():
    mcps = fed.federation_property_definition_mcps("databricks.federation")
    assert len(mcps) == 4
    by_qn = {m.aspect.qualifiedName: m.aspect for m in mcps}
    platform_def = by_qn["databricks.federation.platform"]
    assert isinstance(platform_def, StructuredPropertyDefinitionClass)
    assert platform_def.entityTypes == ["urn:li:entityType:datahub.container"]
    assert platform_def.valueType == "urn:li:dataType:datahub.string"
    allowed = {av.value for av in (platform_def.allowedValues or [])}
    assert "mssql" in allowed and "postgres" in allowed
    # non-enumerated property has no allowedValues
    assert by_qn["databricks.federation.connection"].allowedValues is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k "structured_property_urns or definition_mcps"`
Expected: FAIL — `AttributeError: module ... has no attribute 'structured_property_urns'`.

- [ ] **Step 3: Implement — append to `federation.py`**

Add imports at the top of `federation.py`:

```python
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import PropertyValueClass
from datahub.metadata.urns import (
    ContainerUrn,
    DataTypeUrn,
    EntityTypeUrn,
    StructuredPropertyUrn,
)
```

Append to the module:

```python
FEDERATION_PROPERTY_SUFFIXES: List[str] = [
    "catalog_type",
    "platform",
    "connection",
    "remote_database",
]

_PROPERTY_DISPLAY: Dict[str, str] = {
    "catalog_type": "Catalog Type",
    "platform": "Federation Platform",
    "connection": "Federation Connection",
    "remote_database": "Federation Remote Database",
}

_PROPERTY_DESCRIPTION: Dict[str, str] = {
    "catalog_type": "Unity Catalog catalog type (FOREIGN_CATALOG for Lakehouse Federation).",
    "platform": "DataHub platform of the external system this foreign catalog mirrors.",
    "connection": "Unity Catalog connection backing this foreign catalog.",
    "remote_database": "Name of the external database/project/catalog mirrored by this catalog.",
}


def structured_property_urns(namespace: str) -> Dict[str, str]:
    return {
        suffix: StructuredPropertyUrn(f"{namespace}.{suffix}").urn()
        for suffix in FEDERATION_PROPERTY_SUFFIXES
    }


def federation_property_definition_mcps(
    namespace: str,
) -> List[MetadataChangeProposalWrapper]:
    container_entity_type = EntityTypeUrn(f"datahub.{ContainerUrn.ENTITY_TYPE}").urn()
    value_type = DataTypeUrn("datahub.string").urn()
    mcps: List[MetadataChangeProposalWrapper] = []
    for suffix in FEDERATION_PROPERTY_SUFFIXES:
        qualified_name = f"{namespace}.{suffix}"
        allowed_values = None
        if suffix == "platform":
            allowed_values = [
                PropertyValueClass(value=platform)
                for platform in KNOWN_FEDERATION_PLATFORMS
            ]
        aspect = StructuredPropertyDefinition(
            qualifiedName=qualified_name,
            displayName=_PROPERTY_DISPLAY[suffix],
            description=_PROPERTY_DESCRIPTION[suffix],
            valueType=value_type,
            entityTypes=[container_entity_type],
            cardinality="SINGLE",
            allowedValues=allowed_values,
        )
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=StructuredPropertyUrn(qualified_name).urn(), aspect=aspect
            )
        )
    return mcps
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k "structured_property_urns or definition_mcps"`
Expected: PASS (2 passed).

- [ ] **Step 5: Verify `ContainerUrn.ENTITY_TYPE` resolves to `"container"`**

Run: `python -c "from datahub.metadata.urns import ContainerUrn; print(ContainerUrn.ENTITY_TYPE)"`
Expected: prints `container`. (If it prints something else, update the test's expected `entityTypes` value accordingly — this only affects the assertion string.)

- [ ] **Step 6: Commit**

```bash
git add metadata-ingestion/src/datahub/ingestion/source/unity/federation.py metadata-ingestion/tests/unit/test_unity_catalog_federation.py
git commit -m "feat(ingest/unity): add federation structured-property definitions"
```

---

## Task 6: Report counters

**Files:**
- Modify: `src/datahub/ingestion/source/unity/report.py` (after line 71)

**Interfaces:**
- Produces: `report.num_foreign_catalogs`, `report.num_federation_links_emitted`, `report.num_federation_targets_unresolved`. (`num_federation_connections_list_failed` was added in Task 2.)

- [ ] **Step 1: Add counters**

In `report.py`, directly below `num_federation_connections_list_failed` (added in Task 2) add:

```python
    num_foreign_catalogs: int = 0
    num_federation_links_emitted: int = 0
    num_federation_targets_unresolved: int = 0
```

- [ ] **Step 2: Verify import compiles**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k connections`
Expected: PASS (report still constructs).

- [ ] **Step 3: Commit**

```bash
git add metadata-ingestion/src/datahub/ingestion/source/unity/report.py
git commit -m "feat(ingest/unity): add federation report counters"
```

---

## Task 7: Assign structured properties on the foreign-catalog container + emit definitions

**Files:**
- Modify: `src/datahub/ingestion/source/unity/source.py` (`gen_catalog_containers` ~1201; `get_workunits_internal` ~483)
- Test: `tests/unit/test_unity_catalog_federation.py`

**Interfaces:**
- Consumes: `Catalog.is_foreign_catalog`, `Proxy.connections()`, `resolve_federation_target`, `structured_property_urns`, `federation_property_definition_mcps`.
- Produces: catalog container workunit carrying `structuredProperties`; standalone `propertyDefinition` workunits.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_unity_catalog_federation.py`:

```python
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.schema_classes import StructuredPropertiesClass


def _foreign_catalog():
    ms = Metastore(id="ms", name="ms", comment=None, global_metastore_id=None,
                   metastore_id=None, owner=None, region=None, cloud=None)
    return Catalog(id="c", name="my_catalog", metastore=ms, comment=None, owner=None,
                   type=CatalogType.FOREIGN_CATALOG, connection_name="pg_conn",
                   options={"database": "my_db"})


def _make_source():
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {**_BASE, "include_metastore": False}
        )
        src = UnityCatalogSource(cfg, PipelineContext(run_id="t"))
    return src


def test_foreign_catalog_container_has_structured_properties():
    from unittest.mock import patch as _patch
    src = _make_source()
    src.unity_catalog_api_proxy.connections = lambda: {
        "pg_conn": ConnectionInfo(name="pg_conn", connection_type=ConnectionType.POSTGRESQL)
    }
    wus = list(src.gen_catalog_containers(_foreign_catalog()))
    sp_aspects = [
        wu.get_aspect_of_type(StructuredPropertiesClass)
        for wu in wus
        if wu.get_aspect_of_type(StructuredPropertiesClass) is not None
    ]
    assert len(sp_aspects) == 1
    assigned = {
        p.propertyUrn: p.values[0] for p in sp_aspects[0].properties
    }
    assert assigned["urn:li:structuredProperty:databricks.federation.platform"] == "postgres"
    assert assigned["urn:li:structuredProperty:databricks.federation.remote_database"] == "my_db"
```

`patch`, `CatalogType`, `ConnectionInfo`, `ConnectionType`, `Catalog`, `Metastore`, `UnityCatalogSourceConfig`, `UnityCatalogSource` are already imported in the test file from earlier tasks; add `from unittest.mock import patch` and `from datahub.ingestion.source.unity.source import UnityCatalogSource` at the top if not present.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k foreign_catalog_container`
Expected: FAIL — no `StructuredPropertiesClass` aspect emitted.

- [ ] **Step 3: Implement — imports in `source.py`**

Add to `source.py` imports:

```python
from datahub.ingestion.source.unity import federation
from datahub.ingestion.source.unity.config import FederationLinkType
from datahub.metadata.urns import StructuredPropertyUrn
```

(`federation` is the new module; `FederationLinkType` from config; `StructuredPropertyUrn` from `datahub.metadata.urns` where `TagUrn` etc. are already imported at line 158.)

- [ ] **Step 4: Add a helper to build the catalog's structured-property map**

Add a method to `UnityCatalogSource` (near `gen_catalog_containers`, ~line 1200):

```python
    def _federation_structured_properties(
        self, catalog: Catalog
    ) -> Optional[Dict[StructuredPropertyUrn, str]]:
        """Structured-property assignments for a foreign catalog, or None."""
        if not (
            catalog.is_foreign_catalog
            and self.config.emit_federation_structured_properties
        ):
            return None
        ns = self.config.federation_structured_property_namespace
        urns = federation.structured_property_urns(ns)
        values: Dict[str, str] = {"catalog_type": "FOREIGN_CATALOG"}
        if catalog.connection_name:
            values["connection"] = catalog.connection_name
        detail = self.config.federation_connection_details.get(
            catalog.connection_name or ""
        )
        connection = self.unity_catalog_api_proxy.connections().get(
            catalog.connection_name or ""
        )
        target = federation.resolve_federation_target(
            connection.connection_type if connection else None,
            catalog.options,
            detail.platform if detail else None,
            detail.database if detail else None,
        )
        if target:
            values["platform"] = target.platform
            if target.remote_database:
                values["remote_database"] = target.remote_database
        return {
            StructuredPropertyUrn(f"{ns}.{suffix}"): value
            for suffix, value in values.items()
            if suffix in urns
        }
```

- [ ] **Step 5: Pass the map to `gen_containers`**

In `gen_catalog_containers` (replace the `gen_containers(...)` call at 1213-1229), add `structured_properties=` and count foreign catalogs:

```python
        catalog_container_key = self.gen_catalog_key(catalog)
        structured_properties = self._federation_structured_properties(catalog)
        if catalog.is_foreign_catalog:
            self.report.num_foreign_catalogs += 1
        yield from gen_containers(
            container_key=catalog_container_key,
            name=catalog.name,
            sub_types=[DatasetContainerSubTypes.CATALOG],
            domain_urn=domain_urn,
            parent_container_key=(
                self.gen_metastore_key(catalog.metastore)
                if self.config.include_metastore and catalog.metastore
                else None
            ),
            description=catalog.comment,
            owner_urn=self.get_owner_urn(catalog.owner),
            external_url=f"{self.external_url_base}/{catalog.name}",
            tags=[tag.to_datahub_tag_urn().name for tag in catalog_tags]
            if catalog_tags
            else None,
            structured_properties=structured_properties,
        )
```

- [ ] **Step 6: Run the container test to verify it passes**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k foreign_catalog_container`
Expected: PASS.

- [ ] **Step 7: Emit property definitions once — write the failing test**

Append to the test file:

```python
def test_property_definitions_emitted_once_when_enabled():
    src = _make_source()
    src.unity_catalog_api_proxy.connections = lambda: {}
    wus = list(src._gen_federation_property_definition_workunits())
    qns = {
        wu.get_aspect_of_type(StructuredPropertyDefinitionClass).qualifiedName
        for wu in wus
    }
    assert qns == {
        "databricks.federation.catalog_type",
        "databricks.federation.platform",
        "databricks.federation.connection",
        "databricks.federation.remote_database",
    }


def test_no_property_definitions_when_disabled():
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {**_BASE, "emit_federation_structured_properties": False}
        )
        src = UnityCatalogSource(cfg, PipelineContext(run_id="t"))
    assert list(src._gen_federation_property_definition_workunits()) == []
```

- [ ] **Step 8: Run to verify it fails**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k property_definitions`
Expected: FAIL — method missing.

- [ ] **Step 9: Implement the definitions emitter**

Add to `UnityCatalogSource`:

```python
    def _gen_federation_property_definition_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Emit federation structured-property definitions once per run.

        When a graph is available, skip definitions that already exist so a
        centrally-managed definition is never clobbered.
        """
        if not self.config.emit_federation_structured_properties:
            return
        mcps = federation.federation_property_definition_mcps(
            self.config.federation_structured_property_namespace
        )
        graph = self.ctx.graph
        for mcp in mcps:
            if graph is not None:
                try:
                    existing = graph.get_aspect(
                        mcp.entityUrn, StructuredPropertyDefinitionClass
                    )
                    if existing is not None:
                        continue
                except Exception as e:
                    logger.debug(f"Federation property existence check failed: {e}")
            yield mcp.as_workunit()
```

Add `StructuredPropertyDefinitionClass` to the `datahub.metadata.schema_classes` import group in `source.py` (the import block ending at line 157).

- [ ] **Step 10: Wire it into `get_workunits_internal`**

In `get_workunits_internal` (starts line 483), immediately before `yield from self.process_metastores()` (line 508), add:

```python
        yield from self._gen_federation_property_definition_workunits()
```

- [ ] **Step 11: Run to verify it passes**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k "property_definitions or disabled"`
Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add metadata-ingestion/src/datahub/ingestion/source/unity/source.py metadata-ingestion/tests/unit/test_unity_catalog_federation.py
git commit -m "feat(ingest/unity): mark foreign catalogs with federation structured properties"
```

---

## Task 8: Per-table federation link (siblings / lineage)

**Files:**
- Modify: `src/datahub/ingestion/source/unity/source.py` (`process_table` ~760-894)
- Test: `tests/unit/test_unity_catalog_federation.py`

**Interfaces:**
- Consumes: `resolve_federation_target`, `external_dataset_name`, existing `gen_siblings_workunit` / `gen_lineage_workunit` (source.py:2260-2293), `Catalog.is_foreign_catalog`.
- Produces: per-table `Siblings` or `UpstreamLineage` workunits to the external dataset URN.

- [ ] **Step 1: Write the failing test**

Append to the test file:

```python
from datahub.ingestion.source.unity.proxy_types import Schema, Table
from datahub.metadata.schema_classes import UpstreamLineageClass


def _foreign_table(catalog: Catalog) -> Table:
    schema = Schema(id="c.my_schema", name="my_schema", catalog=catalog, comment=None, owner=None)
    return Table(
        id="c.my_schema.t", name="t", comment=None, schema=schema, columns=[],
        storage_location=None, data_source_format=None, table_type=None, owner=None,
        generation=None, created_at=None, created_by=None, updated_at=None,
        updated_by=None, table_id=None, view_definition=None, properties={},
    )


def _source_with_link(link_type: str, lowercase: bool = False):
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate({
            **_BASE, "include_metastore": False,
            "federation_link_type": link_type,
            "convert_urns_to_lowercase": lowercase,
            "federation_connection_details": {
                "pg_conn": {"platform_instance": "prod-pg"}
            },
        })
        src = UnityCatalogSource(cfg, PipelineContext(run_id="t"))
    src.unity_catalog_api_proxy.connections = lambda: {
        "pg_conn": ConnectionInfo(name="pg_conn", connection_type=ConnectionType.POSTGRESQL)
    }
    return src


def test_federation_siblings_emitted_for_foreign_table():
    src = _source_with_link("siblings")
    catalog = _foreign_catalog()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.t,PROD)"
    wus = list(src._gen_federation_link(dataset_urn, _foreign_table(catalog), catalog))
    from datahub.metadata.schema_classes import SiblingsClass
    siblings = [wu.get_aspect_of_type(SiblingsClass) for wu in wus]
    siblings = [s for s in siblings if s is not None]
    assert siblings, "expected sibling aspects"
    external = "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-pg.my_db.my_schema.t,PROD)"
    assert any(external in s.siblings for s in siblings)


def test_federation_lineage_mode_emits_upstream():
    src = _source_with_link("lineage")
    catalog = _foreign_catalog()
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.t,PROD)"
    wus = list(src._gen_federation_link(dataset_urn, _foreign_table(catalog), catalog))
    up = [wu.get_aspect_of_type(UpstreamLineageClass) for wu in wus]
    up = [u for u in up if u is not None]
    assert up and up[0].upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-pg.my_db.my_schema.t,PROD)"
    )


def test_federation_link_none_emits_nothing():
    src = _source_with_link("none")
    catalog = _foreign_catalog()
    assert list(src._gen_federation_link("x", _foreign_table(catalog), catalog)) == []


def test_federation_link_lowercase_applied():
    src = _source_with_link("lineage", lowercase=True)
    catalog = Catalog(
        id="c", name="My_Catalog",
        metastore=Metastore(id="ms", name="ms", comment=None, global_metastore_id=None,
                            metastore_id=None, owner=None, region=None, cloud=None),
        comment=None, owner=None, type=CatalogType.FOREIGN_CATALOG,
        connection_name="pg_conn", options={"database": "My_DB"},
    )
    schema = Schema(id="c.My_Schema", name="My_Schema", catalog=catalog, comment=None, owner=None)
    table = Table(id="c.My_Schema.T", name="T", comment=None, schema=schema, columns=[],
                  storage_location=None, data_source_format=None, table_type=None, owner=None,
                  generation=None, created_at=None, created_by=None, updated_at=None,
                  updated_by=None, table_id=None, view_definition=None, properties={})
    wus = list(src._gen_federation_link("x", table, catalog))
    up = [wu.get_aspect_of_type(UpstreamLineageClass) for wu in wus if wu.get_aspect_of_type(UpstreamLineageClass)]
    assert up[0].upstreams[0].dataset == (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-pg.my_db.my_schema.t,PROD)"
    )
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k federation_link`
Expected: FAIL — `_gen_federation_link` missing.

- [ ] **Step 3: Implement `_gen_federation_link`**

Add to `UnityCatalogSource`:

```python
    def _gen_federation_link(
        self, dataset_urn: str, table: Table, catalog: Catalog
    ) -> Iterable[MetadataWorkUnit]:
        """Emit the single configured cross-platform link (siblings XOR lineage) from
        a foreign-catalog table to its external source dataset."""
        if (
            not catalog.is_foreign_catalog
            or self.config.federation_link_type == FederationLinkType.NONE
        ):
            return
        detail = self.config.federation_connection_details.get(
            catalog.connection_name or ""
        )
        connection = self.unity_catalog_api_proxy.connections().get(
            catalog.connection_name or ""
        )
        target = federation.resolve_federation_target(
            connection.connection_type if connection else None,
            catalog.options,
            detail.platform if detail else None,
            detail.database if detail else None,
        )
        if target is None:
            self.report.num_federation_targets_unresolved += 1
            logger.warning(
                f"Could not resolve federation target for catalog {catalog.name} "
                f"(connection {catalog.connection_name}); skipping link."
            )
            return

        name = federation.external_dataset_name(
            target, table.schema.name, table.name
        )
        lowercase = self.config.convert_urns_to_lowercase
        if detail is not None and detail.convert_urns_to_lowercase is not None:
            lowercase = detail.convert_urns_to_lowercase
        if lowercase:
            name = name.lower()
        external_urn = make_dataset_urn_with_platform_instance(
            platform=target.platform,
            name=name,
            platform_instance=detail.platform_instance if detail else None,
            env=detail.env if (detail and detail.env) else self.config.env,
        )

        self.report.num_federation_links_emitted += 1
        if self.config.federation_link_type == FederationLinkType.SIBLINGS:
            yield from self.gen_siblings_workunit(dataset_urn, external_urn)
        else:
            yield from self.gen_lineage_workunit(dataset_urn, external_urn)
```

- [ ] **Step 4: Call it from `process_table`**

`process_table` yields table workunits at 878-894. Immediately after that `yield from [...]` block (after line 894, before the method returns), add:

```python
        yield from self._gen_federation_link(dataset_urn, table, table.schema.catalog)
```

`dataset_urn` and `table` are already in scope in `process_table`; `table.schema.catalog` gives the `Catalog`.

- [ ] **Step 5: Run to verify it passes**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v -k federation_link`
Expected: PASS (4 passed).

- [ ] **Step 6: Run the full federation unit file**

Run: `pytest tests/unit/test_unity_catalog_federation.py -v`
Expected: PASS (all).

- [ ] **Step 7: Commit**

```bash
git add metadata-ingestion/src/datahub/ingestion/source/unity/source.py metadata-ingestion/tests/unit/test_unity_catalog_federation.py
git commit -m "feat(ingest/unity): emit siblings/lineage link for foreign-catalog tables"
```

---

## Task 9: Integration golden test

**Files:**
- Modify: `tests/integration/unity/test_unity_catalog_ingest.py` (`register_mock_data` ~189-204; add a test near `test_ingestion` ~555)
- Create: `tests/integration/unity/unity_catalog_federation_mces_golden.json` (generated)

**Interfaces:**
- Consumes: everything above end-to-end via `Pipeline`.

- [ ] **Step 1: Add a foreign catalog + connection to the mock**

In `register_mock_data(workspace_client)` (line 167), extend the `workspace_client.catalogs.list` fixture (currently building `CatalogInfo.from_dict(...)` at ~189-204) to include a foreign catalog, and stub the connections API. Add, right after the existing `catalogs.list` assignment:

```python
    # Lakehouse Federation: a foreign catalog mirroring an external Postgres db.
    existing_catalogs = list(workspace_client.catalogs.list.return_value)
    existing_catalogs.append(
        CatalogInfo.from_dict(
            {
                "name": "federated_catalog",
                "catalog_type": "FOREIGN_CATALOG",
                "connection_name": "pg_conn",
                "options": {"database": "my_db"},
                "comment": "Foreign catalog over Postgres",
                "owner": "account users",
                "metastore_id": "123",
            }
        )
    )
    workspace_client.catalogs.list.return_value = existing_catalogs

    from databricks.sdk.service.catalog import ConnectionInfo, ConnectionType

    workspace_client.connections.list.return_value = [
        ConnectionInfo(name="pg_conn", connection_type=ConnectionType.POSTGRESQL)
    ]
```

Then extend `workspace_client.schemas.list` and the `tables.list` lambda so the new catalog yields one schema (`my_schema`) with one table (`t`). Because `tables.list` is a lambda keyed on nothing, guard by `catalog_name`:

Locate the `tables.list = lambda ...` (line ~227) and replace with a function that branches on `catalog_name`:

```python
    def _tables_list(*args, **kwargs):
        catalog_name = kwargs.get("catalog_name")
        if catalog_name == "federated_catalog":
            return [
                databricks.sdk.service.catalog.TableInfo.from_dict(
                    {
                        "name": "t",
                        "catalog_name": "federated_catalog",
                        "schema_name": "my_schema",
                        "table_type": "FOREIGN",
                        "columns": [
                            {"name": "id", "type_name": "INT", "type_text": "int",
                             "nullable": True, "position": 0}
                        ],
                    }
                )
            ]
        return _original_tables()  # the original list built below

    # (assign the two original tables to a local, then set the lambda)
```

Keep the original two tables for the non-federated catalog by capturing them in `_original_tables`. Mirror the existing `schemas.list` fixture to add `my_schema` under `federated_catalog` similarly (branch on `catalog_name` if needed, or append a `SchemaInfo` with `catalog_name="federated_catalog"`).

> Implementation note: match the exact dict shape the existing `TableInfo.from_dict`/`SchemaInfo.from_dict` fixtures use in this file (copy their keys). Do not invent fields.

- [ ] **Step 2: Add the golden test**

After `test_ingestion` (ends ~624), add:

```python
@time_machine.travel(
    datetime.fromisoformat(FROZEN_TIME).replace(tzinfo=timezone.utc), tick=False
)
def test_federation_ingestion(pytestconfig, tmp_path, requests_mock):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/unity"
    register_mock_api(request_mock=requests_mock)
    output_file_name = "unity_catalog_federation_mcps.json"
    with (
        patch("datahub.ingestion.source.unity.connection.WorkspaceClient") as mock_client,
        patch.object(HiveMetastoreProxy, "get_inspector") as get_inspector,
        patch.object(HiveMetastoreProxy, "_execute_sql") as execute_sql,
    ):
        workspace_client = mock.MagicMock()
        mock_client.return_value = workspace_client
        register_mock_data(workspace_client)
        inspector = mock.MagicMock()
        inspector.get_schema_names.return_value = []
        get_inspector.return_value = inspector
        execute_sql.side_effect = mock_hive_sql

        config_dict = {
            "run_id": "unity-federation-test",
            "pipeline_name": "unity-federation-test-pipeline",
            "source": {
                "type": "unity-catalog",
                "config": {
                    "workspace_url": "https://dummy.cloud.databricks.com",
                    "token": "fake",
                    "include_hive_metastore": False,
                    "include_usage_statistics": False,
                    "warehouse_id": "test",
                    "federation_link_type": "siblings",
                    "federation_connection_details": {
                        "pg_conn": {"platform_instance": "prod-pg", "env": "PROD"}
                    },
                },
            },
            "sink": {"type": "file", "config": {"filename": f"/{tmp_path}/{output_file_name}"}},
        }
        pipeline = Pipeline.create(config_dict)
        pipeline.run()
        pipeline.raise_from_status()
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"/{tmp_path}/{output_file_name}",
            golden_path=f"{test_resources_dir}/unity_catalog_federation_mces_golden.json",
        )
```

- [ ] **Step 3: Generate the golden file**

Run with golden-update enabled:

```bash
pytest tests/integration/unity/test_unity_catalog_ingest.py::test_federation_ingestion -v --update-golden-files
```
Expected: PASS, and `unity_catalog_federation_mces_golden.json` is created.

- [ ] **Step 4: Inspect the golden file**

Confirm it contains:
- a `container` for `federated_catalog` with a `structuredProperties` aspect (platform=`postgres`, remote_database=`my_db`, catalog_type=`FOREIGN_CATALOG`, connection=`pg_conn`),
- four `structuredPropertyDefinition` (propertyDefinition) entities,
- a `siblings` aspect on the databricks table URN pointing at `urn:li:dataset:(urn:li:dataPlatform:postgres,prod-pg.my_db.my_schema.t,PROD)` and the reciprocal on the postgres URN.

Grep check:

```bash
grep -c '"structuredProperties"' tests/integration/unity/unity_catalog_federation_mces_golden.json
grep 'dataPlatform:postgres,prod-pg.my_db.my_schema.t' tests/integration/unity/unity_catalog_federation_mces_golden.json
```
Expected: first ≥ 1; second prints matching lines.

- [ ] **Step 5: Re-run without update to confirm determinism**

Run: `pytest tests/integration/unity/test_unity_catalog_ingest.py::test_federation_ingestion -v`
Expected: PASS.

- [ ] **Step 6: Run the whole Unity integration file to catch regressions**

Run: `pytest tests/integration/unity/test_unity_catalog_ingest.py -v`
Expected: PASS (existing goldens unaffected — the new catalog only adds workunits; if `test_ingestion`'s golden now differs because the shared `register_mock_data` added a catalog, split the fixture so only `test_federation_ingestion` sees the foreign catalog. See note below.)

> **Fixture-isolation note:** `register_mock_data` is shared by all golden tests. Adding the foreign catalog unconditionally will change `unity_catalog_mces_golden.json`. To avoid churn, gate the federation additions behind a parameter: `def register_mock_data(workspace_client, include_federation=False)` and only pass `include_federation=True` from `test_federation_ingestion`. Update the call in `test_federation_ingestion` accordingly. Do NOT regenerate the other goldens.

- [ ] **Step 7: Commit**

```bash
git add metadata-ingestion/tests/integration/unity/test_unity_catalog_ingest.py metadata-ingestion/tests/integration/unity/unity_catalog_federation_mces_golden.json
git commit -m "test(ingest/unity): integration golden for Lakehouse Federation lineage"
```

---

## Task 10: Lint, docs, final verification

**Files:**
- Modify: `metadata-ingestion/docs/sources/databricks/unity-catalog_pre.md` (or the unity source docs) — a short "Lakehouse Federation" subsection.

- [ ] **Step 1: Run lintFix (ruff)**

Run: `../gradlew :metadata-ingestion:lintFix`
Expected: no errors; files reformatted if needed.

- [ ] **Step 2: Run mypy**

Run: `../gradlew :metadata-ingestion:lint`
Expected: no type errors in touched files.

- [ ] **Step 3: Run all federation unit tests + unity source/proxy tests**

Run:
```bash
pytest tests/unit/test_unity_catalog_federation.py tests/unit/test_unity_catalog_source.py tests/unit/test_unity_catalog_proxy.py tests/unit/test_unity_catalog_config.py -v
```
Expected: PASS.

- [ ] **Step 4: Add connector docs**

Add a short subsection to the Unity Catalog source docs describing Lakehouse Federation support (find the doc via `ls metadata-ingestion/docs/sources/databricks/`). Content:

```markdown
### Lakehouse Federation (foreign catalogs)

DataHub detects Unity Catalog **foreign catalogs** (Lakehouse Federation) and links
their tables to the external source dataset it mirrors (PostgreSQL, SQL Server, MySQL,
Snowflake, Redshift, BigQuery, Oracle, Teradata, another Databricks workspace, Glue/Hive).

- The foreign catalog is marked with structured properties (`platform`, `remote_database`,
  `connection`, `catalog_type`).
- `federation_link_type` controls the cross-platform link: `siblings` (default — merges the
  Databricks table and the external table into one logical dataset), `lineage` (an upstream
  edge), or `none`.
- For the link to resolve, the external source must be ingested separately and its
  `platform_instance` / `convert_urns_to_lowercase` must match. Use
  `federation_connection_details` (keyed by connection name) to align them:

  ```yaml
  federation_link_type: siblings
  federation_connection_details:
    pg_conn:
      platform_instance: prod-pg
      env: PROD
  ```
```

- [ ] **Step 5: Format docs**

Run: `../gradlew :datahub-web-react:mdPrettierWrite`
Expected: docs reformatted.

- [ ] **Step 6: Commit**

```bash
git add metadata-ingestion/docs/sources/databricks/
git commit -m "docs(ingest/unity): document Lakehouse Federation support"
```

---

## Self-Review Notes (traceability to the design spec)

- **Spec §A (detection/modelling → structured props, no new subtype/entity):** Tasks 1, 5, 7. `CATALOG` subtype untouched (Task 7 Step 5 keeps `sub_types=[DatasetContainerSubTypes.CATALOG]`).
- **Spec §B (connection resolution + platform map):** Tasks 2, 4. Unmapped/unknown → skip + warn (Task 4 `test_unmapped_connection_type_returns_none`; Task 8 unresolved counter).
- **Spec §C (URN construction + per-connector option key + normalization):** Task 4 (`DATABASE_OPTION_KEY`, two/three-tier, BigQuery `dataProjectId`, Databricks `catalog`) + Task 8 (lowercase + platform_instance alignment — the #1 risk, `test_federation_link_lowercase_applied`).
- **Spec §D (one link type, not both):** Task 3 (`FederationLinkType` enum) + Task 8 (siblings XOR lineage; `none` emits nothing).
- **Spec §E (config surface):** Task 3.
- **Spec risks:** connections permissions → Task 2 (`test_connections_returns_empty_on_error`); org-global definitions → Task 7 Step 9 (graph existence check + `emit_federation_structured_properties` off-switch + configurable namespace); URN mismatch → Task 8.
- **Column-level lineage (spec §D, lineage mode only):** NOT implemented in this plan (the reused `gen_lineage_workunit` is table-level). This is a deliberate scope cut — CLL for a 1:1 mirror is identity and low-value. If required, add a follow-up task extending `gen_lineage_workunit` with identity `fineGrainedLineages` built from `table.columns`, gated by `include_column_lineage`.
