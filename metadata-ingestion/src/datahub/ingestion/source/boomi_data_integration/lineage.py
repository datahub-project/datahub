from dataclasses import dataclass, replace
from typing import List, Optional, Tuple

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
    ConnectionPlatformDetail,
)
from datahub.ingestion.source.boomi_data_integration.constants import (
    ATHENA_TARGET,
    DI_PLATFORM_MAP,
    LOGIC_STEP_TARGET_NAMES,
    SQL_QUERY_STEP_SUFFIX,
    SQL_SCRIPT_STEP_SUFFIX,
    STEP_TARGET_TYPE_TABLE,
    TABLE_NAME_SEPARATOR,
    TARGET_LAYOUTS,
    TargetLayout,
)
from datahub.ingestion.source.boomi_data_integration.models import (
    LogicNode,
    SourceToTargetProperties,
    TableDetails,
)
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)


@dataclass
class TargetLocation:
    target_name: str
    connection_id: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    dataset_id: Optional[str] = None
    catalog_name: Optional[str] = None


@dataclass(frozen=True)
class DatasetScope:
    # How table names read or written through one connection become URNs.
    platform: str
    env: str
    lowercase: bool
    # Segments in a fully qualified name; SQL-parsed tables with any other
    # count would not join the platform's own URNs.
    name_parts: int
    # A longer form SQL may also use: `database.schema.table` on a platform whose
    # target names drop the database when connection_to_platform has none.
    full_name_parts: Optional[int] = None
    platform_instance: Optional[str] = None
    database: Optional[str] = None
    default_schema: Optional[str] = None

    def urn(self, name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=name.lower() if self.lowercase else name,
            platform_instance=self.platform_instance,
            env=self.env,
        )


@dataclass
class TableTarget:
    # One selected table of a multi-table river, or the single target of any
    # other river (with no details). `urn` is None when it did not resolve.
    urn: Optional[str]
    schema_name: Optional[str] = None
    details: Optional[TableDetails] = None


def _join(*parts: Optional[str]) -> Optional[str]:
    if not all(parts):
        return None
    return TABLE_NAME_SEPARATOR.join(part for part in parts if part)


def build_table_name(
    layout: TargetLayout, location: TargetLocation, table: str, database: Optional[str]
) -> Optional[str]:
    # `database` is the connection-level name from connection_to_platform, used
    # where the river itself does not carry the top-level name.
    if layout == TargetLayout.DATABASE_SCHEMA_TABLE:
        return _join(location.database_name, location.schema_name, table)
    if layout == TargetLayout.PROJECT_DATASET_TABLE:
        return _join(database, location.dataset_id, table)
    if layout == TargetLayout.CATALOG_SCHEMA_TABLE:
        return _join(location.catalog_name or database, location.schema_name, table)
    if layout == TargetLayout.SCHEMA_TABLE:
        if database:
            return _join(database, location.schema_name, table)
        return _join(location.schema_name, table)
    return None


def target_name_parts(layout: TargetLayout, database: Optional[str]) -> int:
    if layout == TargetLayout.SCHEMA_TABLE and not database:
        return 2
    return 3


def step_target_name(step_type: str) -> Optional[str]:
    if not step_type.endswith(SQL_QUERY_STEP_SUFFIX):
        return None
    return LOGIC_STEP_TARGET_NAMES.get(step_type[: -len(SQL_QUERY_STEP_SUFFIX)])


def sql_step_target_name(step_type: str) -> Tuple[Optional[str], bool]:
    # The warehouse a SQL query or script step runs on, and whether it is a
    # script (several statements, no target table).
    for suffix, is_script in (
        (SQL_QUERY_STEP_SUFFIX, False),
        (SQL_SCRIPT_STEP_SUFFIX, True),
    ):
        if step_type.endswith(suffix):
            return LOGIC_STEP_TARGET_NAMES.get(step_type[: -len(suffix)]), is_script
    return None, False


class TargetLineageResolver:
    def __init__(
        self,
        config: BoomiDataIntegrationSourceConfig,
        report: BoomiDataIntegrationSourceReport,
    ) -> None:
        self.config = config
        self.report = report

    def connection(
        self, connection_id: Optional[str]
    ) -> Optional[ConnectionPlatformDetail]:
        if connection_id is None:
            return None
        return self.config.connection_to_platform.get(connection_id)

    def scope(
        self,
        platform: str,
        connection: Optional[ConnectionPlatformDetail],
        name_parts: int,
    ) -> DatasetScope:
        lowercase = self.config.convert_lineage_urns_to_lowercase
        if connection is not None and connection.convert_urns_to_lowercase is not None:
            lowercase = connection.convert_urns_to_lowercase
        return DatasetScope(
            platform=platform,
            env=(connection.env if connection else None) or self.config.env,
            lowercase=lowercase,
            name_parts=name_parts,
            platform_instance=connection.platform_instance if connection else None,
            database=connection.database if connection else None,
            default_schema=connection.default_schema if connection else None,
        )

    def target_scope(
        self, target_name: str, connection_id: Optional[str]
    ) -> Optional[DatasetScope]:
        layout = TARGET_LAYOUTS.get(target_name)
        connection = self.connection(connection_id)
        platform = self._platform(target_name, connection)
        if layout is None or layout == TargetLayout.OBJECT_STORAGE or platform is None:
            return None
        database = connection.database if connection else None
        scope = self.scope(platform, connection, target_name_parts(layout, database))
        if layout == TargetLayout.SCHEMA_TABLE and target_name != ATHENA_TARGET:
            # Fully qualified SQL names the database itself, as the platform's own
            # source does. (Athena's three-part form starts with the catalog.)
            return replace(scope, full_name_parts=3)
        return scope

    def _platform(
        self, target_name: str, connection: Optional[ConnectionPlatformDetail]
    ) -> Optional[str]:
        if connection is not None:
            return connection.platform
        return self.config.connection_type_platform_map.get(
            target_name, DI_PLATFORM_MAP.get(target_name)
        )

    def resolve(
        self, location: TargetLocation, table: str, context: str
    ) -> Optional[str]:
        layout = TARGET_LAYOUTS.get(location.target_name)
        scope = self.target_scope(location.target_name, location.connection_id)
        if layout is None or scope is None:
            self.report.targets_unsupported.append(f"{context}: {location.target_name}")
            return None
        if layout == TargetLayout.PROJECT_DATASET_TABLE and not scope.database:
            self.report.targets_missing_bigquery_project.append(
                f"{context}: connection {location.connection_id}"
            )
            self.report.warning(
                title="BigQuery target without a project",
                message=(
                    "Rivers do not carry the BigQuery project. Add the target "
                    "connection to connection_to_platform with the project as "
                    "`database` to get lineage to it."
                ),
                context=f"{context}, connection_id={location.connection_id}",
            )
            return None
        name = build_table_name(layout, location, table, scope.database)
        if name is None:
            self.report.targets_missing_location.append(context)
            return None
        self.report.target_tables_emitted += 1
        return scope.urn(name)

    def river_targets(self, props: SourceToTargetProperties, context: str) -> List[str]:
        return sorted(
            {
                target.urn
                for target in self.river_target_tables(props, context)
                if target.urn
            }
        )

    def river_target_tables(
        self, props: SourceToTargetProperties, context: str
    ) -> List[TableTarget]:
        target = props.target
        entries = [
            (schema.name, entry.details)
            for schema in props.schemas
            for entry in schema.tables
        ]
        # A multi-table river never falls back to target.table_name, even when
        # no table is selected.
        selected = [
            (schema_name, details)
            for schema_name, details in entries
            if details.is_selected
        ]
        resolve_tables = True
        if TARGET_LAYOUTS.get(target.name) == TargetLayout.OBJECT_STORAGE:
            self.report.targets_object_storage_skipped += 1
            resolve_tables = False
        elif selected and target.target_prefix:
            # How the prefix combines with each table name is unconfirmed, so
            # emit nothing rather than guess a name that may not exist.
            self.report.targets_prefix_skipped.append(context)
            resolve_tables = False
        location = TargetLocation(
            target_name=target.name,
            connection_id=target.connection_id,
            database_name=target.database_name,
            schema_name=target.schema_name,
            dataset_id=target.dataset_id,
            catalog_name=target.catalog_name,
        )
        if not entries:
            if not resolve_tables or not target.table_name:
                return []
            return [TableTarget(urn=self.resolve(location, target.table_name, context))]
        tables: List[TableTarget] = []
        for schema_name, details in selected:
            urn: Optional[str] = None
            name = details.target_table or details.source_table
            if resolve_tables and not name:
                self.report.targets_missing_location.append(context)
            elif resolve_tables and name:
                urn = self.resolve(location, name, context)
            tables.append(
                TableTarget(urn=urn, schema_name=schema_name, details=details)
            )
        return tables

    def step_target(self, node: LogicNode, context: str) -> Optional[str]:
        settings = node.target_settings
        if settings is None or settings.target_type != STEP_TARGET_TYPE_TABLE:
            return None
        target_name = step_target_name(node.type)
        if target_name is None:
            self.report.targets_unsupported.append(f"{context}: {node.type}")
            return None
        if not settings.table_name:
            self.report.targets_missing_location.append(context)
            return None
        location = TargetLocation(
            target_name=target_name,
            connection_id=node.connection_id,
            database_name=settings.database_name,
            schema_name=settings.schema_name,
            dataset_id=settings.dataset_id,
            catalog_name=settings.catalog,
        )
        return self.resolve(location, settings.table_name, context)
