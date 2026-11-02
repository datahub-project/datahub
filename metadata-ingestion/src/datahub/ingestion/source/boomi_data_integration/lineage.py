from dataclasses import dataclass
from typing import List, Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
    ConnectionPlatformDetail,
)
from datahub.ingestion.source.boomi_data_integration.constants import (
    DI_PLATFORM_MAP,
    LOGIC_STEP_TARGET_NAMES,
    SQL_QUERY_STEP_SUFFIX,
    STEP_TARGET_TYPE_TABLE,
    TABLE_NAME_SEPARATOR,
    TARGET_LAYOUTS,
    TargetLayout,
)
from datahub.ingestion.source.boomi_data_integration.models import (
    LogicNode,
    SourceToTargetProperties,
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


def step_target_name(step_type: str) -> Optional[str]:
    if not step_type.endswith(SQL_QUERY_STEP_SUFFIX):
        return None
    return LOGIC_STEP_TARGET_NAMES.get(step_type[: -len(SQL_QUERY_STEP_SUFFIX)])


class TargetLineageResolver:
    def __init__(
        self,
        config: BoomiDataIntegrationSourceConfig,
        report: BoomiDataIntegrationSourceReport,
    ) -> None:
        self.config = config
        self.report = report

    def _connection(
        self, connection_id: Optional[str]
    ) -> Optional[ConnectionPlatformDetail]:
        if connection_id is None:
            return None
        return self.config.connection_to_platform.get(connection_id)

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
        connection = self._connection(location.connection_id)
        platform = self._platform(location.target_name, connection)
        if layout is None or layout == TargetLayout.OBJECT_STORAGE or platform is None:
            self.report.targets_unsupported.append(f"{context}: {location.target_name}")
            return None
        database = connection.database if connection else None
        if layout == TargetLayout.PROJECT_DATASET_TABLE and not database:
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
        name = build_table_name(layout, location, table, database)
        if name is None:
            self.report.targets_missing_location.append(context)
            return None
        lowercase = self.config.convert_lineage_urns_to_lowercase
        if connection is not None and connection.convert_urns_to_lowercase is not None:
            lowercase = connection.convert_urns_to_lowercase
        self.report.target_tables_emitted += 1
        return make_dataset_urn_with_platform_instance(
            platform=platform,
            name=name.lower() if lowercase else name,
            platform_instance=connection.platform_instance if connection else None,
            env=(connection.env if connection else None) or self.config.env,
        )

    def river_targets(self, props: SourceToTargetProperties, context: str) -> List[str]:
        target = props.target
        if TARGET_LAYOUTS.get(target.name) == TargetLayout.OBJECT_STORAGE:
            self.report.targets_object_storage_skipped += 1
            return []
        location = TargetLocation(
            target_name=target.name,
            connection_id=target.connection_id,
            database_name=target.database_name,
            schema_name=target.schema_name,
            dataset_id=target.dataset_id,
            catalog_name=target.catalog_name,
        )
        tables = [
            entry.details
            for schema in props.schemas
            for entry in schema.tables
            if entry.details.is_selected
        ]
        if not tables:
            if not target.table_name:
                return []
            urn = self.resolve(location, target.table_name, context)
            return [urn] if urn else []
        if target.target_prefix:
            # How the prefix combines with each table name is unconfirmed, so
            # emit nothing rather than guess a name that may not exist.
            self.report.targets_prefix_skipped.append(context)
            return []
        urns: List[str] = []
        for details in tables:
            table = details.target_table or details.source_table
            if not table:
                self.report.targets_missing_location.append(context)
                continue
            urn = self.resolve(location, table, context)
            if urn:
                urns.append(urn)
        return sorted(set(urns))

    def step_target(self, node: LogicNode, context: str) -> Optional[str]:
        settings = node.target_settings
        if settings is None or settings.target_type != STEP_TARGET_TYPE_TABLE:
            return None
        target_name = step_target_name(node.type)
        if target_name is None or not settings.table_name:
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
