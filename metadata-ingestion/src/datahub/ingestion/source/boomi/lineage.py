import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, TypeVar, Union

import requests
from defusedxml.common import DefusedXmlException
from typing_extensions import LiteralString

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.boomi.client import BoomiApiError, BoomiClient
from datahub.ingestion.source.boomi.component_parser import (
    parse_connection_component,
    parse_database_profile_component,
    parse_operation_component,
)
from datahub.ingestion.source.boomi.config import BoomiSourceConfig
from datahub.ingestion.source.boomi.constants import (
    ACTION_TYPE_LISTEN,
    DRIVER_ID_CUSTOM,
    DRIVER_ID_PLATFORM_MAP,
    NON_DATASET_SUBTYPES,
    PARSED_CONNECTION_SUBTYPES,
    PLATFORMS_WITHOUT_DATABASE,
    SALESFORCE_PLATFORM,
    SUBTYPE_DATABASE,
    SUBTYPE_DATABASE_V2,
    SUBTYPE_SALESFORCE,
    TWO_TIER_PLATFORMS,
    URL_FORMAT_PLACEHOLDER_START,
    OperationDirection,
    UnresolvedReason,
)
from datahub.ingestion.source.boomi.models import (
    ConnectionComponent,
    ConnectionTarget,
    ConnectorActionShape,
    DatabaseProfileComponent,
    JdbcLocation,
    OperationComponent,
    ProcessComponentInfo,
    ProcessLineage,
)
from datahub.ingestion.source.boomi.report import BoomiSourceReport
from datahub.ingestion.source.common.jdbc import parse_jdbc_url
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import create_lineage_from_sql_statements

logger = logging.getLogger(__name__)

T = TypeVar("T")

_QUOTE_CHARS = '[]"`'


def _jdbc_location(url: str) -> JdbcLocation:
    info = parse_jdbc_url(url)
    return JdbcLocation(
        platform=info.platform, database=info.database, schema_name=info.schema_name
    )


def qualify_table_name(
    table: str, platform: str, database: Optional[str], schema: Optional[str]
) -> Optional[str]:
    parts = [part.strip(_QUOTE_CHARS) for part in table.split(".")]
    parts = [part for part in parts if part]
    if not parts:
        return None
    if platform in TWO_TIER_PLATFORMS:
        if len(parts) == 1 and database:
            parts.insert(0, database)
        return ".".join(parts)
    if len(parts) == 1 and schema:
        parts.insert(0, schema)
    if len(parts) < 3 and database:
        parts.insert(0, database)
    return ".".join(parts)


def fill_url_format(connection: ConnectionComponent) -> str:
    # Replaced one by one rather than with str.format, which would fail on any
    # other brace in the URL.
    url = connection.url_format or ""
    values = (
        connection.host,
        connection.port,
        connection.dbname,
        connection.additional,
    )
    for index, value in enumerate(values):
        url = url.replace(f"{{{index}}}", value or "")
    return url


def connection_location(connection: ConnectionComponent) -> JdbcLocation:
    if connection.sub_type == SUBTYPE_DATABASE:
        driver = (connection.driver_id or "").lower()
        if driver == DRIVER_ID_CUSTOM:
            location = _jdbc_location(fill_url_format(connection))
            # A placeholder the template still holds is not a database name.
            if location.database and URL_FORMAT_PLACEHOLDER_START in location.database:
                location.database = None
        else:
            location = JdbcLocation(
                platform=DRIVER_ID_PLATFORM_MAP.get(driver),
                database=connection.dbname,
            )
    elif connection.sub_type == SUBTYPE_DATABASE_V2:
        location = _jdbc_location(connection.url or "")
        if connection.schema_name:
            location.schema_name = connection.schema_name
    elif connection.sub_type == SUBTYPE_SALESFORCE:
        location = JdbcLocation(platform=SALESFORCE_PLATFORM)
    elif len(connection.jdbc_urls) == 1:
        location = _jdbc_location(connection.jdbc_urls[0])
    else:
        location = JdbcLocation()
    return _normalize_location(location)


def _normalize_location(location: JdbcLocation) -> JdbcLocation:
    if location.platform in PLATFORMS_WITHOUT_DATABASE:
        location.database = None
    if location.platform in TWO_TIER_PLATFORMS:
        # MySQL has no schema level: Database V2 puts the database in
        # schemaName, which would otherwise be added as a second segment.
        location.database = location.schema_name or location.database
        location.schema_name = None
    return location


@dataclass
class _ProcessEdges:
    inputs: Set[str] = field(default_factory=set)
    outputs: Set[str] = field(default_factory=set)
    incomplete: bool = False


class BoomiLineageResolver:
    def __init__(
        self,
        config: BoomiSourceConfig,
        client: BoomiClient,
        report: BoomiSourceReport,
    ) -> None:
        self.config = config
        self.client = client
        self.report = report
        self._connections: Dict[str, Optional[ConnectionComponent]] = {}
        self._operations: Dict[str, Optional[OperationComponent]] = {}
        self._profiles: Dict[str, Optional[DatabaseProfileComponent]] = {}
        # Keys of warnings already raised, so each cause is reported once.
        self._warned: Set[str] = set()

    def resolve_process(self, info: ProcessComponentInfo) -> ProcessLineage:
        edges = _ProcessEdges()
        for shape in info.connector_actions:
            self._resolve_shape(shape, edges)
        return ProcessLineage(
            input_datasets=sorted(edges.inputs),
            output_datasets=sorted(edges.outputs),
            child_process_ids=sorted(set(info.process_call_ids)),
            incomplete=edges.incomplete,
        )

    def _resolve_shape(self, shape: ConnectorActionShape, edges: _ProcessEdges) -> None:
        # Listen shapes receive documents pushed to the runtime, so there is no
        # operation that names a dataset.
        if (shape.action_type or "").lower() == ACTION_TYPE_LISTEN:
            self.report.lineage_listen_shapes_skipped += 1
            return
        if not shape.connection_id or not shape.operation_id:
            self._unresolved(UnresolvedReason.MISSING_OPERATION)
            return
        connection = self._get_connection(shape.connection_id)
        if connection is None:
            self._component_unreadable(edges)
            return
        sub_type = connection.sub_type or shape.connector_type or ""
        mapping = self.config.connection_to_platform.get(shape.connection_id)
        if (
            mapping is None
            and sub_type not in self.config.connection_type_platform_map
            and sub_type in NON_DATASET_SUBTYPES
        ):
            self.report.lineage_skipped_subtypes[sub_type] += 1
            return
        target = self._resolve_target(shape.connection_id, connection, sub_type)
        if isinstance(target, UnresolvedReason):
            self._unresolved(target)
            if target == UnresolvedReason.UNSUPPORTED_CONNECTOR:
                self._warn_once(
                    f"unsupported:{sub_type}",
                    title="Unsupported Boomi connector for lineage",
                    message="Connector shapes of this type have no dataset lineage. Map the subType with connection_type_platform_map, or the connection with connection_to_platform.",
                    context=f"subType={sub_type}",
                )
            return
        operation = self._get_operation(shape.operation_id)
        if operation is None:
            self._component_unreadable(edges)
            return
        if operation.direction == OperationDirection.UNKNOWN:
            self._unresolved(UnresolvedReason.UNKNOWN_DIRECTION)
            self._warn_once(
                f"direction:{sub_type}:{operation.operation_type}:{operation.custom_operation_type}",
                title="Unknown Boomi operation direction",
                message="The operation type does not say whether the operation reads or writes, so its lineage is skipped.",
                context=(
                    f"subType={sub_type}, operationType={operation.operation_type}, "
                    f"customOperationType={operation.custom_operation_type}, "
                    f"operationId={shape.operation_id}"
                ),
            )
            return
        urns = self._resolve_datasets(
            sub_type, target, operation, shape.operation_id, edges
        )
        if urns is None:
            return
        if not urns:
            self._unresolved(UnresolvedReason.NO_TABLE)
            return
        destination = (
            edges.inputs
            if operation.direction == OperationDirection.READ
            else edges.outputs
        )
        destination.update(urns)
        self.report.lineage_edges_resolved += len(urns)

    def _resolve_target(
        self, connection_id: str, connection: ConnectionComponent, sub_type: str
    ) -> Union[ConnectionTarget, UnresolvedReason]:
        location = connection_location(connection)
        generic = connection.sub_type not in PARSED_CONNECTION_SUBTYPES
        ambiguous = generic and len(connection.jdbc_urls) > 1
        if ambiguous:
            self._warn_once(
                f"jdbc-ambiguous:{connection_id}",
                title="Ambiguous JDBC URL in Boomi connection",
                message="The connection holds more than one JDBC URL, so its platform and database are not read from it.",
                context=f"subType={sub_type}, connectionId={connection_id}, urls={len(connection.jdbc_urls)}",
            )
        mapping = self.config.connection_to_platform.get(connection_id)
        if mapping is not None:
            if generic and location.platform not in (None, mapping.platform):
                location = JdbcLocation()
            return ConnectionTarget(
                platform=mapping.platform,
                platform_instance=mapping.platform_instance,
                env=mapping.env or self.config.env,
                database=mapping.database or location.database,
                default_schema=mapping.default_schema or location.schema_name,
                convert_urns_to_lowercase=mapping.convert_urns_to_lowercase,
            )
        mapped_platform = self.config.connection_type_platform_map.get(sub_type)
        # A subType mapped in the recipe is an explicit choice, so it wins
        # over a JDBC URL found in a generic connection's fields.
        if (
            generic
            and mapped_platform is not None
            and location.platform not in (None, mapped_platform)
        ):
            self._warn_once(
                f"jdbc-mismatch:{connection_id}",
                title="Boomi JDBC URL disagrees with connection_type_platform_map",
                message="The platform mapped for the subType is used, without the database or schema from the JDBC URL.",
                context=(
                    f"subType={sub_type}, connectionId={connection_id}, "
                    f"mapped={mapped_platform}, jdbc={location.platform}"
                ),
            )
            location = JdbcLocation()
        platform = location.platform or mapped_platform
        if platform is None:
            return (
                UnresolvedReason.AMBIGUOUS_JDBC_URL
                if ambiguous
                else UnresolvedReason.UNSUPPORTED_CONNECTOR
            )
        return ConnectionTarget(
            platform=platform,
            env=self.config.env,
            database=location.database,
            default_schema=location.schema_name,
        )

    def _resolve_datasets(
        self,
        sub_type: str,
        target: ConnectionTarget,
        operation: OperationComponent,
        operation_id: str,
        edges: _ProcessEdges,
    ) -> Optional[List[str]]:
        # None means the edge was already reported as unresolved.
        if sub_type == SUBTYPE_DATABASE:
            profile = self._first_database_profile(operation)
            if profile is None:
                # Profiles are cached, so this re-check makes no new requests.
                if any(
                    self._get_profile(profile_id) is None
                    for profile_id in operation.profile_ids
                ):
                    self._component_unreadable(edges)
                    return None
                return []
            if profile.sql:
                return self._datasets_from_sql(
                    profile.sql, target, operation.direction, operation_id
                )
            return self._datasets_from_table(profile.table_name, target)
        if sub_type == SUBTYPE_SALESFORCE:
            if not operation.object_name:
                return []
            # The Salesforce source keeps object API names as they are, so the
            # name is never lowercased here.
            return [
                make_dataset_urn_with_platform_instance(
                    platform=target.platform,
                    name=operation.object_name,
                    platform_instance=target.platform_instance,
                    env=target.env,
                )
            ]
        if sub_type == SUBTYPE_DATABASE_V2:
            sql = operation.query
        elif len(operation.sql_statements) > 1:
            self._unresolved(UnresolvedReason.AMBIGUOUS_SQL)
            self._warn_once(
                f"sql-ambiguous:{operation_id}",
                title="Ambiguous SQL in Boomi operation",
                message="The operation holds more than one SQL statement, so its lineage is skipped.",
                context=f"subType={sub_type}, operationId={operation_id}",
            )
            return None
        else:
            sql = next(iter(operation.sql_statements), None)
        if sql:
            return self._datasets_from_sql(
                sql, target, operation.direction, operation_id
            )
        return self._datasets_from_table(operation.object_type_id, target)

    def _datasets_from_table(
        self, table: Optional[str], target: ConnectionTarget
    ) -> List[str]:
        if not table:
            return []
        name = qualify_table_name(
            table, target.platform, target.database, target.default_schema
        )
        if name is None:
            return []
        return [self._dataset_urn(name, target)]

    def _datasets_from_sql(
        self,
        sql: str,
        target: ConnectionTarget,
        direction: OperationDirection,
        operation_id: str,
    ) -> Optional[List[str]]:
        # The parser runs without a platform instance; it is added when the
        # URN is rebuilt so casing applies only to the table name.
        result = create_lineage_from_sql_statements(
            queries=sql,
            default_db=target.database,
            default_schema=target.default_schema,
            platform=target.platform,
            platform_instance=None,
            env=target.env,
        )
        error = result.debug_info.error
        # The aggregator drops statements it cannot parse instead of returning
        # an error, so a result with no tables at all is treated as a failure.
        if error is not None or not (result.in_tables or result.out_tables):
            self.report.lineage_sql_parse_failures += 1
            self._unresolved(UnresolvedReason.SQL_PARSE_FAILED)
            self._warn_once(
                f"sql:{target.platform}",
                title="Failed to parse Boomi SQL",
                message="SQL in a database operation or profile could not be parsed or names no tables, so its lineage is skipped.",
                context=(
                    f"platform={target.platform}, operationId={operation_id}, "
                    f"sql_prefix={sql.strip()[:80]!r}"
                ),
                exc=error,
            )
            return None
        tables = (
            result.in_tables
            if direction == OperationDirection.READ
            else result.out_tables
        )
        return sorted(
            {
                self._dataset_urn(DatasetUrn.from_string(urn).name, target)
                for urn in tables
            }
        )

    @staticmethod
    def _dataset_urn(name: str, target: ConnectionTarget) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=target.platform,
            name=name.lower() if target.convert_urns_to_lowercase else name,
            platform_instance=target.platform_instance,
            env=target.env,
        )

    def _first_database_profile(
        self, operation: OperationComponent
    ) -> Optional[DatabaseProfileComponent]:
        for profile_id in operation.profile_ids:
            profile = self._get_profile(profile_id)
            if profile is not None and (profile.sql or profile.table_name):
                return profile
        return None

    def _get_connection(self, component_id: str) -> Optional[ConnectionComponent]:
        return self._cached(self._connections, component_id, parse_connection_component)

    def _get_operation(self, component_id: str) -> Optional[OperationComponent]:
        return self._cached(self._operations, component_id, parse_operation_component)

    def _get_profile(self, component_id: str) -> Optional[DatabaseProfileComponent]:
        return self._cached(
            self._profiles, component_id, parse_database_profile_component
        )

    def _cached(
        self,
        cache: Dict[str, Optional[T]],
        component_id: str,
        parse: Callable[[str], T],
    ) -> Optional[T]:
        # Failures are cached too, so a shared component that cannot be read
        # costs one request rather than one per shape.
        if component_id in cache:
            return cache[component_id]
        parsed: Optional[T] = None
        try:
            xml_text = self.client.get_component_xml(component_id)
            self.report.lineage_components_fetched += 1
            parsed = parse(xml_text)
        except (
            BoomiApiError,
            requests.RequestException,
            DefusedXmlException,
            SyntaxError,
        ) as e:
            # defusedxml's ParseError subclasses SyntaxError.
            self.report.lineage_component_failures.append(component_id)
            self.report.warning(
                title="Failed to read Boomi component for lineage",
                message="Lineage through this connection, operation or profile is skipped.",
                context=f"componentId={component_id}",
                exc=e,
            )
        cache[component_id] = parsed
        return parsed

    def _component_unreadable(self, edges: _ProcessEdges) -> None:
        edges.incomplete = True
        self._unresolved(UnresolvedReason.COMPONENT_UNREADABLE)

    def _unresolved(self, reason: UnresolvedReason) -> None:
        self.report.lineage_edges_unresolved[reason.value] += 1

    def _warn_once(
        self,
        key: str,
        title: LiteralString,
        message: LiteralString,
        context: str,
        exc: Optional[BaseException] = None,
    ) -> None:
        if key in self._warned:
            return
        self._warned.add(key)
        self.report.warning(title=title, message=message, context=context, exc=exc)
