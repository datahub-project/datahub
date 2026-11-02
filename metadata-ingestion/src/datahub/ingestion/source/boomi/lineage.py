import logging
from typing import Callable, Dict, List, Optional, Set, TypeVar
from urllib.parse import parse_qsl, urlparse

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
    JDBC_PATH_DATABASE_PLATFORMS,
    JDBC_PREFIX,
    JDBC_SCHEME_PLATFORM_MAP,
    JDBC_SNOWFLAKE_DATABASE_KEY,
    JDBC_SNOWFLAKE_SCHEMA_KEY,
    JDBC_SQLSERVER_DATABASE_KEYS,
    MSSQL_PLATFORM,
    NON_DATASET_SUBTYPES,
    PLATFORMS_WITHOUT_DATABASE,
    SALESFORCE_PLATFORM,
    SNOWFLAKE_PLATFORM,
    SUBTYPE_DATABASE,
    SUBTYPE_DATABASE_V2,
    SUBTYPE_SALESFORCE,
    TWO_TIER_PLATFORMS,
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
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import create_lineage_from_sql_statements

logger = logging.getLogger(__name__)

T = TypeVar("T")

_QUOTE_CHARS = '[]"`'


def parse_jdbc_url(url: str) -> JdbcLocation:
    if not url.lower().startswith(JDBC_PREFIX):
        return JdbcLocation()
    scheme, _, remainder = url[len(JDBC_PREFIX) :].partition(":")
    platform = JDBC_SCHEME_PLATFORM_MAP.get(scheme.lower())
    if platform is None:
        return JdbcLocation()
    if platform == MSSQL_PLATFORM:
        # jdbc:sqlserver://host:port;databaseName=db;...
        database: Optional[str] = None
        for part in remainder.split(";")[1:]:
            key, _, value = part.partition("=")
            if key.strip().lower() in JDBC_SQLSERVER_DATABASE_KEYS and value.strip():
                database = value.strip()
        return JdbcLocation(platform=platform, database=database)
    parsed = urlparse(remainder)
    if platform == SNOWFLAKE_PLATFORM:
        params = {key.lower(): value for key, value in parse_qsl(parsed.query)}
        return JdbcLocation(
            platform=platform,
            database=params.get(JDBC_SNOWFLAKE_DATABASE_KEY) or None,
            schema_name=params.get(JDBC_SNOWFLAKE_SCHEMA_KEY) or None,
        )
    if platform in JDBC_PATH_DATABASE_PLATFORMS:
        # DB2 appends `:prop=value;` to the database, others use `?` or `;`.
        segment = parsed.path.strip("/").split("/")[0]
        database = segment.split(";")[0].split(":")[0].strip() or None
        return JdbcLocation(platform=platform, database=database)
    # Oracle URLs carry a SID or service name, which is not a URN segment.
    return JdbcLocation(platform=platform)


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


def connection_location(connection: ConnectionComponent) -> JdbcLocation:
    if connection.sub_type == SUBTYPE_DATABASE:
        driver = (connection.driver_id or "").lower()
        if driver == DRIVER_ID_CUSTOM:
            location = parse_jdbc_url(connection.url_format or "")
        else:
            location = JdbcLocation(
                platform=DRIVER_ID_PLATFORM_MAP.get(driver),
                database=connection.dbname,
            )
    elif connection.sub_type == SUBTYPE_DATABASE_V2:
        location = parse_jdbc_url(connection.url or "")
        if connection.schema_name:
            location.schema_name = connection.schema_name
    elif connection.sub_type == SUBTYPE_SALESFORCE:
        location = JdbcLocation(platform=SALESFORCE_PLATFORM)
    else:
        location = JdbcLocation()
    if location.platform in PLATFORMS_WITHOUT_DATABASE:
        location.database = None
    return location


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
        inputs: Set[str] = set()
        outputs: Set[str] = set()
        for shape in info.connector_actions:
            self._resolve_shape(shape, inputs, outputs)
        return ProcessLineage(
            input_datasets=sorted(inputs),
            output_datasets=sorted(outputs),
            child_process_ids=sorted(set(info.process_call_ids)),
        )

    def _resolve_shape(
        self, shape: ConnectorActionShape, inputs: Set[str], outputs: Set[str]
    ) -> None:
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
            self._unresolved(UnresolvedReason.COMPONENT_UNREADABLE)
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
        if target is None:
            self._unresolved(UnresolvedReason.UNSUPPORTED_CONNECTOR)
            self._warn_once(
                f"unsupported:{sub_type}",
                title="Unsupported Boomi connector for lineage",
                message="Connector shapes of this type have no dataset lineage. Map the subType with connection_type_platform_map, or the connection with connection_to_platform.",
                context=f"subType={sub_type}",
            )
            return
        operation = self._get_operation(shape.operation_id)
        if operation is None:
            self._unresolved(UnresolvedReason.COMPONENT_UNREADABLE)
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
        urns = self._resolve_datasets(sub_type, target, operation, shape.operation_id)
        if urns is None:
            return
        if not urns:
            self._unresolved(UnresolvedReason.NO_TABLE)
            return
        destination = (
            inputs if operation.direction == OperationDirection.READ else outputs
        )
        destination.update(urns)
        self.report.lineage_edges_resolved += len(urns)

    def _resolve_target(
        self, connection_id: str, connection: ConnectionComponent, sub_type: str
    ) -> Optional[ConnectionTarget]:
        location = connection_location(connection)
        mapping = self.config.connection_to_platform.get(connection_id)
        if mapping is not None:
            return ConnectionTarget(
                platform=mapping.platform,
                platform_instance=mapping.platform_instance,
                env=mapping.env or self.config.env,
                database=mapping.database or location.database,
                default_schema=mapping.default_schema or location.schema_name,
                convert_urns_to_lowercase=mapping.convert_urns_to_lowercase,
            )
        platform = location.platform or self.config.connection_type_platform_map.get(
            sub_type
        )
        if platform is None:
            return None
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
    ) -> Optional[List[str]]:
        # None means the edge was already reported as unresolved.
        if sub_type == SUBTYPE_DATABASE:
            profile = self._first_database_profile(operation)
            if profile is None:
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
        if sub_type == SUBTYPE_DATABASE_V2 and operation.query:
            return self._datasets_from_sql(
                operation.query, target, operation.direction, operation_id
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
