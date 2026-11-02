import logging
from dataclasses import dataclass, field
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import requests
import sqlglot
from defusedxml.common import DefusedXmlException
from typing_extensions import LiteralString

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.boomi.client import BoomiApiError, BoomiClient
from datahub.ingestion.source.boomi.component_parser import (
    parse_connection_component,
    parse_map_component,
    parse_operation_component,
    parse_profile_component,
)
from datahub.ingestion.source.boomi.config import BoomiSourceConfig
from datahub.ingestion.source.boomi.constants import (
    ACTION_TYPE_LISTEN,
    COMPONENT_TYPE_DB_PROFILE,
    DRIVER_ID_CUSTOM,
    DRIVER_ID_PLATFORM_MAP,
    MAPPING_TYPE_FUNCTION,
    MAPPING_TYPE_PROFILE,
    NON_DATASET_SUBTYPES,
    PARSED_CONNECTION_SUBTYPES,
    PLATFORMS_WITHOUT_DATABASE,
    SALESFORCE_PLATFORM,
    STATEMENT_TYPE_DYNAMIC_PREFIX,
    SUBTYPE_DATABASE,
    SUBTYPE_DATABASE_V2,
    SUBTYPE_SALESFORCE,
    TRANSFORM_FUNCTION,
    TRANSFORM_IDENTITY,
    TRANSFORM_SQL,
    TWO_TIER_PLATFORMS,
    URL_FORMAT_PLACEHOLDER_START,
    ColumnSkipReason,
    MapSkipReason,
    OperationDirection,
    UnresolvedReason,
)
from datahub.ingestion.source.boomi.models import (
    ColumnLineageEdge,
    ConnectionComponent,
    ConnectionTarget,
    ConnectorActionShape,
    JdbcLocation,
    MapComponent,
    MapShape,
    OperationComponent,
    ProcessComponentInfo,
    ProcessLineage,
    ProfileComponent,
    ProfileField,
)
from datahub.ingestion.source.boomi.report import BoomiSourceReport
from datahub.ingestion.source.common.jdbc import parse_jdbc_url
from datahub.metadata.schema_classes import SchemaMetadataClass
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_from_sql_statements,
    create_lineage_sql_parsed_result,
)
from datahub.sql_parsing.sqlglot_utils import get_dialect, parse_statement
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

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
class InsertColumns:
    parsed: bool
    is_insert: bool = False
    # Lowercased SELECT output name -> target column. None when the INSERT has
    # no column list, so its target columns are not known.
    by_output: Optional[Dict[str, str]] = None


def _parse_sql(sql: str, platform: str) -> Optional[sqlglot.exp.Expression]:
    try:
        return parse_statement(sql, get_dialect(platform))
    except (sqlglot.errors.SqlglotError, ValueError, TypeError):
        return None


def select_has_star(sql: str, platform: str) -> bool:
    statement = _parse_sql(sql, platform)
    return isinstance(statement, sqlglot.exp.Select) and statement.is_star


def insert_column_list(sql: str, platform: str) -> Dict[str, str]:
    # Lowercased name -> column, for each column an INSERT lists.
    statement = _parse_sql(sql, platform)
    if not isinstance(statement, sqlglot.exp.Insert) or not isinstance(
        statement.this, sqlglot.exp.Schema
    ):
        return {}
    return {column.name.lower(): column.name for column in statement.this.expressions}


def insert_columns(sql: str, platform: str) -> InsertColumns:
    # The SQL parser names an INSERT's target columns after the SELECT's
    # output names and ignores the INSERT's column list, so they are mapped
    # back by position here.
    statement = _parse_sql(sql, platform)
    if statement is None:
        return InsertColumns(parsed=False)
    if not isinstance(statement, sqlglot.exp.Insert):
        return InsertColumns(parsed=True)
    if not isinstance(statement.this, sqlglot.exp.Schema):
        return InsertColumns(parsed=True, is_insert=True)
    columns = [column.name for column in statement.this.expressions]
    query = statement.expression
    if (
        not isinstance(query, sqlglot.exp.Select)
        or query.is_star
        or len(query.selects) != len(columns)
    ):
        return InsertColumns(parsed=True, is_insert=True, by_output={})
    by_output: Dict[str, str] = {}
    duplicates: Set[str] = set()
    for projection, column in zip(query.selects, columns, strict=True):
        output = projection.alias_or_name.lower()
        if not output:
            continue
        if output in by_output:
            duplicates.add(output)
        by_output[output] = column
    return InsertColumns(
        parsed=True,
        is_insert=True,
        by_output={
            output: column
            for output, column in by_output.items()
            if output not in duplicates
        },
    )


@dataclass(frozen=True)
class MapFieldEdge:
    from_keys: Tuple[str, ...]
    to_key: str
    transform: str


def map_field_edges(component: MapComponent) -> List[MapFieldEdge]:
    # Plain maps never chain functions; chains live in user-defined function
    # components, which are opaque here. A function without profile inputs,
    # such as the current date, and map defaults have no upstream field.
    function_inputs: Dict[str, Set[str]] = {}
    for mapping in component.mappings:
        if (
            mapping.from_type == MAPPING_TYPE_PROFILE
            and mapping.to_type == MAPPING_TYPE_FUNCTION
            and mapping.to_function
        ):
            function_inputs.setdefault(mapping.to_function, set()).add(mapping.from_key)
    edges: List[MapFieldEdge] = []
    for mapping in component.mappings:
        if mapping.to_type != MAPPING_TYPE_PROFILE:
            continue
        if mapping.from_type == MAPPING_TYPE_PROFILE:
            edges.append(
                MapFieldEdge((mapping.from_key,), mapping.to_key, TRANSFORM_IDENTITY)
            )
        elif mapping.from_type == MAPPING_TYPE_FUNCTION and mapping.from_function:
            inputs = function_inputs.get(mapping.from_function)
            if not inputs:
                continue
            function = component.functions.get(mapping.from_function)
            transform = (
                function.type if function and function.type else TRANSFORM_FUNCTION
            )
            edges.append(MapFieldEdge(tuple(sorted(inputs)), mapping.to_key, transform))
    return edges


@dataclass
class _ShapeDatasets:
    urns: List[str]
    sql: Optional[str] = None
    sql_result: Optional[SqlParsingResult] = None


@dataclass
class _ResolvedShape:
    sub_type: str
    target: ConnectionTarget
    operation: OperationComponent
    urns: List[str]


@dataclass
class _FieldColumns:
    # Field URNs of one connector operation's profile fields, by field key.
    columns: Dict[str, List[str]] = field(default_factory=dict)
    skipped: Dict[str, ColumnSkipReason] = field(default_factory=dict)


@dataclass
class _ProcessEdges:
    inputs: Set[str] = field(default_factory=set)
    outputs: Set[str] = field(default_factory=set)
    incomplete: bool = False
    # Connector shape name -> what it resolved to.
    resolved: Dict[str, _ResolvedShape] = field(default_factory=dict)
    # (downstream field URN, transform) -> upstream field URNs.
    columns: Dict[Tuple[str, str], Set[str]] = field(default_factory=dict)

    def add_column_edge(
        self, upstreams: Iterable[str], downstream: str, transform: str
    ) -> None:
        self.columns.setdefault((downstream, transform), set()).update(upstreams)

    def column_lineage(self) -> List[ColumnLineageEdge]:
        return [
            ColumnLineageEdge(
                upstreams=sorted(upstreams), downstream=downstream, transform=transform
            )
            for (downstream, transform), upstreams in sorted(self.columns.items())
        ]


class BoomiLineageResolver:
    def __init__(
        self,
        config: BoomiSourceConfig,
        client: BoomiClient,
        report: BoomiSourceReport,
        graph: Optional[DataHubGraph] = None,
    ) -> None:
        self.config = config
        self.client = client
        self.report = report
        self.graph = graph
        self._connections: Dict[str, Optional[ConnectionComponent]] = {}
        self._operations: Dict[str, Optional[OperationComponent]] = {}
        self._profiles: Dict[str, Optional[ProfileComponent]] = {}
        self._maps: Dict[str, Optional[MapComponent]] = {}
        # Dataset URN -> lowercased column name -> field path, or None when
        # DataHub has no schema for the dataset.
        self._schemas: Dict[str, Optional[Dict[str, str]]] = {}
        # Keys of warnings already raised, so each cause is reported once.
        self._warned: Set[str] = set()

    def resolve_process(self, info: ProcessComponentInfo) -> ProcessLineage:
        edges = _ProcessEdges()
        for shape in info.connector_actions:
            self._resolve_shape(shape, edges)
        if self.config.include_column_lineage and not edges.incomplete:
            for map_shape in info.map_shapes:
                self._resolve_map_shape(info, map_shape, edges)
        return ProcessLineage(
            input_datasets=sorted(edges.inputs),
            output_datasets=sorted(edges.outputs),
            child_process_ids=sorted(set(info.process_call_ids)),
            column_lineage=[] if edges.incomplete else edges.column_lineage(),
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
        datasets = self._resolve_datasets(
            sub_type, target, operation, shape.operation_id, edges
        )
        if datasets is None:
            return
        if not datasets.urns:
            self._unresolved(UnresolvedReason.NO_TABLE)
            return
        destination = (
            edges.inputs
            if operation.direction == OperationDirection.READ
            else edges.outputs
        )
        destination.update(datasets.urns)
        self.report.lineage_edges_resolved += len(datasets.urns)
        if shape.shape_name:
            edges.resolved[shape.shape_name] = _ResolvedShape(
                sub_type=sub_type,
                target=target,
                operation=operation,
                urns=datasets.urns,
            )
        if (
            self.config.include_column_lineage
            and datasets.sql is not None
            and datasets.sql_result is not None
            and operation.direction == OperationDirection.WRITE
        ):
            self._add_sql_column_lineage(
                datasets.sql, datasets.sql_result, sub_type, target, edges
            )

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
    ) -> Optional[_ShapeDatasets]:
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
                return _ShapeDatasets(urns=[])
            if profile.sql:
                return self._datasets_from_sql(
                    profile.sql, target, operation.direction, operation_id
                )
            return _ShapeDatasets(
                urns=self._datasets_from_table(profile.table_name, target)
            )
        if sub_type == SUBTYPE_SALESFORCE:
            if not operation.object_name:
                return _ShapeDatasets(urns=[])
            # The Salesforce source keeps object API names as they are, so the
            # name is never lowercased here.
            return _ShapeDatasets(
                urns=[
                    make_dataset_urn_with_platform_instance(
                        platform=target.platform,
                        name=operation.object_name,
                        platform_instance=target.platform_instance,
                        env=target.env,
                    )
                ]
            )
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
        return _ShapeDatasets(
            urns=self._datasets_from_table(operation.object_type_id, target)
        )

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
    ) -> Optional[_ShapeDatasets]:
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
        return _ShapeDatasets(
            urns=sorted({self._rebuild_urn(urn, target) for urn in tables}),
            sql=sql,
            sql_result=result,
        )

    def _rebuild_urn(self, parsed_urn: str, target: ConnectionTarget) -> str:
        return self._dataset_urn(DatasetUrn.from_string(parsed_urn).name, target)

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
    ) -> Optional[ProfileComponent]:
        for profile_id in operation.profile_ids:
            profile = self._get_profile(profile_id)
            if profile is not None and (profile.sql or profile.table_name):
                return profile
        return None

    def _add_sql_column_lineage(
        self,
        sql: str,
        result: SqlParsingResult,
        sub_type: str,
        target: ConnectionTarget,
        edges: _ProcessEdges,
    ) -> None:
        written = [
            info
            for info in result.column_lineage or []
            if info.downstream.table is not None
        ]
        if not written:
            return
        insert = insert_columns(sql, target.platform)
        for info in written:
            column: Optional[str] = info.downstream.column
            if not insert.parsed:
                column = None
            elif insert.is_insert:
                if insert.by_output is None:
                    self._skip_column(ColumnSkipReason.INSERT_WITHOUT_COLUMN_LIST)
                    continue
                column = insert.by_output.get(info.downstream.column.lower())
            if column is None or info.downstream.table is None:
                self._skip_column(ColumnSkipReason.UNMAPPED_SQL_COLUMN)
                continue
            if not info.upstreams:
                self._skip_column(ColumnSkipReason.NO_SOURCE_COLUMN)
                continue
            downstream = self._field_urn(
                self._rebuild_urn(info.downstream.table, target),
                column,
                sub_type,
                target,
            )
            upstream_datasets = {
                ref.table: self._rebuild_urn(ref.table, target)
                for ref in info.upstreams
            }
            upstreams = [
                self._field_urn(
                    upstream_datasets[ref.table], ref.column, sub_type, target
                )
                for ref in info.upstreams
            ]
            resolved = [urn for urn in upstreams if urn is not None]
            if downstream is None or not resolved:
                self._skip_column(ColumnSkipReason.NOT_IN_SCHEMA)
                continue
            # A statement that writes from a SELECT also reads its sources.
            edges.inputs.update(upstream_datasets.values())
            transform = (
                TRANSFORM_IDENTITY
                if info.logic is not None and info.logic.is_direct_copy
                else TRANSFORM_SQL
            )
            edges.add_column_edge(resolved, downstream, transform)

    def _resolve_map_shape(
        self, info: ProcessComponentInfo, map_shape: MapShape, edges: _ProcessEdges
    ) -> None:
        component = self._get_map(map_shape.map_id)
        if component is None:
            self._map_unreadable(edges)
            return
        from_profile_id = component.from_profile
        to_profile_id = component.to_profile
        reads = [
            shape
            for shape in self._neighbour_operations(
                info, map_shape.shape_name, edges, forward=False
            )
            if shape.operation.direction == OperationDirection.READ
            and from_profile_id in shape.operation.profile_ids
        ]
        writes = [
            shape
            for shape in self._neighbour_operations(
                info, map_shape.shape_name, edges, forward=True
            )
            if shape.operation.direction == OperationDirection.WRITE
            and to_profile_id in shape.operation.profile_ids
        ]
        if not reads or from_profile_id is None:
            self._skip_map(MapSkipReason.NO_MATCHING_READ)
            return
        if not writes or to_profile_id is None:
            self._skip_map(MapSkipReason.NO_MATCHING_WRITE)
            return
        from_profile = self._get_profile(from_profile_id)
        to_profile = self._get_profile(to_profile_id)
        if from_profile is None or to_profile is None:
            self._map_unreadable(edges)
            return
        sources = [
            columns
            for columns in (self._source_columns(read, from_profile) for read in reads)
            if columns is not None
        ]
        if not sources:
            self._skip_map(MapSkipReason.UNSUPPORTED_SOURCE)
            return
        targets = [
            columns
            for columns in (self._target_columns(write, to_profile) for write in writes)
            if columns is not None
        ]
        if not targets:
            self._skip_map(MapSkipReason.UNSUPPORTED_TARGET)
            return
        self.report.column_lineage_maps_resolved += 1
        field_edges = map_field_edges(component)
        for source in sources:
            for target in targets:
                for field_edge in field_edges:
                    downstream = self._columns_for(target, field_edge.to_key)
                    if not downstream:
                        continue
                    upstreams = [
                        urn
                        for key in field_edge.from_keys
                        for urn in self._columns_for(source, key)
                    ]
                    if upstreams:
                        edges.add_column_edge(
                            upstreams, downstream[0], field_edge.transform
                        )

    def _neighbour_operations(
        self,
        info: ProcessComponentInfo,
        start: str,
        edges: _ProcessEdges,
        forward: bool,
    ) -> List[_ResolvedShape]:
        # The connector shapes whose documents reach the map, or that the map's
        # documents reach, without passing another connector or map shape.
        links: Dict[str, List[str]] = {}
        if forward:
            links = info.next_shapes
        else:
            for source, targets in info.next_shapes.items():
                for target in targets:
                    links.setdefault(target, []).append(source)
        connectors = {
            shape.shape_name for shape in info.connector_actions if shape.shape_name
        }
        maps = {shape.shape_name for shape in info.map_shapes}
        seen = {start}
        pending = [start]
        found: List[str] = []
        while pending:
            for name in links.get(pending.pop(), []):
                if name in seen:
                    continue
                seen.add(name)
                if name in connectors:
                    found.append(name)
                elif name not in maps:
                    pending.append(name)
        return [
            edges.resolved[name] for name in sorted(found) if name in edges.resolved
        ]

    def _source_columns(
        self, read: _ResolvedShape, profile: ProfileComponent
    ) -> Optional[_FieldColumns]:
        if len(read.urns) != 1:
            # Only SQL that reads several tables resolves to more than one.
            columns = _FieldColumns()
            for key in profile.fields:
                columns.skipped[key] = ColumnSkipReason.MULTI_TABLE_SQL
            return columns
        dataset_urn = read.urns[0]
        if read.sub_type == SUBTYPE_DATABASE:
            if profile.sql:
                return self._select_columns(profile.sql, read, profile, dataset_urn)
            return self._direct_columns(read, profile, dataset_urn)
        if read.sub_type == SUBTYPE_DATABASE_V2:
            if read.operation.query:
                return self._select_columns(
                    read.operation.query, read, profile, dataset_urn
                )
            return self._direct_columns(read, profile, dataset_urn)
        if read.sub_type == SUBTYPE_SALESFORCE:
            return self._direct_columns(read, profile, dataset_urn)
        return None

    def _target_columns(
        self, write: _ResolvedShape, profile: ProfileComponent
    ) -> Optional[_FieldColumns]:
        if len(write.urns) != 1:
            return None
        dataset_urn = write.urns[0]
        if write.sub_type == SUBTYPE_DATABASE:
            dynamic = (
                (profile.statement_type or "")
                .lower()
                .startswith(STATEMENT_TYPE_DYNAMIC_PREFIX)
            )
            if profile.sql and not (dynamic and profile.table_name):
                return self._insert_field_columns(
                    profile.sql, write, profile, dataset_urn
                )
            return self._direct_columns(write, profile, dataset_urn)
        if write.sub_type == SUBTYPE_DATABASE_V2 and not write.operation.query:
            return self._direct_columns(write, profile, dataset_urn)
        if write.sub_type == SUBTYPE_SALESFORCE:
            return self._direct_columns(write, profile, dataset_urn)
        return None

    def _direct_columns(
        self, shape: _ResolvedShape, profile: ProfileComponent, dataset_urn: str
    ) -> _FieldColumns:
        # Field names are the columns: database profiles list a table's
        # columns, and XML or JSON profiles a record's fields under the root.
        columns = _FieldColumns()
        for key, profile_field in profile.fields.items():
            if not self._is_record_field(profile, profile_field):
                columns.skipped[key] = ColumnSkipReason.NESTED_FIELD
                continue
            self._set_column(columns, key, [profile_field.name], shape, dataset_urn)
        return columns

    def _select_columns(
        self,
        sql: str,
        shape: _ResolvedShape,
        profile: ProfileComponent,
        dataset_urn: str,
    ) -> _FieldColumns:
        # Boomi does not tie profile fields to the SELECT, so a field is only
        # a column when it names an output of a SELECT that reads one table.
        columns = _FieldColumns()
        target = shape.target
        result = create_lineage_sql_parsed_result(
            query=sql,
            default_db=target.database,
            platform=target.platform,
            platform_instance=None,
            env=target.env,
            default_schema=target.default_schema,
        )
        tables = {self._rebuild_urn(urn, target) for urn in result.in_tables}
        if result.debug_info.table_error is not None or tables != {dataset_urn}:
            for key in profile.fields:
                columns.skipped[key] = ColumnSkipReason.MULTI_TABLE_SQL
            return columns
        by_output: Dict[str, List[str]] = {}
        for info in result.column_lineage or []:
            by_output[info.downstream.column.lower()] = [
                ref.column for ref in info.upstreams
            ]
        has_star = select_has_star(sql, target.platform)
        for key, profile_field in profile.fields.items():
            if not self._is_record_field(profile, profile_field):
                columns.skipped[key] = ColumnSkipReason.NESTED_FIELD
                continue
            sources = by_output.get(profile_field.name.lower())
            if sources is None and has_star:
                sources = [profile_field.name]
            if sources is None:
                columns.skipped[key] = ColumnSkipReason.NOT_IN_SELECT
            elif not sources:
                columns.skipped[key] = ColumnSkipReason.NO_SOURCE_COLUMN
            else:
                self._set_column(columns, key, sources, shape, dataset_urn)
        return columns

    def _insert_field_columns(
        self,
        sql: str,
        shape: _ResolvedShape,
        profile: ProfileComponent,
        dataset_urn: str,
    ) -> _FieldColumns:
        # Fields bind to the statement's parameters, not to columns, so only a
        # field named in the INSERT's column list is taken as that column.
        columns = _FieldColumns()
        listed = insert_column_list(sql, shape.target.platform)
        for key, profile_field in profile.fields.items():
            column = listed.get(profile_field.name.lower())
            if column is None:
                columns.skipped[key] = ColumnSkipReason.NOT_IN_INSERT_COLUMNS
            else:
                self._set_column(columns, key, [column], shape, dataset_urn)
        return columns

    @staticmethod
    def _is_record_field(
        profile: ProfileComponent, profile_field: ProfileField
    ) -> bool:
        if profile.component_type == COMPONENT_TYPE_DB_PROFILE:
            return True
        return profile_field.depth == 1

    def _set_column(
        self,
        columns: _FieldColumns,
        key: str,
        names: List[str],
        shape: _ResolvedShape,
        dataset_urn: str,
    ) -> None:
        urns = [
            self._field_urn(dataset_urn, name, shape.sub_type, shape.target)
            for name in names
        ]
        resolved = [urn for urn in urns if urn is not None]
        if resolved:
            columns.columns[key] = resolved
        else:
            columns.skipped[key] = ColumnSkipReason.NOT_IN_SCHEMA

    def _columns_for(self, columns: _FieldColumns, key: str) -> List[str]:
        if key in columns.columns:
            return columns.columns[key]
        self._skip_column(columns.skipped.get(key, ColumnSkipReason.UNKNOWN_FIELD_KEY))
        return []

    def _field_urn(
        self, dataset_urn: str, column: str, sub_type: str, target: ConnectionTarget
    ) -> Optional[str]:
        # Columns follow the table name's casing; the Salesforce source keeps
        # field API names as they are.
        name = column.strip(_QUOTE_CHARS)
        if sub_type != SUBTYPE_SALESFORCE and target.convert_urns_to_lowercase:
            name = name.lower()
        schema = self._schema_fields(dataset_urn)
        if schema is not None:
            # DataHub knows the table, so its own field path is used, and a
            # column it does not have is dropped rather than invented.
            known = schema.get(name.lower())
            if known is None:
                return None
            name = known
        return make_schema_field_urn(dataset_urn, name)

    def _schema_fields(self, dataset_urn: str) -> Optional[Dict[str, str]]:
        if self.graph is None:
            return None
        if dataset_urn in self._schemas:
            return self._schemas[dataset_urn]
        fields: Optional[Dict[str, str]] = None
        try:
            schema = self.graph.get_aspect(dataset_urn, SchemaMetadataClass)
            if schema is not None:
                fields = {}
                for schema_field in schema.fields:
                    path = get_simple_field_path_from_v2_field_path(
                        schema_field.fieldPath
                    )
                    fields.setdefault(path.lower(), path)
        except Exception as e:
            self._warn_once(
                "schema_lookup",
                title="Failed to read dataset schemas from DataHub",
                message="Column lineage uses column names as Boomi gives them for datasets whose schema could not be read.",
                context=f"datasetUrn={dataset_urn}",
                exc=e,
            )
        self._schemas[dataset_urn] = fields
        return fields

    def _get_connection(self, component_id: str) -> Optional[ConnectionComponent]:
        return self._cached(self._connections, component_id, parse_connection_component)

    def _get_operation(self, component_id: str) -> Optional[OperationComponent]:
        return self._cached(self._operations, component_id, parse_operation_component)

    def _get_profile(self, component_id: str) -> Optional[ProfileComponent]:
        return self._cached(self._profiles, component_id, parse_profile_component)

    def _get_map(self, component_id: str) -> Optional[MapComponent]:
        return self._cached(self._maps, component_id, parse_map_component)

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
                message="Lineage through this connection, operation, profile or map is skipped.",
                context=f"componentId={component_id}",
                exc=e,
            )
        cache[component_id] = parsed
        return parsed

    def _map_unreadable(self, edges: _ProcessEdges) -> None:
        # Column lineage that is missing a map would replace the complete
        # lineage stored by an earlier run, so the process keeps that instead.
        edges.incomplete = True
        self._skip_map(MapSkipReason.COMPONENT_UNREADABLE)

    def _skip_map(self, reason: MapSkipReason) -> None:
        self.report.column_lineage_maps_skipped[reason.value] += 1

    def _skip_column(self, reason: ColumnSkipReason) -> None:
        self.report.column_lineage_fields_skipped[reason.value] += 1

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
