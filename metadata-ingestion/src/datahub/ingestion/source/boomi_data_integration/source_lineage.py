from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pydantic import ValidationError
from typing_extensions import LiteralString

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
)
from datahub.ingestion.source.boomi_data_integration.constants import (
    QUOTE_CHARS,
    SOURCE_LAYOUTS,
    SOURCE_PLATFORM_MAP,
    TABLE_NAME_SEPARATOR,
    TRANSFORM_IDENTITY,
    TRANSFORM_SQL,
    ColumnSkipReason,
    LineageSkipReason,
    SourceLayout,
    SourceRunType,
)
from datahub.ingestion.source.boomi_data_integration.lineage import (
    DatasetScope,
    TableTarget,
    TargetLineageResolver,
    sql_step_target_name,
)
from datahub.ingestion.source.boomi_data_integration.models import (
    CustomQuerySettings,
    LogicNode,
    ModifiedColumn,
    SourceSettings,
    SourceToTargetProperties,
    TableDetails,
)
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)
from datahub.ingestion.source.boomi_data_integration.sql_columns import (
    output_columns,
    statement_count,
    update_statements,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaMetadataClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_from_sql_statements,
    create_lineage_sql_parsed_result,
)
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path


@dataclass
class ColumnEdge:
    upstreams: Set[str]
    transform: str


@dataclass
class JobLineage:
    inputs: Set[str] = field(default_factory=set)
    outputs: Set[str] = field(default_factory=set)
    columns: Dict[str, ColumnEdge] = field(default_factory=dict)

    def add_column_edge(
        self, upstreams: List[str], downstream: str, transform: str
    ) -> None:
        edge = self.columns.get(downstream)
        if edge is None:
            self.columns[downstream] = ColumnEdge(set(upstreams), transform)
            return
        edge.upstreams.update(upstreams)
        if edge.transform != transform:
            edge.transform = TRANSFORM_SQL

    def fine_grained(self) -> List[FineGrainedLineageClass]:
        return [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=sorted(edge.upstreams),
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[downstream],
                transformOperation=edge.transform,
            )
            for downstream, edge in sorted(self.columns.items())
        ]


@dataclass
class SqlStatement:
    sql: str
    scope: DatasetScope
    context: str
    is_script: bool = False
    # Where a plain SELECT is loaded, and how its columns are named there.
    select_target: Optional[str] = None
    select_target_scope: Optional[DatasetScope] = None
    # The step or river maps the query's output columns itself.
    has_field_mapping: bool = False
    # A custom query's own output tables are not where the river loads.
    keep_outputs: bool = True
    # Lowercased output column -> target column, or None when the river does
    # not load it. Output columns not listed keep their name.
    select_renames: Dict[str, Optional[str]] = field(default_factory=dict)


def _join(*parts: Optional[str]) -> Optional[str]:
    if not all(parts):
        return None
    return TABLE_NAME_SEPARATOR.join(part for part in parts if part)


def _source_name_parts(layout: SourceLayout, database: Optional[str]) -> int:
    if layout == SourceLayout.DATABASE_SCHEMA_TABLE:
        return 3
    if layout == SourceLayout.SCHEMA_TABLE and database:
        return 3
    return 2


class SourceLineageResolver:
    def __init__(
        self,
        config: BoomiDataIntegrationSourceConfig,
        report: BoomiDataIntegrationSourceReport,
        targets: TargetLineageResolver,
        graph: Optional[DataHubGraph] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.targets = targets
        self.graph = graph
        self._schemas: Dict[str, Optional[Dict[str, str]]] = {}
        self._warned: Set[str] = set()

    def river_lineage(
        self, props: SourceToTargetProperties, context: str
    ) -> JobLineage:
        tables = self.targets.river_target_tables(props, context)
        lineage = JobLineage(outputs={table.urn for table in tables if table.urn})
        source = props.source
        layout = SOURCE_LAYOUTS.get(source.name)
        scope = self._source_scope(source, layout)
        if layout is None or scope is None:
            # Most sources are SaaS APIs with no dataset in DataHub to link to.
            self.report.sources_unsupported[source.name] += 1
            self._skip(LineageSkipReason.UNSUPPORTED_SOURCE)
            return lineage
        if source.run_type == SourceRunType.CUSTOM_QUERY:
            self._custom_query(props, scope, tables, lineage, context)
        elif source.run_type == SourceRunType.MULTI_TABLES:
            self._multi_tables(props, layout, scope, tables, lineage, context)
        else:
            self._skip(LineageSkipReason.UNSUPPORTED_RUN_TYPE)
        return lineage

    def step_lineage(self, node: LogicNode, context: str) -> JobLineage:
        lineage = JobLineage()
        target = self.targets.step_target(node, context)
        if target:
            lineage.outputs.add(target)
        sql = node.sql
        if not sql:
            return lineage
        target_name, is_script = sql_step_target_name(node.type)
        scope = (
            self.targets.target_scope(target_name, node.connection_id)
            if target_name
            else None
        )
        if scope is None:
            self._skip(LineageSkipReason.UNSUPPORTED_STEP_PLATFORM)
            return lineage
        settings = node.target_settings
        self._sql(
            SqlStatement(
                sql=sql,
                scope=scope,
                context=context,
                is_script=is_script,
                select_target=target,
                select_target_scope=scope,
                has_field_mapping=bool(settings and settings.fields),
            ),
            lineage,
        )
        return lineage

    def _source_scope(
        self, source: SourceSettings, layout: Optional[SourceLayout]
    ) -> Optional[DatasetScope]:
        if layout is None:
            return None
        connection = self.targets.connection(source.connection_id)
        platform = (
            connection.platform
            if connection is not None
            else self.config.connection_type_platform_map.get(
                source.name, SOURCE_PLATFORM_MAP.get(source.name)
            )
        )
        if platform is None:
            return None
        database = connection.database if connection else None
        return self.targets.scope(
            platform, connection, _source_name_parts(layout, database)
        )

    def _multi_tables(
        self,
        props: SourceToTargetProperties,
        layout: SourceLayout,
        scope: DatasetScope,
        tables: List[TableTarget],
        lineage: JobLineage,
        context: str,
    ) -> None:
        if not tables:
            return
        if layout == SourceLayout.DATABASE_SCHEMA_TABLE and not scope.database:
            # Neither rivers nor connections carry the source database.
            self.report.sources_missing_database.append(context)
            self._skip(LineageSkipReason.MISSING_SOURCE_DATABASE, len(tables))
            self._warn_once(
                "missing_source_database",
                title="Source database unknown",
                message=(
                    "Rivers do not carry the source database, so source lineage "
                    "is skipped. Add the source connection to "
                    "connection_to_platform with the database as `database`."
                ),
                context=f"{context}, connection_id={props.source.connection_id}",
            )
            return
        target_scope = self.targets.target_scope(
            props.target.name, props.target.connection_id
        )
        for table in tables:
            if table.details is None:
                continue
            name = self._source_table_name(layout, scope, table)
            if name is None:
                self._skip(LineageSkipReason.MISSING_SOURCE_TABLE)
                continue
            source_urn = scope.urn(name)
            lineage.inputs.add(source_urn)
            self.report.source_tables_emitted += 1
            if not self.config.include_column_lineage:
                continue
            if table.urn and target_scope:
                self._mapped_columns(
                    source_urn, scope, table.urn, target_scope, table.details, lineage
                )
            elif table.details.modified_columns:
                self._skip_column(ColumnSkipReason.MISSING_TARGET)

    @staticmethod
    def _source_table_name(
        layout: SourceLayout, scope: DatasetScope, table: TableTarget
    ) -> Optional[str]:
        source_table = table.details.source_table if table.details else None
        if layout == SourceLayout.DATABASE_TABLE:
            return _join(table.schema_name, source_table)
        if scope.database:
            return _join(scope.database, table.schema_name, source_table)
        return _join(table.schema_name, source_table)

    def _mapped_columns(
        self,
        source_urn: str,
        source_scope: DatasetScope,
        target_urn: str,
        target_scope: DatasetScope,
        details: TableDetails,
        lineage: JobLineage,
    ) -> None:
        for raw in details.modified_columns or []:
            if raw is None:
                continue
            try:
                column = ModifiedColumn.model_validate(raw)
            except ValidationError:
                self._skip_column(ColumnSkipReason.INVALID_COLUMN)
                continue
            if not column.is_selected:
                continue
            if column.is_rivery_metadata:
                self._skip_column(ColumnSkipReason.METADATA_COLUMN)
                continue
            if column.expression:
                self._skip_column(ColumnSkipReason.EXPRESSION)
                continue
            if not column.name:
                self._skip_column(ColumnSkipReason.MISSING_NAME)
                continue
            upstream = self._field_urn(source_urn, column.name, source_scope)
            downstream = self._field_urn(
                target_urn, column.alias or column.name, target_scope
            )
            if upstream is None or downstream is None:
                self._skip_column(ColumnSkipReason.NOT_IN_SCHEMA)
                continue
            lineage.add_column_edge([upstream], downstream, TRANSFORM_IDENTITY)

    def _custom_query(
        self,
        props: SourceToTargetProperties,
        scope: DatasetScope,
        tables: List[TableTarget],
        lineage: JobLineage,
        context: str,
    ) -> None:
        raw = props.source.custom_query_source_settings
        try:
            settings = (
                CustomQuerySettings.model_validate(raw) if raw is not None else None
            )
        except ValidationError:
            self._skip(LineageSkipReason.INVALID_QUERY_SETTINGS)
            return
        if settings is None or not settings.query:
            self._skip(LineageSkipReason.MISSING_QUERY)
            return
        target = tables[0] if len(tables) == 1 else None
        query_lineage = JobLineage()
        self._sql(
            SqlStatement(
                sql=settings.query,
                scope=scope,
                context=context,
                select_target=target.urn if target else None,
                select_target_scope=self.targets.target_scope(
                    props.target.name, props.target.connection_id
                ),
                has_field_mapping=bool(settings.fields),
                keep_outputs=False,
                select_renames=self._column_renames(target.details if target else None),
            ),
            query_lineage,
        )
        lineage.inputs.update(query_lineage.inputs)
        lineage.columns.update(query_lineage.columns)

    def _column_renames(
        self, details: Optional[TableDetails]
    ) -> Dict[str, Optional[str]]:
        renames: Dict[str, Optional[str]] = {}
        for raw in (details.modified_columns if details else None) or []:
            try:
                column = ModifiedColumn.model_validate(raw)
            except ValidationError:
                self._skip_column(ColumnSkipReason.INVALID_COLUMN)
                continue
            if not column.name:
                continue
            loaded = (
                column.is_selected
                and not column.is_rivery_metadata
                and not column.expression
            )
            renames[column.name.lower()] = (
                (column.alias or column.name) if loaded else None
            )
        return renames

    def _parse(self, statement: SqlStatement) -> SqlParsingResult:
        scope = statement.scope
        # The parser runs without a platform instance; it is added when the
        # URN is rebuilt so that casing applies only to the table name.
        if statement.is_script:
            return create_lineage_from_sql_statements(
                queries=statement.sql,
                default_db=scope.database,
                default_schema=scope.default_schema,
                platform=scope.platform,
                platform_instance=None,
                env=scope.env,
            )
        return create_lineage_sql_parsed_result(
            query=statement.sql,
            default_db=scope.database,
            default_schema=scope.default_schema,
            platform=scope.platform,
            platform_instance=None,
            env=scope.env,
        )

    def _sql(self, statement: SqlStatement, lineage: JobLineage) -> None:
        scope = statement.scope
        result = self._parse(statement)
        error = result.debug_info.error
        # The multi-statement parser drops statements it cannot parse instead
        # of returning an error, so no tables at all is treated as a failure.
        if error is not None or not (result.in_tables or result.out_tables):
            self.report.sql_parse_failures.append(statement.context)
            self._skip(LineageSkipReason.SQL_PARSE_FAILED)
            self._warn_once(
                f"sql:{scope.platform}",
                title="Failed to parse SQL",
                message=(
                    "SQL in a custom query or logic SQL step could not be parsed "
                    "or names no tables, so its table lineage is skipped."
                ),
                context=(
                    f"{statement.context}, platform={scope.platform}, "
                    f"sql_prefix={statement.sql.strip()[:80]!r}"
                ),
                exc=error,
            )
            return
        in_urns = self._rebuild_all(result.in_tables, scope, statement.context)
        in_urns -= self._update_targets(statement)
        lineage.inputs.update(in_urns)
        self.report.sql_tables_emitted += len(in_urns)
        if statement.keep_outputs:
            out_urns = self._rebuild_all(result.out_tables, scope, statement.context)
            lineage.outputs.update(out_urns)
            self.report.sql_tables_emitted += len(out_urns)
        if self.config.include_column_lineage:
            self._sql_columns(statement, result, lineage)

    def _update_targets(self, statement: SqlStatement) -> Set[str]:
        # The parser lists an UPDATE's own target as read too. Only that table
        # is dropped from the inputs: in a script, a table written by one
        # statement and read by a later one is still an input.
        scope = statement.scope
        targets: Set[str] = set()
        for sql in update_statements(statement.sql, scope.platform):
            result = create_lineage_sql_parsed_result(
                query=sql,
                default_db=scope.database,
                default_schema=scope.default_schema,
                platform=scope.platform,
                platform_instance=None,
                env=scope.env,
            )
            for parsed in result.out_tables:
                urn = self._rebuild(parsed, scope)
                if urn is not None:
                    targets.add(urn)
        return targets

    def _rebuild_all(
        self, parsed_urns: List[str], scope: DatasetScope, context: str
    ) -> Set[str]:
        urns: Set[str] = set()
        for parsed in parsed_urns:
            urn = self._rebuild(parsed, scope)
            if urn is None:
                self._skip(LineageSkipReason.UNQUALIFIED_SQL_TABLE)
                self._warn_once(
                    "unqualified_sql_table",
                    title="SQL table not fully qualified",
                    message=(
                        "A table in a custom query or logic SQL step is missing "
                        "its database or schema, so it is left out of lineage. "
                        "Set `database` and `default_schema` for the connection "
                        "in connection_to_platform."
                    ),
                    context=f"{context}, table={DatasetUrn.from_string(parsed).name}",
                )
                continue
            urns.add(urn)
        return urns

    @staticmethod
    def _rebuild(parsed_urn: str, scope: DatasetScope) -> Optional[str]:
        name = DatasetUrn.from_string(parsed_urn).name
        if len(name.split(TABLE_NAME_SEPARATOR)) != scope.name_parts:
            return None
        return scope.urn(name)

    def _sql_columns(
        self, statement: SqlStatement, result: SqlParsingResult, lineage: JobLineage
    ) -> None:
        written = result.column_lineage or []
        if not written:
            return
        scope = statement.scope
        if statement.is_script and statement_count(statement.sql, scope.platform) > 1:
            self._skip_column(ColumnSkipReason.MULTI_STATEMENT_SCRIPT)
            return
        outputs = output_columns(statement.sql, scope.platform)
        if not statement.keep_outputs and (not outputs.is_select or result.out_tables):
            # A custom query's result loads into the river target, so only a
            # plain SELECT has columns to map there. Tables the SQL writes
            # itself are not the target and get no column edges.
            self._skip_column(ColumnSkipReason.CUSTOM_QUERY_NOT_SELECT)
            return
        for info in written:
            downstream_table: Optional[str] = None
            downstream_scope: Optional[DatasetScope] = scope
            if info.downstream.table is not None:
                downstream_table = self._rebuild(info.downstream.table, scope)
            elif outputs.is_select and statement.has_field_mapping:
                self._skip_column(ColumnSkipReason.CUSTOM_FIELD_MAPPING)
                continue
            elif outputs.is_select and statement.select_target:
                downstream_table = statement.select_target
                downstream_scope = statement.select_target_scope
            else:
                self._skip_column(ColumnSkipReason.NO_TARGET_TABLE)
                continue
            column: Optional[str] = info.downstream.column or None
            if not outputs.parsed:
                column = None
            elif outputs.is_insert or outputs.is_select:
                if outputs.by_output is None:
                    self._skip_column(ColumnSkipReason.INSERT_WITHOUT_COLUMN_LIST)
                    continue
                column = outputs.by_output.get(info.downstream.column.lower())
                if (
                    column is not None
                    and downstream_table == statement.select_target
                    and column.lower() in statement.select_renames
                ):
                    column = statement.select_renames[column.lower()]
            if column is None or downstream_table is None or downstream_scope is None:
                self._skip_column(ColumnSkipReason.UNMAPPED_SQL_COLUMN)
                continue
            # The parser leaves the column empty when it cannot name the source.
            refs = [ref for ref in info.upstreams if ref.column]
            if not refs:
                self._skip_column(ColumnSkipReason.NO_SOURCE_COLUMN)
                continue
            downstream = self._field_urn(downstream_table, column, downstream_scope)
            upstreams: List[str] = []
            for ref in refs:
                table_urn = self._rebuild(ref.table, scope)
                field_urn = (
                    self._field_urn(table_urn, ref.column, scope) if table_urn else None
                )
                if field_urn:
                    upstreams.append(field_urn)
            if downstream is None or not upstreams:
                self._skip_column(ColumnSkipReason.NOT_IN_SCHEMA)
                continue
            transform = (
                TRANSFORM_IDENTITY
                if info.logic is not None and info.logic.is_direct_copy
                else TRANSFORM_SQL
            )
            lineage.add_column_edge(upstreams, downstream, transform)

    def _field_urn(
        self, dataset_urn: str, column: str, scope: DatasetScope
    ) -> Optional[str]:
        name = column.strip(QUOTE_CHARS)
        if not name:
            return None
        if scope.lowercase:
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
                # Match on the simple path, but keep the stored field path for
                # the URN: v2 paths such as "[version=2.0].[type=int].id" are
                # what the dataset's schemaField URNs are built from.
                for schema_field in schema.fields:
                    path = get_simple_field_path_from_v2_field_path(
                        schema_field.fieldPath
                    )
                    fields.setdefault(path.lower(), schema_field.fieldPath)
        except Exception as e:
            self._warn_once(
                "schema_lookup",
                title="Failed to read dataset schemas from DataHub",
                message=(
                    "Column lineage uses column names as the river gives them for "
                    "datasets whose schema could not be read."
                ),
                context=f"dataset_urn={dataset_urn}",
                exc=e,
            )
        self._schemas[dataset_urn] = fields
        return fields

    def _skip(self, reason: LineageSkipReason, count: int = 1) -> None:
        self.report.source_lineage_skipped[reason.value] += count

    def _skip_column(self, reason: ColumnSkipReason) -> None:
        self.report.column_lineage_skipped[reason.value] += 1

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
