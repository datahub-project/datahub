import logging
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.qlik_sense.config import Constant
from datahub.ingestion.source.qlik_sense.constants import (
    SCRIPT_TABLE_LINEAGE_SKIPPED,
    UNQUALIFIED_TABLES_SKIPPED,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    DATABASE_QRI_PREFIX,
    App,
    LineageTable,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.datahub_io import (
    patch_upstreams,
)
from datahub.ingestion.source.qlik_sense.dataset_naming import (
    qlik_app_id,
)
from datahub.ingestion.source.qlik_sense.field_lineage import (
    script_column_lineage,
)
from datahub.ingestion.source.qlik_sense.lib_path import named_space
from datahub.ingestion.source.qlik_sense.reload_sql import merged_lineage
from datahub.ingestion.source.qlik_sense.report_text import (
    bounded_list,
    error_type,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    VARIABLE_REFERENCE_RE,
    ParsedScript,
    ResolvedConnection,
    ScriptTable,
    ScriptTableLineage,
    drop_condition_variables,
    has_qualified_name,
    is_connector_sql,
    parse_script,
    redact_connection,
)
from datahub.ingestion.source.qlik_sense.source.graph_named import GraphNamedMixin
from datahub.ingestion.source.qlik_sense.source_models import (
    BinaryLink,
    IncompleteLineageReason,
    OnlyAddedTablesKey,
    ParsedApp,
    PendingTableLineage,
    ScriptTableKey,
    SpaceNeed,
    UntypedTable,
)
from datahub.ingestion.source.qlik_sense.table_matching import (
    lineage_table_identifier,
    merge_script_table,
    script_table_fields,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    FineGrainedLineage,
    Upstream,
    UpstreamLineage,
)
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
)

logger = logging.getLogger(__name__)


class TableLineageMixin(GraphNamedMixin):
    def _parse_script_table(
        self, app_id: str, script_table: ScriptTable, connection: ResolvedConnection
    ) -> Optional[SqlParsingResult]:
        sql = drop_condition_variables(script_table.sql)
        if "$(" in sql:
            # orders_$(vYear) would parse as a table named orders_$.
            references = VARIABLE_REFERENCE_RE.findall(sql)
            variables = {name.strip() for name in references}
            # Loop and SUB variables stay unknown whatever the recipe says.
            settable = sorted(variables - set(script_table.runtime_variables))
            context = f"app={app_id}, table={script_table.label}"
            if settable:
                context += f", settable={', '.join(settable)}"
            if script_table.runtime_variables:
                context += (
                    f", set at run time={', '.join(script_table.runtime_variables)}"
                )
            if sql.count("$(") > len(references):
                # Nested references such as $(v$(x)) and expressions such as
                # $(=Date(...)) have no single name, so show the SQL itself.
                context += f", sql={sql[:200]}"
            self.reporter.warning(
                title="Qlik load script SQL uses a variable that is not set",
                message=SCRIPT_TABLE_LINEAGE_SKIPPED
                + (
                    " Set the variables listed as settable in "
                    "`script_variables` to include it; no script sets them."
                    if settable
                    else ""
                ),
                context=context,
            )
            return None
        result = self._parse_script_sql(sql, connection)
        if result.in_tables:
            return result
        if result.debug_info.table_error:
            self.reporter.warning(
                title="Unable to parse Qlik load script SQL",
                message=SCRIPT_TABLE_LINEAGE_SKIPPED,
                context=f"app={app_id}, table={script_table.label}, "
                f"error={error_type(result.debug_info.table_error)}",
            )
        else:
            logger.debug(
                f"App {app_id} script table {script_table.label!r} reads no tables"
            )
        return None

    def _parsed_script(self, app: App) -> ParsedScript:
        # Parsed before an on-prem app's read is checked and again when it is
        # emitted, so the last parse is kept.
        if self._last_parsed is not None and self._last_parsed.app is app:
            return self._last_parsed.parsed
        parsed = (
            parse_script(
                app.script,
                self._app_variables(app),
                self._app_own_variables(app).keys(),
            )
            if app.script
            else ParsedScript()
        )
        self._last_parsed = ParsedApp(app=app, parsed=parsed)
        return parsed

    def _build_script_tables(
        self, app: App, parsed: Optional[ParsedScript] = None
    ) -> List[ScriptTableLineage]:
        if not app.script:
            return []
        tables: Dict[ScriptTableKey, ScriptTableLineage] = {}
        # Tables the data model already traces are ingested from it instead.
        traced = {
            name.lower()
            for table in app.tables
            if table.tableQri
            for name in (table.tableName, table.tableAlias)
        }
        unqualified: List[str] = []
        for script_table in (parsed or self._parsed_script(app)).tables:
            if script_table.label.lower() in traced:
                continue
            connection_name = (
                redact_connection(script_table.connection)
                if script_table.connection
                else None
            )
            self.reporter.script_tables_found += 1
            if is_connector_sql(script_table.sql):
                logger.debug(
                    f"App {app.id} script table {script_table.label!r} reads from "
                    "a Qlik connector, not a database table"
                )
                continue
            connection = self._resolve_script_connection(
                script_table.connection, app.spaceId
            )
            logger.debug(
                f"App {app.id} script table {script_table.label!r} via connection "
                f"{connection_name!r} resolved to {connection}"
            )
            if connection is None:
                self.reporter.unresolved_data_connections.append(
                    f"{app.id}: {connection_name}"
                )
                self._note_unseen_space(
                    named_space(script_table.connection or ""),
                    SpaceNeed.DATA_CONNECTIONS,
                )
                continue
            result = self._parse_script_table(app.id, script_table, connection)
            if result is None:
                continue
            upstream_urns = [
                urn
                for urn in result.in_tables
                if has_qualified_name(
                    urn, connection.platform, connection.platform_instance
                )
            ]
            unqualified.extend(
                urn for urn in result.in_tables if urn not in upstream_urns
            )
            if upstream_urns:
                merge_script_table(
                    tables, app.id, script_table, connection_name, upstream_urns, result
                )
        if unqualified:
            self.reporter.warning(
                title="Qlik load script table is missing its database or schema",
                message=UNQUALIFIED_TABLES_SKIPPED,
                context=f"app={app.id}, tables={sorted(set(unqualified))}",
            )
        # A data table and a mapping table with the same identifier share one
        # dataset URN, so only the data table, which charts read, is kept.
        script_tables = [
            lineage
            for key, lineage in tables.items()
            if not (
                key.mapping
                and ScriptTableKey(identifier=key.identifier, mapping=False) in tables
            )
        ]
        self.reporter.script_tables_with_lineage += len(script_tables)
        return script_tables

    def _gen_script_tables_workunit(
        self,
        tables: List[ScriptTableLineage],
        app_id: str,
        replace_lineage: bool = True,
        binary_links: Optional[Dict[str, BinaryLink]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        for table in tables:
            dataset_urn = self._gen_qlik_dataset_urn(table.identifier)
            yield from self._gen_app_table_entity(
                table.identifier,
                self._table_schema(table.identifier, script_table_fields(table)),
                app_id,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetProperties(
                    name=table.label,
                    qualifiedName=table.label,
                    customProperties={
                        Constant.TYPE: "Qlik Script Table",
                        Constant.DATACONNECTORNAME: table.connection or "",
                    },
                ),
            ).as_workunit()
            link = (binary_links or {}).get(dataset_urn)
            if link is not None:
                # Written once the source app's tables are known, so a source
                # not linked this run doesn't wipe the edge.
                self._binary_links.append(
                    link.model_copy(
                        update={
                            "upstream_urns": table.upstream_urns,
                            "script_fields": script_column_lineage(dataset_urn, table)
                            or [],
                        }
                    )
                )
                continue
            if not replace_lineage:
                yield from patch_upstreams(
                    dataset_urn,
                    table.upstream_urns,
                    DatasetLineageType.TRANSFORMED,
                    self._column_lineage(script_column_lineage(dataset_urn, table)),
                    self._superseded_names(table.upstream_urns),
                )
                continue
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                        for urn in table.upstream_urns
                    ],
                    fineGrainedLineages=self._column_lineage(
                        script_column_lineage(dataset_urn, table)
                    ),
                ),
            ).as_workunit()

    def _gen_lineage_tables_workunit(
        self, app: App, script_tables: Dict[str, ScriptTableLineage]
    ) -> Iterable[MetadataWorkUnit]:
        if app.reload_statements is not None:
            self._record_reload_files(app.id, app.reload_statements)
        self._read_database_source_labels(app)
        reload_tables = self._reload_sql_tables(app, script_tables)
        for table in app.lineage_tables:
            script_table = script_tables.get(table.qri)
            reload_table = reload_tables.get(table.qri)
            if reload_table is not None:
                script_table = (
                    reload_table
                    if script_table is None
                    else merged_lineage(script_table, reload_table)
                )
            identifier = lineage_table_identifier(app.id, table)
            dataset_urn = self._gen_qlik_dataset_urn(identifier)
            # The reload SQL only adds lineage: the fields stay the script's,
            # or the graph's, plus those the SQL gives lineage to.
            fields = self._lineage_table_fields(table, script_tables.get(table.qri))
            if reload_table is not None:
                names = {field.name.lower() for field in fields}
                fields.extend(
                    QlikDatasetSchemaField(name=column)
                    for column in reload_table.column_upstreams
                    if column.lower() not in names
                )
            schema = self._table_schema(identifier, fields)
            if schema and all(field.dataType is None for field in schema):
                self._untyped_tables[dataset_urn] = UntypedTable(
                    identifier=identifier, schema_fields=schema
                )
            yield from self._gen_app_table_entity(identifier, schema, app.id)
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetProperties(
                    name=table.label,
                    qualifiedName=table.label,
                    customProperties={
                        Constant.TYPE: DatasetSubTypes.QLIK_TABLE,
                        Constant.QRI: table.qri,
                        **(
                            {Constant.DATACONNECTORNAME: script_table.connection}
                            if script_table and script_table.connection
                            else {}
                        ),
                    },
                ),
            ).as_workunit()
            pending = PendingTableLineage(
                app_id=app.id,
                table_label=table.label,
                dataset_urn=dataset_urn,
                source_qris=table.source_qris,
                script_upstream_urns=script_table.upstream_urns if script_table else [],
                incomplete_reason=self._incomplete_reason(app, table),
                script_fields=(
                    script_column_lineage(dataset_urn, script_table) or []
                    if script_table
                    else []
                ),
                field_source_qris=table.field_source_qris,
            )
            if self._reads_an_app_not_yet_read(app.id, table):
                # Its tables are only known once that app is read.
                self._pending_table_lineage.append(pending)
                continue
            graph_fields = self._graph_column_lineage(
                dataset_urn,
                table.field_source_qris,
                own_app_id=app.id,
                table_source_qris=table.source_qris,
            )
            if dataset_urn in self._unnamed_field_lineage:
                # A source field is named by an item read later in the run, such
                # as a renamed load from a QVD listed after the app, so the
                # table's lineage is written, whole or as a patch, once every
                # name is known.
                self._unnamed_field_lineage.discard(dataset_urn)
                self._pending_table_lineage.append(pending)
                continue
            yield from self._gen_table_lineage(pending, graph_fields)

    def _incomplete_reason(
        self, app: App, table: LineageTable
    ) -> Optional[IncompleteLineageReason]:
        if app.script_read_failed:
            return IncompleteLineageReason.SCRIPT_UNREAD
        if app.tables_read_failed:
            return IncompleteLineageReason.TABLES_UNREAD
        if table.field_lineage_failed:
            return IncompleteLineageReason.FIELD_LINEAGE_UNREAD
        database_sources = [
            qri for qri in table.source_qris if qri.startswith(DATABASE_QRI_PREFIX)
        ]
        if database_sources and app.reload_statements_read_failed:
            return IncompleteLineageReason.RELOAD_SQL_UNREAD
        if database_sources and self._data_connections_unread():
            return IncompleteLineageReason.DATA_CONNECTIONS_UNLISTED
        if any(qri not in table.database_source_labels for qri in database_sources):
            return IncompleteLineageReason.DATABASE_TABLE_UNLABELLED
        return None

    def _reads_an_app_not_yet_read(self, app_id: str, table: LineageTable) -> bool:
        source_apps = {
            qlik_app_id(qri)
            for qri in [
                *table.source_qris,
                *(
                    qri.rpartition("#")[0]
                    for qris in table.field_source_qris.values()
                    for qri in qris
                ),
            ]
        } - {None, app_id}
        return any(
            source_app not in self._apps_tables_known
            and source_app not in self._apps_tables_unknown
            for source_app in source_apps
        )

    def _gen_pending_table_lineage(self) -> Iterable[MetadataWorkUnit]:
        for pending in self._pending_table_lineage:
            graph_fields = self._graph_column_lineage(
                pending.dataset_urn,
                pending.field_source_qris,
                own_app_id=pending.app_id,
                table_source_qris=pending.source_qris,
            )
            yield from self._gen_table_lineage(pending, graph_fields)
        self._pending_table_lineage = []
        for key, labels in self._only_added_tables.items():
            self.reporter.warning(
                title="Lineage of Qlik app tables only added to",
                message="A source of these tables could not be read this run, so "
                "the lineage read this run is added to their existing lineage "
                "instead of replacing it.",
                context=f"app={key.app_id}, reason={key.reason}, "
                f"tables={bounded_list(sorted(labels))}",
            )
        self._only_added_tables = {}
        yield from self._gen_pending_stored_files()
        yield from self._deferred_fingerprints.values()
        self._deferred_fingerprints = {}

    def _gen_table_lineage(
        self, pending: PendingTableLineage, graph_fields: List[FineGrainedLineage]
    ) -> Iterable[MetadataWorkUnit]:
        fine_grained = self._column_lineage([*pending.script_fields, *graph_fields])
        resolved = self._resolve_lineage_sources(
            pending.source_qris, pending.dataset_urn, own_app_id=pending.app_id
        )
        upstream_urns = list(resolved.urns)
        upstream_urns.extend(
            urn for urn in pending.script_upstream_urns if urn not in upstream_urns
        )
        reason = (
            pending.incomplete_reason
            or "; ".join(resolved.unknown_reasons)
            or (
                IncompleteLineageReason.FIELD_NAME_UNREAD
                if pending.dataset_urn in self._unnamed_field_lineage
                else None
            )
        )
        if reason is None:
            # The app owns its tables, so the lineage is written whole, which
            # also drops upstreams the app no longer reads.
            yield MetadataChangeProposalWrapper(
                entityUrn=pending.dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                        for urn in upstream_urns
                    ],
                    fineGrainedLineages=fine_grained,
                ),
            ).as_workunit()
            return
        # A dataset, the load script and the SQL lineage in it, or the name of
        # a source field could not be read this run: replacing the lineage
        # would drop the edges an earlier run wrote, so what was read is added
        # to them instead.
        self._only_added_tables.setdefault(
            OnlyAddedTablesKey(app_id=pending.app_id, reason=reason), []
        ).append(pending.table_label)
        yield from patch_upstreams(
            pending.dataset_urn,
            upstream_urns,
            DatasetLineageType.TRANSFORMED,
            fine_grained,
            self._superseded_names(upstream_urns),
        )
