import logging
from typing import Dict, List, Optional, Set

from datahub.ingestion.source.qlik_sense.constants import UNQUALIFIED_TABLES_SKIPPED
from datahub.ingestion.source.qlik_sense.data_classes import (
    DATABASE_QRI_PREFIX,
    App,
)
from datahub.ingestion.source.qlik_sense.lib_path import _named_space
from datahub.ingestion.source.qlik_sense.reload_sql import (
    READ_QUERY_TYPES,
    RELOAD_SQL_RE,
    RELOAD_UNPARSED_SQL_RE,
    _add_reload_column_lineage,
    _database_qri_platform,
    _label_parts,
    _names_identifier,
    _ParsedReloadSql,
    _ReloadSql,
    _string_literal_table_names,
    _table_clause_names,
    _untraced_source_groups,
    _UntracedSources,
    _urn_names_table,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    BACKTICK_QUOTED_PLATFORMS,
    ResolvedConnection,
    ScriptTableLineage,
    brackets_to_backticks,
    drop_condition_variables,
    has_qualified_name,
    is_connector_sql,
    redact_connection,
)
from datahub.ingestion.source.qlik_sense.source.resolution import SourceResolutionMixin
from datahub.ingestion.source.qlik_sense.source_models import _SpaceNeed
from datahub.ingestion.source.qlik_sense.source_utils import (
    _bounded_list,
    _describe_error,
    _error_type,
)
from datahub.ingestion.source.qlik_sense.table_matching import _lineage_table_identifier
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
    create_schema_resolver,
)

logger = logging.getLogger(__name__)


class ReloadSqlMixin(SourceResolutionMixin):
    def _parse_script_sql(
        self, sql: str, connection: ResolvedConnection
    ) -> SqlParsingResult:
        result = self._parse_sql(sql, connection)
        if (
            result.debug_info.table_error
            and connection.platform in BACKTICK_QUOTED_PLATFORMS
            and "[" in sql
        ):
            # A `[bracket]` identifier only parses on these platforms as a
            # backtick one.
            result = self._parse_sql(brackets_to_backticks(sql), connection)
        return result

    def _parse_sql(self, sql: str, connection: ResolvedConnection) -> SqlParsingResult:
        return create_lineage_sql_parsed_result(
            query=sql,
            default_db=connection.database,
            default_schema=connection.default_schema,
            platform=connection.platform,
            platform_instance=connection.platform_instance,
            env=connection.env,
            graph=self.ctx.graph,
        )

    def _parse_reload_sql(self, app: App, labels: List[str]) -> _ParsedReloadSql:
        """The SQL statements of the app's last reload that may read a table
        one of `labels` names. Qlik ran them with variables set and included
        scripts inlined, so they hold SQL the load script only builds at run
        time, such as from a list of tables in a file."""
        names = {_label_parts(label)[-1] for label in labels}
        parsed = _ParsedReloadSql()
        unqualified: List[str] = []
        unresolved: Set[str] = set()
        for statement in app.reload_statements or []:
            match = RELOAD_SQL_RE.match(statement.statement)
            unparsed = match is None and bool(
                RELOAD_UNPARSED_SQL_RE.match(statement.statement)
            )
            if not match and not unparsed:
                continue
            sql = drop_condition_variables(
                match.group("sql").strip() if match else statement.statement.strip()
            )
            if is_connector_sql(sql):
                continue
            if "$(" in sql:
                parsed.unexpanded_variable = True
                continue
            if not any(_names_identifier(sql.lower(), name) for name in names):
                continue
            if unparsed:
                parsed.skipped_table_names |= _table_clause_names(sql)
                continue
            connection_name = statement.discriminator.strip().rstrip(";").strip()
            connection = self._resolve_script_connection(connection_name, app.spaceId)
            if connection is None:
                unresolved.add(redact_connection(connection_name))
                self._note_unseen_space(
                    _named_space(connection_name), _SpaceNeed.DATA_CONNECTIONS
                )
                parsed.skipped_table_names |= _table_clause_names(sql)
                continue
            result = self._parse_script_sql(sql, connection)
            if result.debug_info.table_error:
                self.reporter.warning(
                    title="Unable to parse Qlik reload SQL",
                    message="Lineage to the tables the statement reads is skipped.",
                    context=f"app={app.id}, connection="
                    f"{redact_connection(connection_name)}, "
                    f"error={_error_type(result.debug_info.table_error)}",
                )
                parsed.skipped_table_names |= _table_clause_names(sql)
                continue
            # An UPDATE, DELETE or MERGE loads no rows, and the tables an
            # INSERT writes are not read.
            read_urns = (
                [urn for urn in result.in_tables if urn not in result.out_tables]
                if result.query_type in READ_QUERY_TYPES
                else []
            )
            if not read_urns:
                # Such as EXEC('SELECT ...'), whose tables the parser can't see.
                parsed.skipped_table_names |= _table_clause_names(sql)
                continue
            parsed.skipped_table_names.update(
                part
                for urn in result.out_tables
                for part in DatasetUrn.from_string(urn).name.lower().split(".")
            )
            # SQL in a string literal runs dynamically and is never parsed.
            parsed.skipped_table_names |= _string_literal_table_names(sql)
            upstream_urns = [
                urn
                for urn in read_urns
                if has_qualified_name(
                    urn, connection.platform, connection.platform_instance
                )
            ]
            statement_unqualified = [
                urn for urn in read_urns if urn not in upstream_urns
            ]
            unqualified.extend(statement_unqualified)
            # A table without its database or schema may be the label's table
            # in another database, so it counts as SQL that could not be read.
            parsed.skipped_table_names.update(
                part
                for urn in statement_unqualified
                for part in DatasetUrn.from_string(urn).name.lower().split(".")
            )
            if upstream_urns:
                parsed.statements.append(
                    _ReloadSql(
                        upstream_urns=upstream_urns,
                        result=result,
                        urns_without_schema=self._urns_without_schema(
                            upstream_urns, result, connection
                        ),
                    )
                )
        for connection_name in sorted(unresolved):
            self.reporter.unresolved_data_connections.append(
                f"{app.id}: {connection_name}"
            )
        if unqualified:
            self.reporter.warning(
                title="Qlik reload SQL table is missing its database or schema",
                message=UNQUALIFIED_TABLES_SKIPPED,
                context=f"app={app.id}, tables={sorted(set(unqualified))}",
            )
        return parsed

    def _urns_without_schema(
        self,
        urns: List[str],
        result: SqlParsingResult,
        connection: ResolvedConnection,
    ) -> List[str]:
        """Which of the tables a statement reads DataHub holds no schema for.
        The parser only counts them for the whole statement."""
        debug_info = result.debug_info
        if (
            self.ctx.graph is None
            or debug_info.table_schemas_resolved >= debug_info.tables_discovered
        ):
            return []
        resolver = create_schema_resolver(
            platform=connection.platform,
            env=connection.env,
            graph=self.ctx.graph,
            platform_instance=connection.platform_instance,
        )
        return [urn for urn in urns if resolver.resolve_urn(urn)[1] is None]

    def _reload_sql_tables(
        self, app: App, script_tables: Dict[str, ScriptTableLineage]
    ) -> Dict[str, ScriptTableLineage]:
        """Lineage of the graph tables' database sources that the load script
        doesn't trace, from the SQL of the app's last reload, by table QRI.
        The graph names each source table by a label such as `dbo.orders`,
        which is linked only when exactly one table the SQL reads matches it."""
        untraced: Dict[str, List[_UntracedSources]] = {}
        for table in app.lineage_tables:
            script_table = script_tables.get(table.qri)
            groups = _untraced_source_groups(
                table.database_source_labels,
                script_table.upstream_urns if script_table else [],
            )
            if groups:
                untraced[table.qri] = groups
        if not untraced or app.reload_statements is None:
            return {}
        parsed = self._parse_reload_sql(
            app,
            [
                label
                for groups in untraced.values()
                for group in groups
                for label in group.labels.values()
            ],
        )
        lineages: Dict[str, ScriptTableLineage] = {}
        for table in app.lineage_tables:
            if table.qri not in untraced:
                continue
            lineage = ScriptTableLineage(
                label=table.label,
                identifier=_lineage_table_identifier(app.id, table),
            )
            for group in untraced[table.qri]:
                urn = self._reload_sql_source(app, parsed, group)
                if urn is None:
                    continue
                if urn not in lineage.upstream_urns:
                    lineage.upstream_urns.append(urn)
                if len(group.labels) == 1 and not group.traced_count:
                    # With several such sources, which one the table is isn't
                    # known, so neither are the graph fields it loads.
                    (source_qri,) = group.labels
                    _add_reload_column_lineage(lineage, table, parsed, source_qri, urn)
            if lineage.upstream_urns:
                self.reporter.reload_sql_tables_with_lineage += 1
                lineages[table.qri] = lineage
        return lineages

    def _reload_sql_source(
        self, app: App, parsed: _ParsedReloadSql, group: _UntracedSources
    ) -> Optional[str]:
        """The one table the reload SQL reads, and the load script doesn't,
        that the graph labels of a group of database sources name."""
        # The SQL may spell a table the script linked in another case.
        script_urns = {urn.lower() for urn in group.script_urns}
        matches = {
            urn
            for statement in parsed.statements
            for urn in statement.upstream_urns
            if urn.lower() not in script_urns
            and any(
                _urn_names_table(urn, qri, label) for qri, label in group.labels.items()
            )
        }
        labels = ", ".join(sorted(set(group.labels.values())))
        unread = parsed.unexpanded_variable or group.name in parsed.skipped_table_names
        untraced_count = len(group.labels) - group.traced_count
        if len(matches) > 1 or (matches and untraced_count > 1):
            self.reporter.ambiguous_reload_sql_sources.append(
                f"{app.id}: {labels} matches {sorted(matches)}"
            )
            return None
        if not matches:
            if not unread:
                self.reporter.unmatched_reload_sql_sources.extend(
                    dict.fromkeys(
                        f"{app.id}: {label} ({_database_qri_platform(qri)})"
                        for qri, label in group.labels.items()
                    )
                )
            return None
        if unread:
            # A statement that could not be read may name another table with
            # the label, so the match is not certain.
            self.reporter.ambiguous_reload_sql_sources.append(
                f"{app.id}: {labels} may also be named by reload SQL that could "
                "not be read"
            )
            return None
        urn = matches.pop()
        if any(urn in statement.urns_without_schema for statement in parsed.statements):
            # Linked as the SQL names it, which may differ in case from the
            # dataset DataHub holds.
            self.reporter.reload_sql_tables_not_in_datahub.append(f"{app.id}: {urn}")
        return urn

    def _read_database_source_labels(self, app: App) -> None:
        """Labels the graph tables' database sources by, such as `dbo.orders`,
        which only the upstream graph gives: from the app's upstream graph when
        a source only it names already had it read, or else from the one-level
        graph alone."""
        unlabelled = [
            (table, qri)
            for table in app.lineage_tables
            for qri in table.source_qris
            if qri.startswith(DATABASE_QRI_PREFIX)
            and qri not in table.database_source_labels
        ]
        # Without the reload's SQL, a label names nothing to link.
        if not unlabelled or app.reload_statements is None:
            return
        upstreams = self._lineage_upstreams.get(app.id)
        if upstreams is not None and upstreams.database_labels is not None:
            labels = upstreams.database_labels
        else:
            try:
                labels = self.qlik_api.get_database_labels(app.qri)
            except Exception as e:
                # The tables stay unlabelled, so their lineage is only added to.
                logger.debug(
                    f"Unable to read database labels of app {app.id}", exc_info=True
                )
                self.reporter.database_label_read_failures.append(
                    f"{app.id}: {_describe_error(e)}"
                )
                return
        for table, qri in unlabelled:
            if qri in labels:
                table.database_source_labels[qri] = labels[qri]

    def _report_reload_sql(self) -> None:
        if self.qlik_api.reload_statement_read_failures:
            self.reporter.warning(
                title="Unable to read the SQL of Qlik apps' last reload",
                message="Database tables that only the SQL of these apps' last "
                "reload names are left out, and the lineage of the apps' tables "
                "that read databases is only added to this run.",
                context=_bounded_list(self.qlik_api.reload_statement_read_failures),
            )
        if self.reporter.database_label_read_failures:
            self.reporter.warning(
                title="Unable to read the labels of Qlik apps' database tables",
                message="The upstream lineage graph that labels the database "
                "tables these apps load from could not be read, so lineage of the "
                "apps' tables that read databases is only added to this run.",
                context=_bounded_list(list(self.reporter.database_label_read_failures)),
            )
        if self.reporter.ambiguous_reload_sql_sources:
            self.reporter.warning(
                title="Qlik lineage graph database table matches several tables",
                message="The SQL of the app's last reload reads more than one "
                "table the graph's label could name, or a statement that may "
                "name it could not be read, so lineage to it is skipped.",
                context=_bounded_list(
                    sorted(self.reporter.ambiguous_reload_sql_sources)
                ),
            )
        if self.reporter.unmatched_reload_sql_sources:
            self.reporter.warning(
                title="Qlik lineage graph database table matches no reload SQL table",
                message="No table the SQL of the app's last reload reads matches "
                "the graph's label and platform, so lineage to it is skipped.",
                context=_bounded_list(
                    sorted(self.reporter.unmatched_reload_sql_sources)
                ),
            )
        if self.reporter.reload_sql_tables_not_in_datahub:
            self.reporter.warning(
                title="Qlik reload SQL table schema not found in DataHub",
                message="These tables are linked as the reload SQL names them. If "
                "the warehouse is ingested with another case, the edge points at "
                "a dataset that doesn't exist; set the warehouse source's URN "
                "case to match.",
                context=_bounded_list(
                    sorted(self.reporter.reload_sql_tables_not_in_datahub)
                ),
            )
