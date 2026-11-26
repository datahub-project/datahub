from typing import Dict, List, Optional

from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    LineageTable,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ScriptTable,
    ScriptTableLineage,
    app_table_identifier,
    apply_crosstable,
    apply_preceding_loads,
    select_list_size,
)
from datahub.ingestion.source.qlik_sense.source_models import _ScriptTableKey
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    column_refs_to_schema_field_urns,
)


def _script_table_fields(
    table: ScriptTableLineage,
) -> List[QlikDatasetSchemaField]:
    return [QlikDatasetSchemaField(name=column) for column in table.column_upstreams]


def _lineage_table_identifier(app_id: str, table: LineageTable) -> str:
    # The identifier load script tables use, so a table keeps its URN
    # whether or not its load script SQL resolves to a database table.
    label = table.label or table.qri.rpartition("#")[2]
    return app_table_identifier(app_id, label)


def _tables_unknown(app: App) -> bool:
    """Whether some of the app's tables may have no URN this run, so a
    table that loads from the app can't be told to load nothing else."""
    return app.tables_read_failed


def _graph_tables_unresolved(app: App) -> bool:
    """Whether a table node of the app's lineage graph may have no URN
    this run, though the app's tables were read."""
    return bool(app.unmatched_graph_tables) or bool(
        app.tables and app.lineage_graph_missing
    )


def _match_script_tables(
    lineage_tables: List[LineageTable], script_tables: List[ScriptTableLineage]
) -> Dict[str, ScriptTableLineage]:
    """Script tables that are also lineage graph tables, by graph table QRI,
    so each table is ingested once."""
    by_label = {
        table.label.lower(): table for table in script_tables if not table.mapping
    }
    return {
        table.qri: by_label[table.label.lower()]
        for table in lineage_tables
        if table.label.lower() in by_label
    }


def _name_unknown_tables(app: App, script_tables: List[ScriptTableLineage]) -> None:
    """Gives a script table the parser could not name its name in the data
    model, when exactly one data model table no other table claims has
    exactly its columns. Otherwise it keeps its made-up name."""
    # A lineage graph table stays claimable: an unlabelled load the graph
    # also traces must take its name, so both are ingested as one table.
    claimed = {
        table.label.lower() for table in script_tables if not table.label_unknown
    }
    unclaimed = [
        table for table in app.data_model_tables if table.name.lower() not in claimed
    ]
    by_fields: Dict[frozenset, List[str]] = {}
    for model_table in unclaimed:
        fields = frozenset(field.name.lower() for field in model_table.fields)
        by_fields.setdefault(fields, []).append(model_table.name)
    unknown = [
        table
        for table in script_tables
        if table.label_unknown and not table.mapping and table.column_upstreams
    ]
    columns = [frozenset(c.lower() for c in t.column_upstreams) for t in unknown]
    for table, table_columns in zip(unknown, columns, strict=True):
        names = by_fields.get(table_columns, [])
        # Two unnamed tables with the same columns can't be told apart.
        if len(names) == 1 and columns.count(table_columns) == 1:
            table.label = names[0]
            table.identifier = app_table_identifier(app.id, names[0])
            table.label_unknown = False


def _merge_tables(
    script_tables: List[ScriptTableLineage],
    tables: Dict[str, ScriptTableLineage],
) -> List[ScriptTableLineage]:
    """App tables found another way, such as loaded from or stored into
    QVDs, merged into the script tables of the same name, so each table is
    ingested once."""
    by_identifier = {
        table.identifier: table for table in script_tables if not table.mapping
    }
    merged = list(script_tables)
    for identifier, qvd_table in tables.items():
        table = by_identifier.get(identifier)
        if table is None:
            merged.append(qvd_table)
            continue
        table.upstream_urns.extend(
            urn for urn in qvd_table.upstream_urns if urn not in table.upstream_urns
        )
        for column, upstreams in qvd_table.column_upstreams.items():
            known = table.column_upstreams.setdefault(column, [])
            known.extend(urn for urn in upstreams if urn not in known)
    return merged


def _merge_script_table(
    tables: Dict[_ScriptTableKey, ScriptTableLineage],
    app_id: str,
    script_table: ScriptTable,
    connection_name: Optional[str],
    upstream_urns: List[str],
    result: SqlParsingResult,
) -> None:
    # Loads of one dataset URN are one table, so labels that differ only
    # in case are merged. Mapping tables are a separate namespace, so a
    # mapping load is kept apart.
    identifier = app_table_identifier(app_id, script_table.label)
    lineage = tables.setdefault(
        _ScriptTableKey(identifier=identifier, mapping=script_table.mapping),
        ScriptTableLineage(
            label=script_table.label,
            identifier=identifier,
            label_unknown=script_table.label_unknown,
            connection=connection_name,
            mapping=script_table.mapping,
        ),
    )
    lineage.upstream_urns.extend(
        urn for urn in upstream_urns if urn not in lineage.upstream_urns
    )
    sql_columns: Dict[str, List[str]] = {}
    for column in result.column_lineage or []:
        sql_columns.setdefault(column.downstream.column, []).extend(
            column_refs_to_schema_field_urns(
                ref for ref in column.upstreams if ref.table in upstream_urns
            )
        )
    fields: Optional[Dict[str, List[str]]] = apply_preceding_loads(
        script_table.preceding_loads, sql_columns
    )
    if script_table.reshaped:
        # Only a CrossTable whose fields are known keeps column lineage;
        # otherwise the fields are not the columns and only table lineage
        # is kept.
        selected = [column.downstream.column for column in result.column_lineage or []]
        fields = apply_crosstable(
            script_table.crosstable,
            script_table.preceding_loads,
            sql_columns,
            # The parser lists columns in SELECT order, but leaves out a *
            # and repeats a name selected twice, which would shift slots.
            sql_columns_in_order=len(set(selected))
            == len(selected)
            == select_list_size(script_table.sql),
        )
    for field, upstreams in (fields or {}).items():
        lineage.column_upstreams.setdefault(field, []).extend(upstreams)
