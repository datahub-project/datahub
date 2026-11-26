from typing import Dict, Iterable, List, Optional, Set

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.qlik_sense.config import Constant, PlatformDetail
from datahub.ingestion.source.qlik_sense.data_classes import (
    KNOWN_DATA_PLATFORM_MAPPING,
    App,
    BoxType,
    LineageTable,
    QlikTable,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ScriptTableLineage,
    has_qualified_name,
)
from datahub.ingestion.source.qlik_sense.source.column_lineage import ColumnLineageMixin
from datahub.ingestion.source.qlik_sense.source_utils import (
    _bounded_list,
    _error_type,
    _own_table_urns,
    _qlik_app_id,
)
from datahub.ingestion.source.qlik_sense.table_matching import (
    _graph_tables_unresolved,
    _lineage_table_identifier,
    _script_table_fields,
    _tables_unknown,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import SubTypes
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result


class AppTablesMixin(ColumnLineageMixin):
    def _gen_app_table_upstream_lineage(
        self, dataset_urn: str, table: QlikTable
    ) -> Optional[MetadataWorkUnit]:
        upstream_dataset_urn: Optional[str] = None
        if table.type == BoxType.BLACKBOX:
            upstream_dataset_platform_detail = (
                self.config.data_connection_to_platform_instance.get(
                    table.dataconnectorName,
                    PlatformDetail(
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    ),
                )
            )
            if not table.selectStatement:
                return None
            platform = upstream_dataset_platform_detail.platform or (
                KNOWN_DATA_PLATFORM_MAPPING.get(
                    table.dataconnectorPlatform, table.dataconnectorPlatform
                )
            )
            # The database and schema Qlik reports for the table win over the
            # connection's configured defaults.
            result = create_lineage_sql_parsed_result(
                query=table.selectStatement.strip(),
                default_db=table.databaseName
                or upstream_dataset_platform_detail.database,
                default_schema=table.schemaName
                or upstream_dataset_platform_detail.default_schema,
                platform=platform,
                # Like script tables, an unset env follows the recipe's env.
                env=upstream_dataset_platform_detail.env
                if "env" in upstream_dataset_platform_detail.model_fields_set
                else self.config.env,
                platform_instance=upstream_dataset_platform_detail.platform_instance,
            )
            if not result.in_tables:
                self.reporter.warning(
                    title="Unable to parse Qlik table SQL",
                    message="Upstream lineage for this Qlik table is skipped.",
                    context=f"table={table.tableName}, connection={table.dataconnectorName}, "
                    f"error={_error_type(result.debug_info.table_error)}",
                )
                return None
            qualified = [
                urn
                for urn in result.in_tables
                if has_qualified_name(
                    urn, platform, upstream_dataset_platform_detail.platform_instance
                )
            ]
            if not qualified:
                self.reporter.warning(
                    title="Qlik table is missing its database or schema",
                    message="Upstream lineage for this Qlik table is skipped. Set "
                    "`database` and `default_schema` for its connection in "
                    "`data_connection_to_platform_instance`.",
                    context=f"table={table.tableName}, upstreams={result.in_tables}",
                )
                return None
            upstream_dataset_urn = qualified[0]
        elif table.type == BoxType.LOADFILE:
            upstream_dataset_urn = self._gen_qlik_dataset_urn(
                f"{table.spaceId}.{table.databaseName}".lower()
            )

        if upstream_dataset_urn:
            # Generate finegrained lineage
            fine_grained_lineages = [
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[
                        builder.make_schema_field_urn(upstream_dataset_urn, field.name)
                    ],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[
                        builder.make_schema_field_urn(dataset_urn, field.name)
                    ],
                )
                for field in table.datasetSchema
            ]
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(
                            dataset=upstream_dataset_urn, type=DatasetLineageType.COPY
                        )
                    ],
                    fineGrainedLineages=self._column_lineage(fine_grained_lineages),
                ),
            ).as_workunit()
        else:
            return None

    def _gen_app_table_properties(
        self, dataset_urn: str, table: QlikTable
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=table.tableName,
            qualifiedName=table.tableAlias,
        )
        dataset_properties.customProperties.update(
            {
                Constant.TYPE: DatasetSubTypes.QLIK_TABLE,
                Constant.DATACONNECTORID: table.dataconnectorid,
                Constant.DATACONNECTORNAME: table.dataconnectorName,
            }
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _get_app_table_identifier(self, table: QlikTable) -> Optional[str]:
        if table.tableQri:
            return table.tableQri.lower()
        return None

    def _gen_app_table_entity(
        self,
        table_identifier: str,
        schema: List[QlikDatasetSchemaField],
        app_id: str,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self._gen_qlik_dataset_urn(table_identifier)
        if app_id in self._binary_source_apps:
            self._app_urns.setdefault(app_id, set()).add(dataset_urn)

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_schema_metadata(table_identifier, schema)

        yield from add_entity_to_container(
            container_key=self._gen_app_key(app_id),
            entity_type="dataset",
            entity_urn=dataset_urn,
        )

        dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.QLIK_TABLE]),
        ).as_workunit()

    def _app_table_urns(self, app: App) -> Dict[str, str]:
        urns: Dict[str, str] = {}
        for table in app.tables:
            if table.tableQri:
                urn = self._gen_qlik_dataset_urn(table.tableQri.lower())
                urns[table.tableQri] = urn
                if table.graphQri:
                    urns[table.graphQri] = urn
        # A tableQri is the table's source, which can be another app's table,
        # such as for a BINARY load, whose own URN must not be replaced.
        urns = _own_table_urns(app.id, urns)
        urns.update(
            (
                table.qri,
                self._gen_qlik_dataset_urn(_lineage_table_identifier(app.id, table)),
            )
            for table in app.lineage_tables
        )
        return urns

    def _register_app_tables(self, app: App) -> None:
        table_urns = self._app_table_urns(app)
        self._app_table_urns_by_qri.update(table_urns)
        self._table_urns_by_app[app.id] = set(table_urns.values())
        self._app_qris[app.id] = app.qri
        if _tables_unknown(app):
            self._apps_tables_unknown.add(app.id)
            return
        self._apps_tables_known.add(app.id)
        if _graph_tables_unresolved(app):
            self._apps_graph_tables_unresolved.add(app.id)
        if app.unmatched_graph_tables:
            self.reporter.warning(
                title="Qlik app tables not matched to their lineage graph",
                message="These tables of the app's Qlik Cloud lineage graph match "
                "no data model table by name, so lineage that other apps' tables "
                "or datasets have from them is only added to.",
                context=f"{app.id}: {_bounded_list(app.unmatched_graph_tables)}",
            )
        previous = self._usable_checkpoint(app.id, [])
        if previous is not None and previous.table_urns_by_qri:
            self._previous_app_table_urns.update(previous.table_urns_by_qri.values())

    def _app_tables_known(self, app_id: str) -> bool:
        if app_id in self._apps_tables_unknown:
            return False
        if app_id in self._apps_tables_known:
            return True
        # Not read or kept this run, so deleted or outside the recipe, unless
        # part of the run could not be read.
        return not self.qlik_api.unread_items

    def _gen_app_tables_workunit(
        self, tables: List[QlikTable], app_id: str
    ) -> Iterable[MetadataWorkUnit]:
        for table in tables:
            table_identifier = self._get_app_table_identifier(table)
            if not table_identifier:
                continue
            dataset_urn = self._gen_qlik_dataset_urn(table_identifier)

            yield from self._gen_app_table_entity(
                table_identifier, table.datasetSchema, app_id
            )

            yield self._gen_app_table_properties(dataset_urn, table)

            upstream_lineage_workunit = self._gen_app_table_upstream_lineage(
                dataset_urn, table
            )
            if upstream_lineage_workunit:
                yield upstream_lineage_workunit

    def _use_data_model_field_names(
        self, script_tables: List[ScriptTableLineage]
    ) -> None:
        """Spells each column as the data model does, so column lineage lands on
        the schema's fields. The SQL parser may change a column's case."""
        for table in script_tables:
            fields = self._data_model_fields.get(table.identifier)
            if not fields:
                continue
            names = {field.name for field in fields}
            by_lower: Dict[str, List[str]] = {}
            for name in names:
                by_lower.setdefault(name.lower(), []).append(name)
            column_upstreams: Dict[str, List[str]] = {}
            for column, upstreams in table.column_upstreams.items():
                matches = by_lower.get(column.lower(), [])
                # Qlik field names are case-sensitive, so a column matching two
                # fields keeps its own spelling.
                name = column if column in names or len(matches) != 1 else matches[0]
                merged = column_upstreams.setdefault(name, [])
                merged.extend(u for u in upstreams if u not in merged)
            table.column_upstreams = column_upstreams

    def _lineage_table_fields(
        self, table: LineageTable, script_table: Optional[ScriptTableLineage]
    ) -> List[QlikDatasetSchemaField]:
        """The fields the load script or, failing that, the field level of the
        lineage graph gives a table, for a table the data model no longer
        holds, such as one the script drops after storing it."""
        parsed = _script_table_fields(script_table) if script_table else []
        return parsed or [QlikDatasetSchemaField(name=name) for name in table.fields]

    def _table_schema(
        self, identifier: str, parsed: List[QlikDatasetSchemaField]
    ) -> List[QlikDatasetSchemaField]:
        """A table's fields as the app's data model holds them, which the load
        script only shows when its SELECT lists them."""
        return self._data_model_fields.get(identifier) or parsed

    def _app_table_fields(
        self, tables: List[QlikTable], script_tables: List[ScriptTableLineage]
    ) -> Dict[str, List[QlikDatasetSchemaField]]:
        table_fields: Dict[str, List[QlikDatasetSchemaField]] = {}
        for table in tables:
            identifier = self._get_app_table_identifier(table)
            if identifier:
                table_fields[self._gen_qlik_dataset_urn(identifier)] = (
                    table.datasetSchema
                )
        for script_table in script_tables:
            # A mapping table is only used by ApplyMap and is not in the data
            # model, so charts cannot read it.
            if script_table.mapping:
                continue
            table_fields[self._gen_qlik_dataset_urn(script_table.identifier)] = (
                self._table_schema(
                    script_table.identifier, _script_table_fields(script_table)
                )
            )
        return table_fields

    def _known_app_table_urns(self) -> Set[str]:
        return self._previous_app_table_urns | {
            urn
            for qri, urn in self._app_table_urns_by_qri.items()
            if _qlik_app_id(qri) in self._apps_tables_known
        }
