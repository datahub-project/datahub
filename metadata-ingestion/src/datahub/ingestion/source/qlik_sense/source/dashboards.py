from typing import Collection, Dict, Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container
from datahub.ingestion.api.incremental_lineage_helper import (
    dashboard_info_patch_builder,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.qlik_sense.config import Constant
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    AppField,
    AppFieldIndex,
    Chart,
    ChartInputs,
    SchemaField as QlikDatasetSchemaField,
    Sheet,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    expand_variables,
    expression_names,
    reads_unknown_fields,
)
from datahub.ingestion.source.qlik_sense.source.app_tables import AppTablesMixin
from datahub.ingestion.source.qlik_sense.source_utils import _patch_workunits
from datahub.metadata.com.linkedin.pegasus2avro.common import SubTypes
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    EdgeClass,
    InputFieldClass,
    InputFieldsClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.specific.chart import ChartPatchBuilder
from datahub.utilities.urns.urn import guess_entity_type


class DashboardMixin(AppTablesMixin):
    def _gen_dashboard_urn(self, dashboard_identifier: str) -> str:
        return builder.make_dashboard_urn(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            name=dashboard_identifier,
        )

    def _gen_dashboard_info_workunit(
        self, sheet: Sheet, app_id: str
    ) -> MetadataWorkUnit:
        dashboard_urn = self._gen_dashboard_urn(sheet.id)
        custom_properties: Dict[str, str] = {"chartCount": str(len(sheet.charts))}
        dashboard_info_cls = DashboardInfoClass(
            title=sheet.title,
            description=sheet.description,
            charts=[
                builder.make_chart_urn(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    name=chart.qId,
                )
                for chart in sheet.charts
            ],
            lastModified=ChangeAuditStampsClass(
                created=self._get_audit_stamp(sheet.createdAt, sheet.ownerId),
                lastModified=self._get_audit_stamp(sheet.updatedAt, sheet.ownerId),
            ),
            customProperties=custom_properties,
            dashboardUrl=f"https://{self.ui_host}/sense/app/{app_id}/sheet/{sheet.id}/state/analysis",
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

    def _chart_inputs(
        self,
        charts: Iterable[Chart],
        variables: Dict[str, str],
        app_fields: AppFieldIndex,
        input_tables_urns: List[str],
        has_unknown_schema: bool,
    ) -> Dict[str, ChartInputs]:
        inputs = {}
        for chart in charts:
            # Read with the app's variables expanded; the chart keeps its
            # expressions as Qlik has them.
            expanded = chart.model_copy(
                update={
                    "field_expressions": [
                        expand_variables(expression, variables)
                        for expression in chart.field_expressions
                    ]
                }
            )
            fields = self._get_chart_input_fields(expanded, app_fields)
            inputs[chart.qId] = ChartInputs(
                fields=fields,
                tables=self._get_chart_inputs(
                    expanded,
                    fields,
                    app_fields,
                    input_tables_urns,
                    has_unknown_schema,
                ),
            )
        return inputs

    def _gen_charts_workunit(
        self,
        charts: List[Chart],
        chart_inputs: Optional[Dict[str, ChartInputs]],
        app_id: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Map Qlik charts to DataHub charts. `chart_inputs` is None when the
        app's tables are not all known."""
        for chart in charts:
            chart_urn = builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=chart.qId,
            )

            yield self._gen_entity_status_aspect(chart_urn)

            custom_properties = {
                "Dimension": str(chart.qDimension),
                "Measure": str(chart.qMeasure),
            }
            inputs = chart_inputs.get(chart.qId) if chart_inputs is not None else None

            if inputs is None:
                # Unknown inputs would overwrite the ones the last full read
                # emitted, so only the other chart fields are patched.
                yield from self._gen_chart_info_patch(
                    chart_urn, chart, custom_properties
                )
            else:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=ChartInfoClass(
                        title=chart.title,
                        description=chart.visualization,
                        lastModified=ChangeAuditStampsClass(),
                        customProperties=custom_properties,
                        inputs=inputs.tables,
                    ),
                ).as_workunit()

            # Emitted even when empty so fields a chart stopped using are cleared,
            # but not from a partial list of a chart whose measures were unread.
            if inputs is not None and not chart.fields_unread:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=InputFieldsClass(
                        fields=[
                            InputFieldClass(
                                schemaFieldUrn=builder.make_schema_field_urn(
                                    field.dataset_urn, field.field.name
                                ),
                                schemaField=self._gen_schema_field(field.field),
                            )
                            for field in inputs.fields
                        ]
                    ),
                ).as_workunit()

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app_id),
                entity_type="chart",
                entity_urn=chart_urn,
            )

    @staticmethod
    def _gen_chart_info_patch(
        chart_urn: str, chart: Chart, custom_properties: Dict[str, str]
    ) -> Iterable[MetadataWorkUnit]:
        # GMS creates missing chart info from its patch template, which also
        # stamps lastModified. An empty stamp here would erase the last one.
        patch = ChartPatchBuilder(chart_urn)
        if chart.title:
            patch.set_title(chart.title)
        if chart.visualization:
            patch.set_description(chart.visualization)
        for key, value in custom_properties.items():
            patch.add_custom_property(key, value)
        yield from _patch_workunits(patch, is_primary_source=True)

    @staticmethod
    def _chart_field_names(expression: str, app_fields: AppFieldIndex) -> List[str]:
        # A dimension is usually a bare field name, which may contain spaces.
        return (
            [expression] if app_fields.get(expression) else expression_names(expression)
        )

    @staticmethod
    def _app_field_key(name: str, app_fields: AppFieldIndex) -> Optional[str]:
        """The app field a chart name reads. A derived field such as
        Date.autoCalendar.Year reads its base field, the longest leading part
        of the name that is a field (field names can contain dots), unless a
        field of that exact name exists."""
        key = name
        while not app_fields.get(key) and "." in key:
            key = key.rsplit(".", 1)[0]
        return key if app_fields.get(key) else None

    @staticmethod
    def _get_chart_input_fields(
        chart: Chart, app_fields: AppFieldIndex
    ) -> List[AppField]:
        matched: Dict[str, AppField] = {}
        for expression in chart.field_expressions:
            for name in DashboardMixin._chart_field_names(expression, app_fields):
                key = DashboardMixin._app_field_key(name, app_fields)
                for app_field in app_fields.get(key) if key else []:
                    matched.setdefault(
                        f"{app_field.dataset_urn}.{app_field.field.name}", app_field
                    )
        return list(matched.values())

    def _get_chart_inputs(
        self,
        chart: Chart,
        chart_fields: List[AppField],
        app_fields: AppFieldIndex,
        input_tables_urns: List[str],
        has_unknown_schema: bool,
    ) -> List[str]:
        # The fields behind a $(variable) expansion that could not be expanded
        # are unknown, so the chart may read any of the app's tables, as may a
        # measure that could not be read, a derived field such as
        # Date.autoCalendar.Year whose base field is not known, or a name that
        # is not a known field while some table's columns are unknown (e.g.
        # SELECT *). Set modifier values only filter.
        unmatched = [
            name
            for expression in chart.field_expressions
            for name in self._chart_field_names(expression, app_fields)
            if self._app_field_key(name, app_fields) is None
        ]
        if (
            not chart.fields_complete
            or any(map(reads_unknown_fields, chart.field_expressions))
            or any("." in name for name in unmatched)
            or (has_unknown_schema and unmatched)
        ):
            return input_tables_urns
        # Otherwise every name is known: a chart that reads no field of the
        # app, such as a text object, has no input table.
        return list(dict.fromkeys(field.dataset_urn for field in chart_fields))

    @staticmethod
    def _index_app_fields(
        tables: Dict[str, List[QlikDatasetSchemaField]],
        script_table_urns: Collection[str] = (),
    ) -> AppFieldIndex:
        index = AppFieldIndex()
        for dataset_urn, fields in tables.items():
            for field in fields:
                app_field = AppField(dataset_urn=dataset_urn, field=field)
                index.exact.setdefault(field.name, []).append(app_field)
                if dataset_urn in script_table_urns:
                    index.folded.setdefault(field.name.lower(), []).append(app_field)
        return index

    @staticmethod
    def _tables_without_chart_path(
        app: App, chart_inputs: Dict[str, ChartInputs], table_urns: List[str]
    ) -> List[str]:
        """The app's tables no chart links to the app. A table reaches the app
        through a chart only if the chart's sheet is written with its charts,
        which needs all of them to have been read."""
        charts_on_complete_sheets = {
            chart.qId
            for sheet in app.sheets
            if sheet.charts_complete
            for chart in sheet.charts
        }
        charted = {
            urn
            for chart_id, inputs in chart_inputs.items()
            if chart_id in charts_on_complete_sheets
            for urn in inputs.tables
        }
        return [urn for urn in table_urns if urn not in charted]

    def _gen_app_dashboard_workunit(
        self,
        app: App,
        owner_username: Optional[str],
        table_urns: Optional[List[str]],
    ) -> Iterable[MetadataWorkUnit]:
        """The app as a dashboard of its sheets, as well as their container.
        `table_urns` are the app's tables no chart reads, such as those of an
        app without sheets, so lineage still reaches the app from them; tables
        a chart reads already do through the chart and its sheet. None when
        the app's tables are not all known."""
        dashboard_urn = self._gen_dashboard_urn(app.id)
        yield self._gen_entity_status_aspect(dashboard_urn)
        sheet_urns = [self._gen_dashboard_urn(sheet.id) for sheet in app.sheets]
        last_modified = ChangeAuditStampsClass(
            created=self._get_audit_stamp(app.createdAt, app.ownerId),
            lastModified=self._get_audit_stamp(app.updatedAt, app.ownerId),
        )
        dashboard_url = f"https://{self.ui_host}/sense/app/{app.id}/overview"
        if table_urns is None and not self.config.include_sheets_and_charts:
            # The app's tables reached it through sheets that are no longer
            # ingested, so the tables known for it are linked directly.
            dataset_urns: List[str] = self._last_known_table_urns(app)
        else:
            dataset_urns = table_urns or []
        info = DashboardInfoClass(
            title=app.qTitle,
            description=app.description or "",
            dashboards=[EdgeClass(destinationUrn=urn) for urn in sheet_urns],
            datasetEdges=[EdgeClass(destinationUrn=urn) for urn in dataset_urns],
            lastModified=last_modified,
            customProperties={Constant.QRI: app.qri},
            dashboardUrl=dashboard_url,
        )
        if app.sheets_complete and table_urns is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn, aspect=info
            ).as_workunit()
        else:
            # Sheets or tables that could not be read this run may still exist,
            # so the edges read are added to the last full read's, while the
            # app's own fields are still set.
            patch = dashboard_info_patch_builder(dashboard_urn, info, None)
            if app.sheets_complete:
                # The sheets are all known, so sheet edges this run doesn't
                # have, such as those of sheets no longer ingested, go, in the
                # same patch so the two can't be applied to different bases.
                for urn in self._stale_sheet_edges(dashboard_urn, sheet_urns):
                    patch.remove_dashboard(urn)
            yield from _patch_workunits(patch, is_primary_source=True)
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=SubTypes(typeNames=[BIAssetSubTypes.QLIK_APP_DASHBOARD]),
        ).as_workunit()
        # Inside the app's own container, with its sheets and tables, so the
        # app is one node when browsing its space.
        yield from add_entity_to_container(
            container_key=self._gen_app_key(app.id),
            entity_type="dashboard",
            entity_urn=dashboard_urn,
        )
        dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
        if dpi_aspect:
            yield dpi_aspect
        if self.config.ingest_owner and owner_username:
            yield self._gen_entity_owner_aspect(dashboard_urn, owner_username)

    def _last_known_table_urns(self, app: App) -> List[str]:
        """The app's tables this run registered, such as those its lineage
        graph names, and those its last read recorded, including on-prem
        tables, which have no QRI and are named after the app."""
        urns = dict.fromkeys(self._app_table_urns(app).values())
        previous = self._usable_checkpoint(app.id, [])
        if previous is not None:
            urns.update(dict.fromkeys((previous.table_urns_by_qri or {}).values()))
            urns.update(
                dict.fromkeys(
                    urn for urn in previous.urns if self._is_app_table_urn(urn, app.id)
                )
            )
        return list(urns)

    def _is_app_table_urn(self, urn: str, app_id: str) -> bool:
        if guess_entity_type(urn) != "dataset":
            return False
        try:
            name = DatasetUrn.from_string(urn).name
        except Exception:
            return False
        instance = self.config.platform_instance
        if instance and name.startswith(f"{instance}."):
            name = name[len(instance) + 1 :]
        return name.startswith(f"{app_id.lower()}.")

    def _stale_sheet_edges(
        self, dashboard_urn: str, sheet_urns: List[str]
    ) -> List[str]:
        graph = self.ctx.graph
        if graph is None:
            return []
        try:
            current = graph.get_aspect(dashboard_urn, DashboardInfoClass)
        except Exception as e:
            self.reporter.warning(
                title="Unable to read Qlik app dashboard",
                message="Sheet edges of the app's dashboard that this run did not "
                "find are kept.",
                context=dashboard_urn,
                exc=e,
            )
            return []
        return [
            edge.destinationUrn
            for edge in (current.dashboards or [] if current else [])
            if edge.destinationUrn not in sheet_urns
        ]

    def _gen_sheets_workunit(
        self,
        app: App,
        chart_inputs: Optional[Dict[str, ChartInputs]],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik Sheet to Datahub dashboard
        """
        for sheet in app.sheets:
            dashboard_urn = self._gen_dashboard_urn(sheet.id)

            yield self._gen_entity_status_aspect(dashboard_urn)

            # A partial chart list would overwrite the one the last full read
            # emitted.
            if sheet.charts_complete:
                yield self._gen_dashboard_info_workunit(sheet, app.id)

            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=SubTypes(typeNames=[BIAssetSubTypes.QLIK_SHEET]),
            ).as_workunit()

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app.id),
                entity_type="dashboard",
                entity_urn=dashboard_urn,
            )

            dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
            if dpi_aspect:
                yield dpi_aspect

            owner_username = self._get_owner_name(sheet.ownerId)
            if self.config.ingest_owner and owner_username:
                yield self._gen_entity_owner_aspect(dashboard_urn, owner_username)

            yield from self._gen_charts_workunit(sheet.charts, chart_inputs, app.id)
