import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container, gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    DERIVED_TAG_URN,
    DIMENSION_TAG_URN,
    MEASURE_TAG_URN,
    MSTR_CUBE_SUBTYPES,
    MSTR_DOT_COLLAPSE_RE,
    MSTR_OBJECT_TIMESTAMP_FORMATS,
    MSTR_WHITESPACE_RE,
    TEMPORAL_TAG_URN,
    USAGE_TARGET_DASHBOARD,
)
from datahub.ingestion.source.microstrategy.lineage import (
    ColumnSetBinding,
    MicroStrategyLineageExtractor,
    ModelLineageIndex,
    bind_visualization_column_sets,
    metric_formula_references,
)
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    DatasourceReference,
    DerivedMetricSpec,
    FolderKey,
    FolderPart,
    GridUnit,
    MetricEnrichment,
    MicroStrategyObject,
    PredefinedFolderResolution,
    Project,
    ProjectKey,
    ReportDefinition,
    ReportDerivedMetric,
    Visualization,
    extract_folder_parts,
    normalize_object_id,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.usage import UsageBucket
from datahub.metadata.schema_classes import (
    AuditStampClass,
    CalendarIntervalClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    ChartUsageStatisticsClass,
    ChartUserUsageCountsClass,
    DashboardInfoClass,
    DashboardUsageStatisticsClass,
    DashboardUserUsageCountsClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    EdgeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    InputFieldClass,
    InputFieldsClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    TimeWindowSizeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from datahub.utilities.dedup_list import deduplicate_list
from datahub.utilities.urns.error import InvalidUrnError


@dataclass
class DatasetSchemaFields:
    """A dataset's schema fields plus an index from source object id to fields
    and the catalog name of each object (keyed by normalized object id)."""

    fields: List[SchemaFieldClass]
    by_object_id: Dict[str, List[SchemaFieldClass]]
    object_names: Dict[str, str]


class MicroStrategyMapper:
    def __init__(self, config: MicroStrategyConfig, report: MicroStrategyReport):
        self.config = config
        self.report = report
        self.lineage = MicroStrategyLineageExtractor(config, report)

    def project_key(self, project_id: str) -> ProjectKey:
        return ProjectKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_id=project_id,
        )

    def folder_key(self, project_id: str, folder_path: str) -> FolderKey:
        return FolderKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_id=project_id,
            folder_path=folder_path,
        )

    def chart_urn(
        self, project_id: str, dashboard_id: str, visualization_key: str
    ) -> str:
        return builder.make_chart_urn(
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            name=f"{project_id}.{dashboard_id}.{visualization_key}".lower(),
        )

    def report_urn(self, project_id: str, report_id: str) -> str:
        return builder.make_chart_urn(
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            name=f"{project_id}.{report_id}".lower(),
        )

    def dashboard_urn(self, project_id: str, dashboard_id: str) -> str:
        return builder.make_dashboard_urn(
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            name=f"{project_id}.{dashboard_id}".lower(),
        )

    def attach_model_lineage(
        self,
        dashboard: DashboardDefinition,
        model_lineage_index: ModelLineageIndex,
    ) -> None:
        for dataset in dashboard.datasets:
            self.attach_dataset_model_lineage(dataset, model_lineage_index)

    def attach_dataset_model_lineage(
        self,
        dataset: DatasetObject,
        model_lineage_index: ModelLineageIndex,
    ) -> None:
        # Union with any SQL-view-derived column lineage already attached: both
        # describe warehouse upstreams for the same dataset field, so a field can
        # legitimately carry edges from both the model and the report SQL.
        merged = {
            field_path: list(upstreams)
            for field_path, upstreams in dataset.field_warehouse_upstreams.items()
        }
        for field_path, upstreams in self._model_field_upstreams(
            dataset,
            model_lineage_index,
        ).items():
            merged[field_path] = sorted(
                set(merged.get(field_path, [])) | set(upstreams)
            )
        dataset.field_warehouse_upstreams = merged

    def attach_derived_metrics(self, dashboard: DashboardDefinition) -> None:
        """Attach visualization-local derived metrics (grid `derived: true`) to
        the dataset backing their column group, and record each member's
        column-group name on that dataset. Derived metrics whose group cannot
        be attributed to a dataset are counted, never silently dropped."""
        dataset_by_id = {dataset.id: dataset for dataset in dashboard.datasets}
        for visualization in dashboard.visualizations:
            if not visualization.column_sets:
                continue
            self._attach_visualization_derived_metrics(
                dashboard, visualization, dataset_by_id
            )

    def _attach_visualization_derived_metrics(
        self,
        dashboard: DashboardDefinition,
        visualization: Visualization,
        dataset_by_id: Dict[str, DatasetObject],
    ) -> None:
        binding = bind_visualization_column_sets(dashboard, visualization)
        bound_ids = set(binding.dataset_id_by_column_set.values())
        catalog_ids_by_dataset = {
            dataset.id: dataset.normalized_object_ids()
            for dataset in dashboard.datasets
        }
        # A group that failed to bind can still attach through the
        # visualization's own unambiguous dataset.
        fallback: Optional[DatasetObject] = None
        if len(bound_ids) == 1:
            fallback = dataset_by_id.get(next(iter(bound_ids)))
        elif len(visualization.datasets) == 1:
            fallback = dataset_by_id.get(visualization.datasets[0])
        for column_set in visualization.column_sets:
            dataset_id = binding.dataset_id_by_column_set.get(column_set.identifier)
            target = dataset_by_id.get(dataset_id) if dataset_id else None
            for metric in column_set.metrics:
                if not metric.id:
                    # An id-less derived element can never attach anywhere.
                    if metric.derived:
                        self.report.report_derived_metric_unattached()
                    continue
                normalized_id = normalize_object_id(metric.id)
                if target is not None and column_set.name:
                    target.column_groups_by_object_id.setdefault(
                        normalized_id, column_set.name
                    )
                if not metric.derived:
                    continue
                attach_to = target if target is not None else fallback
                if attach_to is None:
                    self.report.report_derived_metric_unattached()
                    continue
                # Never shadow a real catalog object with a derived spec.
                if normalized_id in catalog_ids_by_dataset.get(attach_to.id, set()):
                    continue
                attach_to.derived_metrics.setdefault(
                    normalized_id,
                    DerivedMetricSpec(
                        id=metric.id,
                        name=metric.display_name,
                        data_type=metric.data_type,
                        column_set_name=column_set.name,
                        source_visualization_key=visualization.key,
                        source_visualization_name=visualization.name,
                    ),
                )

    def attach_report_derived_metrics(
        self,
        dataset: DatasetObject,
        definitions: Sequence[ReportDerivedMetric],
    ) -> None:
        """Merge a report's derived metric definitions into the dataset's
        derived specs: a grid-derived spec with the same object id (or, failing
        that, the same normalized name) is upgraded in place with the report's
        object name and formula rather than duplicated; definitions no grid
        showed are added, so the dataset lists every derived metric the report
        defines. Catalog objects are never shadowed."""
        catalog_ids = dataset.normalized_object_ids()
        key_by_name = {
            _normalized_name(spec.name): key
            for key, spec in dataset.derived_metrics.items()
        }
        for definition in definitions:
            object_id = normalize_object_id(definition.id)
            if object_id in catalog_ids:
                continue
            key = object_id
            if key not in dataset.derived_metrics:
                key = key_by_name.get(_normalized_name(definition.name), object_id)
            spec = dataset.derived_metrics.get(key)
            if spec is None:
                dataset.derived_metrics[object_id] = DerivedMetricSpec(
                    id=definition.id,
                    name=definition.name,
                    data_type=definition.data_type,
                    expression_text=definition.expression_text,
                    expression_tokens=definition.expression_tokens,
                    definition_source=definition.source,
                )
                continue
            spec.name = definition.name
            spec.data_type = spec.data_type or definition.data_type
            if definition.expression_text or definition.expression_tokens:
                spec.expression_text = definition.expression_text
                spec.expression_tokens = definition.expression_tokens
            spec.definition_source = definition.source

    def dataset_field_paths(self, dataset: DatasetObject) -> List[str]:
        return [spec.field_path for spec in _iter_dataset_fields(dataset)]

    def gen_project_container(
        self,
        project: Project,
        source_warehouses: Sequence[Datasource] = (),
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_project_scanned()
        yield from gen_containers(
            container_key=self.project_key(project.id),
            name=project.name,
            description=project.description,
            sub_types=[BIContainerSubTypes.MICROSTRATEGY_PROJECT],
            extra_properties=self._source_warehouse_summary(source_warehouses),
            external_url=f"{self.config.base_url}/app/{project.id}",
        )

    def gen_folder_containers(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        predefined_folders: Optional[PredefinedFolderResolution] = None,
    ) -> Iterable[MetadataWorkUnit]:
        parts = extract_folder_parts(dashboard_object.model_dump())
        parent_key: Optional[ProjectKey] = self.project_key(project_id)
        current_path = ""
        for part in parts:
            if _is_hidden_folder(part, predefined_folders):
                continue
            name = _resolve_folder_name(part, predefined_folders)
            if not self.config.folder_pattern.allowed(name):
                continue
            current_path = f"{current_path}/{name}" if current_path else name
            folder_key = self.folder_key(project_id, current_path)
            self.report.report_folder_scanned()
            yield from gen_containers(
                container_key=folder_key,
                parent_container_key=parent_key,
                name=name,
                sub_types=[BIContainerSubTypes.MICROSTRATEGY_FOLDER],
            )
            parent_key = folder_key

    def folder_container_for_dashboard(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        predefined_folders: Optional[PredefinedFolderResolution] = None,
    ) -> ProjectKey:
        parts = extract_folder_parts(dashboard_object.model_dump())
        allowed_names = [
            name
            for name in (
                _resolve_folder_name(part, predefined_folders)
                for part in parts
                if not _is_hidden_folder(part, predefined_folders)
            )
            if self.config.folder_pattern.allowed(name)
        ]
        if not allowed_names:
            return self.project_key(project_id)
        return self.folder_key(project_id, "/".join(allowed_names))

    def dataset_folder_parent_key(
        self,
        project_id: str,
        dataset_object: Optional[MicroStrategyObject],
        fallback_key: ProjectKey,
        predefined_folders: Optional[PredefinedFolderResolution] = None,
    ) -> ProjectKey:
        """The folder container a dossier/report source dataset belongs in: the
        dataset object's OWN folder ancestry (a report used as a dossier dataset
        commonly lives in a different, often deeper, folder than the dossier).
        Falls back to the parent dossier/report's folder when the dataset's
        object info or ancestors could not be resolved, which is what the
        connector emitted before it looked datasets up at all."""
        if dataset_object is None:
            return fallback_key
        if not extract_folder_parts(dataset_object.model_dump()):
            return fallback_key
        return self.folder_container_for_dashboard(
            project_id, dataset_object, predefined_folders
        )

    def dataset_external_url(
        self,
        project_id: str,
        parent_id: str,
        dataset: DatasetObject,
        dataset_object: Optional[MicroStrategyObject],
    ) -> str:
        """Library URL for the dataset object itself when Library can open it.
        Library renders dossiers, documents and reports by id, but has no viewer
        for intelligent/super cubes: a cube URL would land on an error page, so
        cubes (and datasets whose object info was unavailable) keep linking to
        the parent dossier/report that embeds them."""
        parent_url = f"{self.config.base_url}/app/{project_id}/{parent_id}"
        if dataset_object is None:
            return parent_url
        if (dataset_object.subtype or "").strip() in MSTR_CUBE_SUBTYPES:
            return parent_url
        return f"{self.config.base_url}/app/{project_id}/{dataset.id}"

    def gen_dataset_workunits(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        dataset: DatasetObject,
        parent_key: ProjectKey,
        dataset_object: Optional[MicroStrategyObject] = None,
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties = self._dataset_custom_properties(
            project_id=project_id,
            dashboard=dashboard,
            dataset=dataset,
        )
        if dataset_object is not None:
            custom_properties.update(self._dashboard_object_properties(dataset_object))
        yield from self._gen_dataset_entity_workunits(
            project_id=project_id,
            parent_id=dashboard.id,
            dataset=dataset,
            parent_key=parent_key,
            custom_properties=custom_properties,
            include_coarse_lineage=self.config.extract_warehouse_lineage,
            external_url=self.dataset_external_url(
                project_id, dashboard.id, dataset, dataset_object
            ),
        )

    def gen_report_source_dataset_workunits(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        dataset: DatasetObject,
        parent_key: ProjectKey,
        dataset_object: Optional[MicroStrategyObject] = None,
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties = self._report_source_dataset_custom_properties(
            project_id=project_id,
            report_object=report_object,
            dataset=dataset,
        )
        if dataset_object is not None:
            custom_properties.update(self._dashboard_object_properties(dataset_object))
        yield from self._gen_dataset_entity_workunits(
            project_id=project_id,
            parent_id=report_object.id,
            dataset=dataset,
            parent_key=parent_key,
            custom_properties=custom_properties,
            include_coarse_lineage=self.config.extract_report_sql_lineage,
            external_url=self.dataset_external_url(
                project_id, report_object.id, dataset, dataset_object
            ),
        )

    def _gen_dataset_entity_workunits(
        self,
        project_id: str,
        parent_id: str,
        dataset: DatasetObject,
        parent_key: ProjectKey,
        custom_properties: Dict[str, str],
        include_coarse_lineage: bool,
        external_url: str,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_dataset_scanned()
        dataset_urn = self.lineage.dataset_urn(project_id, parent_id, dataset)

        yield self._platform_instance_workunit(dataset_urn)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=dataset.name,
                description=dataset.description,
                qualifiedName=f"{project_id}.{parent_id}.{dataset.id}".lower(),
                externalUrl=external_url,
                customProperties=custom_properties,
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.MICROSTRATEGY_DATASET]),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SchemaMetadataClass(
                schemaName=dataset.name,
                platform=builder.make_data_platform_urn(self.config.platform),
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=self._schema_fields(dataset),
            ),
        ).as_workunit()
        fine_grained_lineages = self._fine_grained_lineages(dataset_urn, dataset)
        if fine_grained_lineages:
            self.report.report_model_lineage_edges(len(fine_grained_lineages))
        formula_lineages = self._metric_formula_lineages(dataset_urn, dataset)
        all_fine_grained_lineages = fine_grained_lineages + formula_lineages
        fine_grained_table_urns = _upstream_dataset_urns(
            dataset.field_warehouse_upstreams
        )
        coarse_upstream_urns = (
            sorted(set(dataset.warehouse_upstream_urns))
            if include_coarse_lineage
            else []
        )
        if fine_grained_table_urns:
            # Field-level lineage identifies the tables that actually feed
            # this dataset's fields. SQL-view tables that were only joined
            # for filtering (dimension lookups, calendar subqueries) are not
            # emitted as upstreams — they would fan every dataset out to the
            # whole schema.
            upstream_urns = sorted(fine_grained_table_urns)
            pruned = set(coarse_upstream_urns) - fine_grained_table_urns
            if pruned:
                self.report.report_warehouse_upstreams_pruned(len(pruned))
        else:
            upstream_urns = coarse_upstream_urns
        if upstream_urns:
            self.report.report_warehouse_lineage_edges(len(upstream_urns))
        # Metric-formula edges are same-dataset field-to-field lineage, so the
        # aspect is emitted even when there are no table-level upstreams.
        if upstream_urns or all_fine_grained_lineages:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=upstream_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                        for upstream_urn in upstream_urns
                    ],
                    fineGrainedLineages=all_fine_grained_lineages or None,
                ),
            ).as_workunit()
        yield from add_entity_to_container(
            container_key=parent_key,
            entity_type="dataset",
            entity_urn=dataset_urn,
        )

    def _platform_instance_workunit(self, entity_urn: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.config.platform),
                instance=(
                    builder.make_dataplatform_instance_urn(
                        self.config.platform,
                        self.config.platform_instance,
                    )
                    if self.config.platform_instance
                    else None
                ),
            ),
        ).as_workunit()

    def gen_report_workunits(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        report_definition: Optional[ReportDefinition],
        source_dataset: Optional[DatasetObject],
        parent_key: ProjectKey,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_report_scanned()
        self.report.report_chart_scanned()
        report_urn = self.report_urn(project_id, report_object.id)
        input_urn: Optional[str] = None
        if self.config.extract_lineage and source_dataset is not None:
            input_urn = self.lineage.dataset_urn(
                project_id,
                report_object.id,
                source_dataset,
            )
        inputs: List[str]
        inputs = [input_urn] if input_urn else []
        if inputs:
            self.report.report_chart_lineage_edges(len(inputs))

        yield self._platform_instance_workunit(report_urn)
        yield MetadataChangeProposalWrapper(
            entityUrn=report_urn,
            aspect=ChartInfoClass(
                title=report_object.name,
                description=(
                    (report_definition.description if report_definition else None)
                    or report_object.description
                    or ""
                ),
                lastModified=self._dashboard_audit_stamps(report_object),
                chartUrl=f"{self.config.base_url}/app/{project_id}/{report_object.id}",
                customProperties=self._report_properties(
                    project_id=project_id,
                    report_object=report_object,
                    report_definition=report_definition,
                    source_dataset=source_dataset,
                ),
                inputs=inputs,
                inputEdges=[EdgeClass(destinationUrn=input_urn) for input_urn in inputs]
                or None,
            ),
        ).as_workunit()
        input_fields: Optional[InputFieldsClass] = None
        if input_urn and source_dataset:
            input_fields = self._dataset_input_fields(
                input_urn,
                source_dataset,
                report_definition.object_ids if report_definition else None,
            )
        # Always emit (empty when unresolved) so a previous run's inputFields
        # cannot linger — aspects are replaced wholesale, never auto-deleted.
        yield MetadataChangeProposalWrapper(
            entityUrn=report_urn,
            aspect=input_fields or InputFieldsClass(fields=[]),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=report_urn,
            aspect=SubTypesClass(typeNames=[BIAssetSubTypes.REPORT]),
        ).as_workunit()
        if self.config.ingest_owner and report_object.owner:
            yield self._ownership_workunit(report_urn, report_object.owner)
        yield from add_entity_to_container(
            container_key=parent_key,
            entity_type="chart",
            entity_urn=report_urn,
        )

    def gen_chart_workunits(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        visualization: Visualization,
        parent_key: ProjectKey,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_chart_scanned()
        chart_urn = self.chart_urn(project_id, dashboard.id, visualization.key)
        inputs = (
            self.lineage.visualization_inputs(project_id, dashboard, visualization)
            if self.config.extract_lineage
            else []
        )
        if inputs:
            self.report.report_chart_lineage_edges(len(inputs))
        elif visualization.datasets:
            self.report.report_unresolved_visualization()
        column_group_properties = (
            self._column_groups_property(
                dashboard,
                visualization,
                bind_visualization_column_sets(dashboard, visualization),
            )
            if visualization.column_sets
            else {}
        )

        yield self._platform_instance_workunit(chart_urn)
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ChartInfoClass(
                title=visualization.name,
                description=visualization.type or "",
                lastModified=ChangeAuditStampsClass(),
                chartUrl=f"{self.config.base_url}/app/{project_id}/{dashboard.id}",
                customProperties={
                    **self._visualization_properties(visualization),
                    **column_group_properties,
                },
                inputs=inputs,
                inputEdges=[EdgeClass(destinationUrn=input_urn) for input_urn in inputs]
                or None,
            ),
        ).as_workunit()
        input_fields = self._visualization_input_fields(
            project_id,
            dashboard,
            visualization,
            inputs,
        )
        # Always emit (empty when unresolved) so a previous run's inputFields
        # cannot linger — aspects are replaced wholesale, never auto-deleted.
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=input_fields or InputFieldsClass(fields=[]),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=SubTypesClass(
                typeNames=[BIAssetSubTypes.MICROSTRATEGY_VISUALIZATION]
            ),
        ).as_workunit()
        yield from add_entity_to_container(
            container_key=parent_key,
            entity_type="chart",
            entity_urn=chart_urn,
        )

    def gen_dashboard_workunits(
        self,
        project_id: str,
        dashboard_object: MicroStrategyObject,
        dashboard: DashboardDefinition,
        parent_key: ProjectKey,
        extra_chart_urns: Sequence[str] = (),
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_dashboard_scanned()
        dashboard_urn = self.dashboard_urn(project_id, dashboard.id)
        chart_urns = deduplicate_list(
            [
                self.chart_urn(project_id, dashboard.id, visualization.key)
                for visualization in dashboard.visualizations
            ]
            + list(extra_chart_urns)
        )
        custom_properties = {
            "microstrategyProjectId": project_id,
            "microstrategyDashboardId": dashboard.id,
            "datasetCount": str(len(dashboard.datasets)),
            "visualizationCount": str(len(dashboard.visualizations)),
        }
        custom_properties.update(self._dashboard_object_properties(dashboard_object))
        custom_properties.update(self._dashboard_dependency_properties(dashboard))
        unresolved = self.lineage.unresolved_visualization_datasets(dashboard)
        if unresolved:
            custom_properties["unresolvedVisualizationDatasetIds"] = json.dumps(
                unresolved, sort_keys=True
            )

        dashboard_dataset_edges = (
            self.lineage.dashboard_dataset_urns(project_id, dashboard)
            if self.config.emit_dashboard_dataset_edges
            else None
        )
        if dashboard_dataset_edges:
            self.report.report_dashboard_dataset_edges(len(dashboard_dataset_edges))

        yield self._platform_instance_workunit(dashboard_urn)
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=DashboardInfoClass(
                title=dashboard.name,
                description=dashboard.description or dashboard_object.description or "",
                chartEdges=[
                    EdgeClass(destinationUrn=chart_urn) for chart_urn in chart_urns
                ],
                datasetEdges=[
                    EdgeClass(destinationUrn=dataset_urn)
                    for dataset_urn in dashboard_dataset_edges
                ]
                if dashboard_dataset_edges
                else None,
                lastModified=self._dashboard_audit_stamps(dashboard_object),
                dashboardUrl=f"{self.config.base_url}/app/{project_id}/{dashboard.id}",
                customProperties=custom_properties,
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=SubTypesClass(typeNames=[BIAssetSubTypes.MICROSTRATEGY_DOSSIER]),
        ).as_workunit()
        if self.config.ingest_owner and dashboard_object.owner:
            yield self._ownership_workunit(dashboard_urn, dashboard_object.owner)
        yield from add_entity_to_container(
            container_key=parent_key,
            entity_type="dashboard",
            entity_urn=dashboard_urn,
        )

    def _ownership_workunit(self, entity_urn: str, owner: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=builder.make_user_urn(owner),
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                    )
                ]
            ),
        ).as_workunit()

    def _schema_fields(self, dataset: DatasetObject) -> List[SchemaFieldClass]:
        return self._schema_fields_and_object_map(dataset, report_fields=True).fields

    def _schema_fields_and_object_map(
        self,
        dataset: DatasetObject,
        report_fields: bool = False,
    ) -> DatasetSchemaFields:
        fields: List[SchemaFieldClass] = []
        fields_by_object_id: Dict[str, List[SchemaFieldClass]] = {}
        object_names: Dict[str, str] = {}

        for spec in _iter_dataset_fields(dataset):
            _record_object_name(object_names, spec.item)
            if spec.kind == "metric":
                metric = spec.item
                enrichment = _metric_enrichment_for(dataset, metric)
                schema_field = self._make_schema_field(
                    field_path=spec.field_path,
                    native_type=_field_native_type(metric) or "Metric",
                    description=_metric_field_description(
                        _optional_str(metric.get("description")),
                        enrichment,
                    ),
                    tag_urns=[MEASURE_TAG_URN]
                    if self.config.tag_measures_and_dimensions
                    else [],
                    json_props={
                        "microstrategyObjectId": str(metric.get("id", "")),
                        "microstrategyObjectType": "metric",
                        **_metric_expression_json_props(enrichment),
                        **_column_group_json_props(dataset, metric),
                    },
                    glossary_term_urn=self._term_for(
                        metric, self.config.metric_glossary_term_mapping
                    ),
                    numeric=True,
                )
                fields.append(schema_field)
                _add_schema_field_object_mapping(
                    fields_by_object_id, metric, schema_field
                )
                if report_fields:
                    self.report.report_metric_field()
            elif spec.kind == "derived_metric":
                derived = spec.derived
                if derived is None:
                    continue
                derived_tag_urns: List[str] = []
                if self.config.tag_measures_and_dimensions:
                    derived_tag_urns.append(MEASURE_TAG_URN)
                derived_tag_urns.append(DERIVED_TAG_URN)
                schema_field = self._make_schema_field(
                    field_path=spec.field_path,
                    native_type=derived.data_type or "Derived Metric",
                    description=_derived_metric_description(derived, dataset),
                    tag_urns=derived_tag_urns,
                    json_props={
                        key: value
                        for key, value in {
                            "microstrategyObjectId": derived.id,
                            "microstrategyObjectType": "derivedMetric",
                            "microstrategyDerivedMetricSource": (
                                derived.definition_source or "visualization"
                            ),
                            "microstrategyMetricExpressionText": (
                                derived.expression_text
                            ),
                            "microstrategyMetricExpressionTokens": (
                                derived.expression_tokens
                            ),
                            "microstrategyColumnGroup": derived.column_set_name,
                            "microstrategySourceVisualization": (
                                derived.source_visualization_name
                                or derived.source_visualization_key
                            ),
                        }.items()
                        if value
                    },
                    glossary_term_urn=self._term_for(
                        spec.item, self.config.metric_glossary_term_mapping
                    ),
                    numeric=True,
                )
                fields.append(schema_field)
                _add_schema_field_object_mapping(
                    fields_by_object_id, spec.item, schema_field
                )
                if report_fields:
                    self.report.report_derived_metric_field()
            else:
                attribute = spec.item
                form = spec.form or attribute
                tag_urns: List[str] = []
                if self.config.tag_measures_and_dimensions:
                    tag_urns.append(DIMENSION_TAG_URN)
                    if spec.temporal:
                        tag_urns.append(TEMPORAL_TAG_URN)
                schema_field = self._make_schema_field(
                    field_path=spec.field_path,
                    native_type=_field_native_type(form)
                    or _field_native_type(attribute)
                    or "Attribute",
                    description=_optional_str(form.get("description"))
                    or _optional_str(attribute.get("description")),
                    tag_urns=tag_urns,
                    json_props={
                        "microstrategyObjectId": str(attribute.get("id", "")),
                        "microstrategyObjectType": "attribute",
                        "microstrategyFormId": str(form.get("id", "")),
                        "baseFormCategory": str(form.get("baseFormCategory", "")),
                        "baseFormType": str(form.get("baseFormType", "")),
                    },
                    glossary_term_urn=self._term_for(
                        form,
                        self.config.attribute_glossary_term_mapping,
                        fallback=attribute,
                    ),
                )
                fields.append(schema_field)
                _add_schema_field_object_mapping(
                    fields_by_object_id, attribute, schema_field
                )
                _add_schema_field_object_mapping(
                    fields_by_object_id, form, schema_field
                )
                if report_fields:
                    self.report.report_attribute_field(temporal=spec.temporal)

        return DatasetSchemaFields(
            fields=sorted(fields, key=lambda field: field.fieldPath),
            by_object_id=fields_by_object_id,
            object_names=object_names,
        )

    def _visualization_input_fields(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        visualization: Visualization,
        input_urns: Sequence[str],
    ) -> Optional[InputFieldsClass]:
        """Chart input fields named and ordered the way the grid shows them.

        With a runtime grid definition, each header cell becomes one input
        field in render order: row/column attributes first (unqualified, with
        only the forms the grid displays, so a single displayed form is just
        the attribute name), then each column group's metrics as
        `GROUP.header` where the header is the grid's own text (a dossier
        alias when it differs from the catalog metric name, which is kept in
        the description and jsonProps). Each header cell is attributed to one
        dataset: the group's bound dataset, or for ungrouped cells the first
        input dataset (in group order) that carries the object.

        Objects the visualization references outside any grid cell -- and
        every object when no runtime grid was fetched -- keep the previous
        treatment: emitted from every input dataset, prefixed with the group
        (else dataset) name whenever more than one dataset feeds the chart.

        Only the embedded display copy is renamed; the schemaField urn always
        keeps the dataset's real field path so column lineage is unaffected."""
        if not visualization.object_ids or not input_urns:
            return None

        input_urn_set = set(input_urns)
        binding = (
            bind_visualization_column_sets(dashboard, visualization)
            if visualization.column_sets
            else ColumnSetBinding({}, [])
        )
        input_datasets = [
            dataset
            for dataset in dashboard.datasets
            if self.lineage.dataset_urn(project_id, dashboard.id, dataset)
            in input_urn_set
        ]
        ordered_datasets = _datasets_in_grid_order(
            input_datasets, visualization, binding
        )
        builder_state = _InputFieldBuilder(
            annotate_source=len(input_urn_set) > 1,
            urn_by_dataset_id={
                dataset.id: self.lineage.dataset_urn(project_id, dashboard.id, dataset)
                for dataset in ordered_datasets
            },
            schema_by_dataset_id={
                dataset.id: self._schema_fields_and_object_map(dataset)
                for dataset in ordered_datasets
            },
            group_by_dataset_id=self._column_group_by_dataset_id(
                dashboard, visualization
            ),
        )

        placed_cells: Set[Tuple[str, Optional[str]]] = set()
        for unit in visualization.grid_units:
            if not unit.id:
                continue
            cell = (normalize_object_id(unit.id), unit.column_set_key)
            if cell in placed_cells:
                continue
            placed_cells.add(cell)
            bound_id = (
                binding.dataset_id_by_column_set.get(unit.column_set_key)
                if unit.column_set_key
                else None
            )
            candidates = [
                dataset for dataset in ordered_datasets if dataset.id == bound_id
            ] or ordered_datasets
            builder_state.place_grid_cell(unit, candidates)

        builder_state.place_leftovers(
            ordered_datasets,
            {normalize_object_id(object_id) for object_id in visualization.object_ids},
        )
        if not builder_state.entries:
            return None
        return InputFieldsClass(fields=builder_state.entries)

    @staticmethod
    def _column_group_by_dataset_id(
        dashboard: DashboardDefinition,
        visualization: Visualization,
    ) -> Dict[str, str]:
        """Bound dataset id -> column-group display name for this visualization."""
        if not visualization.column_sets:
            return {}
        binding = bind_visualization_column_sets(dashboard, visualization)
        name_by_identifier = {
            column_set.identifier: column_set.name or column_set.identifier
            for column_set in visualization.column_sets
        }
        return {
            dataset_id: name_by_identifier.get(identifier, identifier)
            for identifier, dataset_id in binding.dataset_id_by_column_set.items()
        }

    def _dataset_input_fields(
        self,
        dataset_urn: str,
        dataset: DatasetObject,
        object_ids: Optional[Sequence[str]] = None,
    ) -> Optional[InputFieldsClass]:
        schema_fields = self._schema_fields_and_object_map(dataset)
        input_fields_by_urn: Dict[str, InputFieldClass] = {}
        if object_ids:
            normalized_object_ids = {
                normalize_object_id(object_id) for object_id in object_ids
            }
            for object_id in normalized_object_ids:
                for schema_field in schema_fields.by_object_id.get(object_id, []):
                    _add_input_field(input_fields_by_urn, dataset_urn, schema_field)
        else:
            for schema_field in schema_fields.fields:
                _add_input_field(input_fields_by_urn, dataset_urn, schema_field)

        return _input_fields_aspect(input_fields_by_urn)

    def _model_field_upstreams(
        self,
        dataset: DatasetObject,
        model_lineage_index: ModelLineageIndex,
    ) -> Dict[str, List[str]]:
        allowed_upstream_urns = set(dataset.warehouse_upstream_urns)
        if not allowed_upstream_urns:
            return {}

        field_upstreams: Dict[str, List[str]] = {}
        for spec in _iter_dataset_fields(dataset):
            if spec.kind == "derived_metric":
                # Visualization-local: no catalog object, so no model lineage.
                continue
            if spec.kind == "metric":
                enrichment = _metric_enrichment_for(dataset, spec.item)
                upstreams = _filter_schema_field_upstreams(
                    model_lineage_index.fact_field_urns(
                        enrichment.fact_ids if enrichment else []
                    ),
                    allowed_upstream_urns,
                )
            else:
                attribute = spec.item
                form = spec.form or attribute
                attribute_id = str(
                    attribute.get("id") or attribute.get("objectId") or ""
                )
                upstreams = _filter_schema_field_upstreams(
                    model_lineage_index.attribute_field_urns(
                        attribute_id,
                        _field_name(form),
                    ),
                    allowed_upstream_urns,
                )
            if upstreams:
                field_upstreams[spec.field_path] = upstreams

        return field_upstreams

    def _metric_formula_lineages(
        self,
        dataset_urn: str,
        dataset: DatasetObject,
    ) -> List[FineGrainedLineageClass]:
        """Field-to-field edges from a catalog metric (or a report-level
        derived metric whose definition was fetched) to the sibling fields its
        formula references as `{Name}` or `[Name]` tokens. Same-dataset edges
        only; references that don't resolve to a field of this dataset are
        counted and skipped rather than guessed."""
        if not self.config.extract_metric_formula_lineage:
            return []
        has_derived_formula = any(
            spec.expression_text for spec in dataset.derived_metrics.values()
        )
        if not dataset.metric_enrichments and not has_derived_formula:
            return []

        specs = list(_iter_dataset_fields(dataset))
        # Case-insensitive name -> field path; first spec wins on collisions
        # (same-named forms), which is fine: either path anchors the sibling.
        path_by_name: Dict[str, str] = {}
        for spec in specs:
            path_by_name.setdefault(spec.field_path.lower(), spec.field_path)
            name = _field_name(spec.item)
            if name:
                path_by_name.setdefault(name.lower(), spec.field_path)

        lineages: List[FineGrainedLineageClass] = []
        unresolved = 0
        for spec in specs:
            expression_text: Optional[str] = None
            if spec.kind == "metric":
                enrichment = _metric_enrichment_for(dataset, spec.item)
                if enrichment is not None:
                    expression_text = enrichment.expression_text
            elif spec.kind == "derived_metric" and spec.derived is not None:
                expression_text = spec.derived.expression_text
            if not expression_text:
                continue
            upstream_paths: Set[str] = set()
            for reference in metric_formula_references(expression_text):
                resolved = path_by_name.get(reference.lower())
                if resolved is None:
                    unresolved += 1
                    self.report.report_metric_formula_unresolved_ref(
                        f"{dataset.name}.{spec.field_path} -> {{{reference}}}"
                    )
                elif resolved != spec.field_path:
                    upstream_paths.add(resolved)
            if upstream_paths:
                lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[
                            builder.make_schema_field_urn(dataset_urn, path)
                            for path in sorted(upstream_paths)
                        ],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            builder.make_schema_field_urn(dataset_urn, spec.field_path)
                        ],
                    )
                )
        if unresolved:
            self.report.report_metric_formula_refs_unresolved(unresolved)
        if lineages:
            self.report.report_metric_formula_lineage_edges(len(lineages))
        return lineages

    def _fine_grained_lineages(
        self,
        dataset_urn: str,
        dataset: DatasetObject,
    ) -> List[FineGrainedLineageClass]:
        lineages: List[FineGrainedLineageClass] = []
        for field_path, upstreams in sorted(dataset.field_warehouse_upstreams.items()):
            upstream_field_urns = sorted(set(upstreams))
            if not upstream_field_urns:
                continue
            lineages.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstream_field_urns,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        builder.make_schema_field_urn(dataset_urn, field_path)
                    ],
                )
            )
        return lineages

    def gen_usage_workunits(
        self,
        entity_kind: str,
        entity_urn: str,
        bucket: UsageBucket,
    ) -> Iterable[MetadataWorkUnit]:
        """Daily usage bucket -> Dashboard/Chart usage statistics aspect."""
        granularity = TimeWindowSizeClass(unit=CalendarIntervalClass.DAY, multiple=1)
        sorted_user_counts = sorted(bucket.user_counts.items())
        aspect: Union[DashboardUsageStatisticsClass, ChartUsageStatisticsClass]
        if entity_kind == USAGE_TARGET_DASHBOARD:
            aspect = DashboardUsageStatisticsClass(
                timestampMillis=bucket.bucket_start_ms,
                eventGranularity=granularity,
                viewsCount=bucket.view_count,
                uniqueUserCount=len(bucket.user_counts) or None,
                userCounts=[
                    DashboardUserUsageCountsClass(
                        user=builder.make_user_urn(user),
                        viewsCount=count,
                    )
                    for user, count in sorted_user_counts
                ]
                or None,
            )
        else:
            aspect = ChartUsageStatisticsClass(
                timestampMillis=bucket.bucket_start_ms,
                eventGranularity=granularity,
                viewsCount=bucket.view_count,
                uniqueUserCount=len(bucket.user_counts) or None,
                userCounts=[
                    ChartUserUsageCountsClass(
                        user=builder.make_user_urn(user),
                        viewsCount=count,
                    )
                    for user, count in sorted_user_counts
                ]
                or None,
            )
        self.report.report_usage_bucket_emitted()
        yield MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=aspect,
        ).as_workunit()

    def _make_schema_field(
        self,
        field_path: str,
        native_type: str,
        description: Optional[str],
        tag_urns: Sequence[str],
        json_props: Dict[str, str],
        glossary_term_urn: Optional[str],
        numeric: bool = False,
    ) -> SchemaFieldClass:
        global_tags = (
            GlobalTagsClass(
                tags=[TagAssociationClass(tag=tag_urn) for tag_urn in tag_urns]
            )
            if tag_urns
            else None
        )
        glossary_terms = None
        if glossary_term_urn:
            glossary_terms = GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn=glossary_term_urn)],
                auditStamp=AuditStampClass(
                    time=0,
                    actor=builder.make_user_urn("datahub"),
                ),
            )

        return SchemaFieldClass(
            fieldPath=field_path,
            type=SchemaFieldDataTypeClass(
                type=NumberTypeClass() if numeric else _schema_type(native_type)
            ),
            nativeDataType=native_type,
            description=description,
            globalTags=global_tags,
            glossaryTerms=glossary_terms,
            jsonProps=json.dumps(json_props, sort_keys=True),
        )

    def _term_for(
        self,
        item: Dict[str, object],
        mapping: Dict[str, str],
        fallback: Optional[Dict[str, object]] = None,
    ) -> Optional[str]:
        keys = [
            str(item.get("id", "")),
            str(item.get("name", "")),
        ]
        if fallback:
            keys.extend([str(fallback.get("id", "")), str(fallback.get("name", ""))])
        for key in keys:
            if key and key in mapping:
                return mapping[key]
        return None

    @staticmethod
    def _column_groups_property(
        dashboard: DashboardDefinition,
        visualization: Visualization,
        binding: ColumnSetBinding,
    ) -> Dict[str, str]:
        """The visualization's column groups as a chart property: group name ->
        member metric names plus the dossier dataset backing the group when
        resolved. Compound grids repeat the same metric names per group with
        different logic behind each, so the grouping is what tells a reader
        what they are actually looking at."""
        dataset_name_by_id = {
            dataset.id: dataset.name for dataset in dashboard.datasets
        }
        groups: Dict[str, Dict[str, object]] = {}
        for column_set in visualization.column_sets:
            if not column_set.name:
                continue
            entry: Dict[str, object] = {}
            dataset_id = binding.dataset_id_by_column_set.get(column_set.identifier)
            if dataset_id:
                entry["dataset"] = dataset_name_by_id.get(dataset_id, dataset_id)
            metric_names = [metric.name for metric in column_set.metrics if metric.name]
            if metric_names:
                entry["metrics"] = metric_names
            groups[column_set.name] = entry
        if not groups:
            return {}
        return {"microstrategyColumnGroups": json.dumps(groups, sort_keys=True)}

    @staticmethod
    def _visualization_properties(visualization: Visualization) -> Dict[str, str]:
        return {
            key: value
            for key, value in {
                "microstrategyVisualizationKey": visualization.key,
                "microstrategyVisualizationType": visualization.type,
                "microstrategyDatasetIds": ",".join(visualization.datasets),
                "microstrategyInputDatasetCount": str(len(visualization.datasets)),
                "microstrategyObjectIdCount": str(len(visualization.object_ids)),
            }.items()
            if value
        }

    def _dataset_custom_properties(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        dataset: DatasetObject,
    ) -> Dict[str, str]:
        properties = {
            "microstrategyProjectId": project_id,
            "microstrategyDashboardId": dashboard.id,
            "microstrategyDatasetId": dataset.id,
        }
        properties.update(_dataset_semantic_count_properties(dataset))
        # Deliberately the FULL SQL-derived table set (audit trail), even when
        # the emitted upstreamLineage aspect is restricted to field-evidenced
        # tables.
        if self.config.extract_warehouse_lineage and dataset.warehouse_upstream_urns:
            properties.update(
                _warehouse_upstream_properties(dataset.warehouse_upstream_urns)
            )
        if dataset.field_warehouse_upstreams:
            upstream_field_urns = {
                field_urn
                for upstreams in dataset.field_warehouse_upstreams.values()
                for field_urn in upstreams
            }
            upstream_dataset_urns = _upstream_dataset_urns(
                dataset.field_warehouse_upstreams
            )
            properties["microstrategyModelLineageFieldCount"] = str(
                len(dataset.field_warehouse_upstreams)
            )
            properties["microstrategyModelLineageUpstreamFieldCount"] = str(
                len(upstream_field_urns)
            )
            properties["microstrategyModelLineageUpstreamDatasetCount"] = str(
                len(upstream_dataset_urns)
            )
        properties.update(self._source_warehouse_properties(dataset.source_warehouse))
        return properties

    def _report_source_dataset_custom_properties(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        dataset: DatasetObject,
    ) -> Dict[str, str]:
        properties = {
            "microstrategyProjectId": project_id,
            "microstrategyReportId": report_object.id,
            "microstrategyReportSourceId": dataset.id,
        }
        properties.update(_dataset_semantic_count_properties(dataset))
        if self.config.extract_report_sql_lineage and dataset.warehouse_upstream_urns:
            properties.update(
                _warehouse_upstream_properties(dataset.warehouse_upstream_urns)
            )
        properties.update(self._source_warehouse_properties(dataset.source_warehouse))
        return properties

    def _report_properties(
        self,
        project_id: str,
        report_object: MicroStrategyObject,
        report_definition: Optional[ReportDefinition],
        source_dataset: Optional[DatasetObject],
    ) -> Dict[str, str]:
        properties: Dict[str, str] = {
            "microstrategyProjectId": project_id,
            "microstrategyReportId": report_object.id,
        }
        properties.update(self._dashboard_object_properties(report_object))
        if report_definition:
            properties.update(
                {
                    "microstrategyReportPromptCount": str(
                        report_definition.prompt_count
                    ),
                    "microstrategyReportHasFilter": str(
                        report_definition.has_filter
                    ).lower(),
                }
            )
        if source_dataset:
            properties.update(
                {
                    "microstrategyReportSourceId": source_dataset.id,
                    "microstrategyReportSourceName": source_dataset.name,
                }
            )
            properties.update(_dataset_semantic_count_properties(source_dataset))
        return {key: value for key, value in properties.items() if value}

    @staticmethod
    def _source_warehouse_properties(
        source_warehouse: Optional[DatasourceReference],
    ) -> Dict[str, str]:
        if not source_warehouse:
            return {}
        values = {
            "microstrategySourceWarehouseId": source_warehouse.id,
            "microstrategySourceWarehouseName": source_warehouse.name,
            "microstrategySourceType": source_warehouse.database_type
            or source_warehouse.datasource_type,
            "microstrategyDatasourceType": source_warehouse.datasource_type,
            "microstrategyDatabaseType": source_warehouse.database_type,
            "microstrategyDatabaseVersion": source_warehouse.database_version,
            "microstrategyDbmsName": source_warehouse.dbms_name,
            "microstrategyConnectionId": source_warehouse.connection_id,
            "microstrategyConnectionName": source_warehouse.connection_name,
            "microstrategyDatabaseName": source_warehouse.database_name,
            "microstrategySchemaName": source_warehouse.schema_name,
        }
        if source_warehouse.connection_embedded is not None:
            values["microstrategyConnectionEmbedded"] = str(
                source_warehouse.connection_embedded
            ).lower()
        return {key: value for key, value in values.items() if value}

    @staticmethod
    def _source_warehouse_summary(
        source_warehouses: Sequence[Datasource],
    ) -> Dict[str, str]:
        if not source_warehouses:
            return {}

        database_types = sorted(
            {
                datasource.database_type
                for datasource in source_warehouses
                if datasource.database_type
            }
        )
        datasource_types = sorted(
            {
                datasource.datasource_type
                for datasource in source_warehouses
                if datasource.datasource_type
            }
        )
        dbms_names = sorted(
            {
                datasource.dbms_name
                for datasource in source_warehouses
                if datasource.dbms_name
            }
        )
        return {
            key: value
            for key, value in {
                "microstrategySourceWarehouseCount": str(len(source_warehouses)),
                "microstrategySourceTypes": json.dumps(database_types),
                "microstrategyDatasourceTypes": json.dumps(datasource_types),
                "microstrategyDbmsNames": json.dumps(dbms_names),
            }.items()
            if value
        }

    @staticmethod
    def _dashboard_dependency_properties(
        dashboard: DashboardDefinition,
    ) -> Dict[str, str]:
        if not dashboard.dependencies:
            return {}
        type_counts: Dict[str, int] = {}
        dependencies = []
        for dependency in dashboard.dependencies:
            dependency_type = dependency.type or "unknown"
            type_counts[dependency_type] = type_counts.get(dependency_type, 0) + 1
            dependencies.append(
                {
                    key: value
                    for key, value in {
                        "id": dependency.id,
                        "name": dependency.name,
                        "type": dependency.type,
                        "subtype": dependency.subtype,
                    }.items()
                    if value
                }
            )
        return {
            "microstrategyDirectDependencyCount": str(len(dashboard.dependencies)),
            "microstrategyDirectDependencyTypeCounts": json.dumps(
                type_counts, sort_keys=True
            ),
            "microstrategyDirectDependencies": json.dumps(dependencies, sort_keys=True),
        }

    @staticmethod
    def _dashboard_object_properties(
        dashboard_object: MicroStrategyObject,
    ) -> Dict[str, str]:
        return {
            key: value
            for key, value in {
                "microstrategyObjectType": dashboard_object.type,
                "microstrategyObjectSubtype": dashboard_object.subtype,
                "microstrategyOwner": dashboard_object.owner,
                "microstrategyDateCreated": dashboard_object.date_created,
                "microstrategyDateModified": dashboard_object.date_modified,
            }.items()
            if value
        }

    @staticmethod
    def _dashboard_audit_stamps(
        dashboard_object: MicroStrategyObject,
    ) -> ChangeAuditStampsClass:
        owner = dashboard_object.owner or "datahub"
        return ChangeAuditStampsClass(
            created=_audit_stamp(dashboard_object.date_created, owner),
            lastModified=_audit_stamp(dashboard_object.date_modified, owner),
        )


def _coerce_list(value: object) -> List[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        nested = value.get("items") or value.get("objects")
        if isinstance(nested, list):
            return nested
        return list(value.values())
    return []


def _add_schema_field_object_mapping(
    fields_by_object_id: Dict[str, List[SchemaFieldClass]],
    item: Dict[str, object],
    field: SchemaFieldClass,
) -> None:
    for key in ("id", "objectId"):
        value = item.get(key)
        if value:
            fields_by_object_id.setdefault(normalize_object_id(value), []).append(field)


def _resolve_folder_name(
    part: FolderPart, predefined_folders: Optional[PredefinedFolderResolution]
) -> str:
    """A folder's raw metadata name, unless its id matches a resolved predefined
    folder (see MSTR_PREDEFINED_FOLDER_LABELS) -- then its MicroStrategy-assigned
    label is used everywhere: pattern matching, container identity, and display,
    so all three never disagree about what a folder is called."""
    if predefined_folders and part.id:
        return predefined_folders.labels.get(normalize_object_id(part.id), part.name)
    return part.name


def _is_hidden_folder(
    part: FolderPart, predefined_folders: Optional[PredefinedFolderResolution]
) -> bool:
    """System containers Strategy Web never shows (the project root folder and
    'Public Objects'); their children re-parent to the nearest kept ancestor."""
    if predefined_folders and part.id:
        return normalize_object_id(part.id) in predefined_folders.hidden_ids
    return False


def _optional_str(value: object) -> Optional[str]:
    if isinstance(value, str) and value:
        return value
    return None


def _field_name(item: Dict[str, object]) -> str:
    for key in ("name", "title", "id"):
        value = item.get(key)
        if value:
            return str(value)
    return "unknown"


def _attribute_field_path(
    attribute: Dict[str, object],
    form: Dict[str, object],
    form_count: int,
) -> str:
    attribute_name = _field_name(attribute)
    form_name = _field_name(form)
    if form_count == 1 and form_name.lower() in {
        "id",
        attribute_name.lower(),
        f"{attribute_name} id".lower(),
    }:
        return attribute_name
    return f"{attribute_name}.{form_name}"


def _dedupe_field_path(name: str, seen: Set[str]) -> str:
    cleaned = MSTR_WHITESPACE_RE.sub(" ", name).strip() or "unknown"
    if cleaned not in seen:
        seen.add(cleaned)
        return cleaned
    suffix = 2
    while f"{cleaned}_{suffix}" in seen:
        suffix += 1
    deduped = f"{cleaned}_{suffix}"
    seen.add(deduped)
    return deduped


def _field_native_type(item: Dict[str, object]) -> Optional[str]:
    for key in ("dataType", "type", "baseFormType"):
        value = item.get(key)
        if value:
            return str(value)
    return None


def _metric_field_description(
    description: Optional[str],
    enrichment: Optional[MetricEnrichment],
) -> Optional[str]:
    """Description first, then the formula as a fenced block (descriptions render as markdown)."""
    expression = enrichment.expression_text if enrichment else None
    if not expression:
        return description
    block = f"```\n{expression}\n```"
    if description:
        return f"{description}\n\n{block}"
    return block


def _input_field_source_context(group_name: Optional[str], dataset_name: str) -> str:
    if group_name:
        return f"**{group_name}** - {dataset_name}"
    return f"**{dataset_name}**"


def _clean_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = MSTR_WHITESPACE_RE.sub(" ", value).strip()
    return cleaned or None


def _record_object_name(object_names: Dict[str, str], item: Dict[str, object]) -> None:
    name = _clean_name(_optional_str(item.get("name")))
    if not name:
        return
    for key in ("id", "objectId"):
        value = item.get(key)
        if value:
            object_names.setdefault(normalize_object_id(value), name)


def _datasets_in_grid_order(
    input_datasets: List[DatasetObject],
    visualization: Visualization,
    binding: ColumnSetBinding,
) -> List[DatasetObject]:
    """Datasets bound to column groups first, in grid order, so an ungrouped
    header (a row attribute) attributes to the leftmost group's dataset;
    unbound input datasets follow in dossier order."""
    ordered: List[DatasetObject] = []
    for column_set in visualization.column_sets:
        bound_id = binding.dataset_id_by_column_set.get(column_set.identifier)
        for dataset in input_datasets:
            if dataset.id == bound_id and dataset not in ordered:
                ordered.append(dataset)
    for dataset in input_datasets:
        if dataset not in ordered:
            ordered.append(dataset)
    return ordered


class _InputFieldBuilder:
    """Accumulates a visualization's input fields in emission order. Display
    copies are renamed to match the grid; schemaField urns keep the dataset's
    real field paths."""

    def __init__(
        self,
        annotate_source: bool,
        urn_by_dataset_id: Dict[str, str],
        schema_by_dataset_id: Dict[str, DatasetSchemaFields],
        group_by_dataset_id: Dict[str, str],
    ) -> None:
        self.annotate_source = annotate_source
        self.urn_by_dataset_id = urn_by_dataset_id
        self.schema_by_dataset_id = schema_by_dataset_id
        self.group_by_dataset_id = group_by_dataset_id
        self.entries: List[InputFieldClass] = []
        self.used_display_names: Set[str] = set()
        self.placed_object_ids: Set[str] = set()

    def emit(
        self,
        dataset: DatasetObject,
        schema_field: SchemaFieldClass,
        display_name: str,
        group_name: Optional[str],
        aliased_object_name: Optional[str],
    ) -> None:
        display_copy = deepcopy(schema_field)
        if display_name in self.used_display_names:
            # Two header cells with identical text from different datasets;
            # keep both visible rather than let the UI merge them.
            display_name = f"{display_name} ({dataset.name})"
        self.used_display_names.add(display_name)
        context_lines: List[str] = []
        if self.annotate_source:
            context_lines.append(_input_field_source_context(group_name, dataset.name))
        json_props = json.loads(display_copy.jsonProps or "{}")
        if group_name:
            json_props["microstrategyColumnGroup"] = group_name
        if aliased_object_name:
            # The grid header is a dossier alias; keep the catalog name.
            context_lines.append(f"MicroStrategy object: {aliased_object_name}")
            json_props["microstrategyObjectName"] = aliased_object_name
        if context_lines:
            context = "\n\n".join(context_lines)
            display_copy.description = (
                f"{context}\n\n{display_copy.description}"
                if display_copy.description
                else context
            )
        display_copy.jsonProps = json.dumps(json_props, sort_keys=True)
        display_copy.fieldPath = display_name
        self.entries.append(
            InputFieldClass(
                schemaFieldUrn=builder.make_schema_field_urn(
                    self.urn_by_dataset_id[dataset.id], schema_field.fieldPath
                ),
                schemaField=display_copy,
            )
        )

    def place_grid_cell(self, unit: GridUnit, candidates: List[DatasetObject]) -> None:
        """Emit one grid header cell from the first candidate dataset that
        carries its object: the displayed forms of an attribute, or the
        metric under the grid's own header text, prefixed with its group."""
        if not unit.id:
            return
        object_id = normalize_object_id(unit.id)
        for dataset in candidates:
            schema = self.schema_by_dataset_id[dataset.id]
            fields = schema.by_object_id.get(object_id, [])
            if not fields:
                continue
            object_name = schema.object_names.get(object_id)
            header = _clean_name(unit.name)
            aliased_object_name = (
                object_name
                if header and object_name and header != object_name
                else None
            )
            fields = _grid_unit_fields(unit, fields, schema)
            for schema_field in fields:
                display_name = _grid_unit_display_name(
                    unit, schema_field, object_name, len(fields)
                )
                if unit.column_set_name:
                    display_name = f"{unit.column_set_name}.{display_name}"
                self.emit(
                    dataset,
                    schema_field,
                    display_name,
                    unit.column_set_name,
                    aliased_object_name,
                )
            self.placed_object_ids.add(object_id)
            return

    def place_leftovers(
        self,
        ordered_datasets: List[DatasetObject],
        visualization_object_ids: Set[str],
    ) -> None:
        """Objects referenced outside any grid header cell (and every object
        when no runtime grid is available): emitted from every input dataset,
        prefixed with the group (else dataset) name when several datasets feed
        the chart, in urn order."""
        leftover_ids = visualization_object_ids - self.placed_object_ids
        leftover_by_urn: Dict[str, Tuple[DatasetObject, SchemaFieldClass, str]] = {}
        for dataset in ordered_datasets:
            schema = self.schema_by_dataset_id[dataset.id]
            group_name = self.group_by_dataset_id.get(dataset.id)
            prefix = (group_name or dataset.name) if self.annotate_source else None
            for object_id in leftover_ids:
                for schema_field in schema.by_object_id.get(object_id, []):
                    schema_field_urn = builder.make_schema_field_urn(
                        self.urn_by_dataset_id[dataset.id], schema_field.fieldPath
                    )
                    display_name = (
                        f"{prefix}.{schema_field.fieldPath}"
                        if prefix
                        else schema_field.fieldPath
                    )
                    leftover_by_urn[schema_field_urn] = (
                        dataset,
                        schema_field,
                        display_name,
                    )
        for schema_field_urn in sorted(leftover_by_urn):
            dataset, schema_field, display_name = leftover_by_urn[schema_field_urn]
            self.emit(
                dataset,
                schema_field,
                display_name,
                self.group_by_dataset_id.get(dataset.id)
                if self.annotate_source
                else None,
                None,
            )


def _grid_unit_fields(
    unit: GridUnit,
    fields: List[SchemaFieldClass],
    schema: DatasetSchemaFields,
) -> List[SchemaFieldClass]:
    """The dataset fields a grid header cell actually displays. A metric cell
    is one field; an attribute cell shows only the forms the grid lists, so
    the other forms of a multi-form attribute are not chart inputs."""
    if unit.kind != "attribute" or len(fields) <= 1:
        return fields
    by_form_id: List[SchemaFieldClass] = []
    for form_id in unit.form_ids:
        for candidate in schema.by_object_id.get(normalize_object_id(form_id), []):
            if any(candidate is field for field in fields) and not any(
                candidate is chosen for chosen in by_form_id
            ):
                by_form_id.append(candidate)
    if by_form_id:
        return by_form_id
    form_names = {name.lower() for name in unit.form_names}
    by_form_name = [
        field
        for field in fields
        if field.fieldPath.rsplit(".", 1)[-1].lower() in form_names
    ]
    return by_form_name or fields


def _grid_unit_display_name(
    unit: GridUnit,
    schema_field: SchemaFieldClass,
    object_name: Optional[str],
    displayed_field_count: int,
) -> str:
    """Header text for one displayed field: the grid's own header for a metric
    (its alias when renamed in the dossier); for an attribute the bare
    attribute name when a single form is shown, else `Attribute.FORM`."""
    header = _clean_name(unit.name) or object_name or schema_field.fieldPath
    if unit.kind == "metric" or displayed_field_count == 1:
        return header
    attribute_name = object_name or header
    if schema_field.fieldPath.startswith(f"{attribute_name}."):
        form_name = schema_field.fieldPath[len(attribute_name) + 1 :]
        return f"{header}.{form_name}"
    return schema_field.fieldPath


def _derived_metric_description(
    derived: DerivedMetricSpec, dataset: DatasetObject
) -> str:
    """Where the derived metric is defined, then its formula as a fenced block
    when a report/document definition exposed one. Only a metric no definition
    exposed is described as visualization-local."""
    if derived.definition_source:
        where = (
            f"report '{dataset.name}'"
            if derived.definition_source == "report"
            else f"the dossier that embeds '{dataset.name}'"
        )
        sentence = f"Derived metric defined on {where}."
        if derived.expression_text:
            return f"{sentence}\n\n```\n{derived.expression_text}\n```"
        return f"{sentence} Its formula is not exposed by the MicroStrategy REST API."
    location = (
        f"the '{derived.column_set_name}' column group of "
        if derived.column_set_name
        else ""
    )
    visualization = (
        derived.source_visualization_name or derived.source_visualization_key
    )
    return (
        f"Derived metric defined in {location}visualization '{visualization}'. "
        "No report or document definition exposes a formula for it."
    )


def _normalized_name(value: str) -> str:
    return MSTR_WHITESPACE_RE.sub(" ", value).strip().lower()


def _column_group_json_props(
    dataset: DatasetObject, item: Dict[str, object]
) -> Dict[str, str]:
    for key in ("id", "objectId"):
        value = item.get(key)
        if value:
            group = dataset.column_groups_by_object_id.get(normalize_object_id(value))
            if group:
                return {"microstrategyColumnGroup": group}
    return {}


def _metric_expression_json_props(
    enrichment: Optional[MetricEnrichment],
) -> Dict[str, str]:
    if enrichment is None:
        return {}
    values = {
        "microstrategyMetricExpressionText": enrichment.expression_text,
        "microstrategyMetricExpressionTokens": enrichment.expression_tokens,
    }
    return {key: str(value) for key, value in values.items() if value}


def _metric_enrichment_for(
    dataset: DatasetObject,
    metric: Dict[str, object],
) -> Optional[MetricEnrichment]:
    metric_id = metric.get("id") or metric.get("objectId")
    if not metric_id:
        return None
    return dataset.metric_enrichments.get(normalize_object_id(metric_id))


@dataclass
class _FieldSpec:
    field_path: str
    kind: Literal["metric", "attribute", "derived_metric"]
    item: Dict[str, object]
    form: Optional[Dict[str, object]] = None
    temporal: bool = False
    derived: Optional[DerivedMetricSpec] = None


def _iter_dataset_fields(dataset: DatasetObject) -> Iterator[_FieldSpec]:
    """Single source of field paths, so schema emission and field lineage always agree."""
    seen: Set[str] = set()
    available_objects = dataset.available_objects or {}

    for metric in _coerce_list(available_objects.get("metrics")):
        if not isinstance(metric, dict):
            continue
        yield _FieldSpec(
            field_path=_dedupe_field_path(_field_name(metric), seen),
            kind="metric",
            item=metric,
        )

    for attribute in _coerce_list(available_objects.get("attributes")):
        if not isinstance(attribute, dict):
            continue
        forms = _coerce_list(attribute.get("forms"))
        if not forms:
            forms = [attribute]
        for form in forms:
            if not isinstance(form, dict):
                continue
            yield _FieldSpec(
                field_path=_dedupe_field_path(
                    _attribute_field_path(attribute, form, len(forms)), seen
                ),
                kind="attribute",
                item=attribute,
                form=form,
                temporal=_is_temporal(form) or _is_temporal(attribute),
            )

    for derived in sorted(
        dataset.derived_metrics.values(), key=lambda spec: (spec.name, spec.id)
    ):
        yield _FieldSpec(
            field_path=_dedupe_field_path(derived.name, seen),
            kind="derived_metric",
            item={"id": derived.id, "name": derived.name},
            derived=derived,
        )


def _add_input_field(
    input_fields_by_urn: Dict[str, InputFieldClass],
    dataset_urn: str,
    schema_field: SchemaFieldClass,
) -> None:
    schema_field_urn = builder.make_schema_field_urn(
        dataset_urn, schema_field.fieldPath
    )
    input_fields_by_urn[schema_field_urn] = InputFieldClass(
        schemaFieldUrn=schema_field_urn,
        schemaField=schema_field,
    )


def _input_fields_aspect(
    input_fields_by_urn: Dict[str, InputFieldClass],
) -> Optional[InputFieldsClass]:
    if not input_fields_by_urn:
        return None
    return InputFieldsClass(
        fields=[
            input_fields_by_urn[schema_field_urn]
            for schema_field_urn in sorted(input_fields_by_urn)
        ]
    )


def _dataset_semantic_count_properties(dataset: DatasetObject) -> Dict[str, str]:
    available_objects = dataset.available_objects or {}
    metrics = [
        item
        for item in _coerce_list(available_objects.get("metrics"))
        if isinstance(item, dict)
    ]
    attributes = [
        item
        for item in _coerce_list(available_objects.get("attributes"))
        if isinstance(item, dict)
    ]
    attribute_form_count = 0
    attribute_schema_field_count = 0
    for attribute in attributes:
        forms = [
            item
            for item in _coerce_list(attribute.get("forms"))
            if isinstance(item, dict)
        ]
        attribute_form_count += len(forms)
        attribute_schema_field_count += len(forms) if forms else 1

    return {
        "microstrategyMetricCount": str(len(metrics)),
        "microstrategyAttributeCount": str(len(attributes)),
        "microstrategyAttributeFormCount": str(attribute_form_count),
        "microstrategySchemaFieldCount": str(
            len(metrics) + attribute_schema_field_count
        ),
        "microstrategyObjectIdCount": str(len(dataset.object_ids)),
    }


def _warehouse_upstream_properties(upstream_urns: Sequence[str]) -> Dict[str, str]:
    upstream_platforms = sorted(
        {
            platform
            for platform in (
                _platform_from_dataset_urn(upstream_urn)
                for upstream_urn in upstream_urns
            )
            if platform
        }
    )
    properties = {
        "microstrategyWarehouseUpstreamCount": str(len(set(upstream_urns))),
    }
    if upstream_platforms:
        properties["microstrategyWarehouseUpstreamPlatforms"] = json.dumps(
            upstream_platforms
        )
    return properties


def _platform_from_dataset_urn(dataset_urn: str) -> Optional[str]:
    try:
        parsed = DatasetUrn.from_string(dataset_urn)
    except InvalidUrnError:
        return None
    return parsed.platform.removeprefix("urn:li:dataPlatform:")


def _schema_field_dataset_urn(schema_field_urn: str) -> Optional[str]:
    try:
        return SchemaFieldUrn.from_string(schema_field_urn).parent
    except InvalidUrnError:
        return None


def _filter_schema_field_upstreams(
    schema_field_urns: Sequence[str],
    allowed_dataset_urns: Set[str],
) -> List[str]:
    allowed_keys = {
        key
        for dataset_urn in allowed_dataset_urns
        for key in _dataset_lineage_match_keys(dataset_urn)
    }
    return sorted(
        {
            schema_field_urn
            for schema_field_urn in schema_field_urns
            if _schema_field_dataset_urn(schema_field_urn) in allowed_dataset_urns
            or bool(
                allowed_keys.intersection(
                    _dataset_lineage_match_keys(
                        _schema_field_dataset_urn(schema_field_urn)
                    )
                )
            )
        }
    )


def _dataset_lineage_match_keys(dataset_urn: Optional[str]) -> Set[str]:
    if not dataset_urn:
        return set()
    try:
        parsed = DatasetUrn.from_string(dataset_urn)
    except InvalidUrnError:
        return set()
    platform = parsed.platform.removeprefix("urn:li:dataPlatform:").lower()
    qualified_name = MSTR_DOT_COLLAPSE_RE.sub(".", parsed.name.strip(".").lower())
    parts = [part for part in qualified_name.split(".") if part]
    keys = {f"{platform}:{qualified_name}"}
    # Require at least schema.table for the relaxed match; a bare table name
    # would attach lineage across identically-named tables in other schemas.
    if len(parts) >= 2:
        keys.add(f"{platform}:table:{parts[-2]}.{parts[-1]}")
    return keys


def _upstream_dataset_urns(field_upstreams: Dict[str, List[str]]) -> Set[str]:
    urns: Set[str] = set()
    for upstreams in field_upstreams.values():
        for field_urn in upstreams:
            dataset_urn = _schema_field_dataset_urn(field_urn)
            if dataset_urn:
                urns.add(dataset_urn)
    return urns


def _audit_stamp(date_value: Optional[str], owner: str) -> Optional[AuditStampClass]:
    timestamp = _parse_microstrategy_time(date_value)
    if timestamp is None:
        return None
    return AuditStampClass(time=timestamp, actor=builder.make_user_urn(owner))


def _parse_microstrategy_time(date_value: Optional[str]) -> Optional[int]:
    if not date_value:
        return None
    normalized = date_value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+0000"
    for format_string in MSTR_OBJECT_TIMESTAMP_FORMATS:
        try:
            parsed = datetime.strptime(normalized, format_string)
            return int(parsed.timestamp() * 1000)
        except ValueError:
            continue
    return None


def _is_temporal(item: Dict[str, object]) -> bool:
    native_type = (_field_native_type(item) or "").lower()
    category = str(item.get("baseFormCategory", "")).lower()
    return any(token in native_type for token in ("date", "time", "timestamp")) or any(
        token in category for token in ("date", "time")
    )


def _schema_type(
    native_type: str,
) -> Union[NullTypeClass, NumberTypeClass, StringTypeClass]:
    lowered = native_type.lower()
    if any(token in lowered for token in ("int", "decimal", "double", "float", "real")):
        return NumberTypeClass()
    if "char" in lowered or "string" in lowered or "text" in lowered:
        return StringTypeClass()
    return NullTypeClass()
