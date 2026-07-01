import json
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

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
    DIMENSION_TAG_URN,
    MEASURE_TAG_URN,
    TEMPORAL_TAG_URN,
)
from datahub.ingestion.source.microstrategy.lineage import (
    MicroStrategyLineageExtractor,
    ModelLineageIndex,
)
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    DatasourceReference,
    FolderKey,
    MSTRObject,
    Project,
    ProjectKey,
    Visualization,
    extract_folder_parts,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
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
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)


class MicroStrategyMapper:
    def __init__(self, config: MicroStrategyConfig, report: MicroStrategyReport):
        self.config = config
        self.report = report
        self.lineage = MicroStrategyLineageExtractor(config)

    def project_key(self, project_id: str) -> ProjectKey:
        return ProjectKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            project_id=project_id,
        )

    def folder_key(self, project_id: str, folder_path: str) -> FolderKey:
        return FolderKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
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
            dataset.field_warehouse_upstreams = self._model_field_upstreams(
                dataset,
                model_lineage_index,
            )

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
        dashboard_object: MSTRObject,
    ) -> Iterable[MetadataWorkUnit]:
        _, parts = extract_folder_parts(dashboard_object.model_dump())
        parent_key: Optional[ProjectKey] = self.project_key(project_id)
        current_path = ""
        for part in parts:
            if not self.config.folder_pattern.allowed(part):
                continue
            current_path = f"{current_path}/{part}" if current_path else part
            folder_key = self.folder_key(project_id, current_path)
            self.report.report_folder_scanned()
            yield from gen_containers(
                container_key=folder_key,
                parent_container_key=parent_key,
                name=part,
                sub_types=[BIContainerSubTypes.MICROSTRATEGY_FOLDER],
            )
            parent_key = folder_key

    def folder_container_for_dashboard(
        self, project_id: str, dashboard_object: MSTRObject
    ) -> ProjectKey:
        _, parts = extract_folder_parts(dashboard_object.model_dump())
        allowed_parts = [
            part for part in parts if self.config.folder_pattern.allowed(part)
        ]
        if not allowed_parts:
            return self.project_key(project_id)
        return self.folder_key(project_id, "/".join(allowed_parts))

    def gen_dataset_workunits(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        dataset: DatasetObject,
        parent_key: ProjectKey,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_dataset_scanned()
        dataset_urn = self.lineage.dataset_urn(project_id, dashboard.id, dataset)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
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
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=dataset.name,
                description=dataset.description,
                qualifiedName=f"{project_id}.{dashboard.id}.{dataset.id}".lower(),
                externalUrl=f"{self.config.base_url}/app/{project_id}/{dashboard.id}",
                customProperties=self._dataset_custom_properties(
                    project_id=project_id,
                    dashboard=dashboard,
                    dataset=dataset,
                ),
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
        coarse_upstream_urns = (
            sorted(set(dataset.warehouse_upstream_urns))
            if self.config.extract_warehouse_lineage
            else []
        )
        if coarse_upstream_urns:
            self.report.report_warehouse_lineage_edges(len(coarse_upstream_urns))
        upstream_urns = sorted(
            set(coarse_upstream_urns).union(
                upstream_urn
                for upstreams in dataset.field_warehouse_upstreams.values()
                for field_urn in upstreams
                for upstream_urn in [_schema_field_dataset_urn(field_urn)]
                if upstream_urn
            )
        )
        if upstream_urns:
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
                    fineGrainedLineages=fine_grained_lineages or None,
                ),
            ).as_workunit()
        yield from add_entity_to_container(
            container_key=parent_key,
            entity_type="dataset",
            entity_urn=dataset_urn,
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

        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
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
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ChartInfoClass(
                title=visualization.name,
                description=visualization.type or "",
                lastModified=ChangeAuditStampsClass(),
                chartUrl=f"{self.config.base_url}/app/{project_id}/{dashboard.id}",
                customProperties=self._visualization_properties(visualization),
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
        if input_fields:
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=input_fields,
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
        dashboard_object: MSTRObject,
        dashboard: DashboardDefinition,
        parent_key: ProjectKey,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_dashboard_scanned()
        dashboard_urn = self.dashboard_urn(project_id, dashboard.id)
        chart_urns = [
            self.chart_urn(project_id, dashboard.id, visualization.key)
            for visualization in dashboard.visualizations
        ]
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

        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
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
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
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
        fields, _ = self._schema_fields_and_object_map(dataset, report_fields=True)
        return fields

    def _schema_fields_and_object_map(
        self,
        dataset: DatasetObject,
        report_fields: bool = False,
    ) -> Tuple[List[SchemaFieldClass], Dict[str, List[SchemaFieldClass]]]:
        fields: List[SchemaFieldClass] = []
        fields_by_object_id: Dict[str, List[SchemaFieldClass]] = {}
        seen: Set[str] = set()
        available_objects = dataset.available_objects or {}
        metrics = _coerce_list(available_objects.get("metrics"))
        attributes = _coerce_list(available_objects.get("attributes"))

        for metric in metrics:
            if not isinstance(metric, dict):
                continue
            name = _field_name(metric)
            field_path = _dedupe_field_path(name, seen)
            field = self._make_schema_field(
                field_path=field_path,
                native_type=_field_native_type(metric) or "Metric",
                description=metric.get("description"),
                tag_urns=[MEASURE_TAG_URN]
                if self.config.tag_measures_and_dimensions
                else [],
                json_props={
                    "microstrategyObjectId": str(metric.get("id", "")),
                    "microstrategyObjectType": "metric",
                    **_metric_expression_json_props(metric),
                },
                glossary_term_urn=self._term_for(
                    metric, self.config.metric_glossary_term_mapping
                ),
                numeric=True,
            )
            fields.append(field)
            _add_schema_field_object_mapping(fields_by_object_id, metric, field)
            if report_fields:
                self.report.report_metric_field()

        for attribute in attributes:
            if not isinstance(attribute, dict):
                continue
            forms = _coerce_list(attribute.get("forms"))
            if not forms:
                forms = [attribute]
            for form in forms:
                if not isinstance(form, dict):
                    continue
                field_path = _attribute_field_path(attribute, form, len(forms))
                field_path = _dedupe_field_path(field_path, seen)
                temporal = _is_temporal(form) or _is_temporal(attribute)
                tag_urns: List[str] = []
                if self.config.tag_measures_and_dimensions:
                    tag_urns.append(DIMENSION_TAG_URN)
                    if temporal:
                        tag_urns.append(TEMPORAL_TAG_URN)
                field = self._make_schema_field(
                    field_path=field_path,
                    native_type=_field_native_type(form)
                    or _field_native_type(attribute)
                    or "Attribute",
                    description=form.get("description") or attribute.get("description"),
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
                fields.append(field)
                _add_schema_field_object_mapping(fields_by_object_id, attribute, field)
                _add_schema_field_object_mapping(fields_by_object_id, form, field)
                if report_fields:
                    self.report.report_attribute_field(temporal=temporal)

        return sorted(fields, key=lambda field: field.fieldPath), fields_by_object_id

    def _visualization_input_fields(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        visualization: Visualization,
        input_urns: Sequence[str],
    ) -> Optional[InputFieldsClass]:
        if not visualization.object_ids or not input_urns:
            return None

        input_urn_set = set(input_urns)
        visualization_object_ids = {
            _normalize_object_id(object_id) for object_id in visualization.object_ids
        }
        input_fields_by_urn: Dict[str, InputFieldClass] = {}
        for dataset in dashboard.datasets:
            dataset_urn = self.lineage.dataset_urn(project_id, dashboard.id, dataset)
            if dataset_urn not in input_urn_set:
                continue
            _, fields_by_object_id = self._schema_fields_and_object_map(dataset)
            for object_id in visualization_object_ids:
                for field in fields_by_object_id.get(object_id, []):
                    schema_field_urn = builder.make_schema_field_urn(
                        dataset_urn,
                        field.fieldPath,
                    )
                    input_fields_by_urn[schema_field_urn] = InputFieldClass(
                        schemaFieldUrn=schema_field_urn,
                        schemaField=field,
                    )

        if not input_fields_by_urn:
            return None
        return InputFieldsClass(
            fields=[
                input_fields_by_urn[schema_field_urn]
                for schema_field_urn in sorted(input_fields_by_urn)
            ]
        )

    def _model_field_upstreams(
        self,
        dataset: DatasetObject,
        model_lineage_index: ModelLineageIndex,
    ) -> Dict[str, List[str]]:
        allowed_upstream_urns = set(dataset.warehouse_upstream_urns)
        if not allowed_upstream_urns:
            return {}

        field_upstreams: Dict[str, List[str]] = {}
        seen: Set[str] = set()
        available_objects = dataset.available_objects or {}

        for metric in _coerce_list(available_objects.get("metrics")):
            if not isinstance(metric, dict):
                continue
            field_path = _dedupe_field_path(_field_name(metric), seen)
            fact_ids = [
                str(fact_id)
                for fact_id in _coerce_list(metric.get("modelFactIds"))
                if fact_id
            ]
            upstreams = _filter_schema_field_upstreams(
                model_lineage_index.fact_field_urns(fact_ids),
                allowed_upstream_urns,
            )
            if upstreams:
                field_upstreams[field_path] = upstreams

        for attribute in _coerce_list(available_objects.get("attributes")):
            if not isinstance(attribute, dict):
                continue
            attribute_id = str(attribute.get("id") or attribute.get("objectId") or "")
            forms = _coerce_list(attribute.get("forms"))
            if not forms:
                forms = [attribute]
            for form in forms:
                if not isinstance(form, dict):
                    continue
                field_path = _attribute_field_path(attribute, form, len(forms))
                field_path = _dedupe_field_path(field_path, seen)
                upstreams = _filter_schema_field_upstreams(
                    model_lineage_index.attribute_field_urns(
                        attribute_id,
                        _field_name(form),
                    ),
                    allowed_upstream_urns,
                )
                if upstreams:
                    field_upstreams[field_path] = upstreams

        return field_upstreams

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
        if self.config.extract_warehouse_lineage and dataset.warehouse_upstream_urns:
            upstream_platforms = sorted(
                {
                    platform
                    for platform in (
                        _platform_from_dataset_urn(upstream_urn)
                        for upstream_urn in dataset.warehouse_upstream_urns
                    )
                    if platform
                }
            )
            properties["microstrategyWarehouseUpstreamCount"] = str(
                len(set(dataset.warehouse_upstream_urns))
            )
            if upstream_platforms:
                properties["microstrategyWarehouseUpstreamPlatforms"] = json.dumps(
                    upstream_platforms
                )
        if dataset.field_warehouse_upstreams:
            upstream_field_urns = {
                field_urn
                for upstreams in dataset.field_warehouse_upstreams.values()
                for field_urn in upstreams
            }
            upstream_dataset_urns = {
                upstream_urn
                for field_urn in upstream_field_urns
                for upstream_urn in [_schema_field_dataset_urn(field_urn)]
                if upstream_urn
            }
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
    def _dashboard_object_properties(dashboard_object: MSTRObject) -> Dict[str, str]:
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
        dashboard_object: MSTRObject,
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
            fields_by_object_id.setdefault(_normalize_object_id(value), []).append(
                field
            )


def _normalize_object_id(value: object) -> str:
    return str(value).strip().upper()


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
    cleaned = re.sub(r"\s+", " ", name).strip() or "unknown"
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


def _metric_expression_json_props(metric: Dict[str, object]) -> Dict[str, str]:
    expression = metric.get("modelExpression")
    if not isinstance(expression, dict):
        return {}
    values = {
        "microstrategyMetricExpressionText": expression.get("text"),
        "microstrategyMetricExpressionTokens": expression.get("tokens"),
    }
    return {key: str(value) for key, value in values.items() if value}


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


def _platform_from_dataset_urn(dataset_urn: str) -> Optional[str]:
    match = re.match(r"^urn:li:dataset:\(urn:li:dataPlatform:([^,]+),", dataset_urn)
    return match.group(1) if match else None


def _schema_field_dataset_urn(schema_field_urn: str) -> Optional[str]:
    match = re.match(
        r"^urn:li:schemaField:\((urn:li:dataset:\(.+\)),.+\)$",
        schema_field_urn,
    )
    return match.group(1) if match else None


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
    match = re.match(
        r"^urn:li:dataset:\(urn:li:dataPlatform:([^,]+),(.+),([^,]+)\)$",
        dataset_urn,
    )
    if not match:
        return set()
    platform = match.group(1).lower()
    qualified_name = re.sub(r"\.+", ".", match.group(2).strip(".").lower())
    parts = [part for part in qualified_name.split(".") if part]
    keys = {f"{platform}:{qualified_name}"}
    if parts:
        keys.add(f"{platform}:table:{parts[-1]}")
    return keys


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
    for format_string in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
    ):
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
