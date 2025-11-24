import logging
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.grafana.field_utils import extract_fields_from_panel
from datahub.ingestion.source.grafana.grafana_config import GrafanaSourceConfig
from datahub.ingestion.source.grafana.lineage import LineageExtractor
from datahub.ingestion.source.grafana.models import Dashboard, Panel
from datahub.ingestion.source.grafana.report import GrafanaSourceReport
from datahub.ingestion.source.grafana.types import CHART_TYPE_MAPPINGS
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StatusClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)


def build_chart_mcps(
    panel: Panel,
    dashboard: Dashboard,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    base_url: str,
    ingest_tags: bool,
) -> Tuple[Optional[str], str, List[MetadataChangeProposalWrapper]]:
    """Build chart metadata change proposals"""
    ds_urn = None
    mcps = []

    chart_urn = make_chart_urn(
        platform,
        f"{dashboard.uid}.{panel.id}",
        platform_instance,
    )

    # Platform instance aspect
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(platform),
                instance=make_dataplatform_instance_urn(
                    platform=platform,
                    instance=platform_instance,
                )
                if platform_instance
                else None,
            ),
        )
    )

    # Status aspect
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=StatusClass(removed=False),
        )
    )

    # Get input datasets (datasource-level, shared across panels)
    input_datasets = []
    if panel.datasource_ref:
        ds_type = panel.datasource_ref.type or "unknown"
        ds_uid = panel.datasource_ref.uid or "unknown"

        # Add Grafana datasource (shared entity, not per-panel)
        dataset_name = f"{ds_type}.{ds_uid}"
        ds_urn = make_dataset_urn_with_platform_instance(
            platform=platform,
            name=dataset_name,
            platform_instance=platform_instance,
            env=env,
        )
        input_datasets.append(ds_urn)

    # Chart info aspect
    title = panel.title or f"Panel {panel.id}"
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ChartInfoClass(
                type=CHART_TYPE_MAPPINGS.get(panel.type) if panel.type else None,
                description=panel.description,
                title=title,
                lastModified=ChangeAuditStampsClass(),
                chartUrl=f"{base_url}/d/{dashboard.uid}?viewPanel={panel.id}",
                customProperties=_build_custom_properties(panel),
                inputs=input_datasets,
            ),
        )
    )

    # Tags aspect
    if dashboard.tags and ingest_tags:
        tags = []
        for tag in dashboard.tags:
            if ":" in tag:
                key, value = tag.split(":", 1)
                tag_urn = make_tag_urn(f"{key}.{value}")
            else:
                tag_urn = make_tag_urn(tag)
            tags.append(TagAssociationClass(tag=tag_urn))

        if tags:
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=GlobalTagsClass(tags=tags),
                )
            )

    return ds_urn, chart_urn, mcps


def build_dashboard_mcps(
    dashboard: Dashboard,
    platform: str,
    platform_instance: Optional[str],
    chart_urns: List[str],
    base_url: str,
    ingest_owners: bool,
    ingest_tags: bool,
) -> Tuple[str, List[MetadataChangeProposalWrapper]]:
    """Build dashboard metadata change proposals"""
    mcps = []
    dashboard_urn = make_dashboard_urn(platform, dashboard.uid, platform_instance)

    # Platform instance aspect
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(platform),
                instance=make_dataplatform_instance_urn(
                    platform=platform,
                    instance=platform_instance,
                )
                if platform_instance
                else None,
            ),
        )
    )

    # Dashboard info aspect
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=DashboardInfoClass(
                description=dashboard.description,
                title=dashboard.title,
                charts=chart_urns,
                lastModified=ChangeAuditStampsClass(),
                dashboardUrl=f"{base_url}/d/{dashboard.uid}",
                customProperties=_build_dashboard_properties(dashboard),
            ),
        )
    )

    # Ownership aspect
    if dashboard.uid and ingest_owners:
        owner = _build_ownership(dashboard)
        if owner:
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dashboard_urn,
                    aspect=owner,
                )
            )

    # Tags aspect
    if dashboard.tags and ingest_tags:
        tags = [TagAssociationClass(tag=make_tag_urn(tag)) for tag in dashboard.tags]
        if tags:
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dashboard_urn,
                    aspect=GlobalTagsClass(tags=tags),
                )
            )

    # Status aspect
    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=StatusClass(removed=False),
        )
    )

    return dashboard_urn, mcps


def _build_custom_properties(panel: Panel) -> Dict[str, str]:
    """Build custom properties for chart"""
    props = {}

    if panel.type:
        props["type"] = panel.type

    if panel.datasource_ref:
        props["datasourceType"] = panel.datasource_ref.type or ""
        props["datasourceUid"] = panel.datasource_ref.uid or ""

    for key in [
        "description",
        "format",
        "pluginVersion",
        "repeatDirection",
        "maxDataPoints",
    ]:
        value = getattr(panel, key, None)
        if value:
            props[key] = str(value)

    if panel.query_targets:
        props["targetsCount"] = str(len(panel.query_targets))

    return props


def _build_dashboard_properties(dashboard: Dashboard) -> Dict[str, str]:
    """Build custom properties for dashboard"""
    props = {}

    if dashboard.timezone:
        props["timezone"] = dashboard.timezone

    if dashboard.schema_version:
        props["schema_version"] = dashboard.schema_version

    if dashboard.version:
        props["version"] = dashboard.version

    if dashboard.refresh:
        props["refresh"] = dashboard.refresh

    return props


def _build_ownership(dashboard: Dashboard) -> Optional[OwnershipClass]:
    """Build ownership information"""
    owners = []

    if dashboard.uid:
        owners.append(
            OwnerClass(
                owner=make_user_urn(dashboard.uid),
                type=OwnershipTypeClass.TECHNICAL_OWNER,
            )
        )

    if dashboard.created_by:
        owner_id = dashboard.created_by.split("@")[0]
        owners.append(
            OwnerClass(
                owner=make_user_urn(owner_id),
                type=OwnershipTypeClass.DATAOWNER,
            )
        )

    return OwnershipClass(owners=owners) if owners else None


def _consolidate_schema_fields(
    panels: List[Panel],
    connection_to_platform_map: Dict[str, Any],
    graph: Optional[DataHubGraph],
    report: GrafanaSourceReport,
) -> Dict[str, SchemaFieldClass]:
    """Consolidate schema fields from multiple panels."""
    all_schema_fields: Dict[str, SchemaFieldClass] = {}
    for panel in panels:
        schema_fields = extract_fields_from_panel(
            panel,
            connection_to_platform_map,
            graph,
            report,
        )
        if schema_fields:
            for field in schema_fields:
                all_schema_fields[field.fieldPath] = field
    return all_schema_fields


def _consolidate_datasource_lineage(
    panels: List[Panel],
    dashboard_uid: str,
    lineage_extractor: Optional[LineageExtractor],
    logger: logging.Logger,
    report: GrafanaSourceReport,
) -> Tuple[Set[str], List[Any]]:
    """Extract and consolidate lineage from multiple panels."""
    all_upstreams: Set[str] = set()
    fine_grained_lineage_set: Set[Tuple[Tuple[Any, ...], Tuple[Any, ...]]] = set()
    all_fine_grained_lineages: List = []

    if not lineage_extractor:
        return all_upstreams, all_fine_grained_lineages

    for panel in panels:
        try:
            lineage = lineage_extractor.extract_panel_lineage(panel, dashboard_uid)
            if lineage and lineage.aspect:
                if hasattr(lineage.aspect, "upstreams"):
                    for upstream in lineage.aspect.upstreams:
                        all_upstreams.add(upstream.dataset)
                if (
                    hasattr(lineage.aspect, "fineGrainedLineages")
                    and lineage.aspect.fineGrainedLineages
                ):
                    for fg_lineage in lineage.aspect.fineGrainedLineages:
                        # Deduplicate based on downstream/upstream field URNs
                        lineage_key = (
                            tuple(sorted(fg_lineage.downstreams or [])),
                            tuple(sorted(fg_lineage.upstreams or [])),
                        )
                        if lineage_key not in fine_grained_lineage_set:
                            fine_grained_lineage_set.add(lineage_key)
                            all_fine_grained_lineages.append(fg_lineage)
        except Exception as e:
            logger.warning(f"Failed to extract lineage for panel {panel.id}: {e}")
            report.report_lineage_extraction_failure()
    return all_upstreams, all_fine_grained_lineages


def build_datasource_mcps(
    dashboard: Dashboard,
    dashboard_container_key: ContainerKey,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    connection_to_platform_map: Dict[str, Any],
    graph: Optional[DataHubGraph],
    report: GrafanaSourceReport,
    config: GrafanaSourceConfig,
    lineage_extractor: Optional[LineageExtractor],
    logger: logging.Logger,
    add_dataset_to_container_fn: Callable[
        [ContainerKey, str], Iterable[MetadataWorkUnit]
    ],
) -> Iterable[MetadataWorkUnit]:
    """Build datasource MCPs (one per unique datasource, shared across panels)."""
    datasource_panels: Dict[str, List[Panel]] = defaultdict(list)
    datasource_types: Dict[str, str] = {}

    for panel in dashboard.panels:
        if panel.datasource_ref:
            ds_type = panel.datasource_ref.type or "unknown"
            ds_uid = panel.datasource_ref.uid or "unknown"

            if ds_type == "unknown" or ds_uid == "unknown":
                report.report_datasource_warning()
                continue

            datasource_panels[ds_uid].append(panel)
            datasource_types[ds_uid] = ds_type

    for ds_uid, panels in datasource_panels.items():
        ds_type = datasource_types[ds_uid]
        dataset_name = f"{ds_type}.{ds_uid}"

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=platform,
            name=dataset_name,
            platform_instance=platform_instance,
            env=env,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(platform),
                instance=make_dataplatform_instance_urn(
                    platform=platform,
                    instance=platform_instance,
                )
                if platform_instance
                else None,
            ),
        ).as_workunit()

        panel_count = len(panels)
        panel_titles = [p.title or f"Panel {p.id}" for p in panels[:3]]
        panel_info = ", ".join(panel_titles)
        if panel_count > 3:
            panel_info += f" and {panel_count - 3} more"

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=f"{ds_uid}",
                description=f"Grafana datasource used by {panel_count} panel(s): {panel_info}",
                customProperties={
                    "type": ds_type,
                    "uid": ds_uid,
                    "panel_count": str(panel_count),
                },
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        all_schema_fields = _consolidate_schema_fields(
            panels, connection_to_platform_map, graph, report
        )
        if all_schema_fields:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName=f"{ds_type}.{ds_uid}",
                    platform=make_data_platform_urn(platform),
                    version=0,
                    fields=list(all_schema_fields.values()),
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                ),
            ).as_workunit()

        if config.ingest_tags and dashboard.tags:
            tags = []
            for tag in dashboard.tags:
                tags.append(TagAssociationClass(tag=make_tag_urn(tag)))

            if tags:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(tags=tags),
                ).as_workunit()

        report.report_dataset_scanned()

        yield from add_dataset_to_container_fn(
            dashboard_container_key,
            dataset_urn,
        )

        if config.include_lineage and lineage_extractor:
            all_upstreams, all_fine_grained_lineages = _consolidate_datasource_lineage(
                panels, dashboard.uid, lineage_extractor, logger, report
            )
            if all_upstreams:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=upstream_urn,
                                type=DatasetLineageTypeClass.TRANSFORMED,
                            )
                            for upstream_urn in sorted(all_upstreams)
                        ],
                        fineGrainedLineages=all_fine_grained_lineages
                        if all_fine_grained_lineages
                        else None,
                    ),
                ).as_workunit()
                report.report_lineage_extracted()
            else:
                report.report_no_lineage()
