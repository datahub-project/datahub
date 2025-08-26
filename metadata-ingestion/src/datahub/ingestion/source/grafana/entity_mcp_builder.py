from typing import Dict, List, Optional, Tuple

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
from datahub.ingestion.source.grafana.models import Dashboard, Panel
from datahub.ingestion.source.grafana.types import CHART_TYPE_MAPPINGS
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    TagAssociationClass,
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

    # Get input datasets
    input_datasets = []
    if panel.datasource_ref:
        ds_type = panel.datasource_ref.type or "unknown"
        ds_uid = panel.datasource_ref.uid or "unknown"

        # Add Grafana dataset
        dataset_name = f"{ds_type}.{ds_uid}.{panel.id}"
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
