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
from datahub.ingestion.source.grafana.models import Dashboard, Panel
from datahub.ingestion.source.grafana.types import CHART_TYPE_MAPPINGS
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ChartInfoClass,
    ChartSnapshotClass,
    DashboardInfoClass,
    DashboardSnapshotClass,
    DataPlatformInstanceClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    TagAssociationClass,
)


def build_chart_mce(
    panel: Panel,
    dashboard: Dashboard,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    base_url: str,
) -> Tuple[Optional[str], ChartSnapshotClass]:
    """Build chart metadata change event"""
    ds_urn = None
    chart_urn = make_chart_urn(
        platform,
        f"{dashboard.uid}.{panel.id}",
        platform_instance,
    )

    chart_snapshot = ChartSnapshotClass(
        urn=chart_urn,
        aspects=[
            DataPlatformInstanceClass(
                platform=make_data_platform_urn(platform),
                instance=make_dataplatform_instance_urn(
                    platform=platform,
                    instance=platform_instance,
                )
                if platform_instance
                else None,
            ),
            StatusClass(removed=False),
        ],
    )

    # Ensure title exists
    title = panel.title or f"Panel {panel.id}"

    # Get input datasets
    input_datasets = []
    if panel.datasource and isinstance(panel.datasource, dict):
        ds_type = panel.datasource.get("type", "unknown")
        ds_uid = panel.datasource.get("uid", "unknown")

        # Add Grafana dataset
        dataset_name = f"{ds_type}.{ds_uid}.{panel.id}"
        ds_urn = make_dataset_urn_with_platform_instance(
            platform=platform,
            name=dataset_name,
            platform_instance=platform_instance,
            env=env,
        )
        input_datasets.append(ds_urn)

    chart_info = ChartInfoClass(
        type=CHART_TYPE_MAPPINGS.get(panel.type),
        description=panel.description,
        title=title,
        lastModified=ChangeAuditStampsClass(),
        chartUrl=f"{base_url}/d/{dashboard.uid}?viewPanel={panel.id}",
        customProperties=_build_custom_properties(panel),
        inputs=input_datasets,
    )
    chart_snapshot.aspects.append(chart_info)

    return ds_urn, chart_snapshot


def build_dashboard_mce(
    dashboard: Dashboard,
    platform: str,
    platform_instance: Optional[str],
    chart_urns: List[str],
    base_url: str,
) -> DashboardSnapshotClass:
    """Build dashboard metadata change event"""
    dashboard_urn = make_dashboard_urn(platform, dashboard.uid, platform_instance)

    dashboard_snapshot = DashboardSnapshotClass(
        urn=dashboard_urn,
        aspects=[
            DataPlatformInstanceClass(
                platform=make_data_platform_urn(platform),
                instance=make_dataplatform_instance_urn(
                    platform=platform,
                    instance=platform_instance,
                )
                if platform_instance
                else None,
            ),
        ],
    )

    # Add basic info
    dashboard_info = DashboardInfoClass(
        description=dashboard.description,
        title=dashboard.title,
        charts=chart_urns,
        lastModified=ChangeAuditStampsClass(),
        dashboardUrl=f"{base_url}/d/{dashboard.uid}",
        customProperties=_build_dashboard_properties(dashboard),
    )
    dashboard_snapshot.aspects.append(dashboard_info)

    # Add ownership
    if dashboard.uid:
        owner = _build_ownership(dashboard)
        if owner:
            dashboard_snapshot.aspects.append(owner)

    # Add tags
    if dashboard.tags:
        tags = [
            TagAssociationClass(tag=make_tag_urn(tag))
            for tag in dashboard.tags
            if isinstance(tag, str)
        ]
        if tags:
            dashboard_snapshot.aspects.append(GlobalTagsClass(tags=tags))

    # Add status
    dashboard_snapshot.aspects.append(StatusClass(removed=False))

    return dashboard_snapshot


def _build_custom_properties(panel: Panel) -> Dict[str, str]:
    """Build custom properties for chart"""
    props = {}

    if panel.type:
        props["type"] = panel.type

    if panel.datasource and isinstance(panel.datasource, dict):
        props["datasourceType"] = panel.datasource.get("type", "")
        props["datasourceUid"] = panel.datasource.get("uid", "")

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

    if panel.targets:
        props["queryCount"] = str(len(panel.targets))

    return props


def _build_dashboard_properties(dashboard: Dashboard) -> Dict[str, str]:
    """Build custom properties for dashboard"""
    props = {
        "version": dashboard.version,
        "schemaVersion": dashboard.schema_version,
        "timezone": dashboard.timezone,
    }

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
        owners.append(
            OwnerClass(
                owner=make_user_urn(dashboard.created_by),
                type=OwnershipTypeClass.DATAOWNER,
            )
        )

    return OwnershipClass(owners=owners) if owners else None
