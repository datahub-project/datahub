import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

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
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.grafana.field_utils import extract_fields_from_panel
from datahub.ingestion.source.grafana.grafana_config import GrafanaSourceConfig
from datahub.ingestion.source.grafana.lineage import LineageExtractor
from datahub.ingestion.source.grafana.models import Dashboard, Panel, QueryInfo
from datahub.ingestion.source.grafana.report import GrafanaSourceReport
from datahub.ingestion.source.grafana.types import CHART_TYPE_MAPPINGS
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sqlglot_utils import try_format_query

# Mapping of Grafana datasource types to their query languages
DATASOURCE_TO_QUERY_LANGUAGE: Dict[str, str] = {
    "prometheus": "PromQL",
    "postgres": "SQL",
    "mysql": "SQL",
    "mssql": "SQL",
    "athena": "SQL",
    "cloudwatch": "CloudWatch Metrics",
    "elasticsearch": "Lucene",
    "influxdb": "InfluxQL",
    "graphite": "Graphite",
}

# Mapping of Grafana datasource types to DataHub platform names for SQL formatting
DATASOURCE_TO_PLATFORM: Dict[str, str] = {
    "postgres": "postgres",
    "mysql": "mysql",
    "mssql": "mssql",
    "athena": "athena",
}


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

    # Get input datasets (per-panel with global uniqueness)
    input_datasets = []
    if panel.datasource_ref:
        ds_type = panel.datasource_ref.type or "unknown"
        ds_uid = panel.datasource_ref.uid or "unknown"

        # Add Grafana per-panel dataset (globally unique)
        dataset_name = f"{ds_type}.{ds_uid}.{dashboard.uid}.{panel.id}"
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
    remove_email_suffix: bool,
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
    if ingest_owners:
        owner = _build_ownership(dashboard, remove_email_suffix)
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


def _format_sql_query(query: str, datasource_type: Optional[str]) -> str:
    """
    Format SQL query using sqlglot if possible.

    This is for DISPLAY purposes only (ViewProperties). The original query with
    Grafana template variables is preserved. For lineage parsing, variables are
    cleaned separately in the LineageExtractor.

    Args:
        query: The SQL query to format
        datasource_type: The Grafana datasource type (e.g., 'postgres', 'athena')

    Returns:
        Formatted SQL query, or original query if formatting fails
    """
    if not datasource_type:
        return query

    platform = DATASOURCE_TO_PLATFORM.get(datasource_type.lower())
    if not platform:
        return query

    # DON'T clean template variables here - preserve original for display
    # Use sqlglot to format the query with proper indentation
    return try_format_query(query, platform, raises=False)


def _extract_query_from_panel(panel: Panel) -> Optional[QueryInfo]:
    """
    Extract query logic from panel targets.

    Returns:
        QueryInfo object containing query text and language, or None if no query found.
        Supported languages: SQL, PromQL, etc.
    """
    if not panel.query_targets:
        return None

    ds_type = panel.datasource_ref.type if panel.datasource_ref else None

    for target in panel.query_targets:
        # Check for SQL queries (case-insensitive)
        for key, value in target.items():
            if key.lower() == "rawsql" and value:
                sql_query = str(value).strip()
                if not sql_query:  # Skip empty queries
                    continue
                # Format SQL query for better readability
                formatted_query = _format_sql_query(sql_query, ds_type)
                try:
                    return QueryInfo(query=formatted_query, language="SQL")
                except ValueError as e:
                    logging.getLogger(__name__).warning(
                        f"Invalid SQL query in panel: {e}"
                    )
                    continue

        # Check for Prometheus queries
        expr = target.get("expr")
        if expr:
            expr_str = str(expr).strip()
            if not expr_str:  # Skip empty queries
                continue
            try:
                return QueryInfo(query=expr_str, language="PromQL")
            except ValueError as e:
                logging.getLogger(__name__).warning(
                    f"Invalid PromQL query in panel: {e}"
                )
                continue

        # Check for other common query fields
        query = target.get("query")
        if query and isinstance(query, str):
            query_str = str(query).strip()
            if not query_str:  # Skip empty queries
                continue
            # Try to detect the query language from datasource type
            if ds_type:
                query_lang = DATASOURCE_TO_QUERY_LANGUAGE.get(ds_type.lower(), "Query")
                # Format SQL queries
                if query_lang == "SQL":
                    query_str = _format_sql_query(query_str, ds_type)
            else:
                query_lang = "Query"
            try:
                return QueryInfo(query=query_str, language=query_lang)
            except ValueError as e:
                logging.getLogger(__name__).warning(f"Invalid query in panel: {e}")
                continue

    return None


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


def _build_ownership(
    dashboard: Dashboard, remove_email_suffix: bool
) -> Optional[OwnershipClass]:
    """Build ownership information"""
    owners = []

    if dashboard.created_by:
        if remove_email_suffix:
            owner_id = dashboard.created_by.split("@")[0]
        else:
            owner_id = dashboard.created_by

        owners.append(
            OwnerClass(
                owner=make_user_urn(owner_id),
                type=OwnershipTypeClass.TECHNICAL_OWNER,
            )
        )

    return OwnershipClass(owners=owners) if owners else None


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
    """Build per-panel dataset MCPs with globally unique URNs."""
    for panel in dashboard.panels:
        if not panel.datasource_ref:
            continue

        ds_type = panel.datasource_ref.type or "unknown"
        ds_uid = panel.datasource_ref.uid or "unknown"

        if ds_type == "unknown" or ds_uid == "unknown":
            report.report_datasource_warning()
            continue

        # Global uniqueness: include dashboard_uid and panel_id
        dataset_name = f"{ds_type}.{ds_uid}.{dashboard.uid}.{panel.id}"
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

        panel_title = panel.title or f"Panel {panel.id}"
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=panel_title,
                description=f"Grafana panel '{panel_title}' in dashboard '{dashboard.title}' using datasource {ds_uid}",
                customProperties={
                    "type": ds_type,
                    "datasource_uid": ds_uid,
                    "panel_id": str(panel.id),
                    "dashboard_uid": dashboard.uid,
                    "dashboard_title": dashboard.title,
                },
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Extract query logic if available
        query_info = _extract_query_from_panel(panel)
        if query_info:
            # Emit VIEW subtype when we have query logic
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[DatasetSubTypes.GRAFANA_DATASET, DatasetSubTypes.VIEW]
                ),
            ).as_workunit()

            # Emit ViewProperties with the query
            # QueryInfo fields are guaranteed to be non-empty due to Pydantic validation
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ViewPropertiesClass(
                    materialized=False,
                    viewLanguage=query_info.language,
                    viewLogic=query_info.query,
                ),
            ).as_workunit()
        else:
            # No query logic available, just emit GRAFANA_DATASET subtype
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[DatasetSubTypes.GRAFANA_DATASET]),
            ).as_workunit()

        # Extract schema from this specific panel
        schema_fields = extract_fields_from_panel(
            panel,
            connection_to_platform_map,
            graph,
            report,
        )
        if schema_fields:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName=f"{ds_type}.{ds_uid}.{panel.id}",
                    platform=make_data_platform_urn(platform),
                    version=0,
                    fields=schema_fields,
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

        # Extract lineage for this specific panel
        if config.include_lineage and lineage_extractor:
            try:
                lineage = lineage_extractor.extract_panel_lineage(panel, dashboard.uid)
                if lineage:
                    yield lineage.as_workunit()
                    report.report_lineage_extracted()
                else:
                    report.report_no_lineage()
            except Exception as e:
                logger.warning(f"Failed to extract lineage for panel {panel.id}: {e}")
                report.report_lineage_extraction_failure()
