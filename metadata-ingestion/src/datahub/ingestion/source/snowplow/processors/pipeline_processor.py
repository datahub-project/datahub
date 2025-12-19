"""
Pipeline Processor for Snowplow connector.

Handles extraction of:
- Event-specific pipelines as DataFlow entities
- Enrichments as DataJob entities
- Loader jobs
- Collector jobs

This processor coordinates pipeline and enrichment metadata extraction.
"""

import logging
import time
from typing import TYPE_CHECKING, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.ingestion.source.snowplow.snowplow_models import (
    Enrichment,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStampClass
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )

logger = logging.getLogger(__name__)

# Warehouse platform mapping from Snowplow destination types to DataHub platforms
WAREHOUSE_PLATFORM_MAP = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "postgres": "postgres",
    "postgresql": "postgres",
}

# Standard Snowplow event columns (non-enriched fields)
# Reference: https://docs.snowplow.io/docs/fundamentals/canonical-event/
SNOWPLOW_STANDARD_COLUMNS = [
    "app_id",
    "platform",
    "etl_tstamp",
    "collector_tstamp",
    "dvce_created_tstamp",
    "event",
    "event_id",
    "txn_id",
    "name_tracker",
    "v_tracker",
    "v_collector",
    "v_etl",
    "user_id",
    "user_ipaddress",
    "user_fingerprint",
    "domain_userid",
    "domain_sessionidx",
    "network_userid",
    "geo_country",
    "geo_region",
    "geo_city",
    "geo_zipcode",
    "geo_latitude",
    "geo_longitude",
    "geo_region_name",
    "ip_isp",
    "ip_organization",
    "ip_domain",
    "ip_netspeed",
    "page_url",
    "page_title",
    "page_referrer",
    "page_urlscheme",
    "page_urlhost",
    "page_urlport",
    "page_urlpath",
    "page_urlquery",
    "page_urlfragment",
    "refr_urlscheme",
    "refr_urlhost",
    "refr_urlport",
    "refr_urlpath",
    "refr_urlquery",
    "refr_urlfragment",
    "refr_medium",
    "refr_source",
    "refr_term",
    "mkt_medium",
    "mkt_source",
    "mkt_term",
    "mkt_content",
    "mkt_campaign",
    "se_category",
    "se_action",
    "se_label",
    "se_property",
    "se_value",
    "tr_orderid",
    "tr_affiliation",
    "tr_total",
    "tr_tax",
    "tr_shipping",
    "tr_city",
    "tr_state",
    "tr_country",
    "ti_orderid",
    "ti_sku",
    "ti_name",
    "ti_category",
    "ti_price",
    "ti_quantity",
    "pp_xoffset_min",
    "pp_xoffset_max",
    "pp_yoffset_min",
    "pp_yoffset_max",
    "useragent",
    "br_name",
    "br_family",
    "br_version",
    "br_type",
    "br_renderengine",
    "br_lang",
    "br_features_pdf",
    "br_features_flash",
    "br_features_java",
    "br_features_director",
    "br_features_quicktime",
    "br_features_realplayer",
    "br_features_windowsmedia",
    "br_features_gears",
    "br_features_silverlight",
    "br_cookies",
    "br_colordepth",
    "br_viewwidth",
    "br_viewheight",
    "os_name",
    "os_family",
    "os_manufacturer",
    "os_timezone",
    "dvce_type",
    "dvce_ismobile",
    "dvce_screenwidth",
    "dvce_screenheight",
    "doc_charset",
    "doc_width",
    "doc_height",
]


class PipelineProcessor(EntityProcessor):
    """
    Processor for extracting pipeline and enrichment metadata from Snowplow BDP.

    Coordinates extraction of:
    - Pipelines (DataFlows)
    - Enrichments (DataJobs)
    - Loader and collector jobs
    """

    def __init__(self, deps: "ProcessorDependencies", state: "IngestionState"):
        """
        Initialize pipeline processor.

        Args:
            deps: Explicit dependencies needed by this processor
            state: Shared mutable state populated during extraction
        """
        super().__init__(deps, state)

    def is_enabled(self) -> bool:
        """Check if pipeline or enrichment extraction is enabled."""
        return (
            self.config.extract_pipelines or self.config.extract_enrichments
        ) and self.deps.bdp_client is not None

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract pipeline and enrichment metadata.

        Yields:
            MetadataWorkUnit: Pipeline and enrichment metadata work units
        """
        # Extract pipelines as DataFlow (BDP only)
        if self.config.extract_pipelines:
            yield from self._extract_pipelines()

        # Extract enrichments as DataJobs (BDP only)
        if self.config.extract_enrichments:
            yield from self._extract_enrichments()

    def _extract_pipelines(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract event-specific pipelines as DataFlow entities.

        NEW ARCHITECTURE (per event specification):
        - One DataFlow per event specification (instead of per physical pipeline)
        - DataFlow represents the data flow for a specific event through the pipeline
        - Inputs: event schema + entity schemas from event specification
        - Tagged with physical pipeline name/ID

        This provides better visibility into which entities flow with which events.
        """
        if not self.deps.bdp_client:
            return

        try:
            # Get event specifications (with entity mappings)
            event_specs = self.deps.bdp_client.get_event_specifications()
            logger.info(
                f"Extracting event-specific DataFlows from {len(event_specs)} event specifications"
            )

            # Get physical pipelines (for tagging)
            physical_pipelines = self.deps.bdp_client.get_pipelines()
            self.report.num_pipelines_found = len(physical_pipelines)

            # Use first pipeline as default (most orgs have one pipeline)
            default_pipeline = physical_pipelines[0] if physical_pipelines else None

            # Cache physical pipeline info for later use by enrichments
            self.state.physical_pipeline = default_pipeline

            # Create DataFlow for each event specification that has entity data
            for event_spec in event_specs:
                # Only create DataFlow if event specification has entity information
                # (otherwise it's just documentation without lineage value)
                event_iglu_uri = event_spec.get_event_iglu_uri()
                entity_iglu_uris = event_spec.get_entity_iglu_uris()

                if not event_iglu_uri:
                    logger.debug(
                        f"Skipping event spec {event_spec.name} - no event URI found"
                    )
                    continue

                self.report.report_pipeline_found()

                # Create DataFlow URN for this event specification
                # Include pipeline ID to ensure uniqueness across multiple pipelines
                pipeline_id = default_pipeline.id if default_pipeline else "unknown"
                dataflow_urn = make_data_flow_urn(
                    orchestrator="snowplow",
                    flow_id=f"{pipeline_id}_event_{event_spec.id}",
                    cluster=self.config.env,
                    platform_instance=self.config.platform_instance,
                )

                # Store mapping for enrichment extraction
                self.state.event_spec_dataflow_urns[event_spec.id] = dataflow_urn

                # Build custom properties
                custom_properties = {
                    "eventSpecId": event_spec.id,
                    "eventIgluUri": event_iglu_uri,
                    "entityCount": str(len(entity_iglu_uris)),
                }

                # Tag with physical pipeline info
                if default_pipeline:
                    custom_properties["physicalPipelineId"] = default_pipeline.id
                    custom_properties["physicalPipelineName"] = default_pipeline.name
                    custom_properties["pipelineStatus"] = default_pipeline.status

                    if default_pipeline.label:
                        custom_properties["pipelineLabel"] = default_pipeline.label

                    if (
                        default_pipeline.config
                        and default_pipeline.config.collector_endpoints
                    ):
                        custom_properties["collectorEndpoints"] = ", ".join(
                            default_pipeline.config.collector_endpoints
                        )

                # Add entity URIs as custom properties (for reference)
                if entity_iglu_uris:
                    custom_properties["entityIgluUris"] = ", ".join(
                        entity_iglu_uris[:5]
                    )  # Limit to avoid too long
                    if len(entity_iglu_uris) > 5:
                        custom_properties["entityIgluUris"] += (
                            f" (+{len(entity_iglu_uris) - 5} more)"
                        )

                # Build description
                pipeline_name = (
                    default_pipeline.name if default_pipeline else "Unknown Pipeline"
                )
                description = f"Snowplow event flow for '{event_spec.name}'"
                if entity_iglu_uris:
                    description += f" with {len(entity_iglu_uris)} entity context(s)"
                if default_pipeline:
                    description += f" (Pipeline: {pipeline_name})"

                # Emit DataFlow info
                dataflow_info = DataFlowInfoClass(
                    name=f"{event_spec.name} ({pipeline_name})",  # Include pipeline name for clarity
                    description=description,
                    customProperties=custom_properties,
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataflow_urn,
                    aspect=dataflow_info,
                ).as_workunit()

                # Emit status
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataflow_urn,
                    aspect=StatusClass(removed=False),
                ).as_workunit()

                # Link to organization container
                if self.config.bdp_connection:
                    org_container_urn = self.urn_factory.make_organization_urn(
                        self.config.bdp_connection.organization_id
                    )
                    container_aspect = ContainerClass(container=org_container_urn)
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataflow_urn,
                        aspect=container_aspect,
                    ).as_workunit()

                logger.debug(
                    f"Extracted event-specific DataFlow: {event_spec.name} (event_spec_id={event_spec.id})"
                )
                self.report.report_pipeline_extracted()

        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="extract event-specific pipelines",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
            )

    def _get_warehouse_table_urn(self) -> Optional[str]:
        """
        Get URN for the warehouse atomic events table (where enriched data lands).

        Uses caching to avoid redundant API calls - the warehouse table is the same
        across all enrichments and the loader job.

        Priority:
        1. Use destinations API (most accurate - actual pipeline configuration)
        2. Fall back to organization's warehouse source (from organizations API)
        3. Return None if none available

        Returns:
            Warehouse table URN, or None if not available
        """
        # Return cached URN if available
        cached_warehouse_urn = self.cache.get("warehouse_table_urn")
        if cached_warehouse_urn is not None:
            return cached_warehouse_urn

        # Try destinations API first (most accurate)
        if self.deps.bdp_client:
            try:
                destinations = self.deps.bdp_client.get_destinations()

                if destinations:
                    # Get the first active destination
                    # TODO: Support multiple destinations per pipeline
                    destination = destinations[0]

                    # Map destination type to DataHub platform
                    # Sanitize platform name: lowercase, replace spaces/hyphens with underscores
                    destination_type_clean = (
                        destination.destination_type.lower()
                        .replace(" ", "_")
                        .replace("-", "_")
                    )
                    warehouse_platform = WAREHOUSE_PLATFORM_MAP.get(
                        destination_type_clean, destination_type_clean
                    )

                    # Extract database and schema from destination config
                    config = destination.target.config
                    database = config.get("database", "unknown_db")
                    schema = config.get("schema", "unknown_schema")

                    # Build warehouse table name: {database}.{schema}.events
                    # Snowplow typically loads to 'events' table
                    # Lowercase to match Snowflake connector URN format
                    warehouse_table_name = f"{database}.{schema}.events".lower()

                    # Generate warehouse table URN
                    warehouse_urn = make_dataset_urn_with_platform_instance(
                        platform=warehouse_platform,
                        name=warehouse_table_name,
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )

                    # Cache the URN for reuse
                    self.cache.set("warehouse_table_urn", warehouse_urn)

                    logger.info(
                        f"Found warehouse destination via destinations API: {warehouse_platform}:{warehouse_table_name}"
                    )
                    return warehouse_urn

                logger.debug("No destinations found via destinations API")

            except Exception as e:
                logger.warning(
                    f"Failed to get warehouse destination from destinations API: {e}"
                )

        # Cache None to avoid repeated failed API calls
        self.cache.set("warehouse_table_urn", None)
        return None

    def _extract_enrichments(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract enrichments as DataJob entities in per-Event-Spec DataFlows.

        NEW ARCHITECTURE (per Event Spec):
        - Creates one set of enrichments PER Event Spec
        - Each enrichment is a DataJob within the Event Spec's DataFlow
        - Input: Single Event Spec dataset (with its fields)
        - Output: Warehouse table (with enriched fields added)

        Flow: Atomic Event + Schemas → Event Spec → Enrichments → Warehouse

        Example: [Event Spec: Add to Cart] → [IP Lookup DataJob] → [Warehouse]
        """
        if not self.deps.bdp_client:
            return

        try:
            # Get warehouse destination table URN (if configured)
            warehouse_table_urn = self._get_warehouse_table_urn()
            if warehouse_table_urn:
                logger.info(
                    f"✅ Enrichments will output to warehouse table: {warehouse_table_urn}"
                )
                logger.info("✅ Column-level lineage will be extracted for enrichments")
            else:
                logger.warning(
                    "⚠️  No warehouse table URN found - column-level lineage will NOT be extracted"
                )
                logger.warning(
                    "⚠️  To enable column-level lineage, ensure your Snowplow pipeline has a destination configured"
                )

            # Get enrichments from physical pipeline (enrichments configured once, apply to all events)
            if not self.state.physical_pipeline:
                logger.warning(
                    "No physical pipeline found - cannot extract enrichments. "
                    "Make sure _extract_pipelines() ran first."
                )
                return

            if not self.state.emitted_event_spec_ids:
                logger.warning(
                    "No event spec IDs available - cannot extract enrichments. "
                    "Make sure event specs were extracted first."
                )
                return

            if not self.state.event_spec_dataflow_urns:
                logger.warning(
                    "No event spec DataFlow URNs available - cannot extract enrichments. "
                    "Make sure _extract_pipelines() ran first."
                )
                return

            enrichments = self.deps.bdp_client.get_enrichments(
                self.state.physical_pipeline.id
            )
            total_enrichments = len(enrichments)
            logger.info(
                f"Found {total_enrichments} enrichments in pipeline {self.state.physical_pipeline.name}"
            )

            # Create enrichments PER Event Spec (instead of once for all Event Specs)
            # Each Event Spec gets its own set of enrichments in its own DataFlow
            for event_spec_id in self.state.emitted_event_spec_ids:
                # Get the DataFlow URN for this Event Spec
                event_spec_dataflow_urn = self.state.event_spec_dataflow_urns.get(
                    event_spec_id
                )
                if not event_spec_dataflow_urn:
                    logger.warning(
                        f"No DataFlow URN found for event spec {event_spec_id} - skipping enrichments"
                    )
                    continue

                # Generate Event Spec dataset URN (same as in event_spec_processor.py)
                event_spec_urn = self.urn_factory.make_event_spec_dataset_urn(
                    event_spec_id
                )

                logger.debug(
                    f"Creating enrichments for Event Spec {event_spec_id} with DataFlow {event_spec_dataflow_urn}"
                )

                # Emit all enrichments for this Event Spec
                for enrichment in enrichments:
                    self.report.report_enrichment_found()

                    if not enrichment.enabled:
                        self.report.report_enrichment_filtered(enrichment.filename)
                        logger.debug(
                            f"Skipping disabled enrichment: {enrichment.filename}"
                        )
                        continue

                    # Emit enrichment DataJob with all aspects
                    # This enrichment reads from THIS Event Spec and writes to warehouse
                    yield from self._emit_enrichment_datajob(
                        enrichment=enrichment,
                        dataflow_urn=event_spec_dataflow_urn,  # Use Event Spec's DataFlow
                        input_dataset_urns=[
                            event_spec_urn
                        ],  # Read from THIS Event Spec only
                        warehouse_table_urn=warehouse_table_urn,
                    )

            self.report.num_enrichments_found = total_enrichments

            # Emit Loader jobs (one per Event Spec → warehouse)
            if warehouse_table_urn:
                for event_spec_id in self.state.emitted_event_spec_ids:
                    event_spec_dataflow_urn = self.state.event_spec_dataflow_urns.get(
                        event_spec_id
                    )
                    if not event_spec_dataflow_urn:
                        continue

                    event_spec_urn = self.urn_factory.make_event_spec_dataset_urn(
                        event_spec_id
                    )

                    yield from self._emit_loader_datajob(
                        dataflow_urn=event_spec_dataflow_urn,  # Use Event Spec's DataFlow
                        input_dataset_urns=[
                            event_spec_urn
                        ],  # Load from THIS Event Spec only
                        warehouse_table_urn=warehouse_table_urn,
                    )

        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="extract enrichments",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
            )

    def _setup_pipeline_and_lineage(self) -> Iterable[MetadataWorkUnit]:
        """
        DEPRECATED: No longer used with per-Event-Spec pipeline architecture.

        Previously created a single global Pipeline DataFlow for all enrichments.
        Now each Event Spec has its own DataFlow (created in _extract_pipelines).

        This method remains for reference but is not called.

        Setup pipeline DataFlow for organizing enrichments and loader jobs.

        With Option A architecture, Event Specs already have lineage from schemas,
        so we only need to create the Pipeline DataFlow container for enrichments.

        Flow: Atomic Event + Schemas → Event Specs → Enrichments → Warehouse
        """
        if not self.state.physical_pipeline:
            logger.debug("No physical pipeline - skipping pipeline setup")
            return

        if not self.state.emitted_event_spec_urns:
            logger.debug("No event spec URNs available - skipping pipeline setup")
            return

        # Create DataFlow for the pipeline
        pipeline_dataflow_urn = make_data_flow_urn(
            orchestrator="snowplow",
            flow_id=f"{self.state.physical_pipeline.id}_pipeline",
            cluster=self.config.env,
        )

        # Store for enrichments to use
        self.state.pipeline_dataflow_urn = pipeline_dataflow_urn

        # Emit DataFlow info
        dataflow_info = DataFlowInfoClass(
            name=f"Snowplow Pipeline ({self.state.physical_pipeline.name})",
            description=(
                f"Snowplow event processing pipeline for {self.state.physical_pipeline.name}.\n\n"
                "This pipeline processes events through:\n"
                "1. **Atomic Event**: Standard Snowplow atomic event fields\n"
                "2. **Schemas**: Event and entity schemas define custom fields\n"
                "3. **Event Specs**: Combine Atomic Event + Schemas (superset of all fields)\n"
                "4. **Enrichments**: Add computed fields (IP Lookup, UA Parser, Campaign Attribution, etc.)\n"
                "5. **Loader**: Writes enriched data to warehouse\n\n"
                "Flow: Atomic Event + Schemas → Event Specs → Enrichments → Warehouse"
            ),
            customProperties={
                "pipelineId": self.state.physical_pipeline.id,
                "pipelineName": self.state.physical_pipeline.name,
                "platform": "snowplow",
                "eventSpecCount": str(len(self.state.emitted_event_spec_urns)),
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=pipeline_dataflow_urn,
            aspect=dataflow_info,
        ).as_workunit()

        # Emit container (organization)
        if self.config.bdp_connection:
            org_container_urn = self.urn_factory.make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=pipeline_dataflow_urn,
                aspect=ContainerClass(container=org_container_urn),
            ).as_workunit()

        logger.info(
            f"✅ Created Pipeline DataFlow for {len(self.state.emitted_event_spec_urns)} event spec(s)"
        )

    def _emit_enrichment_datajob(
        self,
        enrichment: Enrichment,
        dataflow_urn: str,
        input_dataset_urns: List[str],
        warehouse_table_urn: Optional[str],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit all aspects for a single enrichment DataJob."""
        # Create DataJob URN
        datajob_urn = make_data_job_urn_with_flow(
            flow_urn=dataflow_urn,
            job_id=enrichment.id,
        )

        # Extract field-level lineage
        # Enrichments read from Event Spec datasets and write enriched fields to warehouse
        logger.debug(
            f"Extracting field lineage for {enrichment.filename}: "
            f"inputs={len(input_dataset_urns)}, warehouse_urn={'SET' if warehouse_table_urn else 'NONE'}"
        )
        fine_grained_lineages = self._extract_enrichment_field_lineage(
            enrichment=enrichment,
            event_schema_urns=input_dataset_urns,  # Event Spec datasets as inputs
            warehouse_table_urn=warehouse_table_urn,  # Warehouse table as output
        )
        if fine_grained_lineages:
            logger.info(
                f"✅ Extracted {len(fine_grained_lineages)} column-level lineages for {enrichment.filename}"
            )
        else:
            logger.debug(f"No column-level lineage extracted for {enrichment.filename}")

        # Build custom properties
        custom_properties = {
            "enrichmentId": enrichment.id,
            "filename": enrichment.filename,
            "enabled": str(enrichment.enabled),
            "lastUpdate": enrichment.last_update,
        }

        if self.state.physical_pipeline:
            custom_properties["pipelineId"] = self.state.physical_pipeline.id
            custom_properties["pipelineName"] = self.state.physical_pipeline.name

        # Extract enrichment name and description
        enrichment_name = enrichment.filename
        description = f"Snowplow enrichment: {enrichment_name}"

        if enrichment.content and enrichment.content.data:
            enrichment_name = enrichment.content.data.name
            custom_properties["vendor"] = enrichment.content.data.vendor
            custom_properties["schema"] = enrichment.content.schema_ref

            if enrichment.content.data.parameters:
                params_str = str(enrichment.content.data.parameters)
                if len(params_str) > 500:
                    params_str = params_str[:500] + "..."
                custom_properties["parameters"] = params_str

            description = f"{enrichment_name} enrichment"

        if fine_grained_lineages:
            description = self.deps.lineage_builder.build_enrichment_description(
                enrichment_name=enrichment_name,
                fine_grained_lineages=fine_grained_lineages,
            )

        # Emit DataJob info
        datajob_info = DataJobInfoClass(
            name=enrichment_name,
            type="ENRICHMENT",
            description=description,
            customProperties=custom_properties,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=datajob_info,
        ).as_workunit()

        # Emit ownership if configured
        if self.config.enrichment_owner:
            ownership = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=make_user_urn(self.config.enrichment_owner),
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ],
                lastModified=AuditStampClass(
                    time=int(time.time() * 1000),
                    actor=make_user_urn("datahub"),
                ),
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=datajob_urn,
                aspect=ownership,
            ).as_workunit()

        # Emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Emit input/output lineage
        # Input: Event Spec datasets (each contains all fields from its referenced schemas)
        # Output: Warehouse table (with enriched fields added)
        output_datasets = [warehouse_table_urn] if warehouse_table_urn else []
        datajob_input_output = DataJobInputOutputClass(
            inputDatasets=input_dataset_urns,
            outputDatasets=output_datasets,
            fineGrainedLineages=fine_grained_lineages
            if fine_grained_lineages
            else None,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=datajob_input_output,
        ).as_workunit()

        if warehouse_table_urn:
            logger.debug(
                f"Linked enrichment {enrichment_name}: Event → warehouse table"
            )
        else:
            logger.debug(
                f"Linked enrichment {enrichment_name}: Event (no warehouse output)"
            )

        logger.debug(f"Extracted enrichment: {enrichment_name}")
        self.report.report_enrichment_extracted()

    def _emit_loader_datajob(
        self,
        dataflow_urn: str,
        input_dataset_urns: List[str],
        warehouse_table_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit Loader DataJob that loads enriched events from Event Spec datasets to warehouse table.

        This job models the Loader stage of the Snowplow pipeline which writes the fully
        processed and enriched events to the data warehouse (Snowflake, BigQuery, etc.).

        Inputs: Event Spec datasets (all fields including enriched fields)
        Output: Warehouse table
        """
        # Create DataJob URN
        datajob_urn = make_data_job_urn_with_flow(
            flow_urn=dataflow_urn,
            job_id="loader",
        )

        # Emit DataJob info
        datajob_info = DataJobInfoClass(
            name="Loader",
            type="BATCH_SCHEDULED",
            description=(
                "Snowplow Loader that writes enriched events to the data warehouse.\n\n"
                "**Input**: Event Spec datasets (all fields from schemas + enriched fields)\n"
                "**Output**: Data warehouse table (e.g., Snowflake atomic.events)\n\n"
                "The Loader stage runs periodically and writes all processed events to the warehouse "
                "where they can be queried by analytics tools."
            ),
            customProperties={
                "pipelineId": self.state.physical_pipeline.id
                if self.state.physical_pipeline
                else "unknown",
                "pipelineName": self.state.physical_pipeline.name
                if self.state.physical_pipeline
                else "unknown",
                "stage": "loader",
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=datajob_info,
        ).as_workunit()

        # Emit input/output lineage
        # The Loader passes all fields from Event Spec datasets to warehouse table
        datajob_input_output = DataJobInputOutputClass(
            inputDatasets=input_dataset_urns,
            outputDatasets=[warehouse_table_urn],
            fineGrainedLineages=None,  # Could add field-level lineage here in future
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=datajob_input_output,
        ).as_workunit()

        # Emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        logger.info(f"✅ Created Loader job: Event → {warehouse_table_urn}")

    def _extract_enrichment_field_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urns: List[str],
        warehouse_table_urn: Optional[str],
    ) -> List[FineGrainedLineageClass]:
        """
        Extract field-level lineage for an enrichment using registered extractors.

        Args:
            enrichment: The enrichment to extract lineage for
            event_schema_urns: Event Spec dataset URNs (all event specs have the same atomic fields)
            warehouse_table_urn: Warehouse table URN (output for enriched fields)

        Returns:
            List of FineGrainedLineageClass objects representing field transformations
        """
        fine_grained_lineages: List[FineGrainedLineageClass] = []

        # Get extractor for this enrichment type
        extractor = self.deps.enrichment_lineage_registry.get_extractor(enrichment)
        if not extractor:
            logger.debug(
                f"No lineage extractor found for enrichment schema: {enrichment.schema_ref}"
            )
            return fine_grained_lineages

        # IMPORTANT: Enrichments read from Event Spec fields (user_ipaddress, page_urlquery, etc.),
        # which are standard Snowplow atomic fields present on every event spec, and write enriched
        # fields to the warehouse table.
        #
        # Each Event Spec contains all fields from:
        # - Atomic Event: Standard atomic fields (user_ipaddress, page_urlquery, etc.)
        # - Referenced Schemas: Custom event and entity fields
        #
        # Enrichments transform:
        # - Input: Event Spec fields (e.g., user_ipaddress from Event Spec)
        # - Output: Enriched fields in warehouse table (e.g., geo_country in warehouse)
        if not event_schema_urns:
            logger.debug(
                "No event spec URNs available - skipping field lineage extraction"
            )
            return fine_grained_lineages

        if not warehouse_table_urn:
            logger.debug(
                "No warehouse table URN available - skipping field lineage extraction"
            )
            return fine_grained_lineages

        # Use the first event spec URN for field lineage extraction
        # All event specs have the same atomic fields (Atomic Event), so we can use any one
        event_spec_urn = event_schema_urns[0]

        # Extract field lineages using the registered extractor
        # Pass event_spec_urn as input and warehouse_table_urn as output
        try:
            field_lineages = extractor.extract_lineage(
                enrichment=enrichment,
                event_schema_urn=event_spec_urn,  # Use Event Spec as source for fields
                warehouse_table_urn=warehouse_table_urn,  # Use warehouse table as output for enriched fields
            )

            # Convert FieldLineage objects to DataHub FineGrainedLineageClass
            for field_lineage in field_lineages:
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=field_lineage.upstream_fields,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                        downstreams=field_lineage.downstream_fields,
                        transformOperation=field_lineage.transformation_type,
                    )
                )

            if fine_grained_lineages:
                logger.debug(
                    f"Extracted {len(fine_grained_lineages)} field lineages for enrichment {enrichment.filename}"
                )

        except Exception as e:
            logger.warning(
                f"Failed to extract field lineage for enrichment {enrichment.filename}: {e}",
                exc_info=True,
            )

        return fine_grained_lineages
