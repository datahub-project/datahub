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
from typing import TYPE_CHECKING, Iterable, List, Optional, Sequence

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.constants import (
    SNOWPLOW_STANDARD_COLUMNS,
    WAREHOUSE_PLATFORM_MAP,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import (
    Enrichment,
)
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
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
        Extract physical pipeline as a single DataFlow entity.

        OPTION A ARCHITECTURE (Single Physical Pipeline):
        - One DataFlow per physical pipeline (typically just one)
        - Represents the entire Snowplow event processing pipeline
        - All event specs share the same enrichments in a single pipeline
        - Scales O(enrichments) instead of O(events × enrichments)

        This matches Snowplow's physical reality: one pipeline processes ALL events.
        """
        if not self.deps.bdp_client:
            return

        try:
            # Get physical pipelines
            physical_pipelines = self.deps.bdp_client.get_pipelines()
            self.report.num_pipelines_found = len(physical_pipelines)

            if not physical_pipelines:
                logger.warning(
                    "No physical pipelines found - skipping pipeline extraction"
                )
                return

            # Use first pipeline (most orgs have one pipeline)
            pipeline = physical_pipelines[0]
            self.state.physical_pipeline = pipeline

            # Get event specifications count for metadata
            event_specs = self.deps.bdp_client.get_event_specifications()
            event_spec_count = len(event_specs)
            logger.info(
                f"Creating single DataFlow for pipeline '{pipeline.name}' "
                f"(covers {event_spec_count} event specifications)"
            )

            self.report.report_pipeline_found()

            # Create single DataFlow URN for the physical pipeline
            dataflow_urn = make_data_flow_urn(
                orchestrator="snowplow",
                flow_id=pipeline.id,
                cluster=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            # Store single pipeline DataFlow URN
            self.state.pipeline_dataflow_urn = dataflow_urn

            # Track event spec IDs for enrichment extraction
            # When extract_event_specifications is enabled, EventSpecProcessor runs first
            # and populates emitted_event_spec_ids. Otherwise, we track all event specs.
            if not self.state.emitted_event_spec_ids:
                for event_spec in event_specs:
                    self.state.emitted_event_spec_ids.add(event_spec.id)

            # Build custom properties
            custom_properties = {
                "pipelineId": pipeline.id,
                "pipelineName": pipeline.name,
                "pipelineStatus": pipeline.status,
                "eventSpecCount": str(event_spec_count),
                "architecture": "single_physical_pipeline",
            }

            if pipeline.label:
                custom_properties["pipelineLabel"] = pipeline.label

            if pipeline.config and pipeline.config.collector_endpoints:
                custom_properties["collectorEndpoints"] = ", ".join(
                    pipeline.config.collector_endpoints
                )

            # Build description
            description = (
                f"Snowplow event processing pipeline: {pipeline.name}\n\n"
                f"This pipeline processes **{event_spec_count} event specifications** through:\n"
                "1. **Collector**: Receives raw events from trackers\n"
                "2. **Enrichments**: Add computed fields (IP Lookup, UA Parser, etc.)\n"
                "3. **Loader**: Writes enriched data to warehouse\n\n"
                "All events share the same enrichments - this is how Snowplow works physically."
            )

            # Build container aspect conditionally
            container_aspect = None
            if self.config.bdp_connection:
                org_container_urn = self.urn_factory.make_organization_urn(
                    self.config.bdp_connection.organization_id
                )
                container_aspect = ContainerClass(container=org_container_urn)

            # Emit DataFlow aspects
            yield from self.emit_aspects(
                entity_urn=dataflow_urn,
                aspects=[
                    DataFlowInfoClass(
                        name=f"Snowplow Pipeline: {pipeline.name}",
                        description=description,
                        customProperties=custom_properties,
                    ),
                    StatusClass(removed=False),
                    container_aspect,
                ],
            )

            logger.info(
                f"✅ Created single DataFlow for pipeline '{pipeline.name}' "
                f"(will contain enrichments shared by {event_spec_count} event specs)"
            )
            self.report.report_pipeline_extracted()

        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="extract physical pipeline",
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
        Extract enrichments as DataJob entities in the single physical pipeline DataFlow.

        OPTION A ARCHITECTURE (Single Physical Pipeline):
        - Creates enrichments ONCE for the entire pipeline
        - All event specs share the same enrichments (this is how Snowplow works physically)
        - Scales O(enrichments) instead of O(events × enrichments)

        Flow: ALL Event Specs → Enrichments → Warehouse

        Example: [Event Specs: Add to Cart, Page View, ...] → [IP Lookup DataJob] → [Warehouse]
        """
        if not self.deps.bdp_client:
            return

        try:
            # Get warehouse destination table URN (if configured)
            warehouse_table_urn = self._get_warehouse_table_urn()
            if warehouse_table_urn:
                # Store in state for column lineage builder
                self.state.warehouse_table_urn = warehouse_table_urn
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

            # Validate prerequisites for enrichment extraction
            if not self.state.physical_pipeline:
                logger.warning(
                    "No physical pipeline found - cannot extract enrichments. "
                    "Make sure _extract_pipelines() ran first."
                )
                return

            if not self.state.pipeline_dataflow_urn:
                logger.warning(
                    "No pipeline DataFlow URN available - cannot extract enrichments. "
                    "Make sure _extract_pipelines() ran first."
                )
                return

            if not self.state.emitted_event_spec_ids:
                logger.warning(
                    "No event spec IDs available - cannot extract enrichments. "
                    "Make sure event specs were extracted first."
                )
                return

            # Get enrichments from physical pipeline
            enrichments = self.deps.bdp_client.get_enrichments(
                self.state.physical_pipeline.id
            )
            total_enrichments = len(enrichments)
            logger.info(
                f"Found {total_enrichments} enrichments in pipeline {self.state.physical_pipeline.name}"
            )
            self.report.num_enrichments_found = total_enrichments

            # Collect ALL event spec URNs as inputs (shared by all enrichments)
            all_event_spec_urns = [
                self.urn_factory.make_event_spec_dataset_urn(event_spec_id)
                for event_spec_id in self.state.emitted_event_spec_ids
            ]

            logger.info(
                f"Creating enrichments ONCE for pipeline (shared by {len(all_event_spec_urns)} event specs)"
            )

            # Emit enrichments ONCE for the entire pipeline
            # All event specs share the same enrichments (this is how Snowplow works physically)
            for enrichment in enrichments:
                self.report.report_enrichment_found()

                if not enrichment.enabled:
                    self.report.report_enrichment_filtered(enrichment.filename)
                    logger.debug(f"Skipping disabled enrichment: {enrichment.filename}")
                    continue

                # Emit enrichment DataJob with all aspects
                # Input: ALL event specs (they all go through the same enrichments)
                # Output: Warehouse table (where enriched data lands)
                yield from self._emit_enrichment_datajob(
                    enrichment=enrichment,
                    dataflow_urn=self.state.pipeline_dataflow_urn,  # Single pipeline
                    input_dataset_urns=all_event_spec_urns,  # ALL event specs
                    warehouse_table_urn=warehouse_table_urn,
                )

            # Emit Collector job (receives raw events from trackers)
            yield from self._emit_collector_datajob(
                dataflow_urn=self.state.pipeline_dataflow_urn,  # Single pipeline
                output_dataset_urns=all_event_spec_urns,  # ALL event specs
            )

            # Emit single Loader job (loads all enriched events to warehouse)
            if warehouse_table_urn:
                yield from self._emit_loader_datajob(
                    dataflow_urn=self.state.pipeline_dataflow_urn,  # Single pipeline
                    input_dataset_urns=all_event_spec_urns,  # ALL event specs
                    warehouse_table_urn=warehouse_table_urn,
                )

        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="extract enrichments",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
            )

    def _emit_enrichment_datajob(
        self,
        enrichment: Enrichment,
        dataflow_urn: str,
        input_dataset_urns: Sequence[str],
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
            event_schema_urns=list(input_dataset_urns),  # Event Spec datasets as inputs
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

        # Build ownership aspect conditionally
        ownership_aspect = None
        if self.config.enrichment_owner:
            ownership_aspect = OwnershipClass(
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

        # Build input/output lineage
        output_datasets = [warehouse_table_urn] if warehouse_table_urn else []

        # Emit all DataJob aspects using SDK V2 batching pattern
        yield from self.emit_aspects(
            entity_urn=datajob_urn,
            aspects=[
                DataJobInfoClass(
                    name=enrichment_name,
                    type="ENRICHMENT",
                    description=description,
                    customProperties=custom_properties,
                ),
                ownership_aspect,
                StatusClass(removed=False),
                DataJobInputOutputClass(
                    inputDatasets=list(input_dataset_urns),
                    outputDatasets=output_datasets,
                    fineGrainedLineages=fine_grained_lineages
                    if fine_grained_lineages
                    else None,
                ),
            ],
        )

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

    def _emit_collector_datajob(
        self,
        dataflow_urn: str,
        output_dataset_urns: Sequence[str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit Collector DataJob that receives raw events from trackers.

        The Collector is the entry point of the Snowplow pipeline. It receives
        events via HTTP from trackers (web, mobile, server-side SDKs) and outputs
        them for validation and enrichment.

        Inputs: External (trackers via HTTP - not modeled as datasets)
        Outputs: Event Spec datasets (raw events before enrichment)
        """
        # Create DataJob URN
        datajob_urn = make_data_job_urn_with_flow(
            flow_urn=dataflow_urn,
            job_id="collector",
        )

        # Get collector endpoints from pipeline config
        collector_endpoints: List[str] = []
        if (
            self.state.physical_pipeline
            and self.state.physical_pipeline.config
            and self.state.physical_pipeline.config.collector_endpoints
        ):
            collector_endpoints = (
                self.state.physical_pipeline.config.collector_endpoints
            )

        # Build custom properties
        custom_properties = {
            "pipelineId": self.state.physical_pipeline.id
            if self.state.physical_pipeline
            else "unknown",
            "pipelineName": self.state.physical_pipeline.name
            if self.state.physical_pipeline
            else "unknown",
            "stage": "collector",
        }

        if collector_endpoints:
            custom_properties["collectorEndpoints"] = ", ".join(collector_endpoints)
            custom_properties["endpointCount"] = str(len(collector_endpoints))

        # Build description
        description = (
            "Snowplow Collector that receives raw events from trackers.\n\n"
            "**Input**: HTTP requests from trackers (web, mobile, server-side SDKs)\n"
            "**Output**: Event Spec datasets (raw events for validation and enrichment)\n\n"
            "The Collector is the entry point of the Snowplow pipeline. It:\n"
            "- Receives events from multiple sources via HTTP POST/GET\n"
            "- Validates basic request format\n"
            "- Attaches collector payload metadata (collector timestamp, IP, etc.)\n"
            "- Outputs events to the enrichment stream"
        )

        if collector_endpoints:
            endpoints_list = "\n".join(
                f"- {endpoint}" for endpoint in collector_endpoints
            )
            description += f"\n\n**Collector Endpoints**:\n{endpoints_list}"

        # Emit all Collector DataJob aspects
        yield from self.emit_aspects(
            entity_urn=datajob_urn,
            aspects=[
                DataJobInfoClass(
                    name="Collector",
                    type="STREAMING",  # Collector is a real-time streaming ingestion
                    description=description,
                    customProperties=custom_properties,
                ),
                DataJobInputOutputClass(
                    inputDatasets=[],  # Collector receives from external HTTP - no dataset inputs
                    outputDatasets=list(output_dataset_urns),
                    fineGrainedLineages=None,
                ),
                StatusClass(removed=False),
            ],
        )

        logger.info(
            f"✅ Created Collector job with {len(collector_endpoints)} endpoints "
            f"→ {len(output_dataset_urns)} event specs"
        )

    def _emit_loader_datajob(
        self,
        dataflow_urn: str,
        input_dataset_urns: Sequence[str],
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

        # Emit all Loader DataJob aspects using SDK V2 batching pattern
        yield from self.emit_aspects(
            entity_urn=datajob_urn,
            aspects=[
                DataJobInfoClass(
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
                ),
                DataJobInputOutputClass(
                    inputDatasets=list(input_dataset_urns),
                    outputDatasets=[warehouse_table_urn],
                    fineGrainedLineages=None,
                ),
                StatusClass(removed=False),
            ],
        )

        logger.info(f"✅ Created Loader job: Event → {warehouse_table_urn}")

    def _extract_enrichment_field_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urns: List[str],
        warehouse_table_urn: Optional[str],
    ) -> List[FineGrainedLineageClass]:
        """
        Extract field-level lineage for an enrichment using registered extractors.

        Creates lineage entries that connect ALL event specs to the warehouse table,
        since all event specs share the same atomic fields (user_ipaddress, page_urlquery, etc.)
        that enrichments read from.

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

        # Use the first event spec URN for field lineage extraction template
        # All event specs have the same atomic fields (Atomic Event), so we can use any one
        # to discover the field names, then expand to all event specs
        template_event_spec_urn = event_schema_urns[0]

        # Extract field lineages using the registered extractor
        # Pass template_event_spec_urn as input and warehouse_table_urn as output
        try:
            field_lineages = extractor.extract_lineage(
                enrichment=enrichment,
                event_schema_urn=template_event_spec_urn,  # Use one Event Spec as template
                warehouse_table_urn=warehouse_table_urn,  # Use warehouse table as output for enriched fields
            )

            # Convert FieldLineage objects to DataHub FineGrainedLineageClass
            # IMPORTANT: Expand upstream field URNs to include ALL event specs
            # This ensures lineage shows correctly for every event spec, not just the first
            for field_lineage in field_lineages:
                # Expand upstream fields to include same field from ALL event specs
                expanded_upstreams = self._expand_field_urns_to_all_event_specs(
                    template_field_urns=field_lineage.upstream_fields,
                    template_event_spec_urn=template_event_spec_urn,
                    all_event_spec_urns=event_schema_urns,
                )

                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=expanded_upstreams,  # Include fields from ALL event specs
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                        downstreams=field_lineage.downstream_fields,
                        transformOperation=field_lineage.transformation_type,
                    )
                )

            if fine_grained_lineages:
                logger.debug(
                    f"Extracted {len(fine_grained_lineages)} field lineages for enrichment {enrichment.filename} "
                    f"(expanded to {len(event_schema_urns)} event specs)"
                )

        except Exception as e:
            logger.warning(
                f"Failed to extract field lineage for enrichment {enrichment.filename}: {e}",
                exc_info=True,
            )

        return fine_grained_lineages

    def _expand_field_urns_to_all_event_specs(
        self,
        template_field_urns: List[str],
        template_event_spec_urn: str,
        all_event_spec_urns: List[str],
    ) -> List[str]:
        """
        Expand field URNs from one event spec to include the same fields from relevant event specs.

        Expansion rules:
        - ATOMIC fields (user_ipaddress, useragent, etc.): Expand to ALL event specs
        - CUSTOM SCHEMA fields: Expand ONLY to event specs that have the schema containing the field

        This ensures:
        - Atomic fields: lineage shows correctly for ALL event specs (they all have atomic fields)
        - Custom fields: lineage shows ONLY for event specs that actually have the field

        Args:
            template_field_urns: Field URNs from the template event spec
            template_event_spec_urn: The URN of the template event spec used to extract fields
            all_event_spec_urns: All event spec URNs to potentially expand to

        Returns:
            Expanded list of field URNs with correct event spec targeting
        """
        expanded_urns: List[str] = []

        for template_field_urn in template_field_urns:
            # Extract field name from URN
            # Field URN format: urn:li:schemaField:(dataset_urn,field_name)
            field_name = self._extract_field_name_from_urn(template_field_urn)

            if field_name and field_name in SNOWPLOW_STANDARD_COLUMNS:
                # Atomic field: expand to ALL event specs (they all have this field)
                for event_spec_urn in all_event_spec_urns:
                    expanded_urn = template_field_urn.replace(
                        template_event_spec_urn, event_spec_urn
                    )
                    expanded_urns.append(expanded_urn)
            else:
                # Custom schema field: find which event specs have this field
                event_specs_with_field = self._find_event_specs_with_field(
                    field_name=field_name,
                    all_event_spec_urns=all_event_spec_urns,
                )

                if event_specs_with_field:
                    # Expand only to event specs that have this field
                    for event_spec_urn in event_specs_with_field:
                        expanded_urn = template_field_urn.replace(
                            template_event_spec_urn, event_spec_urn
                        )
                        expanded_urns.append(expanded_urn)
                    logger.debug(
                        f"Custom field '{field_name}' expanded to {len(event_specs_with_field)} "
                        f"event specs (out of {len(all_event_spec_urns)} total)"
                    )
                else:
                    # Fallback: keep template if we can't determine which event specs have it
                    expanded_urns.append(template_field_urn)
                    if field_name:
                        logger.debug(
                            f"Custom field '{field_name}' - could not determine event specs, "
                            f"keeping template only"
                        )

        return expanded_urns

    def _find_event_specs_with_field(
        self,
        field_name: Optional[str],
        all_event_spec_urns: List[str],
    ) -> List[str]:
        """
        Find which event specs contain a specific field.

        Uses the state to look up:
        1. Which schemas contain the field (via extracted_schema_fields_by_urn)
        2. Which event specs reference those schemas (via event_spec_to_schema_urns)

        Args:
            field_name: The field name to search for
            all_event_spec_urns: All event spec URNs to check

        Returns:
            List of event spec URNs that have the field
        """
        if not field_name:
            return []

        # Step 1: Find which schemas contain this field
        schemas_with_field: set[str] = set()

        for schema_urn, fields in self.state.extracted_schema_fields_by_urn.items():
            for schema_field in fields:
                # Check if field path matches (could be exact or nested)
                if self._field_matches(schema_field.fieldPath, field_name):
                    schemas_with_field.add(schema_urn)
                    break  # Found in this schema, move to next

        if not schemas_with_field:
            logger.debug(f"Field '{field_name}' not found in any extracted schema")
            return []

        logger.debug(
            f"Field '{field_name}' found in {len(schemas_with_field)} schemas: "
            f"{list(schemas_with_field)[:3]}..."  # Log first 3 for debugging
        )

        # Step 2: Find which event specs reference these schemas
        event_specs_with_field: List[str] = []

        for event_spec_urn in all_event_spec_urns:
            # Get schemas referenced by this event spec
            schema_urns = self.state.event_spec_to_schema_urns.get(event_spec_urn, [])

            # Check if any of them contain our field
            if schemas_with_field.intersection(schema_urns):
                event_specs_with_field.append(event_spec_urn)

        return event_specs_with_field

    def _field_matches(self, field_path: Optional[str], target_field: str) -> bool:
        """
        Check if a field path matches the target field name.

        Handles both exact matches and nested paths:
        - Exact: "customer_email" matches "customer_email"
        - Nested: "checkout.customer_email" matches "customer_email"
        - Prefixed: "contexts_com_acme_checkout_1.customer_email" matches "customer_email"

        Does NOT match partial field names:
        - "customer_email" does NOT match "email" (partial suffix)

        Args:
            field_path: Full field path from schema (e.g., "checkout.customer_email")
            target_field: Target field name to match (e.g., "customer_email")

        Returns:
            True if field matches
        """
        if not field_path or not target_field:
            return False

        # Exact match
        if field_path == target_field:
            return True

        # Check if target is the last segment of a nested path
        # e.g., "checkout.customer_email" ends with ".customer_email"
        # This ensures we only match complete field names after a dot separator
        if field_path.endswith(f".{target_field}"):
            return True

        return False

    def _extract_field_name_from_urn(self, field_urn: str) -> Optional[str]:
        """
        Extract the field name from a schemaField URN.

        Args:
            field_urn: URN like 'urn:li:schemaField:(urn:li:dataset:(...),field_name)'

        Returns:
            Field name (e.g., 'user_ipaddress'), or None if extraction fails
        """
        try:
            # Field URN ends with ,field_name)
            # Find the last comma and extract everything after it until the closing paren
            last_comma_idx = field_urn.rfind(",")
            if last_comma_idx == -1:
                return None
            # Extract field_name from ",field_name)"
            field_name = field_urn[last_comma_idx + 1 : -1]  # Remove , and )
            return field_name.strip()
        except Exception:
            return None
