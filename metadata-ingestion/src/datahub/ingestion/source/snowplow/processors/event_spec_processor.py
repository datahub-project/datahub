"""
Event Specification Processor for Snowplow connector.

Handles extraction of event specifications from Snowplow BDP.
"""

import json
import logging
from typing import TYPE_CHECKING, Iterable, List

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.ingestion.source.snowplow.snowplow_models import EventSpecification
from datahub.metadata.schema_classes import (
    ContainerClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OtherSchemaClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )

logger = logging.getLogger(__name__)


class EventSpecProcessor(EntityProcessor):
    """
    Processor for extracting event specification metadata from Snowplow BDP.

    Event specifications define the structure and rules for events.
    """

    def __init__(self, deps: "ProcessorDependencies", state: "IngestionState"):
        """
        Initialize event spec processor.

        Args:
            deps: Explicit dependencies needed by this processor
            state: Shared mutable state populated during extraction
        """
        super().__init__(deps, state)
        self.event_spec_names: dict[
            str, str
        ] = {}  # Track event spec URN -> name for logging

    def is_enabled(self) -> bool:
        """Check if event specification extraction is enabled."""
        return (
            self.config.extract_event_specifications
            and self.deps.bdp_client is not None
        )

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract event specifications from BDP.

        Event specifications are treated as datasets with references to schemas.

        Yields:
            MetadataWorkUnit: Event specification metadata work units
        """
        if not self.deps.bdp_client:
            return

        try:
            event_specs = self.deps.bdp_client.get_event_specifications()
        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="fetch event specifications",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
            )
            return

        for event_spec in event_specs:
            self.report.report_event_spec_found()

            # Apply filtering
            if not self.config.event_spec_pattern.allowed(event_spec.name):
                self.report.report_event_spec_filtered(event_spec.name)
                continue

            # Track that this event spec was emitted (for container linking and enrichments)
            self.state.emitted_event_spec_ids.add(event_spec.id)

            # Capture first event spec ID and name for parsed events dataset naming
            if self.state.event_spec_id is None:
                self.state.event_spec_id = event_spec.id
                self.state.event_spec_name = event_spec.name

            # Generate and track event spec URN
            event_spec_urn = self.urn_factory.make_event_spec_dataset_urn(event_spec.id)
            self.state.emitted_event_spec_urns.append(event_spec_urn)

            yield from self._process_event_specification(event_spec)

    def _process_event_specification(
        self, event_spec: EventSpecification
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single event specification.

        Args:
            event_spec: Event specification from BDP API

        Yields:
            MetadataWorkUnit: Event specification metadata work units
        """
        # Generate dataset URN for event spec
        dataset_urn = self.urn_factory.make_event_spec_dataset_urn(event_spec.id)

        # Build custom properties
        custom_props = {
            "event_spec_id": event_spec.id,
            "status": event_spec.status or "unknown",
            "created_at": event_spec.created_at or "",
            "updated_at": event_spec.updated_at or "",
        }

        # Add event schema information (new format)
        if event_spec.event:
            event_iglu_uri = event_spec.get_event_iglu_uri()
            if event_iglu_uri:
                custom_props["event_iglu_uri"] = event_iglu_uri

            # Add embedded schema constraints if present
            if "schema" in event_spec.event:
                custom_props["event_schema_constraints"] = json.dumps(
                    event_spec.event["schema"]
                )

        # Add entity schema URIs
        entity_uris = event_spec.get_entity_iglu_uris()
        if entity_uris:
            custom_props["entity_iglu_uris"] = ", ".join(entity_uris)

        # Dataset properties
        dataset_properties = DatasetPropertiesClass(
            name=event_spec.name,
            description=event_spec.description,
            customProperties=custom_props,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        ).as_workunit()

        # SubTypes
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=["snowplow_event_spec"]),
        ).as_workunit()

        # Container (link to organization)
        if self.config.bdp_connection:
            org_urn = self.urn_factory.make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            container = ContainerClass(container=org_urn)

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=container,
            ).as_workunit()

        # Status
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Track extracted event spec
        self.report.report_event_spec_extracted()

        # Add lineage from event spec to referenced schemas
        # Collect all upstream schema URNs from both legacy and new formats
        upstream_urns = []
        upstream_schema_urns: List[str] = []  # Track schema URNs for field lineage

        # Track referenced Iglu URIs for standard schema extraction
        referenced_uris: list[str] = []

        # Route 1: Legacy format with event_schemas list
        if event_spec.event_schemas:
            for schema_ref in event_spec.event_schemas:
                # Create schema URN from vendor/name/version
                schema_urn = self.urn_factory.make_schema_dataset_urn(
                    vendor=schema_ref.vendor,
                    name=schema_ref.name,
                    version=schema_ref.version,
                )
                upstream_urns.append(
                    UpstreamClass(
                        dataset=schema_urn,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                )
                upstream_schema_urns.append(schema_urn)

        # Route 2: New format with event.source + event.schema (Iglu URIs)
        if event_spec.event:
            # Add event schema from event.source
            event_iglu_uri = event_spec.get_event_iglu_uri()
            if event_iglu_uri:
                # Track for standard schema extraction
                referenced_uris.append(event_iglu_uri)

                try:
                    # Parse Iglu URI: iglu:vendor/name/jsonschema/version
                    uri_parts = event_iglu_uri.replace("iglu:", "").split("/")
                    if len(uri_parts) == 4:
                        vendor, name, _, version = uri_parts

                        schema_urn = self.urn_factory.make_schema_dataset_urn(
                            vendor=vendor,
                            name=name,
                            version=version,
                        )
                        upstream_urns.append(
                            UpstreamClass(
                                dataset=schema_urn,
                                type=DatasetLineageTypeClass.TRANSFORMED,
                            )
                        )
                        upstream_schema_urns.append(schema_urn)
                except Exception as e:
                    logger.warning(
                        f"Failed to parse event Iglu URI {event_iglu_uri}: {e}"
                    )

            # Add entity schemas from entities.tracked[]
            entity_iglu_uris = event_spec.get_entity_iglu_uris()
            for entity_uri in entity_iglu_uris:
                # Track for standard schema extraction
                referenced_uris.append(entity_uri)

                try:
                    # Parse Iglu URI: iglu:vendor/name/jsonschema/version
                    uri_parts = entity_uri.replace("iglu:", "").split("/")
                    if len(uri_parts) == 4:
                        vendor, name, _, version = uri_parts

                        entity_urn = self.urn_factory.make_schema_dataset_urn(
                            vendor=vendor,
                            name=name,
                            version=version,
                        )
                        upstream_urns.append(
                            UpstreamClass(
                                dataset=entity_urn,
                                type=DatasetLineageTypeClass.TRANSFORMED,
                            )
                        )
                        upstream_schema_urns.append(entity_urn)
                except Exception as e:
                    logger.warning(f"Failed to parse entity Iglu URI {entity_uri}: {e}")

        # Add referenced URIs to state for standard schema extraction
        if referenced_uris:
            self.state.referenced_iglu_uris.update(referenced_uris)
            logger.debug(
                f"Tracked {len(referenced_uris)} Iglu URI(s) from event spec '{event_spec.name}'"
            )

        # Add Atomic Event as upstream (events are built on atomic event schema)
        if self.state.atomic_event_urn:
            upstream_urns.append(
                UpstreamClass(
                    dataset=self.state.atomic_event_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

        # Store event spec to schema URN mapping for later schema metadata emission
        # (after standard schemas are extracted)
        self.state.event_spec_to_schema_urns[dataset_urn] = upstream_schema_urns
        self.event_spec_names[dataset_urn] = event_spec.name  # Track name for logging

        # Schema metadata and field-level lineage will be emitted later (after standard schemas are extracted)
        # This is deferred because:
        # 1. Event specs often reference standard schemas extracted AFTER event specs
        # 2. Field-level lineage requires fields to exist first (schema metadata must be emitted first)

        # Emit DATASET-LEVEL lineage only (no field-level lineage yet)
        if upstream_urns:
            upstream_lineage = UpstreamLineageClass(
                upstreams=upstream_urns,
                fineGrainedLineages=None,  # Field-level lineage deferred to second pass
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=upstream_lineage,
            ).as_workunit()

            logger.debug(
                f"Emitted dataset-level upstream lineage for event spec '{event_spec.name}' "
                f"({len(upstream_urns)} upstreams)"
            )

    def emit_event_spec_schema_metadata(self) -> Iterable[MetadataWorkUnit]:
        """
        Emit schema metadata and field-level lineage for all event specs after all schemas are extracted.

        This is a second pass that:
        1. Emits complete schema metadata (Atomic Event + all schemas) to event specs
        2. Emits field-level lineage from schemas to event specs (AFTER fields exist)

        Must be called after both custom schemas AND standard schemas are extracted.

        Yields:
            MetadataWorkUnit: Schema metadata and lineage work units for event specs
        """
        if not self.state.event_spec_to_schema_urns:
            logger.info("No event specs to emit schema metadata for")
            return

        logger.info(
            f"Emitting schema metadata and field-level lineage for {len(self.state.event_spec_to_schema_urns)} event spec(s) "
            f"after all schemas extracted"
        )

        for (
            event_spec_urn,
            upstream_schema_urns,
        ) in self.state.event_spec_to_schema_urns.items():
            event_spec_name = self.event_spec_names.get(event_spec_urn, "Unknown")

            # Build complete field list: Atomic Event + referenced schemas
            all_fields = []

            # Add atomic event fields from Atomic Event
            if self.state.atomic_event_fields:
                all_fields.extend(self.state.atomic_event_fields)

            # Add fields from all referenced schemas (custom + standard)
            matched_count = 0
            if self.state.extracted_schema_fields:
                for source_urn, field in self.state.extracted_schema_fields:
                    if source_urn in upstream_schema_urns:
                        all_fields.append(field)
                        matched_count += 1

            logger.info(
                f"Event spec '{event_spec_name}': "
                f"Total fields = {len(all_fields)} "
                f"(Atomic Event: {len(self.state.atomic_event_fields) if self.state.atomic_event_fields else 0}, "
                f"Schemas: {matched_count})"
            )

            # Emit schema metadata if we have fields
            if all_fields:
                event_spec_id = event_spec_urn.split(",")[1]  # Extract ID from URN
                schema_metadata = SchemaMetadataClass(
                    schemaName=f"snowplow/event_spec/{event_spec_id}",
                    platform=f"urn:li:dataPlatform:{self.deps.platform}",
                    version=0,
                    fields=all_fields,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=event_spec_urn,
                    aspect=schema_metadata,
                ).as_workunit()
            else:
                logger.warning(f"Event spec '{event_spec_name}' has no fields to emit")
                continue  # Skip field-level lineage if no fields

            # NOW emit field-level lineage (after fields exist)
            yield from self._emit_field_level_lineage(
                event_spec_urn=event_spec_urn,
                event_spec_name=event_spec_name,
                upstream_schema_urns=upstream_schema_urns,
            )

    def _emit_field_level_lineage(
        self,
        event_spec_urn: str,
        event_spec_name: str,
        upstream_schema_urns: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit field-level lineage for an event spec.

        Must be called AFTER schema metadata is emitted so fields exist.

        Args:
            event_spec_urn: Event spec dataset URN
            event_spec_name: Event spec name (for logging)
            upstream_schema_urns: List of upstream schema URNs

        Yields:
            MetadataWorkUnit: Upstream lineage with field-level lineages
        """
        fine_grained_lineages: List[FineGrainedLineageClass] = []

        # Build upstream dataset list (for dataset-level lineage)
        upstream_datasets = []

        # Add Atomic Event as upstream
        if self.state.atomic_event_urn:
            upstream_datasets.append(
                UpstreamClass(
                    dataset=self.state.atomic_event_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

            # Add field-level lineage from Atomic Event
            if self.state.atomic_event_fields:
                for field in self.state.atomic_event_fields:
                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                make_schema_field_urn(
                                    self.state.atomic_event_urn, field.fieldPath
                                )
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                            downstreams=[
                                make_schema_field_urn(event_spec_urn, field.fieldPath)
                            ],
                        )
                    )

        # Add schema upstreams and field-level lineage
        for schema_urn in upstream_schema_urns:
            upstream_datasets.append(
                UpstreamClass(
                    dataset=schema_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

            # Add field-level lineage from this schema
            if self.state.extracted_schema_fields:
                for source_urn, field in self.state.extracted_schema_fields:
                    if source_urn == schema_urn:
                        fine_grained_lineages.append(
                            FineGrainedLineageClass(
                                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                upstreams=[
                                    make_schema_field_urn(source_urn, field.fieldPath)
                                ],
                                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                                downstreams=[
                                    make_schema_field_urn(
                                        event_spec_urn, field.fieldPath
                                    )
                                ],
                            )
                        )

        # Emit upstream lineage with field-level lineages
        if upstream_datasets:
            upstream_lineage = UpstreamLineageClass(
                upstreams=upstream_datasets,
                fineGrainedLineages=fine_grained_lineages
                if fine_grained_lineages
                else None,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=event_spec_urn,
                aspect=upstream_lineage,
            ).as_workunit()

            logger.info(
                f"Event spec '{event_spec_name}': Emitted {len(fine_grained_lineages)} field-level lineages"
            )
