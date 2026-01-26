"""
Event Specification Processor for Snowplow connector.

Handles extraction of event specifications from Snowplow BDP.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.models.snowplow_models import EventSpecification
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.metadata.schema_classes import (
    ContainerClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StatusClass,
    StructuredPropertiesClass,
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


@dataclass
class UpstreamCollectionResult:
    """Result of collecting upstream URNs for an event spec."""

    upstream_classes: List[UpstreamClass] = field(default_factory=list)
    schema_urns: List[str] = field(default_factory=list)
    referenced_iglu_uris: List[str] = field(default_factory=list)


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
        dataset_urn = self.urn_factory.make_event_spec_dataset_urn(event_spec.id)

        # Emit dataset aspects (properties, subtypes, container, status)
        yield from self._emit_dataset_aspects(dataset_urn, event_spec)

        self.report.report_event_spec_extracted()

        # Collect upstream schema URNs from both API formats
        result = self._collect_upstream_urns(event_spec)

        # Track referenced Iglu URIs for standard schema extraction
        if result.referenced_iglu_uris:
            self.state.referenced_iglu_uris.update(result.referenced_iglu_uris)
            logger.debug(
                f"Tracked {len(result.referenced_iglu_uris)} Iglu URI(s) from event spec '{event_spec.name}'"
            )

        # Add Atomic Event as upstream
        if self.state.atomic_event_urn:
            result.upstream_classes.append(
                UpstreamClass(
                    dataset=self.state.atomic_event_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

        # Store mapping for later schema metadata emission
        self.state.event_spec_to_schema_urns[dataset_urn] = result.schema_urns
        self.event_spec_names[dataset_urn] = event_spec.name

        # Emit dataset-level lineage (field-level deferred to second pass)
        if result.upstream_classes:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(
                    upstreams=result.upstream_classes,
                    fineGrainedLineages=None,
                ),
            ).as_workunit()

            logger.debug(
                f"Emitted dataset-level upstream lineage for event spec '{event_spec.name}' "
                f"({len(result.upstream_classes)} upstreams)"
            )

    def _emit_dataset_aspects(
        self, dataset_urn: str, event_spec: EventSpecification
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit dataset aspects (properties, subtypes, container, status).

        Uses SDK V2 batching pattern via emit_aspects helper.

        Args:
            dataset_urn: Event spec dataset URN
            event_spec: Event specification from BDP API

        Yields:
            MetadataWorkUnit: Dataset aspect work units
        """
        custom_props = self._build_custom_properties(event_spec)

        # Build container aspect conditionally
        container_aspect = None
        if self.config.bdp_connection:
            org_urn = self.urn_factory.make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            container_aspect = ContainerClass(container=org_urn)

        # Emit all aspects using SDK V2 batching pattern
        yield from self.emit_aspects(
            entity_urn=dataset_urn,
            aspects=[
                DatasetPropertiesClass(
                    name=event_spec.name,
                    description=event_spec.description,
                    customProperties=custom_props,
                ),
                SubTypesClass(typeNames=["snowplow_event_spec"]),
                container_aspect,
                StatusClass(removed=False),
            ],
        )

    def _build_custom_properties(
        self, event_spec: EventSpecification
    ) -> dict[str, str]:
        """Build custom properties dict for an event spec."""
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
            if event_spec.event.schema_constraints:
                custom_props["event_schema_constraints"] = json.dumps(
                    event_spec.event.schema_constraints
                )

        # Add entity schema URIs
        entity_uris = event_spec.get_entity_iglu_uris()
        if entity_uris:
            custom_props["entity_iglu_uris"] = ", ".join(entity_uris)

        return custom_props

    def _collect_upstream_urns(
        self, event_spec: EventSpecification
    ) -> UpstreamCollectionResult:
        """
        Collect upstream URNs from both legacy and new API formats.

        Args:
            event_spec: Event specification from BDP API

        Returns:
            UpstreamCollectionResult with collected URNs
        """
        result = UpstreamCollectionResult()

        # Route 1: Legacy format with event_schemas list
        for schema_ref in event_spec.event_schemas:
            schema_urn = self.urn_factory.make_schema_dataset_urn(
                vendor=schema_ref.vendor,
                name=schema_ref.name,
                version=schema_ref.version,
            )
            result.upstream_classes.append(
                UpstreamClass(
                    dataset=schema_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )
            result.schema_urns.append(schema_urn)

        # Route 2: New format with Iglu URIs
        if event_spec.event:
            # Event schema from event.source
            event_uri = event_spec.get_event_iglu_uri()
            if event_uri:
                self._add_iglu_uri_to_result(event_uri, result)

            # Entity schemas from entities.tracked[]
            for entity_uri in event_spec.get_entity_iglu_uris():
                self._add_iglu_uri_to_result(entity_uri, result)

        return result

    def _add_iglu_uri_to_result(
        self, iglu_uri: str, result: UpstreamCollectionResult
    ) -> None:
        """
        Parse an Iglu URI and add it to the collection result.

        Args:
            iglu_uri: Iglu URI in format iglu:vendor/name/jsonschema/version
            result: Result object to update
        """
        result.referenced_iglu_uris.append(iglu_uri)

        parsed = self._parse_iglu_uri(iglu_uri)
        if not parsed:
            return

        vendor, name, version = parsed
        schema_urn = self.urn_factory.make_schema_dataset_urn(
            vendor=vendor, name=name, version=version
        )
        result.upstream_classes.append(
            UpstreamClass(
                dataset=schema_urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
        )
        result.schema_urns.append(schema_urn)

    @staticmethod
    def _parse_iglu_uri(iglu_uri: str) -> Optional[Tuple[str, str, str]]:
        """
        Parse Iglu URI into vendor, name, version.

        Args:
            iglu_uri: URI in format iglu:vendor/name/jsonschema/version

        Returns:
            Tuple of (vendor, name, version) or None if parsing fails
        """
        try:
            uri_parts = iglu_uri.replace("iglu:", "").split("/")
            if len(uri_parts) == 4:
                vendor, name, _, version = uri_parts
                return vendor, name, version
            logger.warning(f"Invalid Iglu URI format: {iglu_uri}")
            return None
        except Exception as e:
            logger.warning(f"Failed to parse Iglu URI {iglu_uri}: {e}")
            return None

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
            # O(k) lookup where k = number of upstream schemas (vs O(n) full scan)
            schema_fields = self.state.get_fields_for_schemas(upstream_schema_urns)
            all_fields.extend(schema_fields)
            matched_count = len(schema_fields)

            # Debug: Log which schema URNs were looked up and which had fields
            if upstream_schema_urns:
                logger.debug(
                    f"Event spec '{event_spec_name}' references {len(upstream_schema_urns)} schema URN(s): "
                    f"{upstream_schema_urns}"
                )
                registered_urns = list(self.state.extracted_schema_fields_by_urn.keys())
                logger.debug(
                    f"Available schema URNs in state ({len(registered_urns)}): "
                    f"{registered_urns[:5]}{'...' if len(registered_urns) > 5 else ''}"
                )
                # Check which upstream URNs are missing
                missing_urns = [
                    urn
                    for urn in upstream_schema_urns
                    if urn not in self.state.extracted_schema_fields_by_urn
                ]
                if missing_urns:
                    logger.warning(
                        f"Event spec '{event_spec_name}': {len(missing_urns)} referenced schema(s) not found "
                        f"in extracted schemas: {missing_urns}"
                    )

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

            # Emit structured properties for event_spec fields
            # These are propagated from the original schema fields
            yield from self._emit_field_structured_properties(
                event_spec_urn=event_spec_urn,
                fields=all_fields,
            )

            # NOW emit field-level lineage (after fields exist)
            yield from self._emit_field_level_lineage(
                event_spec_urn=event_spec_urn,
                event_spec_name=event_spec_name,
                upstream_schema_urns=upstream_schema_urns,
            )

    def _emit_field_structured_properties(
        self,
        event_spec_urn: str,
        fields: List[SchemaFieldClass],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit structured properties for event_spec fields.

        Propagates structured properties from original schema fields to event_spec fields.

        Args:
            event_spec_urn: Event spec dataset URN
            fields: List of schema fields to emit structured properties for

        Yields:
            MetadataWorkUnit: Structured properties work units for each field
        """
        if not self.state.field_structured_properties:
            return

        emitted_count = 0
        for schema_field in fields:
            # Look up structured properties by field path
            properties = self.state.field_structured_properties.get(
                schema_field.fieldPath
            )
            if not properties:
                continue

            # Create SchemaField URN for the event_spec dataset
            schema_field_urn = make_schema_field_urn(
                event_spec_urn, schema_field.fieldPath
            )

            # Emit structured properties aspect
            yield MetadataChangeProposalWrapper(
                entityUrn=schema_field_urn,
                aspect=StructuredPropertiesClass(properties=properties),
            ).as_workunit()
            emitted_count += 1

        if emitted_count > 0:
            logger.debug(
                f"Emitted structured properties for {emitted_count} fields in event spec"
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

            # Add field-level lineage from this schema (O(1) lookup per schema)
            for field in self.state.get_fields_for_schema(schema_urn):
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[make_schema_field_urn(schema_urn, field.fieldPath)],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                        downstreams=[
                            make_schema_field_urn(event_spec_urn, field.fieldPath)
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
