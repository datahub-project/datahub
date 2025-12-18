"""
Data Structure Builder for Snowplow connector.

Handles processing of individual data structures (schemas) including:
- Fetching full schema definitions
- Parsing schema metadata
- Building dataset objects with all aspects
- Field tagging and structured properties
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.schema_parser import SnowplowSchemaParser
from datahub.ingestion.source.snowplow.snowplow_models import DataStructure
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    OwnerClass,
    SchemaMetadataClass,
    StatusClass,
    TagAssociationClass,
)
from datahub.sdk.dataset import Dataset
from datahub.utilities.sentinels import unset

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )
    from datahub.ingestion.source.snowplow.services.column_lineage_builder import (
        ColumnLineageBuilder,
    )

logger = logging.getLogger(__name__)


class DataStructureBuilder:
    """
    Builder for processing Snowplow data structures (schemas).

    Handles the complete lifecycle of processing a data structure from the BDP API,
    including fetching full definitions, parsing schemas, building datasets, and
    emitting all related metadata.
    """

    def __init__(
        self,
        deps: "ProcessorDependencies",
        state: "IngestionState",
        column_lineage_builder: "ColumnLineageBuilder",
    ):
        """
        Initialize data structure builder.

        Args:
            deps: Explicit dependencies (clients, builders, config, etc.)
            state: Shared mutable state
            column_lineage_builder: Builder for column-level lineage
        """
        self.deps = deps
        self.state = state
        self.column_lineage_builder = column_lineage_builder

        # Convenience accessors
        self.config = deps.config
        self.report = deps.report
        self.bdp_client = deps.bdp_client
        self.platform = deps.platform
        self.urn_factory = deps.urn_factory
        self.ownership_builder = deps.ownership_builder
        self.field_tagger = deps.field_tagger

    def process_data_structure(
        self, data_structure: DataStructure
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single data structure (schema).

        Args:
            data_structure: Data structure from BDP API

        Yields:
            MetadataWorkUnit: Schema metadata work units
        """
        # Fetch full schema definition if missing
        data_structure = self._fetch_full_schema_definition(data_structure)

        # Validate and extract basic info
        basic_info = self._validate_and_extract_basic_info(data_structure)
        if not basic_info:
            return

        vendor, name, schema_meta, schema_type, version, schema_identifier = basic_info

        # Track found schema
        self.report.report_schema_found(schema_type)

        # Apply filtering
        if not self._should_process_schema(schema_identifier, schema_type, schema_meta):
            return

        # Capture first event schema for parsed events dataset naming
        self._capture_first_event_schema(vendor, name, schema_type)

        # Build dataset name
        dataset_name = self._build_dataset_name(vendor, name, version)

        # Build dataset properties
        custom_properties = self._build_custom_properties(
            data_structure, vendor, name, version, schema_type, schema_meta
        )

        # Build parent container and owners
        parent_container = self._build_parent_container()
        owners_list = self._build_owners_list(data_structure, schema_identifier)

        # Build subtype and extra aspects
        subtype = f"snowplow_{schema_type}_schema" if schema_type else "snowplow_schema"
        extra_aspects = self._build_extra_aspects(name, schema_type)

        # Parse schema metadata and add tags
        schema_metadata = self._parse_schema_metadata(
            data_structure,
            vendor,
            name,
            version,
            schema_type,
            schema_identifier,
            extra_aspects,
        )

        # Create and yield Dataset
        dataset = self._create_dataset(
            dataset_name,
            name,
            data_structure,
            version,
            custom_properties,
            parent_container,
            subtype,
            owners_list,
            extra_aspects,
        )
        yield from dataset.as_workunits()

        # Post-processing: emit structured properties, cache results, emit lineage
        yield from self._post_process_dataset(
            dataset, data_structure, schema_metadata, vendor, name, version, schema_type
        )

        # Track extracted schema
        self.report.report_schema_extracted(schema_type)

    def _validate_and_extract_basic_info(
        self, data_structure: DataStructure
    ) -> Optional[tuple]:
        """Validate and extract basic information from data structure."""
        if (
            not data_structure.vendor
            or not data_structure.name
            or not data_structure.meta
        ):
            logger.warning(
                f"Data structure missing basic fields (vendor, name, or meta), skipping: {data_structure.hash}"
            )
            return None

        vendor = data_structure.vendor
        name = data_structure.name
        schema_meta = data_structure.meta
        schema_type = schema_meta.schema_type or "event"

        version = self._get_schema_version(data_structure)
        if not version:
            logger.warning(f"Cannot determine version for {vendor}/{name}, skipping")
            return None

        schema_identifier = f"{vendor}/{name}"
        return vendor, name, schema_meta, schema_type, version, schema_identifier

    def _should_process_schema(
        self, schema_identifier: str, schema_type: str, schema_meta: Any
    ) -> bool:
        """Check if schema should be processed based on filters."""
        if not self.config.schema_pattern.allowed(schema_identifier):
            self.report.report_schema_filtered(schema_type, schema_identifier)
            return False

        if schema_type not in self.config.schema_types_to_extract:
            logger.debug(f"Skipping schema {schema_identifier} with type {schema_type}")
            return False

        if schema_meta.hidden and not self.config.include_hidden_schemas:
            self.report.report_hidden_schema(skipped=True)
            logger.debug(f"Skipping hidden schema: {schema_identifier}")
            return False
        elif schema_meta.hidden:
            self.report.report_hidden_schema(skipped=False)

        return True

    def _capture_first_event_schema(
        self, vendor: str, name: str, schema_type: str
    ) -> None:
        """Capture first event schema for parsed events dataset naming."""
        if schema_type == "event" and self.state.first_event_schema_vendor is None:
            self.state.first_event_schema_vendor = vendor
            self.state.first_event_schema_name = name
            logger.debug(
                f"Captured first event schema for Event dataset naming: {vendor}/{name}"
            )

    def _build_dataset_name(self, vendor: str, name: str, version: str) -> str:
        """Build dataset name based on configuration."""
        if self.config.include_version_in_urn:
            return f"{vendor}.{name}.{version}".replace("/", ".")
        return f"{vendor}.{name}".replace("/", ".")

    def _build_custom_properties(
        self,
        data_structure: DataStructure,
        vendor: str,
        name: str,
        version: str,
        schema_type: str,
        schema_meta: Any,
    ) -> Dict[str, str]:
        """Build custom properties for dataset."""
        custom_properties = {
            "vendor": vendor,
            "schemaVersion": version,
            "schema_type": schema_type or "unknown",
            "hidden": str(schema_meta.hidden),
            "igluUri": f"iglu:{vendor}/{name}/jsonschema/{version}",
            **schema_meta.custom_data,
        }
        if data_structure.data:
            custom_properties["format"] = data_structure.data.self_descriptor.format
        return custom_properties

    def _build_parent_container(self) -> Optional[List[str]]:
        """Build parent container URN."""
        if self.config.bdp_connection:
            parent_container_urn = self.urn_factory.make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            return [parent_container_urn]
        return None

    def _build_owners_list(
        self, data_structure: DataStructure, schema_identifier: str
    ) -> Optional[List[OwnerClass]]:
        """Build owners list from deployments."""
        if data_structure.deployments and self.bdp_client:
            created_by, modified_by = (
                self.ownership_builder.extract_ownership_from_deployments(
                    data_structure.deployments
                )
            )
            return self.ownership_builder.build_ownership_list(
                created_by, modified_by, schema_identifier
            )
        return None

    def _build_extra_aspects(self, name: str, schema_type: str) -> List[Any]:
        """Build extra aspects including status and optional tags."""
        extra_aspects: List[Any] = [StatusClass(removed=False)]

        logger.info(
            f"🔍 Dataset tag check for '{name}': field_tagging.enabled={self.config.field_tagging.enabled}, "
            f"tag_event_type={self.config.field_tagging.tag_event_type}"
        )

        if (
            self.config.field_tagging.enabled
            and self.config.field_tagging.tag_event_type
        ):
            logger.info(f"✅ Building dataset tags for '{name}'")
            dataset_tags = self._build_dataset_tags(name, schema_type)
            if dataset_tags:
                extra_aspects.append(dataset_tags)
                logger.info(f"✅ Added dataset tags to '{name}'")
        else:
            logger.info(f"⏭️ Skipping dataset tags for '{name}' (tag_event_type={self.config.field_tagging.tag_event_type})")

        return extra_aspects

    def _parse_schema_metadata(
        self,
        data_structure: DataStructure,
        vendor: str,
        name: str,
        version: str,
        schema_type: str,
        schema_identifier: str,
        extra_aspects: List[Any],
    ) -> Optional[SchemaMetadataClass]:
        """Parse schema metadata and add field tags if enabled."""
        if not data_structure.data:
            logger.info(
                f"Schema definition not available for {vendor}/{name}/{version}, skipping detailed schema metadata"
            )
            return None

        try:
            schema_metadata = SnowplowSchemaParser.parse_schema(
                schema_data=data_structure.data.model_dump(),
                vendor=vendor,
                name=name,
                version=version,
            )

            if self.config.field_tagging.enabled:
                schema_metadata = self._add_field_tags(
                    schema_metadata=schema_metadata,
                    data_structure=data_structure,
                    version=version,
                    schema_type=schema_type,
                )

            extra_aspects.append(schema_metadata)
            return schema_metadata

        except Exception as e:
            error_msg = f"Failed to parse schema {schema_identifier}: {e}"
            self.report.report_schema_parsing_error(error_msg)
            logger.error(error_msg)
            return None

    def _create_dataset(
        self,
        dataset_name: str,
        name: str,
        data_structure: DataStructure,
        version: str,
        custom_properties: Dict[str, str],
        parent_container: Optional[List[str]],
        subtype: str,
        owners_list: Optional[List[OwnerClass]],
        extra_aspects: List[Any],
    ) -> Dataset:
        """Create Dataset object using SDK V2."""
        return Dataset(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=data_structure.data.description
            if data_structure.data
            else None,
            display_name=name,
            external_url=self._get_schema_url(
                data_structure.vendor or "",
                name,
                version,
                data_structure.hash,
            ),
            custom_properties=custom_properties,
            parent_container=parent_container
            if parent_container is not None
            else unset,
            subtype=subtype,
            owners=owners_list,
            extra_aspects=extra_aspects,
        )

    def _post_process_dataset(
        self,
        dataset: Dataset,
        data_structure: DataStructure,
        schema_metadata: Optional[SchemaMetadataClass],
        vendor: str,
        name: str,
        version: str,
        schema_type: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Post-process dataset: emit structured properties, cache results, emit lineage."""
        # Emit field-level structured properties if enabled
        if (
            self.config.field_tagging.enabled
            and self.config.field_tagging.use_structured_properties
            and schema_metadata
        ):
            yield from self._emit_field_structured_properties(
                dataset_urn=str(dataset.urn),
                schema_metadata=schema_metadata,
                data_structure=data_structure,
                version=version,
                schema_type=schema_type,
            )

        # Cache the schema URN for pipeline collector job
        self.state.extracted_schema_urns.append(str(dataset.urn))

        # Cache schema fields for Event dataset
        if schema_metadata and schema_metadata.fields:
            for field in schema_metadata.fields:
                self.state.extracted_schema_fields.append((str(dataset.urn), field))

        # Emit column-level lineage
        if schema_metadata:
            yield from self.column_lineage_builder.emit_column_lineage(
                dataset_urn=str(dataset.urn),
                vendor=vendor,
                name=name,
                version=version,
                schema_metadata=schema_metadata,
            )

    def _fetch_full_schema_definition(
        self, data_structure: DataStructure
    ) -> DataStructure:
        """
        Fetch full schema definition if only minimal info available.

        Args:
            data_structure: Data structure from BDP API

        Returns:
            Updated data structure with full schema definition (if available)
        """
        if (
            data_structure.data is not None
            or not data_structure.hash
            or not self.bdp_client
        ):
            return data_structure

        if not data_structure.deployments:
            logger.warning(
                f"No deployments found for {data_structure.vendor}/{data_structure.name}, cannot determine version to fetch"
            )
            return data_structure

        # Use version from most recent PROD deployment, fall back to any deployment
        prod_deployments = [d for d in data_structure.deployments if d.env == "PROD"]
        if prod_deployments:
            latest_deployment = sorted(
                prod_deployments, key=lambda d: d.ts or "", reverse=True
            )[0]
        else:
            latest_deployment = sorted(
                data_structure.deployments,
                key=lambda d: d.ts or "",
                reverse=True,
            )[0]

        version = latest_deployment.version
        env = latest_deployment.env

        logger.info(
            f"Fetching schema definition for {data_structure.vendor}/{data_structure.name} version {version} from {env}"
        )

        # Fetch using the /versions/{version} endpoint which returns full schema
        full_structure = self.bdp_client.get_data_structure_version(
            data_structure.hash, version, env
        )

        if full_structure and full_structure.data:
            logger.info(
                f"Successfully fetched schema definition with {len(full_structure.data.properties or {})} properties"
            )
            # Preserve metadata from list response
            full_structure.meta = data_structure.meta
            full_structure.deployments = data_structure.deployments
            return full_structure
        else:
            logger.warning(
                f"Could not fetch schema definition for {data_structure.hash}/{version}, skipping field-level metadata"
            )
            return data_structure

    def _get_schema_version(self, data_structure: DataStructure) -> Optional[str]:
        """
        Extract schema version from data structure.

        Args:
            data_structure: Data structure from BDP API

        Returns:
            Schema version string, or None if cannot be determined
        """
        # Get version from schema definition if available
        if data_structure.data:
            return data_structure.data.self_descriptor.version

        # Otherwise use version from latest deployment
        if data_structure.deployments:
            sorted_deployments = sorted(
                data_structure.deployments, key=lambda d: d.ts or "", reverse=True
            )
            version = sorted_deployments[0].version
            logger.info(
                f"Schema definition missing, using version from latest deployment: {version}"
            )
            return version

        return None

    def _build_dataset_tags(
        self, name: str, schema_type: Optional[str]
    ) -> Optional[GlobalTagsClass]:
        """
        Build dataset-level tags.

        Dataset-level tags apply to the schema/dataset as a whole, not individual fields.
        Currently adds:
        - Event type tag: Category derived from schema name (e.g., "checkout" from "checkout_started")

        Args:
            name: Schema name (e.g., "checkout_started")
            schema_type: Schema type ("event" or "entity")

        Returns:
            GlobalTagsClass with dataset-level tags, or None if no tags to add
        """
        tags = set()

        # Event type tag (category derived from name)
        # Extract first word: checkout_started → checkout
        event_name = name.split("_")[0] if "_" in name else name
        event_type_tag = self.config.field_tagging.event_type_pattern.format(
            name=event_name
        )
        tags.add(event_type_tag)

        if not tags:
            return None

        # Convert to TagAssociationClass
        tag_associations = [
            TagAssociationClass(tag=f"urn:li:tag:{tag}") for tag in sorted(tags)
        ]

        return GlobalTagsClass(tags=tag_associations)

    def _add_field_tags(
        self,
        schema_metadata: SchemaMetadataClass,
        data_structure: DataStructure,
        version: str,
        schema_type: str,
    ) -> SchemaMetadataClass:
        """
        Add field-level tags to schema metadata.

        Field tags provide semantic meaning about what kind of data each field contains:
        - PII: Personal identifiable information
        - Email, Phone, IP Address, etc.
        - Event metadata (timestamp, ID, etc.)

        Args:
            schema_metadata: Parsed schema metadata with fields
            data_structure: Data structure from BDP API
            version: Schema version
            schema_type: Schema type ("event" or "entity")

        Returns:
            Updated schema metadata with field tags
        """
        from datahub.ingestion.source.snowplow.field_tagging import FieldTagContext

        if not schema_metadata.fields:
            return schema_metadata

        # Get PII fields if enabled
        pii_fields = set()
        if self.config.field_tagging.use_pii_enrichment:
            pii_fields = self._get_pii_fields()

        # Map schema_type to event_type for field tagging
        # "event" schemas are self-describing events, "entity" schemas are contexts
        event_type = "self_describing" if schema_type == "event" else "context"

        # Update field descriptions with version information if available
        if (
            self.config.field_tagging.track_field_versions
            and self.config.field_tagging.tag_schema_version
        ):
            schema_key = f"{data_structure.vendor}/{data_structure.name}"
            field_version_map = self.state.field_version_cache.get(schema_key, {})

            if field_version_map:
                logger.debug(
                    f"Applying field version info to {len(schema_metadata.fields)} fields in {schema_key} "
                    f"(field_version_map has {len(field_version_map)} entries)"
                )
                for field in schema_metadata.fields:
                    field_added_version = field_version_map.get(field.fieldPath)
                    # Only add version info if field was added in a later version (not 1-0-0)
                    if field_added_version and field_added_version != "1-0-0":
                        version_suffix = f" (Added in version {field_added_version})"
                        # Only append if not already present
                        if field.description and version_suffix not in field.description:
                            logger.debug(
                                f"Updated description for field '{field.fieldPath}' in {schema_key}: "
                                f"added version {field_added_version}"
                            )
                            field.description = f"{field.description}{version_suffix}"
                        elif not field.description:
                            logger.debug(
                                f"Set description for field '{field.fieldPath}' in {schema_key}: "
                                f"Added in version {field_added_version}"
                            )
                            field.description = f"Added in version {field_added_version}"
            else:
                logger.warning(
                    f"Field version map is empty for {schema_key} - field version info will not be added"
                )

        # Get field version map if tracking field versions
        field_version_map = {}
        if (
            self.config.field_tagging.track_field_versions
            and self.config.field_tagging.tag_schema_version
        ):
            field_version_map = self.state.field_version_cache.get(schema_key, {})

        # Add tags to each field
        for field in schema_metadata.fields:
            # Use field's introduction version if available, otherwise use schema version
            field_version = field_version_map.get(field.fieldPath, version)

            # Get deployment info for the version when THIS FIELD was added
            field_deployment_initiator = None
            field_deployment_timestamp = None
            if data_structure.deployments:
                matching_deployments = [
                    d for d in data_structure.deployments if d.version == field_version
                ]
                if matching_deployments:
                    # Use most recent deployment for this field's version
                    deployment = sorted(
                        matching_deployments, key=lambda d: d.ts or "", reverse=True
                    )[0]
                    field_deployment_initiator = deployment.initiator
                    field_deployment_timestamp = deployment.ts

            # Skip version tag if this field was added in initial version
            field_skip_version_tag = field_version == "1-0-0"

            context = FieldTagContext(
                schema_version=field_version,  # Use field's introduction version
                vendor=data_structure.vendor or "",
                name=data_structure.name or "",
                field_name=field.fieldPath,
                field_type=field.nativeDataType,
                field_description=field.description,
                deployment_initiator=field_deployment_initiator,  # Use field-specific deployment info
                deployment_timestamp=field_deployment_timestamp,  # Use field-specific deployment info
                pii_fields=pii_fields,
                event_type=event_type,
                skip_version_tag=field_skip_version_tag,  # Use field-specific flag
            )

            field_tags = self.field_tagger.generate_tags(context)

            if field_tags:
                field.globalTags = field_tags

        return schema_metadata

    def _get_pii_fields(self) -> set:
        """
        Get PII fields from cache.

        PII fields are extracted from enrichment configuration by the main source
        and cached in the state. This method simply returns the cached value.

        Returns:
            Set of field paths marked as PII (empty set if not available)
        """
        # Return cached value if available
        if self.state.pii_fields_cache is not None:
            return self.state.pii_fields_cache

        # PII fields haven't been extracted yet - return empty set
        # The main source will extract them from enrichments if enabled
        return set()

    def _emit_field_structured_properties(
        self,
        dataset_urn: str,
        schema_metadata: SchemaMetadataClass,
        data_structure: DataStructure,
        version: str,
        schema_type: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit field-level structured properties.

        Structured properties provide strongly-typed, searchable metadata on fields.
        Currently supports:
        - PII indicators
        - Semantic types (email, phone, IP, etc.)

        Args:
            dataset_urn: URN of the dataset
            schema_metadata: Parsed schema metadata
            data_structure: Data structure from BDP API
            version: Schema version

        Yields:
            MetadataWorkUnit: Field structured property work units
        """
        from datahub.ingestion.source.snowplow.field_tagging import FieldTagContext

        if not schema_metadata.fields:
            return

        # Get PII fields if enabled
        pii_fields = set()
        if self.config.field_tagging.use_pii_enrichment:
            pii_fields = self._get_pii_fields()

        # Map schema_type to event_type for field tagging
        # "event" schemas are self-describing events, "entity" schemas are contexts
        event_type = "self_describing" if schema_type == "event" else "context"

        # Get field version map if tracking field versions
        schema_key = f"{data_structure.vendor}/{data_structure.name}"
        field_version_map = {}
        if (
            self.config.field_tagging.track_field_versions
            and self.config.field_tagging.tag_schema_version
        ):
            field_version_map = self.state.field_version_cache.get(schema_key, {})
            if field_version_map:
                logger.debug(
                    f"Using field version map for structured properties in {schema_key}"
                )

        # Generate structured properties for each field
        for field in schema_metadata.fields:
            # Use field's introduction version if available, otherwise use schema version
            field_version = field_version_map.get(field.fieldPath, version)

            # Get deployment info for the version when THIS FIELD was added
            field_deployment_initiator = None
            field_deployment_timestamp = None
            if data_structure.deployments:
                matching_deployments = [
                    d for d in data_structure.deployments if d.version == field_version
                ]
                if matching_deployments:
                    # Use most recent deployment for this field's version
                    deployment = sorted(
                        matching_deployments, key=lambda d: d.ts or "", reverse=True
                    )[0]
                    field_deployment_initiator = deployment.initiator
                    field_deployment_timestamp = deployment.ts

            # Skip version tag if this field was added in initial version
            field_skip_version_tag = field_version == "1-0-0"

            context = FieldTagContext(
                schema_version=field_version,  # Use field's introduction version
                vendor=data_structure.vendor or "",
                name=data_structure.name or "",
                field_name=field.fieldPath,
                field_type=field.nativeDataType,
                field_description=field.description,
                deployment_initiator=field_deployment_initiator,  # Use field-specific deployment info
                deployment_timestamp=field_deployment_timestamp,  # Use field-specific deployment info
                pii_fields=pii_fields,
                event_type=event_type,
                skip_version_tag=field_skip_version_tag,  # Use field-specific flag
            )

            yield from self.field_tagger.generate_field_structured_properties(
                dataset_urn=dataset_urn,
                field=field,
                context=context,
            )

    def _get_schema_url(
        self,
        vendor: str,
        name: str,
        version: str,
        schema_hash: Optional[str],
    ) -> Optional[str]:
        """
        Build external URL for schema in BDP Console.

        Args:
            vendor: Schema vendor
            name: Schema name
            version: Schema version
            schema_hash: Schema hash (if available)

        Returns:
            URL to schema in BDP Console, or None if not using BDP
        """
        if not self.config.bdp_connection or not schema_hash:
            return None

        # BDP Console schema URL format:
        # https://console.snowplowanalytics.com/{{org_id}}/data-structures/{{hash}}?version={{version}}
        org_id = self.config.bdp_connection.organization_id
        return f"https://console.snowplowanalytics.com/{org_id}/data-structures/{schema_hash}?version={version}"
