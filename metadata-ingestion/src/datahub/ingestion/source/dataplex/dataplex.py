"""Google Dataplex source for DataHub ingestion.

This source extracts metadata from Google Dataplex, including:
- Entries (Universal Catalog) as Datasets with source platform URNs (bigquery, gcs, etc.)
- Entities (discovered tables/filesets from Lakes/Zones) as Datasets with source platform URNs
- BigQuery Projects as Containers (project-level containers)
- BigQuery Datasets as Containers (dataset-level containers, nested under project containers)
- Dataplex hierarchy (lakes, zones, assets, zone types) preserved as custom properties on datasets

Reference implementation based on VertexAI and BigQuery V2 sources.
"""

import json
import logging
from collections.abc import Mapping
from threading import Lock
from typing import Any, Iterable, Optional

from google.api_core import exceptions
from google.cloud import dataplex_v1
from google.cloud.datacatalog.lineage_v1 import LineageClient
from google.oauth2 import service_account

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, ProjectIdKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntityDataTuple,
    extract_entity_metadata,
    make_audit_stamp,
    make_bigquery_dataset_container_key,
    make_entity_dataset_urn,
    map_dataplex_field_to_datahub,
)
from datahub.ingestion.source.dataplex.dataplex_lineage import DataplexLineageExtractor
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.ingestion.source.sql.sql_utils import (
    gen_database_container,
    gen_schema_container,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TimeStampClass,
    TimeTypeClass,
)
from datahub.metadata.urns import DataPlatformUrn
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)


@platform_name("Dataplex", id="dataplex")
@config_class(DataplexConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.CONTAINERS,
    "Links BigQuery datasets to BigQuery dataset containers. Supports dual API extraction: "
    "Entries API (Universal Catalog) for system-managed resources, and Entities API (Lakes/Zones) for Dataplex-managed assets. "
    "Dataplex hierarchy (lakes, zones, assets) preserved as custom properties.",
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Extract schema information from Entries API (Universal Catalog) and Entities API (discovered tables/filesets). "
    "Schema extraction can be disabled via include_schema config for faster ingestion.",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Extract table-level lineage from Dataplex Lineage API. "
    "Supports configurable retry logic (lineage_max_retries, lineage_retry_backoff_multiplier) for handling transient errors.",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default when stateful ingestion is configured. "
    "Tracks entities from both Entries API (Universal Catalog) and Entities API (Lakes/Zones).",
)
@capability(
    SourceCapability.TEST_CONNECTION,
    "Verifies connectivity to Dataplex API, including both Entries API (Universal Catalog) and Entities API (Lakes/Zones) if enabled.",
)
class DataplexSource(StatefulIngestionSourceBase, TestableSource):
    """Source to ingest metadata from Google Dataplex."""

    platform: str = "dataplex"

    def __init__(self, ctx: PipelineContext, config: DataplexConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report: DataplexReport = DataplexReport()

        # Track entity IDs for lineage extraction
        # Key: project_id, Value: set of tuples (entity_id, zone_id, lake_id)
        self.entity_data_by_project: dict[str, set[EntityDataTuple]] = {}

        creds = self.config.get_credentials()
        credentials = (
            service_account.Credentials.from_service_account_info(creds)
            if creds
            else None
        )

        self.dataplex_client = dataplex_v1.DataplexServiceClient(
            credentials=credentials
        )
        self.metadata_client = dataplex_v1.MetadataServiceClient(
            credentials=credentials
        )
        # Catalog client for Phase 2: Entry Groups and Entries extraction
        self.catalog_client = dataplex_v1.CatalogServiceClient(credentials=credentials)

        # Initialize redundant lineage run skip handler for stateful lineage ingestion
        redundant_lineage_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = (
            None
        )
        if self.config.enable_stateful_lineage_ingestion:
            redundant_lineage_run_skip_handler = RedundantLineageRunSkipHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )

        if self.config.include_lineage:
            self.lineage_client: Optional[LineageClient] = LineageClient(
                credentials=credentials
            )
            self.lineage_extractor: Optional[DataplexLineageExtractor] = (
                DataplexLineageExtractor(
                    config=self.config,
                    report=self.report,
                    lineage_client=self.lineage_client,
                    dataplex_client=self.dataplex_client,
                    redundant_run_skip_handler=redundant_lineage_run_skip_handler,
                )
            )
        else:
            self.lineage_client = None
            self.lineage_extractor = None

        self.asset_metadata: dict[str, tuple[str, str]] = {}
        self.zone_metadata: dict[
            str, str
        ] = {}  # Store zone types for adding to entity custom properties

        # Track BigQuery containers to create (project_id -> set of dataset_ids)
        self.bq_containers: dict[str, set[str]] = {}

        # Thread safety locks for parallel processing
        self._report_lock = Lock()
        self._asset_metadata_lock = Lock()
        self._entity_data_lock = Lock()
        self._zone_metadata_lock = Lock()
        self._bq_containers_lock = Lock()

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to Dataplex API."""
        test_report = TestConnectionReport()
        try:
            config = DataplexConfig.model_validate(config_dict)
            creds = config.get_credentials()
            credentials = (
                service_account.Credentials.from_service_account_info(creds)
                if creds
                else None
            )

            # Test connection by attempting to create a client and list one project
            dataplex_client = dataplex_v1.DataplexServiceClient(credentials=credentials)
            if config.project_ids:
                project_id = config.project_ids[0]
                # Try to list lakes to verify access
                parent = f"projects/{project_id}/locations/{config.location}"
                list(dataplex_client.list_lakes(parent=parent))

            test_report.basic_connectivity = CapabilityReport(capable=True)
        except exceptions.GoogleAPICallError as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"Failed to connect to Dataplex: {e}"
            )
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"Unexpected error: {e}"
            )

        return test_report

    def get_report(self) -> DataplexReport:
        """Return the ingestion report."""
        return self.report

    def get_workunit_processors(self) -> list[Optional[MetadataWorkUnitProcessor]]:
        """
        Get workunit processors for stateful ingestion.

        Returns processors for:
        - Stale entity removal (deletion detection)
        """
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Main function to fetch and yield workunits for various Dataplex resources."""
        # Iterate over all configured projects
        for project_id in self.config.project_ids:
            logger.info(f"Processing Dataplex resources for project: {project_id}")
            yield from self._process_project(project_id)

    def _emit_final_batch(
        self,
        cached_mcps: list[MetadataChangeProposalWrapper],
        total_emitted: int,
        project_id: str,
        resource_type: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit the final batch of MCPs for a resource type.

        Args:
            cached_mcps: List of cached MCPs to emit
            total_emitted: Total count of MCPs emitted so far
            project_id: GCP project ID
            resource_type: Type of resource being emitted ("entities" or "entries")

        Yields:
            MetadataWorkUnit objects
        """
        if cached_mcps:
            yield from auto_workunit(cached_mcps)
            total_emitted += len(cached_mcps)
            logger.info(
                f"Emitted final batch of {len(cached_mcps)} {resource_type} ({total_emitted} total) for project {project_id}"
            )

    def _process_project(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """Process all Dataplex resources for a single project.

        This uses a single-pass approach with batched emission:
        1. Collect entities/entries MCPs in batches and track containers simultaneously
        2. Emit batches as they fill up to keep memory bounded
        3. Emit BigQuery containers (so entities can reference them)
        4. Extract lineage

        Processing order: Entities first, then Entries.
        When both are enabled, entries will overwrite entity metadata for the same table,
        making Universal Catalog the source of truth without requiring deduplication tracking.

        IMPORTANT: When both APIs are enabled and discover the same table:
        - Entry metadata (schema, entry custom properties) REPLACES entity metadata
        - Entity custom properties (lake, zone, asset) are LOST
        - This is DataHub's aspect-level replacement behavior (not a bug)
        - Users should choose ONE API, or use both only for non-overlapping datasets
        - See documentation for details: docs/sources/dataplex/dataplex_pre.md

        Memory optimization: Batched emission prevents memory issues in large deployments
        while maintaining the performance benefit of avoiding duplicate schema extraction.
        """
        # Determine batch size (None means no batching)
        batch_size = self.config.batch_size
        should_batch = batch_size is not None

        # Cache MCPs during the first pass
        cached_entities_mcps: list[MetadataChangeProposalWrapper] = []
        cached_entries_mcps: list[MetadataChangeProposalWrapper] = []
        entities_emitted = 0
        entries_emitted = 0

        # Process Entities API FIRST (if enabled) - collect MCPs and track containers
        if self.config.include_entities:
            logger.info(
                f"Processing entities from Dataplex Entities API for project {project_id}"
            )
            for mcp in self._get_entities_mcps(project_id):
                cached_entities_mcps.append(mcp)

                # Emit batch if we've reached the batch size
                if (
                    should_batch
                    and batch_size
                    and len(cached_entities_mcps) >= batch_size
                ):
                    yield from auto_workunit(cached_entities_mcps)
                    entities_emitted += len(cached_entities_mcps)
                    logger.info(
                        f"Emitted batch of {len(cached_entities_mcps)} entities ({entities_emitted} total) for project {project_id}"
                    )
                    cached_entities_mcps.clear()

            # Emit remaining cached entities MCPs
            yield from self._emit_final_batch(
                cached_entities_mcps, entities_emitted, project_id, "entities"
            )

        # Process Entries API SECOND (if enabled) - collect MCPs and track containers
        # Entries will overwrite any duplicate entity metadata
        if self.config.include_entries:
            logger.info(
                f"Processing entries from Universal Catalog for project {project_id}"
            )
            for mcp in self._get_entries_mcps(project_id):
                cached_entries_mcps.append(mcp)

                # Emit batch if we've reached the batch size
                if (
                    should_batch
                    and batch_size
                    and len(cached_entries_mcps) >= batch_size
                ):
                    yield from auto_workunit(cached_entries_mcps)
                    entries_emitted += len(cached_entries_mcps)
                    logger.info(
                        f"Emitted batch of {len(cached_entries_mcps)} entries ({entries_emitted} total) for project {project_id}"
                    )
                    cached_entries_mcps.clear()

            # Emit remaining cached entries MCPs (will overwrite any duplicate entities)
            yield from self._emit_final_batch(
                cached_entries_mcps, entries_emitted, project_id, "entries"
            )

        # Emit BigQuery containers (so entities can reference them)
        yield from self._gen_bigquery_containers(project_id)

        # Extract lineage for entities (after entities and containers have been processed)
        if self.config.include_lineage and self.lineage_extractor:
            yield from self._get_lineage_workunits(project_id)

    def _serialize_field_value(self, field_value: Any) -> str:
        """Serialize a protobuf field value to string.

        Handles proto MapComposite objects, RepeatedComposite (proto lists), and primitives.

        Args:
            field_value: Value from protobuf message field

        Returns:
            JSON string representation for complex types, string for primitives
        """

        # Handle None
        if field_value is None:
            return ""

        # Get the class name for type checking
        class_name = (
            str(field_value.__class__) if hasattr(field_value, "__class__") else ""
        )

        # Handle proto RepeatedComposite (list-like) objects FIRST
        # This is what contains the list of MapComposite objects
        if "RepeatedComposite" in class_name or "Repeated" in class_name:
            try:
                # RepeatedComposite is iterable, convert items to list
                serializable_list = []
                for item in field_value:
                    item_class = (
                        str(item.__class__) if hasattr(item, "__class__") else ""
                    )
                    if "MapComposite" in item_class:
                        # Convert MapComposite to regular dict with primitive values
                        item_dict = {}
                        for key, value in dict(item).items():
                            # Recursively handle nested proto objects
                            if hasattr(value, "__class__") and (
                                "MapComposite" in str(value.__class__)
                                or "RepeatedComposite" in str(value.__class__)
                            ):
                                # Recursively serialize nested proto objects
                                item_dict[key] = json.loads(
                                    self._serialize_field_value(value)
                                )
                            else:
                                # Primitives - keep as is
                                item_dict[key] = str(value)
                        serializable_list.append(item_dict)
                    else:
                        # Handle primitives in the list
                        serializable_list.append(item)
                return json.dumps(serializable_list)
            except Exception as e:
                logger.warning(
                    f"Failed to serialize RepeatedComposite: {e}. Returning length."
                )
                try:
                    return f"[{len(list(field_value))} items]"
                except Exception:
                    return "[unknown items]"

        # Handle proto MapComposite (dict-like) objects
        if "MapComposite" in class_name:
            try:
                dict_value = dict(field_value)
                return json.dumps(dict_value)
            except Exception as e:
                logger.warning(
                    f"Failed to serialize MapComposite to JSON: {e}. Returning type name."
                )
                return str(type(field_value).__name__)

        # Handle regular Python lists/tuples
        if isinstance(field_value, (list, tuple)):
            # Check if it's a list of proto objects
            if field_value and hasattr(field_value[0], "__class__"):
                first_class = str(field_value[0].__class__)
                if "proto" in first_class.lower() or "MapComposite" in first_class:
                    # Try to serialize as JSON
                    try:
                        # Convert proto objects to dicts if possible
                        serializable_list2: list[Any] = []
                        for item in field_value:
                            if hasattr(item, "__class__") and "MapComposite" in str(
                                item.__class__
                            ):
                                serializable_list2.append(dict(item))
                            else:
                                serializable_list2.append(str(item))
                        return json.dumps(serializable_list2)
                    except Exception as e:
                        logger.warning(
                            f"Failed to serialize list of proto objects: {e}. Returning length."
                        )
                        return f"[{len(field_value)} items]"
            # Regular list of primitives
            return json.dumps(field_value)

        # Handle primitives (str, int, float, bool)
        if isinstance(field_value, (str, int, float, bool)):
            return str(field_value)

        # Fallback: try JSON serialization, then string conversion
        try:
            return json.dumps(field_value)
        except (TypeError, ValueError):
            return str(field_value)

    def _extract_aspects_to_custom_properties(
        self, aspects: Mapping[Any, Any], custom_properties: dict[str, str]
    ) -> None:
        """Extract aspects as custom properties.

        Args:
            aspects: Dictionary of aspects from entry or entity
            custom_properties: Dictionary to update with aspect properties
        """
        for aspect_key, aspect_value in aspects.items():
            aspect_type = aspect_key.split("/")[-1]
            custom_properties[f"dataplex_aspect_{aspect_type}"] = aspect_type

            if hasattr(aspect_value, "data") and aspect_value.data:
                for field_key, field_value in aspect_value.data.items():
                    property_key = f"dataplex_{aspect_type}_{field_key}"
                    custom_properties[property_key] = self._serialize_field_value(
                        field_value
                    )

    def _track_bigquery_container(
        self, project_id: str, dataset_id: str
    ) -> Optional[str]:
        """Track BigQuery dataset for container creation and return container URN.

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID (format: project.dataset)

        Returns:
            Container URN if BigQuery, None otherwise
        """
        with self._bq_containers_lock:
            if project_id not in self.bq_containers:
                self.bq_containers[project_id] = set()
            self.bq_containers[project_id].add(dataset_id)

        bq_dataset_container_key = make_bigquery_dataset_container_key(
            project_id=project_id,
            dataset_id=dataset_id,
            platform="bigquery",
            env=self.config.env,
        )
        return bq_dataset_container_key.as_urn()

    def _construct_mcps(
        self, dataset_urn: str, aspects: list
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Construct MCPs for the given dataset.

        Args:
            dataset_urn: Dataset URN
            aspects: List of aspect objects

        Yields:
            MetadataChangeProposalWrapper objects
        """
        return MetadataChangeProposalWrapper.construct_many(
            entityUrn=dataset_urn,
            aspects=aspects,
        )

    def _extract_entry_custom_properties(
        self, entry: dataplex_v1.Entry, entry_id: str, entry_group_id: str
    ) -> dict[str, str]:
        """Extract custom properties from a Dataplex entry.

        Args:
            entry: Entry object from Catalog API
            entry_id: Entry ID
            entry_group_id: Entry group ID

        Returns:
            Dictionary of custom properties
        """
        custom_properties = {
            "dataplex_ingested": "true",
            "dataplex_entry_id": entry_id,
            "dataplex_entry_group": entry_group_id,
            "dataplex_fully_qualified_name": entry.fully_qualified_name,
        }

        if entry.entry_type:
            custom_properties["dataplex_entry_type"] = entry.entry_type

        if hasattr(entry, "parent_entry") and entry.parent_entry:
            custom_properties["dataplex_parent_entry"] = entry.parent_entry

        # Extract entry source metadata
        if entry.entry_source:
            if hasattr(entry.entry_source, "resource") and entry.entry_source.resource:
                custom_properties["dataplex_source_resource"] = (
                    entry.entry_source.resource
                )
            if hasattr(entry.entry_source, "system") and entry.entry_source.system:
                custom_properties["dataplex_source_system"] = entry.entry_source.system
            if hasattr(entry.entry_source, "platform") and entry.entry_source.platform:
                custom_properties["dataplex_source_platform"] = (
                    entry.entry_source.platform
                )

        # Extract aspects as custom properties
        if entry.aspects:
            self._extract_aspects_to_custom_properties(entry.aspects, custom_properties)

        return custom_properties

    def _extract_entity_custom_properties(
        self,
        entity_full: dataplex_v1.Entity,
        project_id: str,
        lake_id: str,
        zone_id: str,
        entity_id: str,
    ) -> dict[str, str]:
        """Extract custom properties from a Dataplex entity.

        Args:
            entity_full: Full entity object from Dataplex
            project_id: GCP project ID
            lake_id: Dataplex lake ID
            zone_id: Dataplex zone ID
            entity_id: Entity ID

        Returns:
            Dictionary of custom properties
        """
        custom_properties = {
            "dataplex_ingested": "true",
            "dataplex_lake": lake_id,
            "dataplex_zone": zone_id,
            "dataplex_entity_id": entity_id,
        }

        # Add zone type from metadata
        zone_key = f"{project_id}.{lake_id}.{zone_id}"
        with self._zone_metadata_lock:
            if zone_key in self.zone_metadata:
                custom_properties["dataplex_zone_type"] = self.zone_metadata[zone_key]

        if entity_full.data_path:
            custom_properties["data_path"] = entity_full.data_path

        if entity_full.system:
            custom_properties["system"] = entity_full.system.name

        if entity_full.format:
            custom_properties["format"] = entity_full.format.format_.name

        # Extract additional metadata fields
        if entity_full.asset:
            custom_properties["asset"] = entity_full.asset

        if hasattr(entity_full, "catalog_entry") and entity_full.catalog_entry:
            custom_properties["catalog_entry"] = entity_full.catalog_entry

        if hasattr(entity_full, "compatibility") and entity_full.compatibility:
            custom_properties["compatibility"] = str(entity_full.compatibility)

        # Extract aspects as custom properties
        if hasattr(entity_full, "aspects") and entity_full.aspects:
            self._extract_aspects_to_custom_properties(
                entity_full.aspects, custom_properties
            )

        return custom_properties

    def _process_zone_entities(
        self, project_id: str, lake_id: str, zone_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Process all entities for a single zone (called by parallel workers).

        Args:
            project_id: GCP project ID
            lake_id: Dataplex lake ID
            zone_id: Dataplex zone ID

        Yields:
            MetadataChangeProposalWrapper objects for entities in this zone
        """
        entities_parent = f"projects/{project_id}/locations/{self.config.location}/lakes/{lake_id}/zones/{zone_id}"
        entities_request = dataplex_v1.ListEntitiesRequest(parent=entities_parent)

        try:
            entities = self.metadata_client.list_entities(request=entities_request)

            for entity in entities:
                entity_id = entity.id
                logger.debug(
                    f"Processing entity: {entity_id} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
                )

                # Skip invalid entities (empty IDs or placeholder metadata)
                if not entity_id or not entity_id.strip():
                    logger.debug(
                        f"Skipping entity with empty ID in zone {zone_id}, lake {lake_id}"
                    )
                    continue

                if not self.config.filter_config.entities.dataset_pattern.allowed(
                    entity_id
                ):
                    logger.debug(
                        f"Entity {entity_id} filtered out by entities.dataset_pattern"
                    )
                    with self._report_lock:
                        self.report.report_entity_scanned(entity_id, filtered=True)
                    continue

                with self._report_lock:
                    self.report.report_entity_scanned(entity_id)
                logger.debug(
                    f"Processing entity: {entity_id} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
                )

                # Determine source platform and dataset id from asset (bigquery, gcs, etc.)
                # Use double-checked locking to avoid holding lock during API calls
                needs_fetch = False
                asset_name = entity.asset
                source_platform: Optional[str] = None
                dataset_id: Optional[str] = None

                with self._asset_metadata_lock:
                    if asset_name in self.asset_metadata:
                        source_platform, dataset_id = self.asset_metadata[asset_name]
                    else:
                        # Not in cache - need to fetch. Release lock before API call.
                        needs_fetch = True

                if needs_fetch:
                    # Make API call WITHOUT holding the lock (allows parallel processing)
                    fetched_platform, fetched_dataset_id = extract_entity_metadata(
                        project_id,
                        lake_id,
                        zone_id,
                        entity_id,
                        asset_name,
                        self.config.location,
                        self.dataplex_client,
                    )

                    # Re-acquire lock to update cache
                    with self._asset_metadata_lock:
                        # Check again in case another thread already cached it
                        if asset_name in self.asset_metadata:
                            source_platform, dataset_id = self.asset_metadata[
                                asset_name
                            ]
                        else:
                            source_platform = fetched_platform
                            dataset_id = fetched_dataset_id
                            if source_platform and dataset_id:
                                self.asset_metadata[asset_name] = (
                                    source_platform,
                                    dataset_id,
                                )

                # Skip entities where we couldn't determine platform or dataset
                if source_platform is None or dataset_id is None:
                    logger.debug(
                        f"Skipping entity {entity_id} - unable to determine platform or dataset from asset {entity.asset}"
                    )
                    continue

                # Track entity ID for lineage extraction
                with self._entity_data_lock:
                    if project_id not in self.entity_data_by_project:
                        self.entity_data_by_project[project_id] = set[EntityDataTuple]()
                    self.entity_data_by_project[project_id].add(
                        EntityDataTuple(
                            lake_id=lake_id,
                            zone_id=zone_id,
                            entity_id=entity_id,
                            asset_id=entity.asset,
                            source_platform=source_platform,
                            dataset_id=dataset_id,
                        )
                    )

                # Fetch full entity details including schema
                try:
                    get_entity_request = dataplex_v1.GetEntityRequest(
                        name=entity.name,
                        view=dataplex_v1.GetEntityRequest.EntityView.FULL,
                    )
                    entity_full = self.metadata_client.get_entity(
                        request=get_entity_request
                    )
                except exceptions.GoogleAPICallError as e:
                    logger.warning(
                        f"Could not fetch full entity details for {entity_id}: {e}"
                    )
                    entity_full = entity

                # Skip non-table entities (only process TABLE and FILESET types)
                entity_type = (
                    entity_full.type_.name if hasattr(entity_full, "type_") else None
                )
                if entity_type not in ("TABLE", "FILESET"):
                    logger.debug(
                        f"Skipping entity {entity_id} with type {entity_type} - only TABLE and FILESET types are supported"
                    )
                    continue

                # Skip entities that are just asset metadata (entity_id matches asset name)
                # These are placeholder entities that represent the asset itself, not actual tables/files
                if entity.asset and entity_id == entity.asset:
                    logger.debug(
                        f"Skipping entity {entity_id} - entity ID matches asset name, likely asset metadata not a table/file"
                    )
                    continue

                # Generate dataset URN with source platform (bigquery, gcs, etc.)
                dataset_urn = make_entity_dataset_urn(
                    entity_id,
                    project_id,
                    self.config.env,
                    dataset_id=dataset_id,
                    platform=source_platform,
                )

                # Extract schema metadata (if enabled)
                schema_metadata = None
                if self.config.include_schema:
                    schema_metadata = self._extract_schema_metadata(
                        entity_full, dataset_urn, source_platform
                    )

                # Extract custom properties using helper method
                custom_properties = self._extract_entity_custom_properties(
                    entity_full, project_id, lake_id, zone_id, entity_id
                )

                # Build aspects list
                created_time = make_audit_stamp(entity_full.create_time)
                modified_time = make_audit_stamp(entity_full.update_time)
                aspects = [
                    DatasetPropertiesClass(
                        name=entity_id,
                        description=entity_full.description or "",
                        customProperties=custom_properties,
                        created=TimeStampClass(**created_time)
                        if created_time
                        else None,
                        lastModified=TimeStampClass(**modified_time)
                        if modified_time
                        else None,
                    ),
                    DataPlatformInstanceClass(
                        platform=str(DataPlatformUrn(source_platform))
                    ),
                    SubTypesClass(
                        typeNames=[
                            entity_full.type_.name,
                        ]
                    ),
                ]

                # Add schema metadata if available
                if schema_metadata:
                    aspects.append(schema_metadata)

                # Link to source platform container (only for BigQuery)
                if source_platform == "bigquery":
                    container_urn = self._track_bigquery_container(
                        project_id, dataset_id
                    )
                    if container_urn:
                        aspects.append(ContainerClass(container=container_urn))

                # Construct MCPs
                yield from self._construct_mcps(dataset_urn, aspects)

        except exceptions.GoogleAPICallError as e:
            with self._report_lock:
                self.report.report_failure(
                    title=f"Failed to list entities in zone {zone_id}",
                    message=f"Error listing entities in project {project_id}, lake {lake_id}, zone {zone_id}",
                    exc=e,
                )

    def _get_entities_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch entities from Dataplex and generate corresponding MCPs as Datasets.

        This method parallelizes entity extraction at the zone level using ThreadedIteratorExecutor,
        following the pattern established by BigQuery V2.
        """
        parent = f"projects/{project_id}/locations/{self.config.location}"

        # Collect all zones to process in parallel
        zones_to_process = []

        try:
            with self.report.dataplex_api_timer as _:
                lakes_request = dataplex_v1.ListLakesRequest(parent=parent)
                lakes = self.dataplex_client.list_lakes(request=lakes_request)

            for lake in lakes:
                lake_id = lake.name.split("/")[-1]

                if not self.config.filter_config.entities.lake_pattern.allowed(lake_id):
                    continue

                zones_parent = f"projects/{project_id}/locations/{self.config.location}/lakes/{lake_id}"
                zones_request = dataplex_v1.ListZonesRequest(parent=zones_parent)

                try:
                    zones = self.dataplex_client.list_zones(request=zones_request)

                    for zone in zones:
                        zone_id = zone.name.split("/")[-1]

                        if not self.config.filter_config.entities.zone_pattern.allowed(
                            zone_id
                        ):
                            continue

                        # Store zone type for later use in entity custom properties
                        zone_key = f"{project_id}.{lake_id}.{zone_id}"
                        with self._zone_metadata_lock:
                            self.zone_metadata[zone_key] = zone.type_.name

                        # Add this zone to the list for parallel processing
                        zones_to_process.append((project_id, lake_id, zone_id))

                except exceptions.GoogleAPICallError as e:
                    self.report.report_failure(
                        title=f"Failed to list zones in lake {lake_id}",
                        message=f"Error listing zones for entity extraction in project {project_id}",
                        exc=e,
                    )

        except exceptions.GoogleAPICallError as e:
            self.report.report_failure(
                title="Failed to list lakes for entity extraction",
                message=f"Error listing lakes in project {project_id}",
                exc=e,
            )

        # Process zones in parallel using ThreadedIteratorExecutor
        if zones_to_process:
            logger.info(
                f"Processing {len(zones_to_process)} zones in parallel with {self.config.max_workers} workers"
            )
            for wu in ThreadedIteratorExecutor.process(
                worker_func=self._process_zone_entities,
                args_list=zones_to_process,
                max_workers=self.config.max_workers,
            ):
                yield wu

    def _get_entries_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch entries from Universal Catalog and generate MCPs.

        This method uses the Entries API to extract metadata from Universal Catalog.
        It processes entry groups and their entries, extracting aspects as custom properties.

        Uses entries_location if specified, otherwise falls back to location.
        For system entry groups (@bigquery, @pubsub), use multi-region locations (us, eu, asia).
        """
        # Use configured entries_location (defaults to "us")
        entries_location = self.config.entries_location
        parent = f"projects/{project_id}/locations/{entries_location}"

        try:
            with self.report.catalog_api_timer as _:
                entry_groups_request = dataplex_v1.ListEntryGroupsRequest(parent=parent)
                entry_groups = self.catalog_client.list_entry_groups(
                    request=entry_groups_request
                )

            for entry_group in entry_groups:
                entry_group_id = entry_group.name.split("/")[-1]
                logger.debug(f"Processing entry group: {entry_group_id}")
                with self._report_lock:
                    self.report.report_entry_group_scanned()
                entries_request = dataplex_v1.ListEntriesRequest(
                    parent=entry_group.name
                )
                entries = self.catalog_client.list_entries(request=entries_request)

                for entry in entries:
                    entry_id = entry.name.split("/")[-1]
                    logger.debug(f"Processing entry: {entry_id}")

                    # Apply dataset_pattern filter to entries
                    if not self.config.filter_config.entries.dataset_pattern.allowed(
                        entry_id
                    ):
                        logger.debug(
                            f"Entry {entry_id} filtered out by entries.dataset_pattern"
                        )
                        with self._report_lock:
                            self.report.report_entry_scanned(entry_id, filtered=True)
                        continue

                    entry_details_request = dataplex_v1.GetEntryRequest(
                        name=entry.name, view=dataplex_v1.EntryView.ALL
                    )
                    entry_details = self.catalog_client.get_entry(
                        request=entry_details_request
                    )

                    with self._report_lock:
                        self.report.report_entry_scanned(entry_id)
                    yield from self._process_entry(
                        project_id, entry_details, entry_group_id
                    )

        except exceptions.GoogleAPICallError as e:
            self.report.report_failure(
                title="Failed to list entry groups for entity extraction",
                message=f"Error listing entry groups in project {project_id}",
                exc=e,
            )

    def _process_entry(
        self,
        project_id: str,
        entry: dataplex_v1.Entry,
        entry_group_id: str,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Process a single entry from Universal Catalog.

        Args:
            project_id: GCP project ID
            entry: Entry object from Catalog API
            entry_group_id: Entry group ID

        Yields:
            MetadataChangeProposalWrapper objects for the entry
        """
        entry_id = entry.name.split("/")[-1]

        if not entry.fully_qualified_name:
            logger.debug(f"Entry {entry_id} has no fully_qualified_name, skipping")
            return

        fqn = entry.fully_qualified_name
        logger.debug(f"Processing entry with FQN: {fqn}")

        # Apply dataset pattern filter to entry_id
        if not self.config.filter_config.entries.dataset_pattern.allowed(entry_id):
            logger.debug(f"Entry {entry_id} filtered out by entries.dataset_pattern")
            return

        # Parse the FQN to determine platform and dataset_id
        source_platform, dataset_id = self._parse_entry_fqn(fqn)
        if not source_platform or not dataset_id:
            logger.warning(f"Could not parse FQN {fqn} for entry {entry_id}, skipping")
            return

        # Validate that FQN has a table/file component (not just zone/asset metadata)
        if ":" in fqn:
            _, resource_path = fqn.split(":", 1)

            # For BigQuery: should be project.dataset.table (3 parts minimum)
            if source_platform == "bigquery":
                parts = resource_path.split(".")
                if len(parts) < 3:
                    logger.debug(
                        f"Skipping entry {entry_id} with FQN {fqn} - missing table name (only {len(parts)} parts)"
                    )
                    return
                # Check if the table name looks like a zone or asset (common pattern suffixes)
                table_name = parts[-1]
                if any(
                    suffix in table_name.lower()
                    for suffix in ["_zone", "_asset", "zone1", "asset1"]
                ):
                    logger.debug(
                        f"Skipping entry {entry_id} with FQN {fqn} - table name '{table_name}' appears to be zone/asset metadata"
                    )
                    return

            # For GCS: should be bucket/path (2 parts minimum)
            elif source_platform == "gcs":
                parts = resource_path.split("/")
                if len(parts) < 2:
                    logger.debug(
                        f"Skipping entry {entry_id} with FQN {fqn} - missing file path (only {len(parts)} parts)"
                    )
                    return
                # Check if the file/object name looks like an asset
                object_name = parts[-1]
                if any(
                    suffix in object_name.lower() for suffix in ["_asset", "asset1"]
                ):
                    logger.debug(
                        f"Skipping entry {entry_id} with FQN {fqn} - object name '{object_name}' appears to be asset metadata"
                    )
                    return

        # Track entry for lineage extraction (entries don't have lake/zone/asset info,
        # but lineage API only needs FQN which we can construct from entry metadata)
        with self._entity_data_lock:
            if project_id not in self.entity_data_by_project:
                self.entity_data_by_project[project_id] = set[EntityDataTuple]()
            self.entity_data_by_project[project_id].add(
                EntityDataTuple(
                    lake_id="",  # Not available in Entry objects
                    zone_id="",  # Not available in Entry objects
                    entity_id=entry_id,
                    asset_id="",  # Not available in Entry objects
                    source_platform=source_platform,
                    dataset_id=dataset_id,
                    is_entry=True,  # Flag that this is from Entries API
                )
            )

        # Generate dataset URN using the full resource path from FQN
        # For BigQuery: bigquery:project.dataset.table -> use full path
        # This ensures consistency with entity URNs
        if ":" in fqn:
            _, resource_path = fqn.split(":", 1)
            dataset_name = resource_path
        else:
            dataset_name = entry_id

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=source_platform,
            name=dataset_name,
            platform_instance=None,
            env=self.config.env,
        )
        logger.debug(
            f"Created dataset URN for entry {entry_id} (FQN: {fqn}): {dataset_urn}"
        )

        # Extract custom properties using helper method
        custom_properties = self._extract_entry_custom_properties(
            entry, entry_id, entry_group_id
        )

        # Try to extract schema from entry aspects (if enabled)
        schema_metadata = None
        if self.config.include_schema:
            schema_metadata = self._extract_schema_from_entry_aspects(
                entry, entry_id, source_platform
            )

        # Build aspects list
        created_time = (
            make_audit_stamp(entry.entry_source.create_time)
            if entry.entry_source.create_time
            else None
        )
        modified_time = (
            make_audit_stamp(entry.entry_source.update_time)
            if entry.entry_source.update_time
            else None
        )
        aspects = [
            DatasetPropertiesClass(
                name=entry_id,
                description=entry.entry_source.description or "",
                customProperties=custom_properties,
                created=TimeStampClass(**created_time) if created_time else None,
                lastModified=TimeStampClass(**modified_time) if modified_time else None,
            ),
            DataPlatformInstanceClass(platform=str(DataPlatformUrn(source_platform))),
        ]

        # Add schema metadata if available
        if schema_metadata:
            aspects.append(schema_metadata)
            logger.debug(
                f"Added schema metadata for entry {entry_id} with {len(schema_metadata.fields)} fields"
            )

        # Link to source platform container (only for BigQuery)
        if source_platform == "bigquery":
            # Extract project_id and dataset from the full FQN
            # dataset_id format: project.dataset.table
            parts = dataset_id.split(".")
            if len(parts) >= 3:
                bq_project_id = parts[0]
                bq_dataset_id = parts[1]
                container_urn = self._track_bigquery_container(
                    bq_project_id, bq_dataset_id
                )
                if container_urn:
                    aspects.append(ContainerClass(container=container_urn))
            else:
                logger.warning(
                    f"Could not extract BigQuery project and dataset from dataset_id '{dataset_id}' for entry {entry_id}"
                )

        # Construct MCPs
        yield from self._construct_mcps(dataset_urn, aspects)

    def _parse_entry_fqn(self, fqn: str) -> tuple[str, str]:
        """Parse fully qualified name to extract platform and dataset_id.

        Args:
            fqn: Fully qualified name (e.g., 'bigquery:project.dataset.table')

        Returns:
            Tuple of (platform, dataset_id)
            - For BigQuery: dataset_id is 'project.dataset.table'
            - For GCS: dataset_id is 'bucket/path'
        """
        if ":" not in fqn:
            return "", ""

        platform, resource_path = fqn.split(":", 1)

        if platform == "bigquery":
            # BigQuery FQN format: bigquery:project.dataset.table
            # Return the full project.dataset.table as dataset_id
            parts = resource_path.split(".")
            if len(parts) >= 3:
                # Full table reference: project.dataset.table
                return platform, resource_path
            elif len(parts) == 2:
                # Dataset reference (legacy): project.dataset
                logger.warning(
                    f"BigQuery FQN '{fqn}' only has 2 parts (project.dataset), expected 3 (project.dataset.table)"
                )
                return platform, resource_path
            else:
                logger.warning(
                    f"BigQuery FQN '{fqn}' has unexpected format, expected 'bigquery:project.dataset.table'"
                )
                return platform, resource_path
        elif platform == "gcs":
            # GCS FQN format: gcs:bucket/path
            # Return the full bucket/path as dataset_id
            return platform, resource_path

        # For other platforms, return the full resource_path
        return platform, resource_path

    def _extract_schema_metadata(
        self, entity: dataplex_v1.Entity, dataset_urn: str, platform: str
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema metadata from Dataplex entity."""
        if not entity.schema or not entity.schema.fields:
            return None

        fields = []
        for field in entity.schema.fields:
            field_path = field.name

            field_type = map_dataplex_field_to_datahub(field)

            schema_field = SchemaFieldClass(
                fieldPath=field_path,
                type=field_type,
                nativeDataType=dataplex_v1.types.Schema.Type(field.type_).name,
                description=field.description or "",
                nullable=True,  # Dataplex doesn't explicitly track nullability
                recursive=False,
            )

            # Handle nested fields
            if field.fields:
                schema_field.type = SchemaFieldDataTypeClass(type=RecordTypeClass())
                # Add nested fields
                for nested_field in field.fields:
                    nested_field_path = f"{field_path}.{nested_field.name}"
                    nested_type = map_dataplex_field_to_datahub(nested_field)
                    nested_schema_field = SchemaFieldClass(
                        fieldPath=nested_field_path,
                        type=nested_type,
                        nativeDataType=dataplex_v1.types.Schema.Type(
                            nested_field.type_
                        ).name,
                        description=nested_field.description or "",
                        nullable=True,
                        recursive=False,
                    )
                    fields.append(nested_schema_field)

            fields.append(schema_field)

        return SchemaMetadataClass(
            schemaName=entity.id,
            platform=str(DataPlatformUrn(platform)),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

    def _extract_field_value(
        self, field_data: Any, field_key: str, default: str = ""
    ) -> str:
        """Extract a field value from protobuf field data (dict or object).

        Args:
            field_data: Field data (dict or protobuf object)
            field_key: Primary key to look for
            default: Default value if not found

        Returns:
            Extracted value as string
        """
        if isinstance(field_data, dict):
            val = field_data.get(field_key)
            if val is None:
                return default
            return (
                val.string_value
                if hasattr(val, "string_value")
                else str(val)
                if val
                else default
            )
        else:
            val = getattr(field_data, field_key, None)
            return str(val) if val else default

    def _process_schema_field_item(
        self, field_value: Any, entry_id: str
    ) -> Optional[Any]:
        """Process a single schema field item from protobuf data.

        Args:
            field_value: Field value from schema fields list
            entry_id: Entry ID for logging

        Returns:
            Field data object or None
        """
        if hasattr(field_value, "struct_value"):
            # Protobuf Value with struct_value
            return dict(field_value.struct_value.fields)
        elif hasattr(field_value, "__getitem__") or hasattr(field_value, "__dict__"):
            # Direct object or dict-like (proto.marshal objects)
            try:
                return (
                    dict(field_value) if hasattr(field_value, "items") else field_value
                )
            except (TypeError, AttributeError):
                return field_value
        return None

    def _extract_schema_from_entry_aspects(
        self, entry: dataplex_v1.Entry, entry_id: str, platform: str
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema metadata from Entry aspects.

        Looks for schema-type aspects in the entry and extracts column/field information.
        The schema aspect is typically stored at 'dataplex-types.global.schema'.

        Args:
            entry: Entry object from Catalog API
            entry_id: Entry ID for naming the schema
            platform: Platform name (bigquery, gcs, etc.)

        Returns:
            SchemaMetadataClass if schema aspect found, None otherwise
        """
        if not entry.aspects:
            logger.debug(f"Entry {entry_id} has no aspects")
            return None

        # Log all available aspect types for debugging
        aspect_keys = list(entry.aspects.keys())
        logger.debug(f"Entry {entry_id} has aspects: {aspect_keys}")

        # Look for the standard Dataplex schema aspect type
        # According to Dataplex docs, schema aspects are at:
        # "dataplex-types.global.schema" or similar paths
        schema_aspect = None
        schema_aspect_key = None

        # First, try the standard Dataplex schema aspect type
        for aspect_key in entry.aspects:
            # Check for the global schema aspect type (most common)
            if "dataplex-types.global.schema" in aspect_key or aspect_key.endswith(
                "/schema"
            ):
                schema_aspect = entry.aspects[aspect_key]
                schema_aspect_key = aspect_key
                logger.debug(
                    f"Found schema aspect for entry {entry_id} at key: {aspect_key}"
                )
                break

        # Fallback: Look for any aspect with "schema" in the name
        if not schema_aspect:
            for aspect_key, aspect_value in entry.aspects.items():
                aspect_type = aspect_key.split("/")[-1]
                if "schema" in aspect_type.lower():
                    schema_aspect = aspect_value
                    schema_aspect_key = aspect_key
                    logger.debug(
                        f"Found schema-like aspect for entry {entry_id} at key: {aspect_key}"
                    )
                    break

        if not schema_aspect:
            logger.debug(
                f"No schema aspect found for entry {entry_id}. Available aspects: {aspect_keys}"
            )
            return None

        if not hasattr(schema_aspect, "data") or not schema_aspect.data:
            logger.debug(
                f"Schema aspect {schema_aspect_key} for entry {entry_id} has no data"
            )
            return None

        # Extract schema fields from aspect data
        fields: list[SchemaFieldClass] = []
        try:
            # The aspect.data is a Struct (protobuf)
            data_dict = dict(schema_aspect.data)
            logger.debug(
                f"Schema aspect data keys for entry {entry_id}: {list(data_dict.keys())}"
            )

            # Common field names in schema aspects: columns, fields, schema
            schema_fields_data = (
                data_dict.get("columns")
                or data_dict.get("fields")
                or data_dict.get("schema")
            )

            if not schema_fields_data:
                logger.debug(
                    f"No column/field data found in schema aspect for entry {entry_id}. "
                    f"Available keys: {list(data_dict.keys())}"
                )
                return None

            # The schema_fields_data can be either:
            # 1. A protobuf Value with list_value attribute (from some aspects)
            # 2. A RepeatedComposite (proto.marshal list-like object) that can be iterated directly
            logger.debug(
                f"Processing schema fields for entry {entry_id}, type: {type(schema_fields_data).__name__}"
            )

            # Try to iterate schema_fields_data - it could be a RepeatedComposite or list_value
            schema_items = None
            if hasattr(schema_fields_data, "list_value"):
                # Protobuf Value with list_value
                schema_items = schema_fields_data.list_value.values
                logger.debug(
                    f"Found {len(schema_items)} fields in list_value for entry {entry_id}"
                )
            elif hasattr(schema_fields_data, "__iter__"):
                # RepeatedComposite or other iterable (can iterate directly)
                schema_items = list(schema_fields_data)
                logger.debug(
                    f"Found {len(schema_items)} fields in iterable for entry {entry_id}"
                )

            if schema_items:
                for field_value in schema_items:
                    field_data = self._process_schema_field_item(field_value, entry_id)
                    if field_data:
                        # Extract field name, type, and description using helper
                        field_name = self._extract_field_value(
                            field_data, "name"
                        ) or self._extract_field_value(field_data, "column")
                        field_type = self._extract_field_value(
                            field_data, "type"
                        ) or self._extract_field_value(field_data, "dataType", "string")
                        field_desc = self._extract_field_value(
                            field_data, "description"
                        )

                        if field_name:
                            # Map the type string to DataHub schema type
                            datahub_type = self._map_aspect_type_to_datahub(
                                str(field_type)
                            )

                            schema_field = SchemaFieldClass(
                                fieldPath=str(field_name),
                                type=datahub_type,
                                nativeDataType=str(field_type),
                                description=field_desc,
                                nullable=True,
                                recursive=False,
                            )
                            fields.append(schema_field)
                            logger.debug(
                                f"Extracted field '{field_name}' ({field_type}) for entry {entry_id}"
                            )

            if not fields:
                logger.debug(
                    f"No schema fields extracted from entry {entry_id} aspects"
                )
                return None

            logger.info(f"Extracted {len(fields)} schema fields for entry {entry_id}")
            return SchemaMetadataClass(
                schemaName=entry_id,
                platform=str(DataPlatformUrn(platform)),
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=fields,
            )

        except Exception as e:
            logger.warning(
                f"Failed to extract schema from entry {entry_id} aspects: {e}",
                exc_info=True,
            )
            return None

    def _map_aspect_type_to_datahub(self, type_str: str) -> SchemaFieldDataTypeClass:
        """Map aspect schema type string to DataHub schema type.

        Args:
            type_str: Type string from aspect data (e.g., "STRING", "INTEGER", "BOOLEAN")

        Returns:
            SchemaFieldDataTypeClass for DataHub
        """
        type_str_upper = type_str.upper()

        # Map common types
        if type_str_upper in ("STRING", "VARCHAR", "CHAR", "TEXT"):
            return SchemaFieldDataTypeClass(type=StringTypeClass())
        elif type_str_upper in (
            "INTEGER",
            "INT",
            "INT64",
            "LONG",
        ) or type_str_upper in ("FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"):
            return SchemaFieldDataTypeClass(type=NumberTypeClass())
        elif type_str_upper in ("BOOLEAN", "BOOL"):
            return SchemaFieldDataTypeClass(type=BooleanTypeClass())
        elif type_str_upper in ("TIMESTAMP", "DATETIME", "DATE", "TIME"):
            return SchemaFieldDataTypeClass(type=TimeTypeClass())
        elif type_str_upper in ("BYTES", "BINARY"):
            return SchemaFieldDataTypeClass(type=BytesTypeClass())
        elif type_str_upper in ("RECORD", "STRUCT"):
            return SchemaFieldDataTypeClass(type=RecordTypeClass())
        elif type_str_upper == "ARRAY":
            return SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"]))
        else:
            # Default to string for unknown types
            return SchemaFieldDataTypeClass(type=StringTypeClass())

    def _gen_bigquery_project_container(
        self, project_id: str
    ) -> Iterable[MetadataWorkUnit]:
        """Generate BigQuery project container entity."""
        database_container_key = ProjectIdKey(
            project_id=project_id,
            platform="bigquery",
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

        yield from gen_database_container(
            database=project_id,
            database_container_key=database_container_key,
            sub_types=[DatasetContainerSubTypes.BIGQUERY_PROJECT],
            name=project_id,
            qualified_name=project_id,
        )

    def _gen_bigquery_dataset_container(
        self, project_id: str, dataset_id: str
    ) -> Iterable[MetadataWorkUnit]:
        """Generate BigQuery dataset container entity."""
        # Create keys for project and dataset containers
        database_container_key: ContainerKey = ProjectIdKey(
            project_id=project_id,
            platform="bigquery",
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

        schema_container_key = make_bigquery_dataset_container_key(
            project_id=project_id,
            dataset_id=dataset_id,
            platform="bigquery",
            env=self.config.env,
        )

        yield from gen_schema_container(
            database=project_id,
            schema=dataset_id,
            qualified_name=f"{project_id}.{dataset_id}",
            sub_types=[DatasetContainerSubTypes.BIGQUERY_DATASET],
            schema_container_key=schema_container_key,
            database_container_key=database_container_key,
        )

    def _gen_bigquery_containers(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """
        Generate BigQuery container entities for a project.

        Creates project container and dataset containers for all datasets
        discovered from Dataplex entities.
        """
        datasets = self.bq_containers.get(project_id, set())
        if not datasets:
            return

        logger.info(
            f"Creating BigQuery containers for project {project_id}: {len(datasets)} datasets"
        )

        # Emit project container first
        yield from self._gen_bigquery_project_container(project_id)

        # Emit dataset containers
        for dataset_id in sorted(datasets):  # Sort for deterministic order
            yield from self._gen_bigquery_dataset_container(project_id, dataset_id)

    def _get_lineage_workunits(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """
        Extract lineage for entities in a project.

        Args:
            project_id: GCP project ID

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if not self.lineage_extractor:
            return

        # Get entity IDs that were processed for this project
        entity_data = self.entity_data_by_project.get(project_id, set())
        if not entity_data:
            logger.info(
                f"No entities found for lineage extraction in project {project_id}"
            )
            return

        logger.info(
            f"Extracting lineage for {len(entity_data)} entities in project {project_id}"
        )

        try:
            yield from self.lineage_extractor.get_lineage_workunits(
                project_id, entity_data
            )
        except Exception as e:
            logger.warning(f"Failed to extract lineage for project {project_id}: {e}")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DataplexSource":
        """Factory method to create DataplexSource instance."""
        config = DataplexConfig.model_validate(config_dict)
        return cls(ctx, config)
