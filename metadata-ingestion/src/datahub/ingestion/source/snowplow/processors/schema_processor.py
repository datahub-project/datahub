"""
Schema Processor for Snowplow connector.

Handles extraction of event and entity schemas from:
- Snowplow BDP Console API (managed deployments)
- Iglu Schema Registry (open-source deployments)
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple

import requests

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.constants import SchemaType, infer_schema_type
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.ingestion.source.snowplow.snowplow_models import (
    DataStructure,
    IgluSchema,
)
from datahub.metadata.schema_classes import StatusClass
from datahub.sdk.dataset import Dataset

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )
    from datahub.ingestion.source.snowplow.services.data_structure_builder import (
        DataStructureBuilder,
    )

logger = logging.getLogger(__name__)


class SchemaProcessor(EntityProcessor):
    """
    Processor for extracting schema metadata from Snowplow.

    Handles both BDP-based extraction (with full deployment history and ownership)
    and Iglu-based extraction (open-source mode with manual schema configuration).
    """

    def __init__(
        self,
        deps: "ProcessorDependencies",
        state: "IngestionState",
        data_structure_builder: "DataStructureBuilder",
    ):
        """
        Initialize schema processor.

        Args:
            deps: Explicit dependencies needed by this processor
            state: Shared mutable state populated during extraction
            data_structure_builder: Builder for processing individual data structures
        """
        super().__init__(deps, state)
        self.data_structure_builder = data_structure_builder

    def is_enabled(self) -> bool:
        """
        Check if schema extraction is enabled.

        Schema extraction is always enabled as it's the core functionality.
        Other features (event specs, tracking scenarios) are optional.
        """
        return True

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract schema metadata from BDP or Iglu.

        Yields:
            MetadataWorkUnit: Schema metadata work units
        """
        if self.deps.bdp_client:
            # Extract from BDP Console API
            yield from self._extract_schemas_from_bdp()
        elif self.deps.iglu_client:
            # Extract from Iglu registry (open-source mode)
            logger.info(
                "Running in Iglu-only mode. Extracting schemas from Iglu registry."
            )
            yield from self._extract_schemas_from_iglu()
        else:
            logger.error("No API client configured for schema extraction")

    def _get_data_structures_filtered(self) -> List[DataStructure]:
        """
        Get data structures from BDP with pagination and timestamp filtering.

        Performance optimizations (Phase 1):
        - Caching: Returns cached data if available (prevents redundant API calls)
        - Parallel fetching: Fetches deployments concurrently when enabled

        Returns:
            List of data structures, filtered by deployed_since if configured
        """
        if not self.deps.bdp_client:
            return []

        # Check cache first (Phase 1 optimization)
        cached_data_structures = self.cache.get("data_structures")
        if cached_data_structures is not None:
            logger.info(
                f"Using cached data structures ({len(cached_data_structures)} schemas)"
            )
            return cached_data_structures

        # Fetch all data structures with pagination
        data_structures = self._fetch_all_data_structures()
        if not data_structures:
            return []

        # Fetch deployment history if needed
        if self.config.field_tagging.track_field_versions:
            self._fetch_deployments(data_structures)
            # Build field version mappings for description updates
            if self.config.field_tagging.tag_schema_version:
                self._build_field_version_mappings(data_structures)

        # Apply filters
        data_structures = self._filter_by_deployed_since(data_structures)
        data_structures = self._filter_by_schema_pattern(data_structures)

        # Cache the result (Phase 1 optimization)
        self.cache.set("data_structures", data_structures)
        logger.debug(f"Cached {len(data_structures)} data structures for reuse")

        return data_structures

    def _fetch_all_data_structures(self) -> List[DataStructure]:
        """Fetch all data structures with error handling."""
        try:
            return self.deps.bdp_client.get_data_structures(
                page_size=self.config.schema_page_size
            )
        except (requests.RequestException, ValueError) as e:
            # Expected errors: API failures, parsing errors
            self.report.report_failure(
                title="Failed to fetch data structures",
                message="Unable to retrieve schemas from BDP API. Check API credentials and network connectivity.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )
            return []
        except Exception as e:
            # Unexpected error - indicates a bug
            logger.error("Unexpected error fetching data structures", exc_info=True)
            self.report.report_failure(
                title="Unexpected error fetching data structures",
                message="This may indicate a bug in the connector. Please report this issue.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )
            return []

    def _fetch_deployments(self, data_structures: List[DataStructure]) -> None:
        """Fetch deployment history for all schemas (parallel or sequential)."""
        logger.info(
            "Field version tracking enabled - fetching full deployment history for all schemas"
        )

        schemas_needing_deployments = [ds for ds in data_structures if ds.hash]

        if (
            self.config.performance.enable_parallel_fetching
            and len(schemas_needing_deployments) > 1
        ):
            self._fetch_deployments_parallel(schemas_needing_deployments)
        else:
            self._fetch_deployments_sequential(schemas_needing_deployments)

    def _fetch_deployments_parallel(self, schemas: List[DataStructure]) -> None:
        """Fetch deployments in parallel using ThreadPoolExecutor."""
        max_workers = self.config.performance.max_concurrent_api_calls
        logger.info(
            f"Fetching deployments in parallel (max_workers={max_workers}) "
            f"for {len(schemas)} schemas"
        )

        def fetch_deployments_for_schema(
            ds: DataStructure,
        ) -> Tuple[str, str, Optional[List], Optional[Exception]]:
            """
            Fetch deployments for a schema without mutating the object.

            Returns:
                Tuple of (vendor, name, deployments, error)
            """
            try:
                if self.deps.bdp_client and ds.hash:
                    deployments = self.deps.bdp_client.get_data_structure_deployments(
                        ds.hash
                    )
                    logger.debug(
                        f"Fetched {len(deployments) if deployments else 0} deployments "
                        f"for {ds.vendor}/{ds.name}"
                    )
                    return ds.vendor, ds.name, deployments, None
                return ds.vendor, ds.name, None, None
            except Exception as e:
                return ds.vendor, ds.name, None, e

        # Collect results from all threads
        deployment_map = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(fetch_deployments_for_schema, ds) for ds in schemas
            ]

            for future in as_completed(futures):
                vendor, name, deployments, error = future.result()
                if error:
                    logger.warning(
                        f"Failed to fetch deployments for {vendor}/{name}: {error}"
                    )
                else:
                    # Store in map for later assignment
                    deployment_map[f"{vendor}/{name}"] = deployments

        # Safely update all schemas in single thread after all fetches complete
        for ds in schemas:
            schema_key = f"{ds.vendor}/{ds.name}"
            if schema_key in deployment_map:
                ds.deployments = deployment_map[schema_key]

        logger.info(
            f"Completed parallel deployment fetching for {len(schemas)} schemas "
            f"({len(deployment_map)} successful)"
        )

    def _fetch_deployments_sequential(self, schemas: List[DataStructure]) -> None:
        """Fetch deployments sequentially."""
        for ds in schemas:
            try:
                if ds.hash:
                    deployments = self.deps.bdp_client.get_data_structure_deployments(
                        ds.hash
                    )
                    ds.deployments = deployments
                    logger.debug(
                        f"Fetched {len(deployments)} deployments for {ds.vendor}/{ds.name}"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to fetch deployments for {ds.vendor}/{ds.name}: {e}"
                )

    def _filter_by_deployed_since(
        self, data_structures: List[DataStructure]
    ) -> List[DataStructure]:
        """Filter data structures by deployment timestamp."""
        if not self.config.deployed_since:
            return data_structures

        try:
            since_dt = datetime.fromisoformat(
                self.config.deployed_since.replace("Z", "+00:00")
            )

            filtered_structures = []
            for ds in data_structures:
                if ds.deployments:
                    for dep in ds.deployments:
                        if dep.ts and self._is_deployment_recent(dep.ts, since_dt):
                            filtered_structures.append(ds)
                            break

            logger.info(
                f"Filtered schemas by deployed_since={self.config.deployed_since}: "
                f"{len(filtered_structures)}/{len(data_structures)} schemas"
            )
            return filtered_structures

        except ValueError as e:
            logger.warning(
                f"Invalid deployed_since timestamp format '{self.config.deployed_since}': {e}. "
                f"Using all schemas."
            )
            return data_structures

    def _is_deployment_recent(self, dep_ts: str, since_dt: datetime) -> bool:
        """Check if deployment timestamp is recent."""
        try:
            dep_dt = datetime.fromisoformat(dep_ts.replace("Z", "+00:00"))
            return dep_dt >= since_dt
        except ValueError as e:
            logger.warning(f"Failed to parse deployment timestamp '{dep_ts}': {e}")
            return False

    def _filter_by_schema_pattern(
        self, data_structures: List[DataStructure]
    ) -> List[DataStructure]:
        """Filter data structures by schema pattern."""
        if not self.config.schema_pattern:
            return data_structures

        filtered_by_pattern = []
        for ds in data_structures:
            if not ds.vendor or not ds.name:
                logger.warning(
                    f"Data structure missing vendor or name (hash={ds.hash}), skipping pattern check"
                )
                continue

            schema_identifier = f"{ds.vendor}/{ds.name}"

            if self.config.schema_pattern.allowed(schema_identifier):
                filtered_by_pattern.append(ds)
            else:
                schema_type = (
                    ds.meta.schema_type if ds.meta and ds.meta.schema_type else "event"
                )
                self.report.report_schema_filtered(schema_type, schema_identifier)
                logger.debug(
                    f"Schema {schema_identifier} filtered out by schema_pattern"
                )

        logger.info(
            f"Filtered schemas by schema_pattern: "
            f"{len(filtered_by_pattern)}/{len(data_structures)} schemas"
        )
        return filtered_by_pattern

    def _extract_schemas_from_bdp(self) -> Iterable[MetadataWorkUnit]:
        """Extract schemas from BDP Console API."""
        if not self.deps.bdp_client:
            return

        # Get data structures with pagination and filtering
        data_structures = self._get_data_structures_filtered()

        for data_structure in data_structures:
            yield from self._process_data_structure(data_structure)

    def _extract_schemas_from_iglu(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract schemas from Iglu Schema Registry (Iglu-only mode).

        Uses automatic schema discovery via list_schemas() endpoint.
        Requires Iglu Server 0.6+ with /api/schemas endpoint support.
        """
        if not self.deps.iglu_client:
            return

        # Automatic schema discovery via /api/schemas endpoint
        schema_uris = self.deps.iglu_client.list_schemas()

        if not schema_uris:
            logger.error(
                "No schemas found in Iglu registry. Either the registry is empty or "
                "your Iglu Server doesn't support the /api/schemas endpoint (requires Iglu Server 0.6+)."
            )
            self.report.report_failure(
                title="No schemas found",
                message="Automatic schema discovery returned no results",
                context="Iglu-only mode requires Iglu Server 0.6+ with /api/schemas endpoint support",
            )
            return

        logger.info(
            f"Using automatic schema discovery: found {len(schema_uris)} schemas in Iglu registry"
        )
        yield from self._extract_schemas_from_uris(schema_uris)

    def _extract_schemas_from_uris(
        self, schema_uris: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Extract schemas from a list of Iglu URIs (automatic discovery).

        Args:
            schema_uris: List of schema URIs in format 'iglu:vendor/name/format/version'
        """
        for uri in schema_uris:
            if not self.deps.iglu_client:
                logger.warning("Iglu client not configured, skipping URI extraction")
                return
            parsed = self.deps.iglu_client.parse_iglu_uri(uri)
            if not parsed:
                logger.warning(f"Skipping invalid Iglu URI: {uri}")
                continue

            try:
                # Fetch schema from Iglu
                iglu_schema = self.deps.iglu_client.get_schema(
                    vendor=parsed["vendor"],
                    name=parsed["name"],
                    format=parsed["format"],
                    version=parsed["version"],
                )

                if iglu_schema:
                    yield from self._process_iglu_schema(
                        iglu_schema=iglu_schema,
                        vendor=parsed["vendor"],
                        name=parsed["name"],
                        version=parsed["version"],
                    )
                else:
                    logger.warning(f"Schema not found in Iglu: {uri}")

            except Exception as e:
                logger.error(f"Failed to fetch schema {uri} from Iglu: {e}")
                self.report.report_failure(
                    title=f"Failed to fetch schema {uri}",
                    message="Error fetching schema from Iglu registry",
                    context=uri,
                    exc=e,
                )

    def _process_iglu_schema(
        self,
        iglu_schema: IgluSchema,
        vendor: str,
        name: str,
        version: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single schema from Iglu registry.

        Args:
            iglu_schema: Schema from Iglu API
            vendor: Schema vendor
            name: Schema name
            version: Schema version
        """
        # Determine schema type (event or entity)
        # In Iglu-only mode, we don't have meta.schema_type, so infer from schema itself
        # Snowplow convention: contexts are entities, rest are events
        schema_type = infer_schema_type(name)

        # Track found schema
        self.report.report_schema_found(schema_type)

        # Build schema identifier for filtering
        schema_identifier = f"{vendor}/{name}"

        # Apply filtering
        if not self.config.schema_pattern.allowed(schema_identifier):
            self.report.report_schema_filtered(schema_type, schema_identifier)
            return

        # Filter by schema type
        if (
            schema_type == SchemaType.EVENT.value
            and not self.config.extract_event_schemas
            or schema_type == SchemaType.ENTITY.value
            and not self.config.extract_entity_schemas
        ):
            self.report.report_schema_filtered(schema_type, schema_identifier)
            logger.debug(
                f"Schema {schema_identifier} filtered out by schema type: {schema_type}"
            )
            return

        logger.info(f"Processing schema from Iglu: {schema_identifier} ({schema_type})")

        # Generate dataset URN
        dataset_urn = self.urn_factory.make_schema_dataset_urn(
            vendor=vendor,
            name=name,
            version=version,
        )

        # Use SDK V2 for dataset creation
        dataset = Dataset(
            urn=dataset_urn,
            name=name,
            description=iglu_schema.description,
            platform=self.deps.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            tags=[schema_type, "iglu"],
            status=StatusClass(removed=False),
            # Note: No ownership in Iglu-only mode (no deployment history)
        )

        # Parse schema fields
        if iglu_schema.data and iglu_schema.data.properties:
            from datahub.ingestion.source.snowplow.schema_parser import (
                SnowplowSchemaParser,
            )

            parser = SnowplowSchemaParser()
            fields = parser.parse_schema_fields(
                properties=iglu_schema.data.properties,
                required_fields=iglu_schema.data.required or [],
                parent_path="",
            )
            dataset.schema_metadata.fields.extend(fields)

        # Emit dataset
        for mcp in dataset.generate_mcp():
            yield mcp.as_workunit()

        # Track extracted schema
        self.report.report_schema_extracted(schema_type)

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
            or not self.deps.bdp_client
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
        full_structure = self.deps.bdp_client.get_data_structure_version(
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

    def _process_data_structure(
        self, data_structure: DataStructure
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single data structure (schema).

        Args:
            data_structure: Data structure from BDP API

        Yields:
            MetadataWorkUnit: Schema metadata work units
        """
        # Delegate to DataStructureBuilder
        yield from self.data_structure_builder.process_data_structure(data_structure)

    def _build_field_version_mappings(
        self, data_structures: List[DataStructure]
    ) -> None:
        """
        Build field version mappings for all schemas.

        Loops through data structures and builds a mapping of field paths to their
        introduction version for each schema. Stores results in state cache.

        Args:
            data_structures: List of data structures with deployment history
        """
        logger.info("Building field version mappings for description updates")
        for data_structure in data_structures:
            schema_key = f"{data_structure.vendor}/{data_structure.name}"
            field_version_map = self._build_field_version_mapping(data_structure)
            if field_version_map:
                self.state.field_version_cache[schema_key] = field_version_map
                logger.debug(f"Mapped {len(field_version_map)} fields for {schema_key}")

    def _build_field_version_mapping(
        self, data_structure: DataStructure
    ) -> Dict[str, str]:
        """
        Build mapping of field paths to the version they were first added in.

        Compares all versions of a schema to determine when each field was introduced.

        Args:
            data_structure: Data structure with deployments containing version history

        Returns:
            Dict mapping field_path -> version_added (e.g., {"email": "1-0-0", "phone": "1-1-0"})
        """
        schema_key = f"{data_structure.vendor}/{data_structure.name}"
        field_version_map: Dict[str, str] = {}

        # Need BDP client and deployments for version history
        if (
            not self.deps.bdp_client
            or not data_structure.deployments
            or not data_structure.hash
        ):
            logger.debug(
                f"Cannot track field versions for {schema_key}: missing BDP client, deployments, or hash"
            )
            return {}

        # Extract all unique versions from deployments and sort
        versions = sorted(
            set(d.version for d in data_structure.deployments if d.version),
            key=lambda v: self._parse_schemaver(v),
        )

        if not versions:
            logger.debug(f"No versions found in deployments for {schema_key}")
            return {}

        logger.info(
            f"Tracking field versions for {schema_key}: {len(versions)} versions to compare"
        )

        # Fetch all versions in parallel to avoid N+1 query pattern
        def fetch_one_version(version: str) -> Tuple[str, Optional[DataStructure]]:
            """Fetch a single version of the schema."""
            try:
                version_ds = self.deps.bdp_client.get_data_structure_version(
                    data_structure.hash, version
                )
                return (version, version_ds)
            except Exception as e:
                logger.warning(
                    f"Failed to fetch version {version} of {schema_key}: {e}"
                )
                return (version, None)

        # Fetch all versions in parallel
        version_schemas: Dict[str, Optional[DataStructure]] = {}
        max_workers = min(10, len(versions))  # Limit concurrent requests

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_one_version, v) for v in versions]

            for future in as_completed(futures):
                version, version_ds = future.result()
                version_schemas[version] = version_ds

        logger.debug(f"Fetched {len(version_schemas)} version schemas for {schema_key}")

        # Track fields seen in each version
        previous_fields: set = set()

        # Process versions in chronological order
        for version in versions:
            version_ds = version_schemas.get(version)

            if not version_ds or not version_ds.data:
                logger.warning(
                    f"Could not fetch schema for {schema_key} version {version}"
                )
                continue

            # Extract field paths from this version
            current_fields = set(version_ds.data.properties.keys())

            # Fields that are new in this version
            new_fields = current_fields - previous_fields

            # Record when each new field was added
            for field_path in new_fields:
                if field_path not in field_version_map:
                    field_version_map[field_path] = version
                    logger.debug(
                        f"Field '{field_path}' added in version {version} of {schema_key}"
                    )

            # Update for next iteration
            previous_fields = current_fields

        logger.info(
            f"Completed field version tracking for {schema_key}: "
            f"{len(field_version_map)} fields mapped"
        )

        return field_version_map

    @staticmethod
    def _parse_schemaver(version: str) -> Tuple[int, int, int]:
        """
        Parse SchemaVer version string into tuple for sorting.

        Args:
            version: Version string like "1-0-0" or "2-1-3"

        Returns:
            Tuple of (model, revision, addition) for comparison
        """
        try:
            parts = version.split("-")
            return (int(parts[0]), int(parts[1]), int(parts[2]))
        except (ValueError, IndexError):
            logger.warning(f"Invalid SchemaVer format: {version}, using (0,0,0)")
            return (0, 0, 0)
