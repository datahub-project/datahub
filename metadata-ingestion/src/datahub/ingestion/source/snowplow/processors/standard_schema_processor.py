"""
Standard Schema Processor for Snowplow connector.

Extracts Snowplow standard schemas from Iglu Central that are referenced by event specifications.
Standard schemas (vendor: com.snowplowanalytics.*) are not available in the BDP Data Structures API
but are publicly accessible from Iglu Central.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set

import requests

from datahub.emitter.mcp_builder import ContainerKey, gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.builders.container_keys import (
    SnowplowOrganizationKey,
    SnowplowVendorKey,
)
from datahub.ingestion.source.snowplow.constants import (
    DatasetSubtype,
    get_schema_subtype,
    infer_schema_type,
)
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.ingestion.source.snowplow.utils.schema_parser import SnowplowSchemaParser
from datahub.metadata.schema_classes import (
    ContainerClass,
    GlobalTagsClass,
    StatusClass,
    TagAssociationClass,
)
from datahub.sdk.dataset import Dataset

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )

logger = logging.getLogger(__name__)


class StandardSchemaProcessor(EntityProcessor):
    """
    Processor for extracting Snowplow standard schemas from Iglu Central.

    Collects Iglu URIs referenced by event specifications, filters for standard schemas
    (com.snowplowanalytics.*), fetches them from Iglu Central, and creates dataset entities.
    """

    def __init__(self, deps: "ProcessorDependencies", state: "IngestionState"):
        """
        Initialize standard schema processor.

        Args:
            deps: Explicit dependencies needed by this processor
            state: Shared mutable state populated during extraction
        """
        super().__init__(deps, state)
        self.iglu_central_url = self.config.iglu_central_url
        self._emitted_vendors: Set[str] = set()

    def is_enabled(self) -> bool:
        """Check if standard schema extraction is enabled."""
        return self.config.extract_standard_schemas and self.deps.bdp_client is not None

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract standard schemas referenced by event specifications.

        Strategy:
        1. Collect all Iglu URIs from event specs (stored in state)
        2. Filter for Snowplow standard schemas (com.snowplowanalytics.*)
        3. Fetch schemas from Iglu Central
        4. Create dataset entities

        Yields:
            MetadataWorkUnit: Schema metadata work units
        """
        if not self.deps.bdp_client:
            return

        # Collect referenced standard schema URIs
        standard_schema_uris = self._collect_standard_schema_uris()

        if not standard_schema_uris:
            logger.info(
                "No Snowplow standard schemas referenced by event specifications"
            )
            return

        logger.info(
            f"Found {len(standard_schema_uris)} Snowplow standard schema(s) referenced by event specs. "
            f"Fetching from Iglu Central..."
        )

        # Fetch and process each standard schema
        for iglu_uri in standard_schema_uris:
            try:
                yield from self._process_standard_schema(iglu_uri)
            except Exception as e:
                logger.warning(
                    f"Failed to extract standard schema {iglu_uri} from Iglu Central: {e}",
                    exc_info=True,
                )
                self.report.report_warning(
                    title="Failed to extract standard schema from Iglu Central",
                    message=f"Failed to extract standard schema {iglu_uri}: {e}. "
                    "Lineage from event specs to this schema may be incomplete.",
                )

    def _collect_standard_schema_uris(self) -> Set[str]:
        """
        Collect Iglu URIs for Snowplow standard schemas from state.

        Returns:
            Set of Iglu URIs for standard schemas (com.snowplowanalytics.*)
        """
        standard_uris: Set[str] = set()

        # Get URIs tracked during event spec processing
        for uri in self.state.referenced_iglu_uris:
            if self._is_standard_schema(uri):
                standard_uris.add(uri)

        return standard_uris

    def _is_standard_schema(self, iglu_uri: str) -> bool:
        """
        Check if Iglu URI is a Snowplow standard schema.

        Args:
            iglu_uri: Iglu URI (e.g., iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0)

        Returns:
            True if standard schema (vendor starts with com.snowplowanalytics)
        """
        # Parse vendor from Iglu URI
        try:
            # Format: iglu:vendor/name/format/version
            uri_without_prefix = iglu_uri.replace("iglu:", "")
            vendor = uri_without_prefix.split("/")[0]
            return vendor.startswith("com.snowplowanalytics")
        except (IndexError, AttributeError) as e:
            logger.warning(
                f"Failed to parse Iglu URI '{iglu_uri}' for standard schema check: {e}. "
                f"This schema will be treated as non-standard."
            )
            return False

    def _process_standard_schema(self, iglu_uri: str) -> Iterable[MetadataWorkUnit]:
        """
        Fetch and process a single standard schema from Iglu Central.

        Args:
            iglu_uri: Iglu URI to fetch

        Yields:
            MetadataWorkUnit: Schema metadata work units
        """
        # Parse Iglu URI
        parsed_uri = self._parse_iglu_uri(iglu_uri)
        if not parsed_uri:
            logger.warning(f"Failed to parse Iglu URI: {iglu_uri}")
            return

        vendor = parsed_uri["vendor"]
        name = parsed_uri["name"]
        format_type = parsed_uri["format"]
        version = parsed_uri["version"]

        logger.info(
            f"Fetching standard schema from Iglu Central: {vendor}/{name}/{version}"
        )

        # Fetch schema from Iglu Central
        schema_data = self._fetch_from_iglu_central(vendor, name, format_type, version)

        if not schema_data:
            logger.warning(
                f"Could not fetch schema {vendor}/{name}/{version} from Iglu Central"
            )
            return

        # Determine schema type (event or entity)
        # Convention: context/entity schemas contain "context" in name, others are events
        schema_type = infer_schema_type(name)

        # Build dataset name (following same pattern as data_structure_builder)
        dataset_name = f"{vendor}.{name}"
        if self.config.include_version_in_urn:
            dataset_name = f"{dataset_name}.{version}"

        # Extract description from schema
        description = schema_data.get(
            "description", f"Snowplow standard {schema_type} schema"
        )

        # Build custom properties
        custom_properties = {
            "iglu_uri": iglu_uri,
            "source": "iglu_central",
            "vendor": vendor,
            "schema_type": schema_type,
            "version": version,
        }

        # Build extra aspects (status is required)
        extra_aspects: List[Any] = [StatusClass(removed=False)]

        # Parse schema metadata if available
        schema_metadata = None
        if "properties" in schema_data:
            try:
                schema_metadata = SnowplowSchemaParser.parse_schema(
                    schema_data=schema_data,
                    vendor=vendor,
                    name=name,
                    version=version,
                )
                if schema_metadata:
                    extra_aspects.append(schema_metadata)
            except Exception as e:
                logger.warning(
                    f"Failed to parse schema fields for {vendor}/{name}/{version}: {e}"
                )

        # Add tags as extra aspect (following data_structure_builder pattern)
        tags_to_add = [schema_type, "snowplow_standard", f"vendor:{vendor}"]
        tag_associations = [
            TagAssociationClass(tag=f"urn:li:tag:{tag}") for tag in sorted(tags_to_add)
        ]
        extra_aspects.append(GlobalTagsClass(tags=tag_associations))

        # Set container via extra_aspects; browse path is computed by auto_browse_path_v2
        if self.config.bdp_connection:
            yield from self._emit_vendor_container(vendor)
            leaf_container_urn = self._get_leaf_container_urn(vendor)
            if leaf_container_urn:
                extra_aspects.append(ContainerClass(container=leaf_container_urn))

        # Create dataset using SDK V2 (following data_structure_builder pattern)
        dataset = Dataset(
            platform=self.deps.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=description,
            display_name=name,
            custom_properties=custom_properties,
            subtype=get_schema_subtype(schema_type)
            if schema_type
            else DatasetSubtype.SCHEMA,
            extra_aspects=extra_aspects,
        )

        # Emit dataset
        yield from dataset.as_workunits()

        # Track URN and fields in state for event spec schema metadata
        # Use the URN factory to create the same URN that event specs will reference
        schema_urn = self.deps.urn_factory.make_schema_dataset_urn(
            vendor=vendor, name=name, version=version
        )
        self.state.extracted_schema_urns.append(schema_urn)

        # Track fields if schema metadata was parsed (indexed by URN for O(1) lookups)
        if (
            schema_metadata
            and hasattr(schema_metadata, "fields")
            and schema_metadata.fields
        ):
            self.state.register_schema_fields(schema_urn, schema_metadata.fields)
            logger.info(
                f"Tracked {len(schema_metadata.fields)} fields from standard schema {vendor}/{name}/{version}"
            )

        # Track that this schema was extracted
        self.report.report_schema_extracted(schema_type)
        logger.info(
            f"Successfully extracted standard schema: {vendor}/{name}/{version}"
        )

    def _get_leaf_container_urn(self, vendor: str) -> Optional[str]:
        """Return the URN of the leaf vendor container for this vendor namespace.

        Only ContainerClass is set on the dataset; browsePathsV2 is computed
        automatically by the framework's auto_browse_path_v2 stream processor.
        """
        if not self.config.bdp_connection:
            return None

        org_id = self.config.bdp_connection.organization_id
        vendor_key = SnowplowVendorKey(
            organization_id=org_id,
            vendor=vendor,
            platform=self.deps.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        return str(vendor_key.as_urn())

    def _emit_vendor_container(self, vendor: str) -> Iterable[MetadataWorkUnit]:
        """Emit nested vendor containers by splitting the vendor namespace on dots.

        For example, 'com.snowplowanalytics.snowplow' emits three containers:
          Organization → com → snowplowanalytics → snowplow
        Shared prefixes (e.g., 'com') are emitted once and reused.
        """
        if not self.config.bdp_connection:
            return

        org_id = self.config.bdp_connection.organization_id
        segments = vendor.split(".")

        for i in range(len(segments)):
            vendor_path = ".".join(segments[: i + 1])

            if vendor_path in self._emitted_vendors:
                continue
            self._emitted_vendors.add(vendor_path)

            if i == 0:
                parent_key: ContainerKey = SnowplowOrganizationKey(
                    organization_id=org_id,
                    platform=self.deps.platform,
                    instance=self.config.platform_instance,
                    env=self.config.env,
                )
            else:
                parent_path = ".".join(segments[:i])
                parent_key = SnowplowVendorKey(
                    organization_id=org_id,
                    vendor=parent_path,
                    platform=self.deps.platform,
                    instance=self.config.platform_instance,
                    env=self.config.env,
                )

            vendor_key = SnowplowVendorKey(
                organization_id=org_id,
                vendor=vendor_path,
                platform=self.deps.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield from gen_containers(
                container_key=vendor_key,
                name=segments[i],
                sub_types=[DatasetSubtype.SCHEMA],
                parent_container_key=parent_key,
                extra_properties={
                    "vendor": vendor_path,
                },
            )

    def _parse_iglu_uri(self, iglu_uri: str) -> Optional[Dict[str, str]]:
        """
        Parse Iglu URI into components.

        Args:
            iglu_uri: Iglu URI (e.g., iglu:vendor/name/format/version)

        Returns:
            Dict with vendor, name, format, version or None if invalid
        """
        try:
            # Remove iglu: prefix
            uri_without_prefix = iglu_uri.replace("iglu:", "")

            # Split by /
            parts = uri_without_prefix.split("/")

            if len(parts) != 4:
                return None

            return {
                "vendor": parts[0],
                "name": parts[1],
                "format": parts[2],
                "version": parts[3],
            }
        except Exception as e:
            logger.warning(f"Failed to parse Iglu URI {iglu_uri}: {e}")
            return None

    def _fetch_from_iglu_central(
        self, vendor: str, name: str, format_type: str, version: str
    ) -> Optional[Dict]:
        """
        Fetch schema from Iglu Central.

        Args:
            vendor: Schema vendor
            name: Schema name
            format_type: Format (usually 'jsonschema')
            version: Schema version (e.g., '1-0-0')

        Returns:
            Schema data dict or None if not found
        """
        # Iglu Central URL format: http://iglucentral.com/schemas/{vendor}/{name}/{format}/{version}
        url = f"{self.iglu_central_url}/schemas/{vendor}/{name}/{format_type}/{version}"

        try:
            if not url.startswith(("http://", "https://")):
                raise ValueError("Invalid URL scheme")
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"Schema not found in Iglu Central: {url}")
                return None
            else:
                logger.warning(
                    f"Iglu Central returned status {response.status_code} for {url}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logger.warning(f"Error fetching from Iglu Central {url}: {e}")
            return None
