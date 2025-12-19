"""
Standard Schema Processor for Snowplow connector.

Extracts Snowplow standard schemas from Iglu Central that are referenced by event specifications.
Standard schemas (vendor: com.snowplowanalytics.*) are not available in the BDP Data Structures API
but are publicly accessible from Iglu Central.
"""

import logging
from typing import TYPE_CHECKING, Dict, Iterable, Optional, Set

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.constants import infer_schema_type
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.metadata.schema_classes import StatusClass
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
                    f"Failed to extract standard schema {iglu_uri} from Iglu Central: {e}"
                )

    def _collect_standard_schema_uris(self) -> Set[str]:
        """
        Collect Iglu URIs for Snowplow standard schemas from state.

        Returns:
            Set of Iglu URIs for standard schemas (com.snowplowanalytics.*)
        """
        standard_uris: Set[str] = set()

        # Get URIs tracked during event spec processing
        if hasattr(self.state, "referenced_iglu_uris"):
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
        except Exception:
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
        from typing import Any, List

        extra_aspects: List[Any] = [StatusClass(removed=False)]

        # Parse schema metadata if available
        schema_metadata = None
        if "properties" in schema_data:
            from datahub.ingestion.source.snowplow.schema_parser import (
                SnowplowSchemaParser,
            )

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
        from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

        tags_to_add = [schema_type, "snowplow_standard", f"vendor:{vendor}"]
        tag_associations = [
            TagAssociationClass(tag=f"urn:li:tag:{tag}") for tag in sorted(tags_to_add)
        ]
        extra_aspects.append(GlobalTagsClass(tags=tag_associations))

        # Create dataset using SDK V2 (following data_structure_builder pattern)
        dataset = Dataset(
            platform=self.deps.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=description,
            display_name=name,
            custom_properties=custom_properties,
            subtype=f"snowplow_{schema_type}_schema",
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

        # Track fields if schema metadata was parsed
        if schema_metadata and hasattr(schema_metadata, "fields"):
            for field in schema_metadata.fields:
                self.state.extracted_schema_fields.append((schema_urn, field))
            logger.info(
                f"Tracked {len(schema_metadata.fields)} fields from standard schema {vendor}/{name}/{version}"
            )

        # Track that this schema was extracted
        self.report.report_schema_extracted(schema_type)
        logger.info(
            f"Successfully extracted standard schema: {vendor}/{name}/{version}"
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
        import requests

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
