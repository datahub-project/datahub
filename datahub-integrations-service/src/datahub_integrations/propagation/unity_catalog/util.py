# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Deque, Optional, Tuple

import cachetools
from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.com.linkedin.pegasus2avro.container import ContainerProperties
from datahub.metadata.urns import (
    ContainerUrn,
    DatasetUrn,
    SchemaFieldUrn,
    TagUrn,
)
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph

from datahub_integrations.propagation.external_entities import (
    PlatformResourceRepository,
)
from datahub_integrations.propagation.unity_catalog.config import (
    UnityCatalogConnectionConfig,
)
from datahub_integrations.propagation.unity_catalog.tag_entities import (
    UnityCatalogTagId,
    UnityCatalogTagPlatformResource,
    UnityCatalogTagSyncContext,
)
from datahub_integrations.propagation.unity_catalog.unity_resource_manager import (
    UnityResourceManager,
)

logger: logging.Logger = logging.getLogger(__name__)

# Configuration constants
MAX_ERRORS_PER_HOUR = int(os.getenv("MAX_ERRORS_PER_HOUR", 15))
CACHE_TTL_SECONDS = 300
CACHE_MAX_SIZE = 500
TAG_KEY_MAX_LENGTH = 255
TAG_VALUE_MAX_LENGTH = 1000


class EntityType(Enum):
    """Enum for supported entity types."""

    DATASET = "dataset"
    SCHEMA_FIELD = "schema_field"
    CONTAINER = "container"


@dataclass
class ParsedEntity:
    """Container for parsed entity information."""

    entity_type: EntityType
    urn: Urn
    dataset_urn: Optional[DatasetUrn] = None
    field_path: Optional[str] = None


@dataclass
class ContainerInfo:
    """Container information extracted from properties."""

    catalog: Optional[str]
    schema: Optional[str]

    @property
    def is_catalog_only(self) -> bool:
        return self.catalog is not None and self.schema is None

    @property
    def is_valid(self) -> bool:
        return bool(self.catalog)


class UrnValidator:
    """Validates URNs for Unity Catalog operations."""

    @staticmethod
    def is_urn_allowed(urn: str) -> bool:
        """Check if URN is allowed for Unity Catalog operations."""
        try:
            parsed_urn = Urn.create_from_string(urn)

            if isinstance(parsed_urn, SchemaFieldUrn):
                parsed_urn = Urn.create_from_string(parsed_urn.parent)

            if isinstance(parsed_urn, ContainerUrn):
                return True

            if isinstance(parsed_urn, DatasetUrn):
                return parsed_urn.get_data_platform_urn().platform_name == "databricks"

            return False
        except Exception as e:
            logger.error(f"Error parsing URN {urn}: {e}")
            return False


class TagTransformer:
    """Handles tag transformations between DataHub and Unity Catalog formats."""

    @staticmethod
    def str_to_uc_tag(label: str) -> str:
        """Convert string to Unity Catalog compatible tag format."""
        return re.sub(r"[^a-zA-Z0-9_]", "_", label)

    @staticmethod
    def datahub_tag_to_uc_tag(tag_urn: TagUrn) -> Tuple[str, str]:
        """Convert DataHub tag URN to Unity Catalog key-value pair."""
        splits = tag_urn.name.split(":", 2)

        if len(splits) == 2:
            key, value = splits[0], splits[1]
        else:
            key, value = tag_urn.name, ""

        key = TagTransformer.str_to_uc_tag(key)
        # Truncate to max lengths
        return key[:TAG_KEY_MAX_LENGTH], value[:TAG_VALUE_MAX_LENGTH]


class EntityParser:
    """Parses and validates entity URNs."""

    @staticmethod
    def parse_entity_urn(entity_urn: str) -> ParsedEntity:
        """Parse entity URN and return structured information."""
        parsed_urn = Urn.create_from_string(entity_urn)

        if isinstance(parsed_urn, DatasetUrn):
            return ParsedEntity(
                entity_type=EntityType.DATASET, urn=parsed_urn, dataset_urn=parsed_urn
            )
        elif isinstance(parsed_urn, SchemaFieldUrn):
            dataset_urn = DatasetUrn.create_from_string(parsed_urn.parent)
            return ParsedEntity(
                entity_type=EntityType.SCHEMA_FIELD,
                urn=parsed_urn,
                dataset_urn=dataset_urn,
                field_path=parsed_urn.field_path,
            )
        elif isinstance(parsed_urn, ContainerUrn):
            return ParsedEntity(entity_type=EntityType.CONTAINER, urn=parsed_urn)
        else:
            raise ValueError(
                f"Invalid entity URN {entity_urn}. Only Dataset, SchemaField, and Container URNs are supported."
            )


class ErrorTracker:
    """Tracks and manages error rates to prevent account lockouts."""

    def __init__(self, max_errors_per_hour: int = MAX_ERRORS_PER_HOUR):
        self.error_timestamps: Deque[datetime] = deque()
        self.error_threshold = max_errors_per_hour

    def log_error(self) -> None:
        """Log a new error occurrence."""
        self.error_timestamps.append(datetime.now())
        self._cleanup_old_errors()

    def too_many_errors(self) -> bool:
        """Check if error threshold has been exceeded."""
        self._cleanup_old_errors()
        return (
            len(self.error_timestamps) >= self.error_threshold
            if len(self.error_timestamps) > 0
            else False
        )

    def _cleanup_old_errors(self) -> None:
        """Remove error timestamps older than one hour."""
        one_hour_ago = datetime.now() - timedelta(hours=1)
        while self.error_timestamps and self.error_timestamps[0] < one_hour_ago:
            self.error_timestamps.popleft()


class UnityCatalogTagHelper(Closeable):
    """Helper class for managing Unity Catalog tags and descriptions."""

    def __init__(
        self,
        config: UnityCatalogConnectionConfig,
        graph: AcrylDataHubGraph,
        unity_catalog_resource_helper: UnityResourceManager,
        platform_instance: Optional[str] = None,
    ):
        self.config = config
        self.graph = graph
        self.platform_instance = platform_instance
        self.unity_catalog_resource_helper = unity_catalog_resource_helper
        self.platform_resource_repository = PlatformResourceRepository(
            graph=self.graph.graph
        )
        self.error_tracker = ErrorTracker()

        # Initialize cache
        self.platform_resource_cache: cachetools.TTLCache = cachetools.TTLCache(
            ttl=CACHE_TTL_SECONDS, maxsize=CACHE_MAX_SIZE
        )

        logger.info("UnityCatalogTagHelper initialized.")

    def apply_description(self, entity_urn: str, docs: str) -> None:
        """Apply description to Unity Catalog entity."""
        try:
            parsed_entity = EntityParser.parse_entity_urn(entity_urn)

            if parsed_entity.entity_type == EntityType.CONTAINER:
                assert isinstance(parsed_entity.urn, ContainerUrn), (
                    "Parsed entity URN must be a ContainerUrn for containers."
                )
                self._apply_container_description(parsed_entity.urn, docs)
            elif parsed_entity.entity_type == EntityType.DATASET:
                assert isinstance(parsed_entity.urn, DatasetUrn), (
                    "Parsed entity URN must be a DatasetUrn for containers."
                )
                assert parsed_entity.dataset_urn is not None
                self._apply_table_description(parsed_entity.dataset_urn, docs)
            elif parsed_entity.entity_type == EntityType.SCHEMA_FIELD:
                assert isinstance(parsed_entity.urn, SchemaFieldUrn), (
                    "Parsed entity URN must be a SchemaFieldUrn for containers."
                )
                self._apply_column_description(parsed_entity, docs)

        except Exception as e:
            logger.error(f"Failed to apply description to {entity_urn}: {e}")
            self.error_tracker.log_error()

    def apply_tag(self, entity_urn: str, tag_urn: str) -> None:
        """Apply tag to Unity Catalog entity."""
        try:
            parsed_entity = EntityParser.parse_entity_urn(entity_urn)
            tag_urn_typed = Urn.from_string(tag_urn)

            if not isinstance(tag_urn_typed, TagUrn):
                logger.warning(f"Invalid tag URN: {tag_urn}")
                return

            logger.info(f"Applying tag {tag_urn_typed} to entity {entity_urn}")

            if parsed_entity.entity_type == EntityType.CONTAINER:
                assert isinstance(parsed_entity.urn, ContainerUrn), (
                    "Parsed entity URN must be a ContainerUrn for containers."
                )

                self._apply_container_tag(parsed_entity.urn, tag_urn_typed)
            elif parsed_entity.entity_type == EntityType.DATASET:
                assert parsed_entity.dataset_urn is not None, (
                    "Parsed entity URN must have a DatasetUrn for datasets."
                )
                self._apply_table_tag(parsed_entity.dataset_urn, tag_urn_typed)
            elif parsed_entity.entity_type == EntityType.SCHEMA_FIELD:
                self._apply_column_tag(parsed_entity, tag_urn_typed)

        except Exception as e:
            logger.error(f"Failed to apply tag {tag_urn} to {entity_urn}: {e}")
            self.error_tracker.log_error()

    def remove_tag(self, entity_urn: str, tag_urn: str) -> None:
        """Remove tag from Unity Catalog entity."""
        if not UrnValidator.is_urn_allowed(entity_urn):
            return

        try:
            parsed_entity = EntityParser.parse_entity_urn(entity_urn)
            tag_urn_typed = Urn.from_string(tag_urn)

            if not isinstance(tag_urn_typed, TagUrn):
                logger.warning(f"Invalid tag URN: {tag_urn}")
                return

            logger.info(f"Removing tag {tag_urn_typed} from entity {entity_urn}")

            if parsed_entity.entity_type == EntityType.CONTAINER:
                assert isinstance(parsed_entity.urn, ContainerUrn), (
                    "Parsed entity URN must be a ContainerUrn for containers."
                )
                self._remove_container_tag(parsed_entity.urn, tag_urn_typed)
            elif parsed_entity.entity_type == EntityType.DATASET:
                assert parsed_entity.dataset_urn is not None, (
                    "Parsed entity URN must have a DatasetUrn for datasets."
                )
                self._remove_table_tag(parsed_entity.dataset_urn, tag_urn_typed)
            elif parsed_entity.entity_type == EntityType.SCHEMA_FIELD:
                self._remove_column_tag(parsed_entity, tag_urn_typed)

        except Exception as e:
            logger.error(f"Failed to remove tag {tag_urn} from {entity_urn}: {e}")
            self.error_tracker.log_error()

    def _apply_container_description(
        self, container_urn: ContainerUrn, docs: str
    ) -> None:
        """Apply description to container (catalog or schema)."""
        container_info = self._get_container_info(container_urn.urn())

        if not container_info.is_valid:
            logger.error(f"Container {container_urn} missing required properties")
            return

        if container_info.is_catalog_only:
            assert container_info.catalog is not None
            success = self.unity_catalog_resource_helper.set_catalog_description(
                catalog=container_info.catalog,
                description=docs,
            )
        else:
            assert container_info.catalog is not None
            assert container_info.schema is not None
            success = self.unity_catalog_resource_helper.set_schema_description(
                catalog=container_info.catalog,
                schema=container_info.schema,
                description=docs,
            )

        if success:
            logger.info(f"Applied description to container {container_urn}")
        else:
            logger.error(f"Failed to apply description to container {container_urn}")

    def _apply_table_description(self, dataset_urn: DatasetUrn, docs: str) -> None:
        """Apply description to table."""
        catalog, schema, table = dataset_urn.name.split(".")

        success = self.unity_catalog_resource_helper.set_table_description(
            catalog=catalog,
            schema=schema,
            table=table,
            description=docs,
        )

        if success:
            logger.info(f"Applied description to table {dataset_urn.name}")
        else:
            logger.error(f"Failed to apply description to table {dataset_urn.name}")

    def _apply_column_description(self, parsed_entity: ParsedEntity, docs: str) -> None:
        """Apply description to column."""
        assert parsed_entity.dataset_urn is not None, (
            "Parsed entity URN must have a DatasetUrn for columns."
        )

        assert parsed_entity.field_path is not None, (
            "Parsed entity must have a field_path for columns."
        )

        simplified_field_path = get_simple_field_path_from_v2_field_path(
            parsed_entity.field_path
        )
        catalog, schema, table = parsed_entity.dataset_urn.name.split(".")

        success = self.unity_catalog_resource_helper.set_column_description(
            catalog=catalog,
            schema=schema,
            table=table,
            column=simplified_field_path,
            description=docs,
        )

        if success:
            logger.info(f"Applied description to column {simplified_field_path}")
        else:
            logger.error(
                f"Failed to apply description to column {simplified_field_path}"
            )

    def _apply_container_tag(
        self, container_urn: ContainerUrn, tag_urn: TagUrn
    ) -> None:
        """Apply tag to container."""
        container_info = self._get_container_info(container_urn.urn())

        if not container_info.is_valid:
            logger.info(
                f"Container {container_urn} is not a Unity Catalog container. Skipping..."
            )
            return

        tag_key, tag_value = TagTransformer.datahub_tag_to_uc_tag(tag_urn)

        if container_info.is_catalog_only:
            assert container_info.catalog is not None, (
                "Container info must have a catalog for catalog-only containers."
            )

            success = self.unity_catalog_resource_helper.add_catalog_tags(
                catalog=container_info.catalog,
                tags={tag_key: tag_value},
            )
        else:
            assert container_info.catalog is not None, (
                "Container info must have a catalog for schema containers."
            )
            assert container_info.schema is not None, (
                "Container info must have a schema for schema containers."
            )

            success = self.unity_catalog_resource_helper.add_schema_tags(
                catalog=container_info.catalog,
                schema=container_info.schema,
                tags={tag_key: tag_value},
            )

        if success:
            self._create_platform_resource(tag_urn)
            target = (
                f"catalog {container_info.catalog}"
                if container_info.is_catalog_only
                else f"schema {container_info.catalog}.{container_info.schema}"
            )
            logger.info(f"Applied tag {tag_urn} to {target}")

    def _apply_table_tag(self, dataset_urn: DatasetUrn, tag_urn: TagUrn) -> None:
        """Apply tag to table."""
        tag_key, tag_value = TagTransformer.datahub_tag_to_uc_tag(tag_urn)
        catalog, schema, table = dataset_urn.name.split(".")
        full_table_name = f"{catalog}.{schema}.{table}"

        logger.info(f"Applying tag {tag_key}:{tag_value} to table {full_table_name}")

        success = self.unity_catalog_resource_helper.add_table_tags(
            catalog=catalog,
            schema=schema,
            table=table,
            tags={tag_key: tag_value},
        )

        if success:
            self._create_platform_resource(tag_urn)
            logger.info(f"Applied tag {tag_urn} to table {full_table_name}")

    def _apply_column_tag(self, parsed_entity: ParsedEntity, tag_urn: TagUrn) -> None:
        """Apply tag to column."""

        assert parsed_entity.dataset_urn is not None, (
            "Parsed entity URN must have a DatasetUrn for columns."
        )

        assert parsed_entity.field_path is not None, (
            "Parsed entity must have a field_path for columns."
        )

        simplified_field_path = get_simple_field_path_from_v2_field_path(
            parsed_entity.field_path
        )
        tag_key, tag_value = TagTransformer.datahub_tag_to_uc_tag(tag_urn)
        catalog, schema, table = parsed_entity.dataset_urn.name.split(".")
        full_table_name = f"{catalog}.{schema}.{table}"

        logger.info(
            f"Applying tag {tag_key}:{tag_value} to column {simplified_field_path} in table {full_table_name}"
        )

        success = self.unity_catalog_resource_helper.add_column_tags(
            catalog=catalog,
            schema=schema,
            table=table,
            column=simplified_field_path,
            tags={tag_key: tag_value},
        )

        if success:
            self._create_platform_resource(tag_urn)
            logger.info(f"Applied tag {tag_urn} to column {simplified_field_path}")

    def _remove_container_tag(
        self, container_urn: ContainerUrn, tag_urn: TagUrn
    ) -> None:
        """Remove tag from container."""
        container_info = self._get_container_info(container_urn.urn())

        if not container_info.is_valid:
            logger.error(f"Container {container_urn} missing required properties")
            return

        tag_key, _ = TagTransformer.datahub_tag_to_uc_tag(tag_urn)

        if container_info.is_catalog_only:
            assert container_info.catalog is not None, (
                "Container info must have a catalog for catalog-only containers."
            )

            self.unity_catalog_resource_helper.remove_catalog_tags(
                catalog=container_info.catalog,
                tag_keys=[tag_key],
            )
        else:
            assert container_info.catalog is not None, (
                "Container info must have a catalog for schema containers."
            )
            assert container_info.schema is not None, (
                "Container info must have a schema for schema containers."
            )

            self.unity_catalog_resource_helper.remove_schema_tags(
                catalog=container_info.catalog,
                schema=container_info.schema,
                tag_keys=[tag_key],
            )

    def _remove_table_tag(self, dataset_urn: DatasetUrn, tag_urn: TagUrn) -> None:
        """Remove tag from table."""
        tag_key, _ = TagTransformer.datahub_tag_to_uc_tag(tag_urn)
        catalog, schema, table = dataset_urn.name.split(".")

        logger.info(f"Removing tag {tag_key} from table {dataset_urn.name}")

        self.unity_catalog_resource_helper.remove_table_tags(
            catalog=catalog,
            schema=schema,
            table=table,
            tag_keys=[tag_key],
        )

    def _remove_column_tag(self, parsed_entity: ParsedEntity, tag_urn: TagUrn) -> None:
        """Remove tag from column."""

        assert parsed_entity.dataset_urn is not None, (
            "Parsed entity URN must have a DatasetUrn for columns."
        )

        assert parsed_entity.field_path is not None, (
            "Parsed entity must have a field_path for columns."
        )

        tag_key, _ = TagTransformer.datahub_tag_to_uc_tag(tag_urn)
        catalog, schema, table = parsed_entity.dataset_urn.name.split(".")

        logger.info(
            f"Removing tag {tag_key} from column {parsed_entity.field_path} in table {parsed_entity.dataset_urn.name}"
        )

        self.unity_catalog_resource_helper.remove_column_tags(
            catalog=catalog,
            schema=schema,
            table=table,
            column=parsed_entity.field_path,
            tag_keys=[tag_key],
        )
        logger.info(
            f"Tag {tag_key} from column {parsed_entity.field_path} in table {parsed_entity.dataset_urn.name} was removed successfully."
        )

    def _create_platform_resource(self, tag_urn: TagUrn) -> None:
        """Create platform resource for tag synchronization."""
        try:
            tag_sync_context = UnityCatalogTagSyncContext()
            platform_resource_id = UnityCatalogTagId.from_datahub_urn(
                platform_resource_repository=self.platform_resource_repository,
                tag_sync_context=tag_sync_context,
                urn=tag_urn.urn(),
                graph=self.graph.graph,
            )

            logger.info(f"Created platform resource {platform_resource_id}")

            unity_catalog_tag = UnityCatalogTagPlatformResource.get_from_datahub(
                platform_resource_id, self.platform_resource_repository, True
            )
            unity_catalog_tag.datahub_linked_resources().add(tag_urn.urn())
            platform_resource = unity_catalog_tag.as_platform_resource()
            platform_resource.to_datahub(self.graph.graph)

        except ValueError as e:
            logger.error(f"Error creating platform resource for tag {tag_urn}: {e}")
            # Don't re-raise as this is not critical for the main operation

    @cachetools.cached(cache=cachetools.LRUCache(maxsize=CACHE_MAX_SIZE))
    def _get_container_info(self, container_urn: str) -> ContainerInfo:
        """Get container information from properties."""
        logger.info(f"Fetching container properties for {container_urn}")

        container_properties = self.graph.graph.get_aspect(
            container_urn, ContainerProperties
        )

        if container_properties is None:
            logger.warning(f"No ContainerProperties found for {container_urn}")
            return ContainerInfo(catalog=None, schema=None)

        properties = container_properties.to_obj()
        custom_props = properties.get("customProperties", {})

        return ContainerInfo(
            catalog=custom_props.get("catalog"), schema=custom_props.get("unity_schema")
        )

    @property
    def too_many_errors(self) -> bool:
        """Check if error threshold has been exceeded."""
        return self.error_tracker.too_many_errors()

    def close(self) -> None:
        """Clean up resources."""
        logger.info("UnityCatalogTagHelper closed.")
