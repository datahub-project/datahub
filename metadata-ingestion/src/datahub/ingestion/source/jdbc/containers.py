"""Container-related classes for JDBC source."""

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Any

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.jdbc.constants import ContainerType
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    StatusClass,
    SubTypesClass,
)


class JDBCContainerKey(ContainerKey):
    """Container key for JDBC entities"""

    key: str


@dataclass
class SchemaPath:
    """Represents a schema path with proper handling of components."""

    parts: List[str]
    database: Optional[str] = None

    @classmethod
    def from_schema_name(
        cls, schema_name: str, database: Optional[str] = None
    ) -> "SchemaPath":
        """Create SchemaPath from schema name and optional database."""
        return cls(parts=schema_name.split("."), database=database)

    def get_container_paths(self) -> Set[str]:
        """Get all possible container paths from this schema path."""
        return {".".join(self.parts[: i + 1]) for i in range(len(self.parts))}

    def get_full_path(self, include_database: bool = True) -> List[str]:
        """Get full path including database if present and requested."""
        if include_database and self.database:
            return [self.database] + self.parts[:-1]
        return self.parts[:-1]

    def get_container_name(self) -> str:
        """Get the name of the final container in the path."""
        return self.parts[-1]

    def get_parent_path(self) -> Optional["SchemaPath"]:
        """Get parent path if it exists."""
        if len(self.parts) > 1:
            return SchemaPath(parts=self.parts[:-1], database=self.database)
        return None


class ContainerMetadata:
    """Class to handle container metadata generation."""

    def __init__(
        self,
        container_key: "JDBCContainerKey",
        name: str,
        container_type: ContainerType,
        parent_key: Optional["JDBCContainerKey"] = None,
        description: Optional[str] = None,
        custom_properties: Optional[Dict] = None,
        platform: str = None,
        platform_instance: Optional[str] = None,
    ):
        self.container_key = container_key
        self.name = name
        self.container_type = container_type
        self.parent_key = parent_key
        self.description = description
        self.custom_properties = custom_properties or {}
        self.platform = platform
        self.platform_instance = platform_instance

    def generate_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Generate all metadata workunits for this container."""

        # Container properties
        yield MetadataChangeProposalWrapper(
            entityUrn=self.container_key.as_urn(),
            aspect=ContainerPropertiesClass(
                name=self.name,
                description=self.description,
                customProperties=self.custom_properties,
            ),
        ).as_workunit()

        # Parent container relationship
        if self.parent_key:
            yield MetadataChangeProposalWrapper(
                entityUrn=self.container_key.as_urn(),
                aspect=ContainerClass(container=self.parent_key.as_urn()),
            ).as_workunit()

        # Platform instance
        yield MetadataChangeProposalWrapper(
            entityUrn=self.container_key.as_urn(),
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(
                    self.platform, self.platform_instance
                )
                if self.platform_instance
                else None,
            ),
        ).as_workunit()

        # Container type
        yield MetadataChangeProposalWrapper(
            entityUrn=self.container_key.as_urn(),
            aspect=SubTypesClass(typeNames=[self.container_type.value]),
        ).as_workunit()

        # Status
        yield MetadataChangeProposalWrapper(
            entityUrn=self.container_key.as_urn(),
            aspect=StatusClass(removed=False),
        ).as_workunit()


class ContainerRegistry:
    """Registry to track and manage containers."""

    def __init__(self):
        self.emitted_containers: Set[str] = set()
        self.container_hierarchy: Dict[str, ContainerMetadata] = {}

    def has_container(self, container_urn: str) -> bool:
        """Check if container has been emitted."""
        return container_urn in self.emitted_containers

    def register_container(self, container: ContainerMetadata) -> None:
        """Register a container in the registry."""
        container_urn = container.container_key.as_urn()
        if not self.has_container(container_urn):
            self.emitted_containers.add(container_urn)
            self.container_hierarchy[container_urn] = container

    def get_container(self, container_urn: str) -> Optional[ContainerMetadata]:
        """Get container metadata by URN."""
        return self.container_hierarchy.get(container_urn)

    def get_containers_by_type(
        self, container_type: ContainerType
    ) -> List[ContainerMetadata]:
        """Get all containers of a specific type."""
        return [
            container
            for container in self.container_hierarchy.values()
            if container.container_type == container_type
        ]


class SchemaContainerBuilder:
    """Builder class for schema containers."""

    def __init__(self, source: Any, registry: ContainerRegistry):
        self.source = source
        self.registry = registry

    def build_container(
        self, schema_path: SchemaPath, container_type: ContainerType
    ) -> ContainerMetadata:
        """Build a container for the given schema path."""
        full_path = schema_path.get_full_path()
        container_name = schema_path.get_container_name()

        # Get parent container if exists
        parent_path = schema_path.get_parent_path()
        parent_key = None
        if parent_path:
            parent_key = self.source.get_container_key(
                parent_path.get_container_name(), parent_path.get_full_path()
            )

        # Create container key
        container_key = self.source.get_container_key(container_name, full_path)

        return ContainerMetadata(
            container_key=container_key,
            name=container_name,
            container_type=container_type,
            parent_key=parent_key,
            description=f"Schema {'.'.join(schema_path.parts)}",
            custom_properties={"full_path": ".".join(schema_path.parts)},
            platform=self.source.platform,
            platform_instance=self.source.platform_instance,
        )
