import logging
import time
import uuid
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union

from datahub._codegen.aspect import _Aspect
from datahub.emitter.mce_builder import (
    make_dataplatform_instance_urn,
    make_domain_urn,
    make_group_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioContainer,
    DremioDataset,
    DremioDatasetColumn,
    DremioDatasetType,
    DremioGlossaryTerm,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    AuditStampClass,
    BooleanTypeClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    BytesTypeClass,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DateTypeClass,
    DomainsClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    MySqlDDLClass,
    NullTypeClass,
    NumberTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    QuantileClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    TimeStampClass,
    TimeTypeClass,
    ViewPropertiesClass,
)

logger = logging.getLogger(__name__)
namespace = uuid.NAMESPACE_DNS


class DremioContainerKey(ContainerKey):
    key: str


class SchemaFieldTypeMapper:
    # From https://docs.dremio.com/cloud/reference/sql/data-types/

    FIELD_TYPE_MAPPING: Dict[str, Type] = {
        # Bool
        "boolean": BooleanTypeClass,
        # Binary
        "binary varying": BytesTypeClass,
        # Numbers
        "decimal": NumberTypeClass,
        "integer": NumberTypeClass,
        "bigint": NumberTypeClass,
        "float": NumberTypeClass,
        "double": NumberTypeClass,
        # Dates and times
        "timestamp": DateTypeClass,
        "date": DateTypeClass,
        "time": TimeTypeClass,
        # Strings
        "char": StringTypeClass,
        "character": StringTypeClass,
        "character varying": StringTypeClass,
        # Records
        "row": RecordTypeClass,
        "struct": RecordTypeClass,
        "list": RecordTypeClass,
        "map": RecordTypeClass,
        # Arrays
        "array": ArrayTypeClass,
    }

    @classmethod
    def get_field_type(
        cls, data_type: str, data_size: Optional[int] = None
    ) -> Tuple["SchemaFieldDataTypeClass", str]:
        """
        Maps a Dremio data type and size to a DataHub SchemaFieldDataTypeClass and native data type string.

        :param data_type: The data type string from Dremio.
        :param data_size: The size of the data type, if applicable.
        :return: A tuple containing a SchemaFieldDataTypeClass instance and the native data type string.
        """
        if not data_type:
            logger.warning("Empty data_type provided, defaulting to NullTypeClass.")
            type_class = NullTypeClass
            native_data_type = "NULL"
        else:
            data_type = data_type.lower()
            type_class = cls.FIELD_TYPE_MAPPING.get(data_type, NullTypeClass)

            if data_size:
                native_data_type = f"{data_type}({data_size})"
            else:
                native_data_type = data_type

        try:
            schema_field_type = SchemaFieldDataTypeClass(type=type_class())
            logger.debug(
                f"Mapped data_type '{data_type}' with size '{data_size}' to type class "
                f"'{type_class.__name__}' and native data type '{native_data_type}'."
            )
        except Exception as e:
            logger.error(
                f"Error initializing SchemaFieldDataTypeClass with type '{type_class.__name__}': {e}"
            )
            schema_field_type = SchemaFieldDataTypeClass(type=NullTypeClass())

        return schema_field_type, native_data_type


class DremioAspects:
    def __init__(
        self,
        platform: str,
        ui_url: str,
        env: str,
        ingest_owner: bool,
        domain: Optional[str] = None,
        platform_instance: Optional[str] = None,
    ):
        self.platform = platform
        self.platform_instance = platform_instance
        self.env = env
        self.domain = domain
        self.ui_url = ui_url
        self.ingest_owner = ingest_owner

    def get_container_key(
        self, name: Optional[str], path: Optional[List[str]]
    ) -> DremioContainerKey:
        key = name
        if path:
            key = ".".join(path) + "." + name if name else ".".join(path)

        return DremioContainerKey(
            platform=self.platform,
            instance=self.platform_instance,
            env=str(self.env),
            key=key,
        )

    def get_container_urn(
        self, name: Optional[str] = None, path: Optional[List[str]] = []
    ) -> str:
        container_key = self.get_container_key(name, path)
        return container_key.as_urn()

    def create_domain_aspect(self) -> Optional[_Aspect]:
        if self.domain:
            if self.domain.startswith("urn:li:domain:"):
                return DomainsClass(domains=[self.domain])
            return DomainsClass(
                domains=[
                    make_domain_urn(
                        str(uuid.uuid5(namespace, self.domain)),
                    )
                ]
            )
        return None

    def populate_container_mcp(
        self, container_urn: str, container: DremioContainer
    ) -> Iterable[MetadataWorkUnit]:
        # Container Properties
        container_properties = self._create_container_properties(container)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=container_properties,
            aspectName=ContainerPropertiesClass.ASPECT_NAME,
        )
        yield mcp.as_workunit()

        if not container.path:
            browse_paths_v2 = self._create_browse_paths_containers(container)
            if browse_paths_v2:
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=container_urn,
                    aspect=browse_paths_v2,
                )
                yield mcp.as_workunit()

        # Container Class Folders
        container_class = self._create_container_class(container)
        if container_class:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=container_urn,
                aspect=container_class,
            )
            yield mcp.as_workunit()

        # Data Platform Instance
        data_platform_instance = self._create_data_platform_instance()
        if data_platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=container_urn,
                aspect=data_platform_instance,
            )
            yield mcp.as_workunit()

        # SubTypes
        subtypes = SubTypesClass(typeNames=[container.subclass])
        mcp = MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=subtypes,
        )
        yield mcp.as_workunit()

        # Status
        status = StatusClass(removed=False)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=status,
        )
        yield mcp.as_workunit()

    def populate_dataset_mcp(
        self, dataset_urn: str, dataset: DremioDataset
    ) -> Iterable[MetadataWorkUnit]:
        # Dataset Properties
        dataset_properties = self._create_dataset_properties(dataset)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        )
        yield mcp.as_workunit()

        # Ownership
        ownership = self._create_ownership(dataset)
        if ownership:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ownership,
            )
            yield mcp.as_workunit()

        # SubTypes
        subtypes = SubTypesClass(typeNames=[dataset.dataset_type.value])
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=subtypes,
        )
        yield mcp.as_workunit()

        # Data Platform Instance
        data_platform_instance = self._create_data_platform_instance()
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=data_platform_instance,
        )
        yield mcp.as_workunit()

        # Container Class
        container_class = self._create_container_class(dataset)
        if container_class:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=container_class,
            )
            yield mcp.as_workunit()

        # View Definition
        if dataset.dataset_type == DremioDatasetType.VIEW:
            view_definition = self._create_view_properties(dataset)
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_definition,
            )
            yield mcp.as_workunit()

        # Glossary Terms
        if dataset.glossary_terms:
            glossary_terms = self._create_glossary_terms(dataset)
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=glossary_terms,
            )
            yield mcp.as_workunit()

        # Schema Metadata
        if dataset.columns:
            schema_metadata = self._create_schema_metadata(dataset)
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=schema_metadata,
            )
            yield mcp.as_workunit()

        else:
            logger.warning(
                f"Dataset {dataset.path}.{dataset.resource_name} has not been queried in Dremio"
            )
            logger.warning(
                f"Dataset {dataset.path}.{dataset.resource_name} will have a null schema"
            )

        # Status
        status = StatusClass(removed=False)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=status,
        )
        yield mcp.as_workunit()

    def populate_glossary_term_mcp(
        self, glossary_term: DremioGlossaryTerm
    ) -> Iterable[MetadataWorkUnit]:
        glossary_term_info = self._create_glossary_term_info(glossary_term)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=glossary_term.urn,
            aspect=glossary_term_info,
        )
        yield mcp.as_workunit()

    def populate_profile_aspect(self, profile_data: Dict) -> DatasetProfileClass:
        field_profiles = [
            self._create_field_profile(field_name, field_stats)
            for field_name, field_stats in profile_data.get("column_stats", {}).items()
        ]
        return DatasetProfileClass(
            timestampMillis=round(time.time() * 1000),
            rowCount=profile_data.get("row_count"),
            columnCount=profile_data.get("column_count"),
            fieldProfiles=field_profiles,
        )

    def _create_container_properties(
        self, container: DremioContainer
    ) -> ContainerPropertiesClass:
        return ContainerPropertiesClass(
            name=container.container_name,
            qualifiedName=f"{'.'.join(container.path) + '.' if container.path else ''}{container.container_name}",
            description=container.description,
            env=self.env,
        )

    def _create_browse_paths_containers(
        self, entity: DremioContainer
    ) -> Optional[BrowsePathsV2Class]:
        paths = []

        if entity.subclass == "Dremio Space":
            paths.append(BrowsePathEntryClass(id="Spaces"))
        elif entity.subclass == "Dremio Source":
            paths.append(BrowsePathEntryClass(id="Sources"))
        if paths:
            return BrowsePathsV2Class(path=paths)
        return None

    def _create_container_class(
        self, entity: Union[DremioContainer, DremioDataset]
    ) -> Optional[ContainerClass]:
        if entity.path:
            return ContainerClass(container=self.get_container_urn(path=entity.path))
        return None

    def _create_data_platform_instance(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=f"urn:li:dataPlatform:{self.platform}",
            instance=(
                make_dataplatform_instance_urn(self.platform, self.platform_instance)
                if self.platform_instance
                else None
            ),
        )

    def _create_dataset_properties(
        self, dataset: DremioDataset
    ) -> DatasetPropertiesClass:
        return DatasetPropertiesClass(
            name=dataset.resource_name,
            qualifiedName=f"{'.'.join(dataset.path)}.{dataset.resource_name}",
            description=dataset.description,
            externalUrl=self._create_external_url(dataset=dataset),
            created=TimeStampClass(
                time=round(
                    datetime.strptime(
                        dataset.created, "%Y-%m-%d %H:%M:%S.%f"
                    ).timestamp()
                    * 1000
                )
                if hasattr(dataset, "created")
                else 0,
            ),
        )

    def _create_external_url(self, dataset: DremioDataset) -> str:
        container_type = "source"
        dataset_url_path = '"' + dataset.path[0] + '"/'

        if len(dataset.path) > 1:
            dataset_url_path = (
                dataset_url_path + '"' + '"."'.join(dataset.path[1:]) + '".'
            )

        if dataset.dataset_type == DremioDatasetType.VIEW:
            container_type = "space"
        elif dataset.path[0].startswith("@"):
            container_type = "home"

        return f'{self.ui_url}/{container_type}/{dataset_url_path}"{dataset.resource_name}"'

    def _create_ownership(self, dataset: DremioDataset) -> Optional[OwnershipClass]:
        if self.ingest_owner and dataset.owner:
            owner_urn = (
                make_user_urn(dataset.owner)
                if dataset.owner_type == "USER"
                else make_group_urn(dataset.owner)
            )
            ownership: OwnershipClass = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                    )
                ]
            )
            return ownership

        return None

    def _create_glossary_terms(self, entity: DremioDataset) -> GlossaryTermsClass:
        return GlossaryTermsClass(
            terms=[
                GlossaryTermAssociationClass(urn=term.urn)
                for term in entity.glossary_terms
            ],
            auditStamp=AuditStampClass(
                time=round(time.time() * 1000),
                actor="urn:li:corpuser:admin",
            ),
        )

    def _create_schema_metadata(self, dataset: DremioDataset) -> SchemaMetadataClass:
        return SchemaMetadataClass(
            schemaName=f"{'.'.join(dataset.path)}.{dataset.resource_name}",
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            fields=[self._create_schema_field(column) for column in dataset.columns],
            platformSchema=MySqlDDLClass(""),
            hash="",
        )

    def _create_schema_field(self, column: DremioDatasetColumn) -> SchemaFieldClass:
        type_class, native_data_type = SchemaFieldTypeMapper.get_field_type(
            column.data_type,
            column.column_size,
        )
        return SchemaFieldClass(
            fieldPath=column.name,
            type=type_class,
            nativeDataType=native_data_type,
            nullable=column.is_nullable == "YES",
        )

    def _create_view_properties(
        self, dataset: DremioDataset
    ) -> Optional[ViewPropertiesClass]:
        if not dataset.sql_definition:
            return None
        return ViewPropertiesClass(
            materialized=False,
            viewLanguage="SQL",
            viewLogic=dataset.sql_definition,
        )

    def _create_glossary_term_info(
        self, glossary_term: DremioGlossaryTerm
    ) -> GlossaryTermInfoClass:
        return GlossaryTermInfoClass(
            definition="",
            termSource=self.platform,
            name=glossary_term.glossary_term,
        )

    def _create_field_profile(
        self, field_name: str, field_stats: Dict
    ) -> DatasetFieldProfileClass:
        quantiles = field_stats.get("quantiles")
        return DatasetFieldProfileClass(
            fieldPath=field_name,
            uniqueCount=field_stats.get("distinct_count"),
            nullCount=field_stats.get("null_count"),
            min=str(field_stats.get("min")) if field_stats.get("min") else None,
            max=str(field_stats.get("max")) if field_stats.get("max") else None,
            mean=str(field_stats.get("mean")) if field_stats.get("mean") else None,
            median=str(field_stats.get("median"))
            if field_stats.get("median")
            else None,
            stdev=str(field_stats.get("stdev")) if field_stats.get("stdev") else None,
            quantiles=[
                QuantileClass(quantile=str(0.25), value=str(quantiles[0])),
                QuantileClass(quantile=str(0.75), value=str(quantiles[1])),
            ]
            if quantiles
            else None,
            sampleValues=field_stats.get("sample_values"),
        )
