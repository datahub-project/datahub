"""This Module contains controller functions for dremio source"""

__author__ = "Shabbir Mohammed Hussain, Shehroz Abdullah, Hamza Rehman, Jonny Dixon"

from datetime import datetime

import time

import uuid

import logging
from typing import Dict, Optional, Union, List, Tuple

from datahub._codegen.aspect import _Aspect
from datahub.emitter.mce_builder import (
    make_dataplatform_instance_urn,
    make_container_urn,
    make_user_urn,
    make_group_urn,
    make_domain_urn,
)
from datahub.ingestion.source.dremio.dremio_profiling import DremioProfiler
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    MySqlDDLClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
    BytesTypeClass,
    RecordTypeClass,
    ArrayTypeClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    TimeStampClass,
    SubTypesClass,
    DataPlatformInstanceClass,
    FabricTypeClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    GlossaryTermsClass,
    GlossaryTermAssociationClass,
    ViewPropertiesClass,
    AuditStampClass,
    ContainerClass,
    ContainerPropertiesClass,
    GlossaryTermInfoClass,
    StatusClass,
    DatasetProfileClass,
    DatasetFieldProfileClass,
    DomainsClass,
    QuantileClass,
)

from datahub.ingestion.source.dremio.dremio_api import (
    DremioDataset,
    DremioContainer,
    DremioGlossaryTerm,
)

logger = logging.getLogger(__name__)
namespace = uuid.NAMESPACE_DNS

class SchemaFieldTypeMapper:
    TYPE_MAPPING = {
        "boolean": BooleanTypeClass(),
        "binary varying": BytesTypeClass(),
        "decimal": NumberTypeClass(),
        "integer": NumberTypeClass(),
        "bigint": NumberTypeClass(),
        "float": NumberTypeClass(),
        "double": NumberTypeClass(),
        "timestamp": DateTypeClass(),
        "date": DateTypeClass(),
        "time": TimeTypeClass(),
        "char": StringTypeClass(),
        "character": StringTypeClass(),
        "character varying": StringTypeClass(),
        "row": RecordTypeClass(),
        "struct": RecordTypeClass(),
        "list": RecordTypeClass(),
        "map": RecordTypeClass(),
        "array": ArrayTypeClass(),
    }

    @classmethod
    def get_field_type(cls, data_type: str, data_size: int) -> Tuple[str, SchemaFieldDataTypeClass, str]:
        data_type = data_type.lower()
        data_size_str = f"({data_size})" if data_size else ""
        native_data_type = f"{data_type}{data_size_str}"

        type_class = cls.TYPE_MAPPING.get(data_type, NullTypeClass())
        return data_type, SchemaFieldDataTypeClass(type=type_class), native_data_type


class DremioAspects:
    def __init__(
            self,
            platform: str,
            profiler: DremioProfiler,
            domain: Optional[str] = None,
            platform_instance: Optional[str] = None,
            env: Optional[Union[FabricTypeClass, str]] = FabricTypeClass.PROD,
            profiling_enabled: bool = False
    ):
        self.platform = platform
        self.platform_instance = platform_instance
        self.env = env
        self.domain = domain
        self.profiler = profiler
        self.profiling_enabled = profiling_enabled

    def get_container_urn(self, name: str, path: Optional[List[str]]) -> str:
        if path:
            return make_container_urn(
                guid=str(
                    uuid.uuid5(
                        namespace,
                        self.platform + "".join(path) + name + self.platform_instance
                    )
                )
            )
        return make_container_urn(
            guid=str(
                uuid.uuid5(namespace, self.platform + name + self.platform_instance)
            )
        )

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

    def populate_container_aspects(self, container: DremioContainer) -> Dict[str, _Aspect]:
        aspects = {}
        aspects[ContainerPropertiesClass.ASPECT_NAME] = self._create_container_properties(container)
        aspects[BrowsePathsV2Class.ASPECT_NAME] = self._create_browse_paths(container)
        aspects[ContainerClass.ASPECT_NAME] = self._create_container_class(container)
        aspects[DataPlatformInstanceClass.get_aspect_name()] = self._create_data_platform_instance()
        aspects[SubTypesClass.ASPECT_NAME] = SubTypesClass(typeNames=[container.subclass])
        aspects[StatusClass.ASPECT_NAME] = StatusClass(removed=False)

        if container.glossary_terms:
            aspects[GlossaryTermsClass.ASPECT_NAME] = self._create_glossary_terms(container)

        return aspects

    def populate_dataset_aspects(self, dataset: DremioDataset) -> Dict[str, _Aspect]:
        aspects = {}
        aspects[DatasetPropertiesClass.ASPECT_NAME] = self._create_dataset_properties(dataset)
        aspects[OwnershipClass.ASPECT_NAME] = self._create_ownership(dataset)
        aspects[SubTypesClass.ASPECT_NAME] = SubTypesClass(typeNames=[dataset.dataset_type.value])
        aspects[DataPlatformInstanceClass.ASPECT_NAME] = self._create_data_platform_instance()
        aspects[BrowsePathsV2Class.ASPECT_NAME] = self._create_browse_paths(dataset)
        aspects[ContainerClass.ASPECT_NAME] = self._create_container_class(dataset)

        if dataset.glossary_terms:
            aspects[GlossaryTermsClass.ASPECT_NAME] = self._create_glossary_terms(dataset)

        if dataset.columns:
            aspects[SchemaMetadataClass.ASPECT_NAME] = self._create_schema_metadata(dataset)
            if self.profiling_enabled:
                profile_data = dataset.get_profile_data(self.profiler)
                profile_aspect = self.populate_profile_aspect(profile_data)
                try:
                    aspects[DatasetProfileClass.ASPECT_NAME] = profile_aspect
                except Exception as exc:
                    logger.error(exc)
        else:
            logger.warning(f"Dataset {dataset.path}.{dataset.resource_name} has not been queried in Dremio")
            logger.warning(f"Dataset {dataset.path}.{dataset.resource_name} will have a null schema")

        aspects[StatusClass.ASPECT_NAME] = StatusClass(removed=False)
        return aspects

    def populate_glossary_term_aspects(self, glossary_term: DremioGlossaryTerm) -> Dict[str, _Aspect]:
        return {
            GlossaryTermInfoClass.ASPECT_NAME: self._create_glossary_term_info(glossary_term),
        }

    def populate_profile_aspect(self, profile_data: Dict) -> DatasetProfileClass:
        field_profiles = [self._create_field_profile(field_name, field_stats)
                          for field_name, field_stats in profile_data.get("column_stats", {}).items()]
        return DatasetProfileClass(
            timestampMillis=round(time.time() * 1000),
            rowCount=profile_data.get("row_count"),
            columnCount=profile_data.get("column_count"),
            fieldProfiles=field_profiles,
        )

    def _create_container_properties(self, container: DremioContainer) -> ContainerPropertiesClass:
        return ContainerPropertiesClass(
            name=container.container_name,
            qualifiedName=f"{'.'.join(container.path) + '.' if container.path else ''}{container.container_name}",
            description=container.description,
            env=self.env
        )

    def _create_browse_paths(self, entity) -> BrowsePathsV2Class:
        if hasattr(entity, 'path') and entity.path:
            return BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(
                        id=entity.path[browse_path_level],
                        urn=self.get_container_urn(
                            name=entity.container_name if hasattr(entity, 'container_name') else "",
                            path=entity.path[:browse_path_level],
                        ),
                    ) for browse_path_level in range(len(entity.path))
                ]
            )
        return BrowsePathsV2Class(path=[])

    def _create_container_class(self, entity) -> ContainerClass:
        if hasattr(entity, 'path') and entity.path:
            return ContainerClass(
                container=self.get_container_urn(
                    path=entity.path,
                    name="",
                )
            )
        return None

    def _create_data_platform_instance(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=f"urn:li:dataPlatform:{self.platform}",
            instance=(
                make_dataplatform_instance_urn(self.platform, self.platform_instance)
                if self.platform_instance else None
            )
        )

    def _create_dataset_properties(self, dataset: DremioDataset) -> DatasetPropertiesClass:
        return DatasetPropertiesClass(
            name=dataset.resource_name,
            qualifiedName=f"{'.'.join(dataset.path)}.{dataset.resource_name}",
            description=dataset.description,
            created=TimeStampClass(
                time=round(
                    datetime.strptime(dataset.created, '%Y-%m-%d %H:%M:%S.%f').timestamp() * 1000
                ),
            ),
        )

    def _create_ownership(self, dataset: DremioDataset) -> OwnershipClass:
        owner = make_user_urn(dataset.owner) if dataset.owner_type == "USER" else make_group_urn(dataset.owner)
        return OwnershipClass(
            owners=[
                OwnerClass(
                    owner=owner,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            ]
        )

    def _create_glossary_terms(
            self,
            dataset: Union[
                DremioDataset,
                DremioContainer,
            ],
    ) -> GlossaryTermsClass:
        return GlossaryTermsClass(
            terms=[
                GlossaryTermAssociationClass(urn=term.urn)
                for term in dataset.glossary_terms
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
            fields=[
                self._create_schema_field(column)
                for column in dataset.columns
            ],
            platformSchema=MySqlDDLClass(""),
            hash=""
        )

    def _create_schema_field(self, column) -> SchemaFieldClass:
        data_type, type_class, native_data_type = SchemaFieldTypeMapper.get_field_type(
            column.data_type,
            column.column_size,
        )
        return SchemaFieldClass(
            fieldPath=column.name,
            type=type_class,
            nativeDataType=native_data_type,
            nullable=column.is_nullable == "YES",
        )

    def _get_profile_data(self, dataset: DremioDataset) -> Dict:
        return self.profiler.profile_table(
            f"{'.'.join(dataset.path)}.{dataset.resource_name}",
            [(col.name, col.data_type) for col in dataset.columns]
        )

    def _create_view_properties(self, dataset: DremioDataset) -> ViewPropertiesClass:
        return ViewPropertiesClass(
            materialized=False,
            viewLanguage="SQL",
            viewLogic=dataset.sql_definition,
        )

    def _create_glossary_term_info(self, glossary_term: DremioGlossaryTerm) -> GlossaryTermInfoClass:
        return GlossaryTermInfoClass(
            definition="",
            termSource=self.platform,
            name=glossary_term.glossary_term,
        )

    def _create_field_profile(self, field_name: str, field_stats: Dict) -> DatasetFieldProfileClass:
        quantiles = field_stats.get("quantiles")
        return DatasetFieldProfileClass(
            fieldPath=field_name,
            uniqueCount=field_stats.get("distinct_count"),
            nullCount=field_stats.get("null_count"),
            min=str(field_stats.get("min")) if field_stats.get("min") else None,
            max=str(field_stats.get("max")) if field_stats.get("max") else None,
            mean=str(field_stats.get("mean")) if field_stats.get("mean") else None,
            median=str(field_stats.get("median")) if field_stats.get("median") else None,
            stdev=str(field_stats.get("stdev")) if field_stats.get("stdev") else None,
            quantiles=[
                QuantileClass(quantile=str(0.25), value=str(quantiles[0])) if quantiles else None,
                QuantileClass(quantile=str(0.75), value=str(quantiles[1])) if quantiles else None]
            if quantiles else None,
            sampleValues=field_stats.get("sample_values")
        )
