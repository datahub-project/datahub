"""This Module contains controller functions for dremio source"""

__author__ = "Shabbir Mohammed Hussain, Shehroz Abdullah, Hamza Rehman, Jonny Dixon"

from datetime import datetime

import time

import uuid

import logging
from typing import Dict, Optional, Union, List

from datahub._codegen.aspect import _Aspect
from datahub.emitter.mce_builder import make_dataplatform_instance_urn, make_container_urn, make_user_urn, \
    make_group_urn
from datahub.ingestion.source.metadata.business_glossary import make_glossary_term_urn
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
    DatasetKeyClass,
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
    GlossaryTermKeyClass,
    GlossaryTermInfoClass,
    StatusClass
)

from datahub.ingestion.source.dremio.dremio_api import (
    DremioDataset,
    DremioContainer,
    DremioGlossaryTerm
)

logger = logging.getLogger(__name__)
namespace = uuid.NAMESPACE_DNS


class DremioAspects:
    def __init__(
        self,
        platform: str,
        platform_instance: Optional[str]=None,
        env: Optional[Union[FabricTypeClass, str]]=FabricTypeClass.PROD,
    ):
        self.platform=platform
        self.platform_instance = platform_instance
        self.env = env


    # def get_datasets(self):
    #     """Get datasets from Dremio and filter the datasets"""
    #     allowed_datasets_query = DremioSQLQueries.QUERY_DATASETS.format(
    #         collect_pds=self.collect_pds,
    #         collect_system_tables=self.collect_system_tables
    #     )
    #
    #     filtered_datasets = self.dremio_api.execute_query(
    #         query=allowed_datasets_query
    #     )
    #
    #     schema_allow = self.__compile_expressions(
    #         expressions=self.schema_pattern.allow
    #     )
    #     schema_deny = self.__compile_expressions(
    #         expressions=self.schema_pattern.deny
    #     )
    #
    #     distinct_schemas = list({d.get("TABLE_SCHEMA") for d in filtered_datasets})
    #
    #     schemas = self.get_validated_schemas(
    #         schemas=distinct_schemas
    #     )
    #
    #     for row in filtered_datasets:
    #         schema = row["TABLE_SCHEMA"]
    #         table = row["TABLE_NAME"]
    #         typ = row["TABLE_TYPE"]
    #         definition = row["VIEW_DEFINITION"]
    #
    #         proceed = (
    #             True
    #             if len(schema_allow) == 0
    #             else any([regex.match(schema) for regex in schema_allow])
    #         )
    #
    #         proceed = proceed and not any(
    #             [
    #                 regex.match(schema) for regex in schema_deny
    #             ]
    #         )
    #
    #         for sch in schemas:
    #             if sch.get("original_path") == schema:
    #                 final_schema = sch.get("formatted_path")
    #
    #                 if proceed:
    #                     yield final_schema, table, typ, definition
    #
    #                 break

    # def get_validated_schemas(self, schemas):
    #     formatted_schemas: List[Dict] = []
    #     for schema in schemas:
    #         formatted_schemas.append(
    #             self.dremio_api.validate_schema_format(
    #                 schema=schema
    #             )
    #         )
    #     return formatted_schemas

    # def __compile_expressions(
    #         self,
    #         expressions: list,
    # ):
    #     return (
    #         [re.compile(pattern, re.IGNORECASE) for pattern in expressions if pattern]
    #         if expressions
    #         else []
    #     )

    def get_container_urn(
            self,
            name: str,
            path: Optional[List[str]],
    ) -> str:

        if path:
            return make_container_urn(
                guid=str(
                    uuid.uuid5(
                        namespace,
                        self.platform +
                        "".join(path) +
                        name +
                        self.platform_instance
                    )
                )
            )

        return make_container_urn(
            guid=str(
                uuid.uuid5(
                    namespace,
                    self.platform +
                    name +
                    self.platform_instance
                )
            )
        )


    def populate_container_aspects(
            self,
            container: DremioContainer,
    ) -> Dict[str, _Aspect]:
        aspects = {}

        aspects["containerProperties"] = (
            ContainerPropertiesClass(
                name=container.container_name,
                qualifiedName=f"{'.'.join(container.path) + '.' if container.path else ''}{container.container_name}",
                description=container.description,
                env=self.env
            )
        )

        if container.path:

            aspects["browsePathsV2"] = (
                BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(
                            id=container.path[browse_path_level],
                            urn=self.get_container_urn(
                                name=container.container_name,
                                path=container.path[:browse_path_level],
                            ),
                        ) for browse_path_level in range(len(container.path))
                    ]
                )
            )

            aspects["container"] = (
                ContainerClass(
                    container=self.get_container_urn(
                        path=container.path,
                        name="",
                    )
                )
            )

        else:
            aspects["browsePathsV2"] = (
                BrowsePathsV2Class(
                    path=[]
                )
            )


        aspects["dataPlatformInstance"] = (
            DataPlatformInstanceClass(
                platform=f"urn:li:dataPlatform:dremio",
                instance=(
                    make_dataplatform_instance_urn(
                        self.platform, self.platform_instance,
                    )
                    if self.platform_instance
                    else None
                )
            )
        )

        aspects["subTypes"] = (
            SubTypesClass(
                typeNames=[
                    container.subclass,
                ]
            )
        )

        aspects["status"] = (
            StatusClass(
                removed=False,
            )
        )

        return aspects

    def populate_dataset_aspects(
            self,
            dataset: DremioDataset,
    ) -> Dict[str, _Aspect]:
        aspects = {}

        # aspects["datasetKey"] = (
        #     DatasetKeyClass(
        #         platform=self.platform,
        #         name=f"{'.'.join(dataset.path)}.{dataset.resource_name}",
        #         origin=self.env
        #     )
        # )

        aspects["datasetProperties"] = (
            DatasetPropertiesClass(
                name=dataset.resource_name,
                qualifiedName=f"{'.'.join(dataset.path)}.{dataset.resource_name}",
                description=dataset.description,
                created=TimeStampClass(
                    time=round(
                        datetime.strptime(
                            dataset.created,
                            '%Y-%m-%d %H:%M:%S.%f'
                        ).timestamp() * 1000
                    ),
                ),
            )
        )

        if dataset.owner_type == "USER":
            owner = make_user_urn(dataset.owner)
        else:
            owner = make_group_urn(dataset.owner)

        aspects["ownership"] = (
            OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=owner,
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                    )
                ]
            )
        )

        aspects["subTypes"] = (
            SubTypesClass(
                typeNames=[
                    dataset.dataset_type.value,
                ]
            )
        )

        aspects["dataPlatformInstance"] = (
            DataPlatformInstanceClass(
                platform=f"urn:li:dataPlatform:dremio",
                instance=(
                    make_dataplatform_instance_urn(
                        self.platform, self.platform_instance,
                    )
                    if self.platform_instance
                    else None
                )
            )
        )

        aspects["browsePathsV2"] = (
            BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(
                        id=dataset.path[browse_path_level],
                        urn=self.get_container_urn(
                            path=dataset.path[:browse_path_level],
                            name="",
                        ),
                    ) for browse_path_level in range(len(dataset.path))
                ]
            )
        )

        aspects["container"] = (
            ContainerClass(
                container=self.get_container_urn(
                    path=dataset.path,
                    name="",
                )
            )
        )

        if dataset.glossary_terms:
            aspects["glossaryTerms"] = (
                GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn=term.urn,
                        )
                    for term in dataset.glossary_terms
                    ],
                    auditStamp=AuditStampClass(
                        time=round(time.time() * 1000),
                        actor="urn:li:corpuser:admin",
                    ),
                )
            )

        if dataset.columns:
            aspects["schemaMetadata"] = (
                SchemaMetadataClass(
                    schemaName=f"{'.'.join(dataset.path)}.{dataset.resource_name}",
                    platform="urn:li:dataPlatform:dremio",
                    version=0,
                    fields=[
                        SchemaFieldClass(
                            fieldPath=column.name,
                            type=SchemaFieldDataTypeClass(
                                type=self.__get_schema_field_type(
                                    column.data_type,
                                    column.column_size
                                )[1],
                            ),
                            nativeDataType=self.__get_schema_field_type(
                                column.data_type,
                                column.column_size
                            )[2],
                            nullable=column.is_nullable=="YES",
                        )
                        for column in dataset.columns
                    ],
                    platformSchema=MySqlDDLClass(""),
                    hash=""
                )
            )

            if dataset.sql_definition:
                aspects["viewProperties"] = (
                    ViewPropertiesClass(
                        materialized=False,
                        viewLanguage="SQL",
                        viewLogic=dataset.sql_definition,
                    )
                )

        else:
            logger.warning(f"Dataset {dataset.path}.{dataset.resource_name} has not been queried in Dremio")
            logger.warning(f"Dataset {dataset.path}.{dataset.resource_name} will have a null schema")

        aspects["status"] = (
            StatusClass(
                removed=False,
            )
        )

        return aspects

    def populate_glossary_term_aspects(
            self,
            glossary_term: DremioGlossaryTerm,
    ) -> Dict[str, _Aspect]:
        aspects = {}

        aspects["glossaryTermKey"] = (
            GlossaryTermKeyClass(
                name=glossary_term.glossary_term
            )
        )

        return aspects

    def __get_schema_field_type(
            self,
            data_type: str,
            data_size: int,
    ):
        data_type = data_type.lower()
        data_size = f"({data_size})" if data_size else ""
        if data_type == "boolean":
            return data_type, BooleanTypeClass(), f"{data_type}{data_size}"
        if data_type == "binary varying":
            return data_type, BytesTypeClass(), f"{data_type}{data_size}"
        if data_type in ["decimal", "integer", "bigint", "float", "double"]:
            return data_type, NumberTypeClass(), f"{data_type}{data_size}"
        if data_type in ["timestamp", "date"]:
            return data_type, DateTypeClass(), f"{data_type}{data_size}"
        if data_type == "time":
            return data_type, TimeTypeClass(), f"{data_type}{data_size}"
        if data_type in ["char", "character", "character varying"]:
            return data_type, StringTypeClass(), f"{data_type}{data_size}"
        if data_type in ["row", "struct", "list", "map"]:
            return data_type, RecordTypeClass(), f"{data_type}{data_size}"
        if data_type == "array":
            return data_type, ArrayTypeClass(), f"{data_type}{data_size}"
        return data_type, NullTypeClass(), f"{data_type}{data_size}"