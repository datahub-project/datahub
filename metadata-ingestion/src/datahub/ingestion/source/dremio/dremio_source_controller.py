"""This Module contains controller functions for dremio source"""

__author__ = "Shabbir Mohammed Hussain, Shehroz Abdullah, Hamza Rehman"

import re
import logging
from typing import List, Dict, Optional

from datahub.configuration.common import AllowDenyPattern
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    MySqlDDLClass,
    SchemaFieldClass,
    GlobalTagsClass,
    TagAssociationClass,
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
)

from datahub.ingestion.source.dremio.dremio_api import DremioAPIOperations, DremioQuery
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries

logger = logging.getLogger(__name__)


class DremioController:
    schema_pattern: Optional[AllowDenyPattern]
    collect_pds: bool
    collect_system_tables: bool
    dremio_api: DremioAPIOperations

    def __init__(self, dremio_args: dict):
        self.schema_pattern = dremio_args.get("schema_pattern")
        self.collect_pds = dremio_args.get("collect_pds")
        self.collect_system_tables = dremio_args.get("collect_system_tables")
        self.dremio_api = DremioAPIOperations(dremio_args)

    def get_datasets(self):
        """Get datasets from Dremio and filter the datasets"""
        allowed_datasets_query = DremioSQLQueries.QUERY_DATASETS.format(
            collect_pds=self.collect_pds,
            collect_system_tables=self.collect_system_tables
        )

        filtered_datasets = self.dremio_api.execute_query(
            query=allowed_datasets_query
        )

        schema_allow = self._compile_expressions(
            expressions=self.schema_pattern.allow
        )
        schema_deny = self._compile_expressions(
            expressions=self.schema_pattern.deny
        )

        distinct_schemas = list({d.get("TABLE_SCHEMA") for d in filtered_datasets})

        schemas = self.get_validated_schemas(
            schemas=distinct_schemas
        )

        for row in filtered_datasets:
            schema = row["TABLE_SCHEMA"]
            table = row["TABLE_NAME"]
            typ = row["TABLE_TYPE"]
            definition = row["VIEW_DEFINITION"]

            proceed = (
                True
                if len(schema_allow) == 0
                else any([regex.match(schema) for regex in schema_allow])
            )

            proceed = proceed and not any(
                [
                    regex.match(schema) for regex in schema_deny
                ]
            )

            for sch in schemas:
                if sch.get("original_path") == schema:
                    final_schema = sch.get("formatted_path")

                    if proceed:
                        yield final_schema, table, typ, definition

                    break

    def get_validated_schemas(self, schemas):
        formatted_schemas: List[Dict] = []
        for schema in schemas:
            formatted_schemas.append(
                self.dremio_api.validate_schema_format(
                    schema=schema
                )
            )
        return formatted_schemas

    def populate_dataset_aspects(
            self,
            schema: str,
            folder_path: List[str],
            table_name: str,
            all_tables_and_columns: Dict
    ) -> Dict[str, object]:
        aspects = {
            'datasetProperties': self.__prepare_dataset_properties(
                schema,
                table_name
            )
        }

        if folder_path:
            aspects['browsePathsV2'] = self.__prepare_browse_path(folder_path)

        if all_tables_and_columns.get(f"{schema}.{table_name}") is not None:
            result = all_tables_and_columns.get(f"{schema}.{table_name}")
            aspects['schemaMetadata'] = self.__prepare_schema_metadata(schema, table_name, result)
        else:
            logger.warning(f"Dataset {schema}.{table_name} has not been queried in Dremio")
            logger.warning(f"Dataset {schema}.{table_name} will have a null schema")

        return aspects

    @staticmethod
    def __prepare_dataset_properties(schema: str, table_name: str) -> DatasetPropertiesClass:
        return DatasetPropertiesClass(
            name=table_name,
            qualifiedName=f"{schema}.{table_name}"
        )

    @staticmethod
    def __prepare_schema_metadata(
            schema: str,
            table_name: str,
            result: List[Dict],
    ) -> SchemaMetadataClass:
        fields = [
            SchemaFieldClass(
                fieldPath=row['COLUMN_NAME'],
                type=SchemaFieldDataTypeClass(
                    type=DremioController.__get_schema_field_type(
                        row["DATA_TYPE"],
                        row["COLUMN_SIZE"]
                    )[1],
                ),
                nativeDataType=DremioController.__get_schema_field_type(
                    row["DATA_TYPE"],
                    row["COLUMN_SIZE"]
                )[2],
                nullable=row['IS_NULLABLE'].lower() == "yes",
            )
            for row in result
        ]
        return SchemaMetadataClass(
            schemaName=f"{schema}.{table_name}",
            platform="urn:li:dataPlatform:dremio",
            version=0,
            fields=fields,
            platformSchema=MySqlDDLClass(""),
            hash=""
        )

    @staticmethod
    def __prepare_browse_path(folder_path: List[str]) -> BrowsePathsV2Class:
        dataset_path: List[BrowsePathEntryClass] = []
        for browse_path_level in folder_path:
            dataset_path.append(
                BrowsePathEntryClass(
                    id=browse_path_level
                )
            )

        return BrowsePathsV2Class(dataset_path)

    @staticmethod
    def prepare_tag_aspects(tag_urns: List[str]) -> Optional[GlobalTagsClass]:
        if not tag_urns:
            return None

        tags = [TagAssociationClass(tag=tag_urn) for tag_urn in tag_urns if tag_urn]
        if not tags:
            return None

        return GlobalTagsClass(tags=tags)

    @staticmethod
    def _compile_expressions(
            expressions: list
    ):
        return (
            [re.compile(pattern, re.IGNORECASE) for pattern in expressions if pattern]
            if expressions
            else []
        )

    def get_parents(self, schema: str, dataset: str):
        return self.dremio_api.get_view_parents(schema, dataset)

    @staticmethod
    def __get_schema_field_type(
            data_type: str,
            data_size: str
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

    def get_dremio_sources(self) -> List[Dict[str, str]]:
        """
        Fetch and return Dremio source information.
        """
        return self.dremio_api.get_sources()

    def get_dremio_edition(self) -> bool:
        return self.dremio_api.test_for_enterprise_edition()

    def get_all_queries(self) -> list[DremioQuery]:
        return self.dremio_api.extract_all_queries()

    def get_all_tables_and_columns(self) -> Dict:
        return self.dremio_api.retrieve_table_and_column_list()


class DremioHelper:
    dremio_dataset_catalog: List[Dict[str, List]] = []

    @classmethod
    def dataset_alternative_names(cls, dataset_name: str) -> None:
        dataset_parts = dataset_name.split(".")
        alternatives = []
        for dataset_part in range(len(dataset_parts)):
            alternatives.append(".".join(dataset_parts[:-dataset_part]))
        cls.dremio_dataset_catalog.append(
            {
                "dataset_name": dataset_name.lower(),
                "alternatives": alternatives
            })