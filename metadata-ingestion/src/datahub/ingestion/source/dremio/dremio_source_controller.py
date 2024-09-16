"""This Module contains controller functions for dremio source"""

import re
import time
from typing import Dict, List, Optional

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter import mce_builder
from datahub.ingestion.source.dremio.dremio_api import DremioAPIOperations
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    AuditStampClass,
    BooleanTypeClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    BytesTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    MySqlDDLClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    TimeTypeClass,
    ViewPropertiesClass,
)


class DremioController:
    schema_pattern: Optional[AllowDenyPattern]
    table_allow: bool
    collect_pds: bool
    collect_system_tables: bool
    dremio_api: DremioAPIOperations

    def __init__(self, dremio_args: dict):
        self.schema_pattern: AllowDenyPattern = dremio_args.get("schema_pattern")
        self.table_allow = dremio_args.get("table_allow")
        self.collect_pds = dremio_args.get("collect_pds")
        self.collect_system_tables = dremio_args.get("collect_system_tables")
        self.dremio_api = DremioAPIOperations(dremio_args)

    def get_datasets(self):
        """Get datasets from Dremio and filter the datasets"""
        allowed_datasets_query = DremioSQLQueries.QUERY_DATASETS.format(
            table_allow=self.table_allow,
            collect_pds=self.collect_pds,
            collect_system_tables=self.collect_system_tables,
        )

        filtered_datasets = self.dremio_api.execute_query(query=allowed_datasets_query)

        schema_allow = self._compile_expressions(expressions=self.schema_pattern.allow)
        schema_deny = self._compile_expressions(expressions=self.schema_pattern.deny)

        distinct_schemas = list({d.get("TABLE_SCHEMA") for d in filtered_datasets})

        schemas = self.get_validated_schemas(schemas=distinct_schemas)

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
                [regex.match(schema) for regex in schema_deny]
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
                self.dremio_api.validate_schema_format(schema=schema)
            )
        return formatted_schemas

    def populate_dataset_aspects(
        self,
        mce: MetadataChangeEventClass,
        schema,
        folder_path,
        table_name,
        all_tables_and_columns,
    ):
        """Create dataset entity for Acryl"""

        if all_tables_and_columns.get(f"{schema}.{table_name}") is not None:
            result = all_tables_and_columns.get(f"{schema}.{table_name}")
            self.__add_names_aspect(mce, schema, table_name)
            if result[0]["TABLE_TYPE"] == "VIEW":
                self._add_view_definition_aspect(
                    mce=mce,
                    view_definition=result[0]["VIEW_DEFINITION"],
                )
                self._add_data_product_link_aspect(
                    mce=mce,
                    dremio_url=self.dremio_api.dremio_url,
                    schema=schema,
                    table_name=table_name,
                    typ="space",
                )

            else:
                self._add_data_product_link_aspect(
                    mce, self.dremio_api.dremio_url, schema, table_name, "source"
                )

            schema_aspect = self.__add_schema_aspect(mce, schema, table_name)
            for row in result:
                schema_aspect.fields.append(self.__create_schema_field(row))

            if folder_path:
                self.__add_browse_path_aspect(mce, folder_path)

            return True, schema_aspect
        else:
            return False, []

    @staticmethod
    def populate_tag_aspects(mce: MetadataChangeEventClass, tag_urns: list):
        """Apply tags as per recipe"""
        if not tag_urns:
            return

        tags = [TagAssociationClass(tag_urn) for tag_urn in tag_urns if tag_urn]
        if not tags:
            return

        aspect = mce_builder.get_or_add_aspect(mce, GlobalTagsClass([]))
        aspect.tags.extend(tags)

    @staticmethod
    def populate_path_aspects(mce: MetadataChangeEventClass, paths: list):
        """Apply dataset browse path"""

        paths = [path for path in paths if path]
        if paths:
            mce_builder.get_or_add_aspect(mce, BrowsePathsV2Class([]))
        return

    @staticmethod
    def _add_view_definition_aspect(
        mce: MetadataChangeEventClass,
        view_definition: str,
    ):

        mce_builder.get_or_add_aspect(
            mce,
            ViewPropertiesClass(
                materialized=False,
                viewLanguage="SQL",
                viewLogic=view_definition,
            ),
        )

        mce_builder.get_or_add_aspect(
            mce,
            SubTypesClass(
                typeNames=[
                    "View",
                ]
            ),
        )

    @staticmethod
    def _compile_expressions(expressions: list):
        return (
            [re.compile(pattern, re.IGNORECASE) for pattern in expressions if pattern]
            if expressions
            else []
        )

    @staticmethod
    def __add_names_aspect(mce: MetadataChangeEventClass, schema: str, table_name: str):
        aspect = mce_builder.get_or_add_aspect(mce, DatasetPropertiesClass())
        aspect.name = table_name
        aspect.qualifiedName = f"{schema}.{table_name}"

    @staticmethod
    def _add_data_product_link_aspect(
        mce: MetadataChangeEventClass,
        dremio_url: str,
        schema: str,
        table_name: str,
        typ: str,
    ):
        pass
        # TODO Uncomment when this is properly implemented
        # domain, *rest_of_schema = schema.split(".")
        # dp_dremio_link = (
        #     f"{dremio_url}/{typ}/{domain}",
        #     f"{'/' if rest_of_schema else ''}",
        #     f"""{'.'.join([f'"{elem}"' for elem in rest_of_schema])}""",
        #     f""".{table_name if '.' not in table_name else f'"{table_name}"'}""",
        # )

        # aspect = mce_builder.get_or_add_aspect(mce, InstitutionalMemoryClass([]))

    @staticmethod
    def __add_schema_aspect(
        mce: MetadataChangeEventClass, schema: str, table_name: str
    ):
        aspect = mce_builder.get_or_add_aspect(
            mce, SchemaMetadataClass._construct_with_defaults()
        )

        aspect.schemaName = f"{schema}.{table_name}"
        aspect.platform = mce_builder.make_data_platform_urn("dremio")
        aspect.version = 0
        aspect.hash = ""
        aspect.platformSchema = MySqlDDLClass("")
        aspect.fields = []
        aspect.created = aspect.lastModified = AuditStampClass(
            int(time.time() * 1000), "urn:li:corpuser:admin"
        )

        return aspect

    @staticmethod
    def __add_browse_path_aspect(mce: MetadataChangeEventClass, folder_path: List):
        return mce_builder.get_or_add_aspect(
            mce,
            BrowsePathsV2Class(
                path=[BrowsePathEntryClass(id=folder) for folder in folder_path]
            ),
        )

    def __create_schema_field(self, row: dict) -> SchemaFieldClass:
        fp_type, type_cls, raw_type = self.__get_schema_field_type(
            row["DATA_TYPE"], row["COLUMN_SIZE"]
        )
        return SchemaFieldClass(
            f"[version=2.0].[type={fp_type}].{row['COLUMN_NAME']}",
            SchemaFieldDataTypeClass(type_cls),
            raw_type,
            nullable=f"{row['IS_NULLABLE']}".lower() == "yes",
        )

    def get_parents(self, schema: str, dataset: str):
        return self.dremio_api.get_view_parents(schema, dataset)

    @staticmethod
    def __get_schema_field_type(data_type: str, data_size: str):
        data_type = data_type.lower()
        data_size = f"({data_size})" if data_size else ""
        if data_type == "boolean":
            return data_type, BooleanTypeClass(), f"{data_type}{data_size}"
        elif data_type == "binary varying":
            return data_type, BytesTypeClass(), f"{data_type}{data_size}"
        elif data_type in ["decimal", "integer", "bigint", "float", "double"]:
            return data_type, NumberTypeClass(), f"{data_type}{data_size}"
        elif data_type in ["timestamp", "date"]:
            return data_type, DateTypeClass(), f"{data_type}{data_size}"
        elif data_type == "time":
            return data_type, TimeTypeClass(), f"{data_type}{data_size}"
        elif data_type in ["char", "character", "character varying"]:
            return data_type, StringTypeClass(), f"{data_type}{data_size}"
        elif data_type in ["row", "struct", "list", "map"]:
            return data_type, RecordTypeClass(), f"{data_type}{data_size}"
        elif data_type == "array":
            return data_type, ArrayTypeClass(), f"{data_type}{data_size}"
        else:
            return data_type, NullTypeClass(), f"{data_type}{data_size}"


class DremioHelper:
    dremio_dataset_catalog: List[Dict[str, List]] = []

    @classmethod
    def dataset_alternative_names(cls, dataset_name: str) -> None:
        dataset_parts = dataset_name.split(".")
        alternatives = []
        for dataset_part in range(len(dataset_parts)):
            alternatives.append(".".join(dataset_parts[:-dataset_part]))
        cls.dremio_dataset_catalog.append(
            {"dataset_name": dataset_name.lower(), "alternatives": alternatives}
        )
