from dataclasses import dataclass
from functools import partial
from typing import Iterable, List

import datahub.emitter.mcp_builder as builder
from datahub.emitter.mce_builder import (
    make_container_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.unity import proxy
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    MySqlDDLClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    SubTypesClass,
)


@dataclass
class Report(SourceReport):
    scanned_metastore: int = 0
    scanned_catalog: int = 0
    scanned_schema: int = 0
    scanned_table: int = 0

    def increment_scanned_metastore(self, count: int = 1) -> None:
        self.scanned_metastore = self.scanned_metastore + count

    def increment_scanned_catalog(self, count: int = 1) -> None:
        self.scanned_catalog = self.scanned_catalog + count

    def increment_scanned_schema(self, count: int = 1) -> None:
        self.scanned_schema = self.scanned_schema + count

    def increment_scanned_table(self, count: int = 1) -> None:
        self.scanned_table = self.scanned_table + count


class Emitter:
    _unity_catalog_api_proxy: proxy.UnityCatalogApiProxy
    _platform_name: str
    platform_instance_name: str
    report: Report

    def __init__(
        self,
        platform_name: str,
        platform_instance_name: str,
        unity_catalog_api_proxy: proxy.UnityCatalogApiProxy,
    ):
        self._unity_catalog_api_proxy = unity_catalog_api_proxy
        self._platform_name = platform_name
        self._platform_instance_name = platform_instance_name
        self.report = Report()

    def emit(self) -> Iterable[List[MetadataChangeProposalWrapper]]:
        for metastores in self._unity_catalog_api_proxy.metastores():
            yield [
                mcp for mcp in map(_create_container_property_aspect_mcp, metastores)
            ]
            yield [
                mcp for mcp in map(_create_container_sub_type_aspect_mcp, metastores)
            ]
            yield [
                mcp
                for mcp in map(
                    partial(_create_container_dataplatform_aspect_mcp, self), metastores
                )
            ]

            self.report.increment_scanned_metastore(len(metastores))
            # We can replace map with ThreadPoolExecutor.map if needed in later phase
            for mcps in map(partial(_emit_catalog_mcps, self), metastores):
                yield mcps


def _create_container_property_aspect_mcp(
    common_property: proxy.CommonProperty,
) -> MetadataChangeProposalWrapper:
    urn: str = make_container_urn(common_property.id)
    return MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=urn,
        aspect=ContainerPropertiesClass(
            name=common_property.name,
        ),
    )


def _create_container_sub_type_aspect_mcp(
    common_property: proxy.CommonProperty,
) -> MetadataChangeProposalWrapper:
    urn: str = make_container_urn(common_property.id)
    return MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=urn,
        aspect=SubTypesClass(typeNames=[common_property.type]),
    )


def _create_container_parent_aspect_mcp(
    parent: proxy.CommonProperty, child: proxy.CommonProperty
) -> MetadataChangeProposalWrapper:
    parent_urn: str = make_container_urn(parent.id)
    child_urn: str = make_container_urn(child.id)

    return MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=child_urn,
        aspect=ContainerClass(container=parent_urn),
    )


def _create_container_dataplatform_aspect_mcp(
    self: Emitter, common_property: proxy.CommonProperty
) -> MetadataChangeProposalWrapper:
    urn: str = builder.make_container_urn(common_property.id)
    return MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=urn,
        aspect=DataPlatformInstanceClass(
            platform=make_data_platform_urn(self._platform_name),
            instance=make_dataplatform_instance_urn(
                self._platform_name, self._platform_instance_name
            ),
        ),
    )


def _emit_catalog_mcps(
    self: Emitter, metastore: proxy.Metastore
) -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []

    for catalogs in self._unity_catalog_api_proxy.catalogs(metastore=metastore):
        # Add property aspect
        mcps.extend(
            [mcp for mcp in map(_create_container_property_aspect_mcp, catalogs)]
        )
        # Add subtype aspect
        mcps.extend(
            [mcp for mcp in map(_create_container_sub_type_aspect_mcp, catalogs)]
        )
        # Add dataplatform aspect
        mcps.extend(
            [
                mcp
                for mcp in map(
                    partial(_create_container_dataplatform_aspect_mcp, self), catalogs
                )
            ]
        )
        # Add parent container aspect
        mcps.extend(
            [
                mcp
                for mcp in map(
                    partial(_create_container_parent_aspect_mcp, metastore), catalogs
                )
            ]
        )
        self.report.increment_scanned_catalog(len(catalogs))
        for schema_mcps in map(partial(_emit_schema_mcps, self), catalogs):
            mcps.extend(schema_mcps)

    return mcps


def _emit_schema_mcps(
    self: Emitter, catalog: proxy.Catalog
) -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []

    for schemas in self._unity_catalog_api_proxy.schemas(catalog=catalog):
        mcps.extend(
            [mcp for mcp in map(_create_container_property_aspect_mcp, schemas)]
        )
        mcps.extend(
            [mcp for mcp in map(_create_container_sub_type_aspect_mcp, schemas)]
        )
        mcps.extend(
            [
                mcp
                for mcp in map(
                    partial(_create_container_dataplatform_aspect_mcp, self), schemas
                )
            ]
        )
        # Add parent container aspect
        mcps.extend(
            [
                mcp
                for mcp in map(
                    partial(_create_container_parent_aspect_mcp, catalog), schemas
                )
            ]
        )
        self.report.increment_scanned_schema(len(schemas))
        # Add table mcps
        for table_mcps in map(partial(_emit_table_mcps, self), schemas):
            mcps.extend(table_mcps)

    return mcps


def _emit_table_mcps(
    self: Emitter, schema: proxy.Schema
) -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []
    for tables in self._unity_catalog_api_proxy.tables(schema=schema):
        mcps.extend(
            [
                mcp
                for mcp in map(
                    partial(_create_table_parent_aspect_mcp, self, schema), tables
                )
            ]
        )
        mcps.extend(
            [
                mcp
                for mcp in map(partial(_create_table_property_aspect_mcp, self), tables)
            ]
        )
        mcps.extend(
            [
                mcp
                for mcp in map(partial(_create_table_sub_type_aspect_mcp, self), tables)
            ]
        )
        mcps.extend(
            [
                mcp
                for mcp in map(
                    partial(_create_schema_metadata_aspect_mcp, self), tables
                )
            ]
        )
        self.report.increment_scanned_table(len(tables))

    return mcps


def _create_table_parent_aspect_mcp(
    self: Emitter, parent: proxy.CommonProperty, child: proxy.CommonProperty
) -> MetadataChangeProposalWrapper:
    parent_urn: str = make_container_urn(parent.id)
    child_urn: str = make_dataset_urn_with_platform_instance(
        platform=self._platform_name,
        platform_instance=self._platform_instance_name,
        name=child.id,
    )

    return MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=child_urn,
        aspect=ContainerClass(container=parent_urn),
    )


def _create_table_property_aspect_mcp(
    self: Emitter, table: proxy.Table
) -> MetadataChangeProposalWrapper:
    dataset_urn: str = make_dataset_urn_with_platform_instance(
        platform=self._platform_name,
        platform_instance=self._platform_instance_name,
        name=table.id,
    )
    return MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=dataset_urn,
        aspect=DatasetPropertiesClass(
            name=table.name,
            customProperties={
                "storage_location": table.storage_location,
                "data_source_format": table.data_source_format,
            },
        ),
    )


def _create_table_sub_type_aspect_mcp(
    self: Emitter, table: proxy.Table
) -> MetadataChangeProposalWrapper:
    dataset_urn: str = make_dataset_urn_with_platform_instance(
        platform=self._platform_name,
        platform_instance=self._platform_instance_name,
        name=table.id,
    )
    return MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=dataset_urn,
        aspect=SubTypesClass(typeNames=[table.table_type]),
    )


def _create_schema_metadata_aspect_mcp(
    self: Emitter, table: proxy.Table
) -> MetadataChangeProposalWrapper:
    schema_fields: List[SchemaFieldClass] = [
        schema_field for schema_field in map(_create_schema_field, table.columns)
    ]
    dataset_urn: str = make_dataset_urn_with_platform_instance(
        platform=self._platform_name,
        platform_instance=self._platform_instance_name,
        name=table.id,
    )
    return MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=dataset_urn,
        aspect=SchemaMetadataClass(
            schemaName=table.id,
            platform=make_data_platform_urn(self._platform_name),
            fields=schema_fields,
            hash="",
            version=0,
            platformSchema=MySqlDDLClass(tableSchema=""),
        ),
    )


def _create_schema_field(column: proxy.Column) -> SchemaFieldClass:
    return SchemaFieldClass(
        fieldPath=column.name,
        type=column.type_name,
        nativeDataType=column.type_text,
        nullable=column.nullable,
    )
