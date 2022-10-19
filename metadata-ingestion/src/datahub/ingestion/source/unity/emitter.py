from typing import Iterable, List
from functools import partial
from dataclasses import dataclass

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataplatform_instance_urn, make_container_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.unity import proxy
import datahub.emitter.mcp_builder as builder
from datahub.metadata.schema_classes import ChangeTypeClass, ContainerPropertiesClass, SubTypesClass, \
    DataPlatformInstanceClass, ContainerClass


@dataclass
class Report:
    scanned_metastore: int = 0
    scanned_catalog: int = 0
    scanned_schema: int = 0
    scanned_table: int = 0

    def increment_scanned_metastore(self, count: int = 1):
        self.scanned_metastore = self.scanned_metastore + count

    def increment_scanned_catalog(self, count: int = 1):
        self.scanned_catalog = self.scanned_catalog + count

    def increment_scanned_schema(self, count: int = 1):
        self.scanned_schema = self.scanned_schema + count

    def increment_scanned_table(self, count: int = 1):
        self.scanned_table = self.scanned_table + count


class Emitter:
    def __init__(self, platform_name: str, platform_instance_name: str,
                 unity_catalog_api_proxy: proxy.UnityCatalogApiProxy):
        self._unity_catalog_api_proxy = unity_catalog_api_proxy
        self._platform_name = platform_name
        self._platform_instance_name = platform_instance_name
        self._report = Report()

    def emit(self) -> Iterable[List[MetadataChangeProposalWrapper]]:
        for metastores in self._unity_catalog_api_proxy.metastores():
            yield [mcp for mcp in map(_create_container_property_aspect_mcp, metastores)]
            yield [mcp for mcp in map(_create_container_sub_type_aspect_mcp, metastores)]
            yield [mcp for mcp in map(partial(_create_container_dataplatform_aspect_mcp, self), metastores)]

            self._report.increment_scanned_metastore(len(metastores))
            # We can replace map with ThreadPoolExecutor.map if needed in later phase
            for mcps in map(partial(_emit_catalog_mcps, self), metastores):
                yield mcps


def _create_container_property_aspect_mcp(common_property: proxy.CommonProperty):
    urn: str = make_container_urn(common_property.id)
    return MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=urn,
        aspect=ContainerPropertiesClass(
            name=common_property.name,
        ),
    )


def _create_container_sub_type_aspect_mcp(common_property: proxy.CommonProperty):
    urn: str = make_container_urn(common_property.id)
    return MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=urn,
        aspect=SubTypesClass(typeNames=[common_property.type]),
    )


def _create_container_parent_aspect_mcp(parent: proxy.CommonProperty, child: proxy.CommonProperty):
    parent_urn: str = make_container_urn(parent.id)
    child_urn: str = make_container_urn(child.id)

    return MetadataChangeProposalWrapper(
            entityType="container",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=child_urn,
            aspect=ContainerClass(container=parent_urn),
        )


def _create_container_dataplatform_aspect_mcp(self: Emitter,
                                              common_property: proxy.CommonProperty):
    urn: str = builder.make_container_urn(common_property.id)
    return MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=urn,
        aspect=DataPlatformInstanceClass(
            platform=make_data_platform_urn(self._platform_name),
            instance=make_dataplatform_instance_urn(self._platform_name, self._platform_instance_name),
        ),
    )


def _emit_catalog_mcps(self: Emitter, metastore: proxy.Metastore) -> List[MetadataChangeProposalWrapper]:
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
            [mcp for mcp in map(partial(_create_container_dataplatform_aspect_mcp, self), catalogs)]
        )
        # Add parent container aspect
        mcps.extend(
            [mcp for mcp in map(partial(_create_container_parent_aspect_mcp, metastore), catalogs)]
        )
        self._report.increment_scanned_catalog(len(catalogs))
        for schema_mcps in map(partial(_emit_schema_mcps, self), catalogs):
            mcps.extend(schema_mcps)

    return mcps


def _emit_schema_mcps(self: Emitter, catalog: proxy.Catalog) -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []

    for schemas in self._unity_catalog_api_proxy.schemas(catalog=catalog):
        mcps.extend(
            [mcp for mcp in map(_create_container_property_aspect_mcp, schemas)]
        )
        mcps.extend(
            [mcp for mcp in map(_create_container_sub_type_aspect_mcp, schemas)]
        )
        mcps.extend(
            [mcp for mcp in map(partial(_create_container_dataplatform_aspect_mcp, self), schemas)]
        )
        # Add parent container aspect
        mcps.extend(
            [mcp for mcp in map(partial(_create_container_parent_aspect_mcp, catalog), schemas)]
        )

        self._report.increment_scanned_schema(len(schemas))

    return mcps
