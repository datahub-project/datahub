"""
Meta Data Ingestion From the Microsoft OLAP Cubes
"""
import json
import logging
from typing import Iterable, List, Tuple

from datahub.emitter.mcp_builder import add_dataset_to_container
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit

from ssas.config import SsasServerHTTPSourceConfig
from ssas.domains import (
    Catalog,
    Cube,
    CubeDependency,
    OLAPLineageStream,
    SSASDataSet,
)
from ssas.ssas_core import SsasSource
from ssas.utils import DNSHostNameResolver
from ssas.xmla_server_response_error import XMLAServerResponseError

from .api import MultidimensionSsasAPI
from .domains import (
    DataSource,
    XMLACube,
    XMLACubesContainer,
    XMLADataBase,
    XMLADataBasesContainer,
    XMLADimensionsContainer,
    XMLAMeasures,
)

LOGGER = logging.getLogger(__name__)


@platform_name("SSAS Multidimension")
@support_status(SupportStatus.UNKNOWN)
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
class SsasMultidimensionSource(SsasSource):
    """
    This plugin extracts:
    - MS SSAS multidimension cubes;
    - dimensions and measures of cubes;
    - additional information as properties.
    """

    def __init__(self, config: SsasServerHTTPSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.ssas_client = MultidimensionSsasAPI(config)
        self.auth = self.ssas_client.auth_credentials
        self.config = config

    def _get_default_ssas_instance_from_config(self, server: str):
        return self.config.default_ssas_instances_by_server.get(server, None)

    def _get_catalog(self, database: XMLADataBase) -> Catalog:
        """
        Build datahub catalog entity.

        :param database: database representation from xmla response.
        :return: datahub catalog entity.
        """
        return Catalog(
            host_port=self.source_config.host,
            instance=self.source_config.ssas_instance,
            name=database.name,
            env=self.source_config.env,
        )

    def _get_olap_cube(
        self, cube: XMLACube, database: XMLADataBase
    ) -> Tuple[Cube, OLAPLineageStream]:
        """
        Build datahub cube entity.

        :param cube: cube representation from xmla response.
        :param catalog: datahub catalog entity for binding.
        :return: datahub cube entity.
        """
        catalog = self._get_catalog(database)
        database_sources = database.sources
        olap_cube = Cube(
            instance=self.source_config.instance,
            host_port=self.source_config.host,
            name=cube.name,
            env=self.source_config.env,
            flow=catalog,
        )
        datasets_stream = self._get_cube_dependency(cube, database_sources)
        return olap_cube, datasets_stream

    def _get_cube_dependency(
        self, cube: XMLACube, catalog_sources: List[DataSource]
    ) -> OLAPLineageStream:
        """
        Build cube lineage entity.

        :param cube: cube representation from xmla response.
        :param catalog_sources: list of catalog data sources.
        :return: datahub lineage entity.
        """
        upstream_dependencies = []
        cube_sources_ids = cube.sources_ids
        cube_sources = [
            source
            for source in catalog_sources
            if source.name in cube_sources_ids or source.id in cube_sources_ids
        ]

        for dependency in cube_sources:

            server = dependency.server

            if self.source_config.use_dns_resolver:
                resolver = DNSHostNameResolver(
                    hostname=server, dns_suffix_list=self.source_config.dns_suffixes
                )
                server = resolver.primary_hostname

            upstream_dependencies.append(
                CubeDependency(
                    source=dependency.source,
                    server=server,
                    instance=dependency.instance if dependency.instance is not None else self._get_default_ssas_instance_from_config(server=server),
                    db=dependency.db,
                    schema=dependency.schema,
                    name=dependency.name,
                    type=dependency.type.upper(),
                    env=self.source_config.env,
                )
            )
        return OLAPLineageStream(dependencies=upstream_dependencies)

    def get_dimensions(self, cube: XMLACube) -> XMLADimensionsContainer:
        """
        Get list of dimensions for current cube.

        :param cube: cube entity.
        :return: cube dimensions representation.
        """
        return cube.dimensions

    def get_measures(self, cube: XMLACube) -> XMLAMeasures:
        """
        Get list of measures for current cube.

        :param cube: cube entity.
        :return: cube measures representation.
        """
        return cube.measures

    def get_cubes(self, database: XMLADataBase) -> XMLACubesContainer:
        """
        Get list of cubes for current database.

        :param database: database entity.
        :return: database cubes representation.
        """
        return database.cubes

    def get_databases(self) -> XMLADataBasesContainer:
        """
        Get list of available databases.

        :return: server databases representation.
        """
        xmla_server = self.ssas_client.get_server()
        return xmla_server.databases

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        for database in self.get_databases():
            catalog = self._get_catalog(database)

            container_key = self.gen_key(catalog.formatted_name)

            yield from self.create_emit_containers(
                container_key=container_key,
                name=catalog.formatted_name,
                sub_types=["SSAS catalog"],
                parent_container_key=None,
            )

            for cube in self.get_cubes(database):
                olap_cube, datasets_stream = self._get_olap_cube(cube, database)
                data_set = SSASDataSet(
                    entity=olap_cube, incoming=datasets_stream.as_datasets_urn_list
                )

                for name, value in cube.additional_info.items():
                    data_set.add_property(name, value)

                try:
                    for dimension in self.get_dimensions(cube):
                        data_set.add_property(
                            f"Dimension {dimension.name}",
                            json.dumps(dimension.additional_info, ensure_ascii=False),
                        )
                except XMLAServerResponseError as e:
                    LOGGER.critical(f"{XMLAServerResponseError.__name__}Getting dimension for database: {database.name}; cube: {cube.name} failed: {e}.{XMLAServerResponseError.__name__}")
                try:
                    for measure in self.get_measures(cube):
                        data_set.add_property(
                            f"Measure {measure.name}",
                            json.dumps(measure.additional_info, ensure_ascii=False),
                        )
                except XMLAServerResponseError as e:
                    LOGGER.critical(f"{XMLAServerResponseError.__name__}Getting measure for database: {database.name}; cube: {cube.name} failed: {e}.{XMLAServerResponseError.__name__}")

                yield from self.mapper.construct_set_workunits(data_set)
                yield from add_dataset_to_container(
                    container_key=container_key, dataset_urn=data_set.urn
                )
