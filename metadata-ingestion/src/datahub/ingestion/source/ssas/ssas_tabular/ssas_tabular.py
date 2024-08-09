"""
 Meta Data Ingestion from the Microsoft OLAP Cubes
"""

import json
import logging
from dataclasses import dataclass, field as dataclass_field
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
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

from ssas.config import SsasServerHTTPSourceConfig
from ssas.domains import Catalog, Cube, OLAPLineageStream, SSASDataSet
from ssas.ssas_core import SsasSource
from ssas.xmla_server_response_error import XMLAServerResponseError

from .api import TabularSsasAPI
from .domains import XMLACube, XMLADataBase, XMLADimension, XMLAMeasure

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


@dataclass
class SsasSourceReport(SourceReport):
    """Class for source report"""

    scanned_report: int = 0
    filtered_reports: List[str] = dataclass_field(default_factory=list)

    def report_scanned(self, count: int = 1) -> None:
        self.scanned_report += count

    def report_dropped(self, view: str) -> None:
        self.filtered_reports.append(view)


@platform_name("SSAS Tabular")
@support_status(SupportStatus.UNKNOWN)
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
class SsasTabularSource(SsasSource):
    """Class build datahub entities from tabular SSAS"""

    def __init__(self, config: SsasServerHTTPSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.ssas_client = TabularSsasAPI(config)
        self.auth = self.ssas_client.auth_credentials

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
        return Cube(
            instance=self.source_config.ssas_instance,
            host_port=self.source_config.host,
            name=cube.name,
            env=self.source_config.env,
            flow=catalog,
        ), OLAPLineageStream(dependencies=[])

    def get_dimensions(
        self, cube: XMLACube, database: XMLADataBase
    ) -> List[XMLADimension]:
        """
        Build list dimensions entities.

        :param cube: cube representation from xmla response.
        :param database: datahub catalog entity for binding.
        :return: list dimensions entities.
        """

        return self.ssas_client.get_dimensions_by_cube(
            catalog_name=database.name, cube_name=cube.name
        )

    def get_measures(self, cube: XMLACube, database: XMLADataBase) -> List[XMLAMeasure]:
        """
        Build list measures entities.

        :param cube: cube representation from xmla response.
        :param database: datahub catalog entity for binding.
        :return: list measures entities.
        """

        return self.ssas_client.get_measures_by_cube(
            catalog_name=database.name, cube_name=cube.name
        )

    def get_cubes(self, database: XMLADataBase) -> List[XMLACube]:
        """
        Build list OLAP cubes entities.

        :param database: datahub catalog entity for binding.
        :return: list OLAP cubes entities.
        """

        return self.ssas_client.get_cubes_by_catalog(catalog=database.name)

    def get_databases(self) -> List[XMLADataBase]:
        """
        Build list SSAS catalogs entities.

        :return: list catalogs entities.
        """

        return self.ssas_client.get_catalogs()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Build and return metadata workunits and represent olap cube structure.

        :return: generator metadata workunits.
        """

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
                    for dimension in self.get_dimensions(cube, database):
                        data_set.add_property(
                            f"Dimension {dimension.name}",
                            json.dumps(dimension.additional_info, ensure_ascii=False),
                        )
                except XMLAServerResponseError as e:
                    LOGGER.critical(f"{XMLAServerResponseError.__name__}Getting dimension for database: {database.name}; cube: {cube.name} failed: {e}.{XMLAServerResponseError.__name__}")

                try:
                    for measure in self.get_measures(cube, database):
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
