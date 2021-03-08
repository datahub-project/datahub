import logging
import time
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Tuple

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport, WorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    EnumTypeClass,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
import json
from pprint import pformat

logger = logging.getLogger(__name__)


@dataclass
class DBTSourceReport(SourceReport):
    tables_scanned = 0
    filtered: List[str] = field(default_factory=list)

    def report_table_scanned(self, table_name: str) -> None:
        self.tables_scanned += 1

    def report_dropped(self, table_name: str) -> None:
        self.filtered.append(table_name)


class DBTManifestConfig(ConfigModel):
    manifest_path: str = None
    catalog_path: str = None

def get_dataset_name(dbt_node) -> str:
    return dbt_node['relation_name']

class DBTColumn():
    name: str
    comment: str
    index: int
    dataType: str

class DBTNode():
    dbt_name: str
    database: str
    schema: str
    dbt_file_path: str
    node_type: str # table, view, ephemeral
    dependencies: list[str]
    columns: list[DBTColumn]

def loadManifestAndCatalog(manifest_path, catalog_path) -> list[DBTNode]:
    nodes = list[DBTNode]

    with open(manifest_path, "r") as f:
        dbt_manifest_json = json.load(f)
        nodes = dbt_manifest_json['nodes']
        sources = dbt_manifest_json['sources']

        logger.info('loading {} nodes, {} sources'.format(len(nodes), len(sources)))

        for key in nodes:
            node = nodes[key]
            dataset_name = get_dataset_name(node) 

            dbtNode = DBTNode()
            dbtNode.dbt_name = key
            dbtNode.database = node['database']
            dbtNode.schema = node['schema']
            dbtNode.dbt_file_path = node['original_file_path']
            dbtNode.node_type = node['config']['materialized']
            dbtNode.dependencies = node['depends_on']['nodes']
            if dbtNode.node_type != 'ephemeral':
                dbtNode.columns = getColumns(sources[dbtNode.dbt_name])
            logger.info(pformat(vars(dbtNode)))



class DBTManifestSource(Source):
    """A Base class for all SQL Sources that use SQLAlchemy to extend"""
    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTManifestConfig.parse_obj(config_dict)
        return cls(config, ctx, "dbt")

    def __init__(self, config: DBTManifestConfig, ctx: PipelineContext, platform: str):
        super().__init__(ctx)
        self.config = config
        self.platform = platform
        self.report = DBTSourceReport()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        env: str = "PROD"
        sql_config = self.config
        platform = self.platform

        logger.info(self)

        nodes = loadManifestAndCatalog(self.config.manifest_path, self.config.catalog_path)

        #logger.info(nodes)
        # with open(self.config.manifest_path, "r") as f:
        #     dbt_manifest_json =  json.load(f)
        #     nodes = dbt_manifest_json['nodes']
        #     sources = dbt_manifest_json['sources']
        #     logger.info('loading {} nodes, {} sources'.format(len(nodes), len(sources)))

        #     for key in nodes:
        #         node = nodes[key]
        #         mce = MetadataChangeEvent()

        #         dataset_name = get_dataset_name(node)
                
        #         dataset_snapshot = DatasetSnapshot()
        #         dataset_snapshot.urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})"
                
        #         # schema_metadata = get_schema_metadata(
        #         #     self.report, dataset_name, platform, columns
        #         # )

        #         # dataset_snapshot.aspects.append(schema_metadata)
        #         mce.proposedSnapshot = dataset_snapshot

        #         wu = MetadataWorkUnit(id=dataset_name, mce=mce)

        #         self.report.report_workunit(wu)
        #         yield wu


    def get_report(self):
        return self.report

    def close(self):
        pass
