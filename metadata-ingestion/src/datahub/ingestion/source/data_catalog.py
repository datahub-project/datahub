import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, Union

import pyorient
from pyorient import OrientRecord

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BrowsePathsClass,
    DatasetPropertiesClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)


def map_snapshot(table: OrientRecord) -> MetadataWorkUnit:
    if table.externalType == "kafka_topic":
        platform = "kafka"
    elif table.externalType == "mssql_table":
        platform = "mssql"
    else:
        raise ValueError(f"Unknown external type: {table.externalType}")

    properties = DatasetPropertiesClass(
        name=table.oRecordData["name"],
        description=table.oRecordData["description"],
        customProperties=table.oRecordData["customFields"],
    )

    # adjust parents for mssql
    browse_paths = BrowsePathsClass(
        [f"/prod/{platform}/{table.country}/{table.location}/{table.db}"]
    )

    columns = json.loads(table.columns)
    schema = SchemaMetadataClass(
        schemaName=platform,
        version=1,
        hash="",
        platform=f"urn:li:dataPlatform:{platform}",
        platformSchema=SchemalessClass(),
        fields=[map_column(c) for c in columns],
    )

    snapshot = DatasetSnapshot(
        urn=f"urn:li:dataset:(urn:li:dataPlatform:{platform},{table.name},PROD)",
        aspects=[properties, browse_paths, schema],
    )

    mce = MetadataChangeEvent(proposedSnapshot=snapshot)
    return MetadataWorkUnit(table.name, mce=mce)


def map_column(column: Dict[str, str]) -> SchemaFieldClass:
    data_type = column.get("dataType", "").lower()

    type_class: Union["StringTypeClass", "BooleanTypeClass", "NumberTypeClass"]
    if data_type == "string":
        type_class = StringTypeClass()
    elif data_type == "boolean":
        type_class = BooleanTypeClass()
    elif data_type == "integer":
        type_class = NumberTypeClass()
    else:
        type_class = StringTypeClass()

    return SchemaFieldClass(
        fieldPath=column["name"],
        description=column.get("description"),
        type=SchemaFieldDataTypeClass(type=type_class),
        nativeDataType=data_type,
    )


class DataCatalogSourceConfig(ConfigModel):
    orientDbHost: str
    orientDbPort: int
    serverUser: str
    serverPassword: str
    databaseName: str
    databaseUser: str
    databasePassword: str


@dataclass
class DataCatalogSource(Source):
    config: DataCatalogSourceConfig
    client: pyorient.OrientDB
    report: SourceReport

    def __init__(self, config: DataCatalogSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.client = pyorient.OrientDB(config.orientDbHost, config.orientDbPort)
        self.client.set_session_token(True)
        self.client.connect(config.serverUser, config.serverPassword)
        self.client.db_open(
            config.databaseName, config.databaseUser, config.databasePassword
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = DataCatalogSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        # adjust for mssql, remove limit
        query = (
            "SELECT *, "
            "inE('Has').outV().name[0] AS db, "
            "inE('Has').outV().inE('Has').outV().code[0] AS location, "
            "inE('Has').outV().inE('Has').outV().inE('Has').outV().name[0] AS country, "
            "outE('TableHasColumn').inV().toJson() as columns "
            "FROM Table "
            'WHERE externalType = "kafka_topic" AND deletedOn = 0 '
            "LIMIT 100"
        )
        for table in self.client.query(query):
            yield map_snapshot(table)

    def get_report(self):
        return self.report

    def close(self):
        self.client.close()
