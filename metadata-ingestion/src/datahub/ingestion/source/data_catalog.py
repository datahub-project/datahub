import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Union

import pyorient
from pyorient import OrientRecord

import datahub.emitter.mce_builder as builder
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
    KafkaSchemaClass,
    SchemaMetadataClass,
    StringTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)

logger = logging.getLogger(__name__)


def get_query(external_type: str, rid: Optional[str], limit: int) -> str:
    if external_type == "kafka_topic":
        query = (
            "SELECT *, "
            "inE('Has').outV().name[0] AS db, "
            "inE('Has').outV().inE('Has').outV().code[0] AS location, "
            "outE('TableHasColumn').inV().toJson() as columns "
            "FROM Table "
            'WHERE externalType = "kafka_topic" AND deletedOn = 0 '
        )
    elif external_type == "mssql_table":
        query = (
            "SELECT *, "
            "inE('Has').outV().name[0] AS schema, "
            "inE('Has').outV().inE('Has').outV().name[0] AS db, "
            "inE('Has').outV().inE('Has').outV().technicalOwnerName[0] AS dbTechnicalOwner,"
            "inE('Has').outV().inE('Has').outV().inE('Has').outV().code[0] AS location, "
            "outE('TableHasColumn').inV().toJson() as columns "
            "FROM Table "
            'WHERE externalType = "mssql_table" AND deletedOn = 0 '
        )
    else:
        raise ValueError(f"Unknown external type: {external_type}")

    if rid is not None:
        query += f"AND @rid > {rid} "

    query += f"LIMIT {limit}"

    return query


def map_snapshot(table: OrientRecord) -> MetadataWorkUnit:
    name = table.oRecordData.get("name")
    external_type = table.oRecordData.get("externalType")
    if external_type == "kafka_topic":
        platform = "kafka"
        platform_schema = KafkaSchemaClass.construct_with_defaults()
        parents = [
            table.oRecordData.get("location").lower(),
            table.oRecordData.get("db"),
        ]
    elif external_type == "mssql_table":
        platform = "mssql"
        platform_schema = SchemalessClass()
        parents = [
            table.oRecordData.get("location").lower(),
            table.oRecordData.get("db"),
            table.oRecordData.get("schema"),
        ]
    else:
        raise ValueError(f"Unknown external type: {external_type}")

    properties = DatasetPropertiesClass(
        name=name,
        description=table.oRecordData.get("description"),
        customProperties=table.oRecordData.get("customFields"),
        qualifiedName=f"{'.'.join(parents)}.{name}"
    )

    browse_paths = BrowsePathsClass([f"/prod/{platform}/{'/'.join(parents)}/{name}"])

    columns = json.loads(table.columns)
    schema = SchemaMetadataClass(
        schemaName=platform,
        version=1,
        hash="",
        platform=f"urn:li:dataPlatform:{platform}",
        platformSchema=platform_schema,
        fields=[map_column(c) for c in columns],
    )

    technical_owner_name = table.oRecordData.get("dbTechnicalOwner")
    if technical_owner_name:
        ownership = builder.make_ownership_aspect_from_urn_list([builder.make_group_urn(technical_owner_name)],
                                                                OwnershipSourceTypeClass.SERVICE,
                                                                OwnershipTypeClass.TECHNICAL_OWNER)
        aspects = [properties, browse_paths, schema, ownership]

    else:
        aspects = [properties, browse_paths, schema]

    snapshot = DatasetSnapshot(
        urn=f"urn:li:dataset:(urn:li:dataPlatform:{platform},{'.'.join(parents)}.{name},PROD)",
        aspects=aspects,
    )

    mce = MetadataChangeEvent(proposedSnapshot=snapshot)
    return MetadataWorkUnit(properties.qualifiedName, mce=mce)


def map_column(column: Dict[str, str]) -> SchemaFieldClass:
    data_type = column.get("dataType")
    data_type = data_type.lower() if data_type is not None else "undefined"
    type_class = get_type_class(data_type)

    return SchemaFieldClass(
        fieldPath=column["name"],
        description=column.get("description"),
        type=SchemaFieldDataTypeClass(type=type_class),
        nativeDataType=data_type,
    )


def get_type_class(type_str: str):
    type_class: Union[
        "StringTypeClass", "BooleanTypeClass", "NumberTypeClass", "BytesTypeClass", "DateTypeClass", "NullTypeClass"]
    if type_str in ["string",
                    "char", "nchar",
                    "varchar", "varchar(n)", "varchar(max)",
                    "nvarchar", "nvarchar(max)",
                    "text"]:
        return StringTypeClass()
    elif type_str in ["bit", "boolean"]:
        return BooleanTypeClass()
    elif type_str in ["integer", "int", "tinyint", "smallint", "bigint",
                      "float", "real", "decimal", "numeric", "money"]:
        return NumberTypeClass()
    elif type_str in ["binary", "varbinary", "varbinary(max)"]:
        return BytesTypeClass()
    elif type_str in ["date", "smalldatetime", "datetime", "datetime2", "timestamp"]:
        return DateTypeClass()
    else:
        return NullTypeClass()


class DataCatalogSourceConfig(ConfigModel):
    orientDbHost: str
    orientDbPort: int
    serverUser: str
    serverPassword: str
    databaseName: str
    databaseUser: str
    databasePassword: str
    externalType: str
    limit: Optional[int]


@dataclass
class DataCatalogSource(Source):
    batch_size = 1000
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
        rs = self.client.query(
            get_query(self.config.externalType, None, self.batch_size)
        )
        total = 0
        while len(rs) > 0:
            for table in rs:
                total += 1
                if (self.config.limit is not None) and (total > self.config.limit):
                    return
                yield map_snapshot(table)
            rid = rs[-1]._rid
            rs = self.client.query(
                get_query(self.config.externalType, rid, self.batch_size)
            )

    def get_report(self):
        return self.report

    def close(self):
        self.client.close()
