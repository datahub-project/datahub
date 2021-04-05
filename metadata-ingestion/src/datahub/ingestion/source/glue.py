import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import boto3

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp, Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    MapTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


class GlueSourceConfig(ConfigModel):
    env: str = "PROD"
    databases: Optional[List[str]] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    aws_region: str

    @property
    def glue_client(self):
        if (
            self.aws_access_key_id
            and self.aws_secret_access_key
            and self.aws_session_token
        ):
            return boto3.client(
                "glue",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                region_name=self.aws_region,
            )
        elif self.aws_access_key_id and self.aws_secret_access_key:
            return boto3.client(
                "glue",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region,
            )
        else:
            return boto3.client("glue", region_name=self.aws_region)


@dataclass
class GlueSourceReport(SourceReport):
    tables_scanned = 0

    def report_table_scanned(self) -> None:
        self.tables_scanned += 1


class GlueSource(Source):
    source_config: GlueSourceConfig
    report = GlueSourceReport()

    def __init__(self, config: GlueSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = GlueSourceReport()
        self.glue_client = config.glue_client
        self.env = config.env
        self.databases = config.databases

    @classmethod
    def create(cls, config_dict, ctx):
        config = GlueSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        def get_all_tables(database_names: Optional[List[str]]):
            def get_tables_from_database(database_name: str, tables: List):
                kwargs = {"DatabaseName": database_name}
                while True:
                    data = self.glue_client.get_tables(**kwargs)
                    tables += data["TableList"]
                    if "NextToken" in data:
                        kwargs["NextToken"] = data["NextToken"]
                    else:
                        break
                return tables

            def get_tables_from_all_databases():
                tables = []
                kwargs: Dict = {}
                while True:
                    data = self.glue_client.search_tables(**kwargs)
                    tables += data["TableList"]
                    if "NextToken" in data:
                        kwargs["NextToken"] = data["NextToken"]
                    else:
                        break
                return tables

            if database_names:
                all_tables: List = []
                for database in database_names:
                    all_tables += get_tables_from_database(database, all_tables)
            else:
                all_tables = get_tables_from_all_databases()
            return all_tables

        tables = get_all_tables(self.databases)

        for table in tables:
            table_name = table["Name"]
            database_name = table["DatabaseName"]
            full_table_name = f"{database_name}.{table_name}"

            self.report.report_table_scanned()
            mce = self._extract_record(table, full_table_name)
            workunit = MetadataWorkUnit(id=f"glue-{full_table_name}", mce=mce)
            self.report.report_workunit(workunit)
            yield workunit

    def _extract_record(self, table: Dict, table_name: str) -> MetadataChangeEvent:
        def get_owner(time) -> OwnershipClass:
            owner = table.get("Owner")
            if owner:
                owners = [
                    OwnerClass(
                        owner=f"urn:li:corpuser:{owner}",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            else:
                owners = []
            return OwnershipClass(
                owners=owners,
                lastModified=AuditStampClass(
                    time=time,
                    actor="urn:li:corpuser:datahub",
                ),
            )

        def get_dataset_properties() -> DatasetPropertiesClass:
            return DatasetPropertiesClass(
                description=table.get("Description"),
                customProperties={
                    **table.get("Parameters", {}),
                    **{
                        k: str(v)
                        for k, v in table["StorageDescriptor"].items()
                        if k not in ["Columns", "Parameters"]
                    },
                },
                uri=table.get("Location"),
                tags=[],
            )

        def get_schema_metadata(glue_source: GlueSource):
            schema = table["StorageDescriptor"]["Columns"]
            fields: List[SchemaField] = []
            for field in schema:
                schema_field = SchemaField(
                    fieldPath=field["Name"],
                    nativeDataType=field["Type"],
                    type=get_column_type(
                        glue_source, field["Type"], table_name, field["Name"]
                    ),
                    description=field.get("Comment"),
                    recursive=False,
                    nullable=True,
                )
                fields.append(schema_field)
            return SchemaMetadata(
                schemaName=table_name,
                version=0,
                fields=fields,
                platform="urn:li:dataPlatform:glue",
                created=AuditStamp(time=sys_time, actor="urn:li:corpuser:etl"),
                lastModified=AuditStamp(time=sys_time, actor="urn:li:corpuser:etl"),
                hash="",
                platformSchema=MySqlDDL(tableSchema=""),
            )

        sys_time = int(time.time() * 1000)
        metadata_record = MetadataChangeEvent()
        dataset_snapshot = DatasetSnapshot(
            urn=f"urn:li:dataset:(urn:li:dataPlatform:glue,{table_name},{self.env})",
            aspects=[],
        )

        dataset_snapshot.aspects.append(Status(removed=False))
        dataset_snapshot.aspects.append(get_owner(sys_time))
        dataset_snapshot.aspects.append(get_dataset_properties())
        dataset_snapshot.aspects.append(get_schema_metadata(self))

        metadata_record.proposedSnapshot = dataset_snapshot

        return metadata_record

    def get_report(self):
        return self.report


def get_column_type(
    glue_source: GlueSource, field_type: str, table_name: str, field_name: str
) -> SchemaFieldDataType:
    field_type_mapping = {
        "array": ArrayTypeClass,
        "bigint": NumberTypeClass,
        "binary": BytesTypeClass,
        "boolean": BooleanTypeClass,
        "char": StringTypeClass,
        "date": DateTypeClass,
        "decimal": NumberTypeClass,
        "double": NumberTypeClass,
        "float": NumberTypeClass,
        "int": NumberTypeClass,
        "integer": NumberTypeClass,
        "interval": TimeTypeClass,
        "long": NumberTypeClass,
        "map": MapTypeClass,
        "null": NullTypeClass,
        "set": ArrayTypeClass,
        "smallint": NumberTypeClass,
        "string": StringTypeClass,
        "struct": MapTypeClass,
        "timestamp": TimeTypeClass,
        "tinyint": NumberTypeClass,
        "union": UnionTypeClass,
        "varchar": StringTypeClass,
    }

    if field_type in field_type_mapping.keys():
        type_class = field_type_mapping[field_type]
    elif field_type.startswith("array"):
        type_class = ArrayTypeClass
    elif field_type.startswith("map") or field_type.startswith("struct"):
        type_class = MapTypeClass
    elif field_type.startswith("set"):
        type_class = ArrayTypeClass
    else:
        glue_source.report.report_warning(
            field_type,
            f"The type '{field_type}' is not recognised for field '{field_name}' in table '{table_name}', setting as StringTypeClass.",
        )
        type_class = StringTypeClass
    data_type = SchemaFieldDataType(type=type_class())
    return data_type
