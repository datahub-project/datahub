import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Sequence, Type

from delta_sharing.delta_sharing import SharingClient
from delta_sharing.protocol import Table, DeltaSharingProfile
from delta_sharing.rest_client import Metadata, QueryTableMetadataResponse

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigBase
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent, MetadataChangeProposal
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    StringTypeClass,
    TimeTypeClass,
    RecordTypeClass,
    ChangeTypeClass, 
    DataPlatformInstanceClass, 
    DatasetPropertiesClass
)

# TODO: how do we support domains

LOGGER = logging.getLogger(__name__)

# TODO: can be removed if delta-sharing accepts pull request and new version is released
@dataclass(frozen=True)
class QueryTableMetadataResponse_extended(QueryTableMetadataResponse):
    table: Table


# TODO: can be removed if delta-sharing accepts pull request and new version is released
class SharingClient_extended(SharingClient):
    """
    An extension of the delta sharing class SharingClient in order to query metadata including the table origin.
    This is done in order to ingest more easily.
    """

    def query_table_metadata(self, table: Table) -> QueryTableMetadataResponse_extended:
        """
        List all metadata for a specified table in a Delta Sharing Server.
        :return: all metadata in a specified table.
        """

        response = self._rest_client.query_table_metadata(table=table)

        return QueryTableMetadataResponse_extended(
            protocol=response.protocol, metadata=response.metadata, table=table
        )

    def query_all_table_metadata(self) -> Sequence[QueryTableMetadataResponse_extended]:
        """
        List all metadata in all tables that can be accessed by you in a Delta Sharing Server.
        :return: all metadata that can be accessed.
        """
        tables = self.list_all_tables()
        querytablesmetadata = [
            self.query_table_metadata(table=table) for table in tables
        ]

        return querytablesmetadata


class DeltaLakeSourceConfig(DatasetSourceConfigBase):

    url: str
    token: str
    share_credentials_version: int = 1

    share_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    schema_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


# map delta-sharing types to DataHub classes
_field_type_mapping: Dict[str, Type] = {
    "array": ArrayTypeClass,
    "boolean": BooleanTypeClass,
    "binary": BytesTypeClass,  # TODO: might need to change
    "short": NumberTypeClass,
    "integer": NumberTypeClass,
    "short": NumberTypeClass,
    "byte": BytesTypeClass,
    "float": NumberTypeClass,
    "double": NumberTypeClass,
    "string": StringTypeClass,
    "date": DateTypeClass,
    "timestamp": TimeTypeClass,
    "map": MapTypeClass,
    "struct": RecordTypeClass,  # TODO: needs custom handling if we want to show nested field in UI
}


@dataclass
class DeltaLakeSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)
    scanned: List[str] = field(default_factory=list)

    def report_table_scanned(self, name: str) -> None:
        self.scanned.append(name)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


@dataclass
class DeltaLakeSource(Source):
    config: DeltaLakeSourceConfig
    report: DeltaLakeSourceReport
    platform: str = "delta_lake"

    def __init__(self, ctx: PipelineContext, config: DeltaLakeSourceConfig):
        super().__init__(ctx)
        self.config = config
        self.report = DeltaLakeSourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = DeltaLakeSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_metadata(
        self, config: DeltaLakeSourceConfig
    ) -> List[QueryTableMetadataResponse_extended]:
        # Get the access keys for delta-sharing & start the client
        profile = DeltaSharingProfile(
            share_credentials_version=config.share_credentials_version,
            endpoint=config.url,
            bearer_token=config.token,
        )
        client = SharingClient_extended(profile)

        # get all shared metadata
        metadata_list = client.query_all_table_metadata()

        return metadata_list

    def get_workunits(self) -> List[MetadataWorkUnit]:

        # get all metadata from API
        metadata_list = self.get_metadata(config=self.config)

        for metadata in metadata_list:

            # filter data based on share
            if not self.config.share_pattern.allowed(metadata.table.share):
                self.report.report_dropped(metadata.table.share)
                continue

            # filter data based on schema
            if not self.config.schema_pattern.allowed(metadata.table.schema):
                self.report.report_dropped(metadata.table.schema)
                continue

            # filter data based on table
            if not self.config.table_pattern.allowed(metadata.table.name):
                self.report.report_dropped(metadata.table.name)
                continue

            # collect results
            yield from self._create_delta_workunit(metadata)
            # for mcp in self._create_delta_workunit2(metadata):
            #     dataset_name = f"{metadata.table.share}.{metadata.table.schema}.{metadata.table.name}"
            #     wu = MetadataWorkUnit(id=dataset_name, mcp=mcp)
            #     self.report.report_workunit(wu)
            #     yield wu                


    def _create_delta_workunit2(
        self,
        metadata: QueryTableMetadataResponse_extended,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_table_scanned(metadata.table.name)

        dataset_name = f"{metadata.table.share}.{metadata.table.schema}.{metadata.table.name}"
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        # from metadata top level: get md.description, md.format, md.partitioncolumns
        custom_properties = {
            "Id": metadata.metadata.id,
            "Format": metadata.metadata.format.provider,
            "PartitionColumns": metadata.metadata.partition_columns,
        }

        dataset_properties = DatasetPropertiesClass(
            tags=[],
            description=metadata.metadata.description,
            customProperties=custom_properties,
        )

        yield MetadataChangeProposal(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="datasetProperties",
            aspect=dataset_properties,
            changeType=ChangeTypeClass.UPSERT)


        # TODO: add documentation that ownership not implemented because not available in API

        # TODO: add docu & message that stats are not implemented yet.
        # this has to be collected from file object!

        # build schema
        # from md.schemaObject = struct(type, fields). N.B. ignore top-level struct (just container!)
        # from md.schemaObject  -> md.so.name, md.so.type (struct or atomic), md.so.nullable, md.so.metadata.comment (if exists)
        schema_metadata = self._create_schema_metadata(dataset_name, metadata)
        
        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="schemaMetadata",
            aspect=schema_metadata,
            changeType=ChangeTypeClass.UPSERT,
        )

        # emit status
        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="status",
            aspect=StatusClass(removed=False),
            changeType=ChangeTypeClass.UPSERT,
        )

        # emit datset workunit
        #mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        #workunit = MetadataWorkUnit(id=dataset_name, mce=mce)
        #self.report.report_workunit(workunit)

        #yield workunit

        #add instance via mcp
        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform,
                        self.config.platform_instance
                    )
                )
            )
            



    def _create_delta_workunit(
        self,
        metadata: QueryTableMetadataResponse_extended,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_table_scanned(metadata.table.name)

        dataset_name = f"{metadata.table.share}.{metadata.table.schema}.{metadata.table.name}"
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[],  # we append to this list later on
        )

        # from metadata top level: get md.description, md.format, md.partitioncolumns
        # custom_properties = {
        #     "Id": metadata.metadata.id,
        #     "Format": metadata.metadata.format.provider,
        #     "PartitionColumns": metadata.metadata.partition_columns,
        # }

        # dataset_properties = DatasetPropertiesClass(
        #     tags=[],
        #     description=metadata.metadata.description,
        #     customProperties=custom_properties,
        # )
        # dataset_snapshot.aspects.append(dataset_properties)

        # TODO: add documentation that ownership not implemented because not available in API

        # TODO: add docu & message that stats are not implemented yet.
        # this has to be collected from file object!

        # build schema
        # from md.schemaObject = struct(type, fields). N.B. ignore top-level struct (just container!)
        # from md.schemaObject  -> md.so.name, md.so.type (struct or atomic), md.so.nullable, md.so.metadata.comment (if exists)
        schema_metadata = self._create_schema_metadata(dataset_name, metadata)
        dataset_snapshot.aspects.append(schema_metadata)

        # emit datset workunit
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        workunit = MetadataWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(workunit)

        yield workunit

        #add instance via mcp
        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform,
                        self.config.platform_instance
                    )
                )
            )
            workunit2=MetadataWorkUnit(id=f"{dataset_urn}-dataPlatformInstance", mcp=mcp)
            yield workunit2


    def _create_schema_metadata(
        self, dataset_name: str, metadata: QueryTableMetadataResponse_extended
    ) -> SchemaMetadata:
        schema_fields = self._get_schema_fields(metadata.metadata)

        # TODO: add documentation that version read.out not implemented in pypi version of API yet

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=metadata.metadata.schema_string),
            fields=schema_fields,
        )
        return schema_metadata

    def _get_schema_fields(self, metadata: Metadata) -> List[SchemaField]:
        canonical_schema = []

        # get schema strings
        columns = json.loads(metadata.schema_string)
        columns = columns["fields"]  # get rid of other "hull"

        for column in columns:
            if isinstance(column["type"], dict):
                # nested type
                self.report.report_warning(
                    column["name"],
                    "Warning {} is a nested field this will not be processed properly and it will displayed poorly in UI.".format(
                        column["name"]
                    ),
                )
                datahubName = column["name"]
                nativeType = column["type"].get("type")
                datahubType = _field_type_mapping.get(
                    nativeType, NullTypeClass
                )  # NullTypeClass if we cannot map
                datahubDescription = column["metadata"].get("comment")
                datahubJsonProps = json.dumps(column["type"])

                datahubField = SchemaField(
                    fieldPath=datahubName,
                    type=SchemaFieldDataType(type=datahubType()),
                    nativeDataType=nativeType,
                    nullable=column["nullable"],
                    description=datahubDescription,
                    jsonProps=datahubJsonProps,
                )
            else:
                # primitive type
                datahubName = column["name"]
                nativeType = column["type"]
                datahubType = _field_type_mapping.get(
                    nativeType, NullTypeClass
                )  # NullTypeClass if we cannot map
                datahubDescription = column["metadata"].get("comment")

                datahubField = SchemaField(
                    fieldPath=datahubName,
                    type=SchemaFieldDataType(type=datahubType()),
                    nativeDataType=nativeType,
                    nullable=column["nullable"],
                    description=datahubDescription,
                )

            canonical_schema.append(datahubField)
        return canonical_schema

    def get_report(self) -> SourceReport:
        return self.report

    def close(self):
        pass


if __name__ == "__main__":

    import delta_sharing

    # # Get the access keys for delta-sharing & start the client
    # profile = delta_sharing.protocol.DeltaSharingProfile(
    #     share_credentials_version=1,
    #     endpoint="https://sharing.delta.io/delta-sharing/",
    #     bearer_token="faaie590d541265bcab1f2de9813274bf233",
    # )
    # client = SharingClient_extended(profile)

    # # get all shared metadata
    # metadata_list = client.query_all_table_metadata()

    # # Filter tables, schemas and shares
    # filter_table = AllowDenyPattern(allow=["COVID_19_NYT", "lending_club"], deny=["LA"])
    # metadata_list = [
    #     metadata
    #     for metadata in metadata_list
    #     if filter_table.allowed(metadata.table.name)
    # ]

    # # prepare load for metadata from ...
    # test = DeltaLakeSource(
    #     ctx=PipelineContext(run_id="delta-lake-source-test"),
    #     config=DeltaLakeSourceConfig(url="url", token="x"),
    # )
    # test2 = test._get_schema_fields(metadata_list[0].metadata)

    from datahub.ingestion.run.pipeline import Pipeline

    # The pipeline configuration is similar to the recipe YAML files provided to the CLI tool.
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "mysql",
                "config": {
                    "username": "root",
                    "password": "example",
                    "database": "metagalaxy",
                    "host_port": "localhost:53307",
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": "/tmp/myswql_lake_mces.json"},
            },
        }
    )

    # Run the pipeline and report the results.
    pipeline.run()


    # The pipeline configuration is similar to the recipe YAML files provided to the CLI tool.
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "delta_lake",
                "config":{
                    "url": "https://sharing.delta.io/delta-sharing/",
                    "token": "faaie590d541265bcab1f2de9813274bf233",
                    "share_credentials_version": 1,
                    "platform_instance": "core_finance"
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": "/tmp/delta_lake_mces.json"},
            },
        }
    )


    # Run the pipeline and report the results.
    pipeline.run()
