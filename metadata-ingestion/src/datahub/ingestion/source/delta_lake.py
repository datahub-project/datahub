import json
import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Dict, Iterable, List, Sequence, TextIO, Type, Union, Any

from delta_sharing.delta_sharing import SharingClient
from delta_sharing.protocol import DeltaSharingProfile, Table

# ignore because no proper type hinting in delta-sharing source!
from delta_sharing.rest_client import retry_with_exponential_backoff  # type: ignore
from delta_sharing.rest_client import (
    DataSharingRestClient,
    Metadata,
    QueryTableMetadataResponse,
)

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigBase
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    ChangeTypeClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DateTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

# TODO: how do we support domains

LOGGER = logging.getLogger(__name__)

# TODO: can be removed if delta-sharing accepts pull request and new version is released


@dataclass(frozen=True)
class QueryTableMetadataResponse_extended(QueryTableMetadataResponse):
    table: Table


# TODO: can be removed if delta-sharing version >0.4.0 is released and required
@dataclass(frozen=True)
class QueryTableVersionResponse:
    table: Table
    delta_table_version: int


# TODO: can be removed if delta-sharing version >0.4.0 is released and required
class DataSharingRestClient_extended(DataSharingRestClient):
    # ignore because no proper type hinting in delta-sharing source!
    @retry_with_exponential_backoff  # type: ignore
    def query_table_version(self, table: Table) -> QueryTableVersionResponse:
        headers = self._head_internal(
            f"/shares/{table.share}/schemas/{table.schema}/tables/{table.name}"
        )

        # it's a bug in the server if it doesn't return delta-table-version in the header
        if "delta-table-version" not in headers:
            raise LookupError("Missing delta-table-version header")

        table_version = int(str(headers.get("delta-table-version")))
        return QueryTableVersionResponse(table=table, delta_table_version=table_version)

    def _head_internal(self, target: str) -> Dict[str, str]:
        assert target.startswith("/"), "Targets should start with '/'"
        response = self._session.head(f"{self._profile.endpoint}{target}")
        try:
            response.raise_for_status()
            headers = response.headers
            return headers
        finally:
            response.close()


# TODO: can be removed if delta-sharing accepts pull request and new version is released
class SharingClient_extended(SharingClient):
    """
    An extension of the delta sharing class SharingClient in order to query metadata including the table origin.
    This is done in order to ingest more easily.
    """

    # TODO: can be removed if delta-sharing version >0.4.0 is released and required
    def __init__(
        self, profile: Union[str, BinaryIO, TextIO, Path, DeltaSharingProfile]
    ):
        if not isinstance(profile, DeltaSharingProfile):
            profile = DeltaSharingProfile.read_from_file(profile)
        self._profile = profile
        self._rest_client = DataSharingRestClient_extended(profile)

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

    def query_table_version(self, table: Table) -> QueryTableVersionResponse:
        """
        List the version of a specified table in a Delta Sharing Server.
        :return: version of a specified table.
        """
        return self._rest_client.query_table_version(table=table)


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
    "binary": BytesTypeClass, 
    "short": NumberTypeClass,
    "integer": NumberTypeClass,
    "long": NumberTypeClass,
    "byte": BytesTypeClass,
    "float": NumberTypeClass,
    "double": NumberTypeClass,
    "string": StringTypeClass,
    "date": DateTypeClass,
    "timestamp": TimeTypeClass,
    "map": MapTypeClass,
    "struct": RecordTypeClass,
}

_field_avro_mapping: Dict[str, Type] = {
    "array": "array",
    "boolean": "boolean",
    "binary": "bytes", 
    "short": "int",
    "integer": "int",
    "long": "long",
    "byte": "bytes",
    "float": "float",
    "double": "double",
    "string": "string",
#    "date": "date", #handled separately
#    "timestamp": "timestamp-milis", #handled separately
    "map": "map",
    "struct": "struct",
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
    ) -> Sequence[QueryTableMetadataResponse_extended]:
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

    def get_metadata_version(
        self, config: DeltaLakeSourceConfig, table: Table
    ) -> QueryTableVersionResponse:
        # Get the access keys for delta-sharing & start the client
        profile = DeltaSharingProfile(
            share_credentials_version=config.share_credentials_version,
            endpoint=config.url,
            bearer_token=config.token,
        )
        client = SharingClient_extended(profile)

        # get all shared metadata
        metadata_version = client.query_table_version(table=table)

        return metadata_version

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

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

    def _create_delta_workunit(
        self,
        metadata: QueryTableMetadataResponse_extended,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_table_scanned(metadata.table.name)

        dataset_name = (
            f"{metadata.table.share}.{metadata.table.schema}.{metadata.table.name}"
        )
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
        custom_properties = {
            "Id": metadata.metadata.id,
            "Format": metadata.metadata.format.provider,
            "PartitionColumns": ",".join(metadata.metadata.partition_columns),
        }

        dataset_properties = DatasetPropertiesClass(
            tags=[],
            description=metadata.metadata.description,
            customProperties=custom_properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # TODO: add documentation that ownership not implemented because not available in API

        # TODO: add docu & message that stats are not implemented yet.
        # this has to be collected from file object!

        # build schema
        # from md.schemaObject = struct(type, fields). N.B. ignore top-level struct (just container!)
        # from md.schemaObject  -> md.so.name, md.so.type (struct or atomic), md.so.nullable, md.so.metadata.comment (if exists)
        schema_metadata = self._create_schema_metadata(dataset_name, metadata)
        dataset_snapshot.aspects.append(schema_metadata)

        # add Status
        dataset_snapshot.aspects.append(StatusClass(removed=False))

        # emit datset workunit
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        workunit = MetadataWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(workunit)

        yield workunit

        # add instance via mcp
        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            )
            workunit2 = MetadataWorkUnit(
                id=f"{dataset_urn}-dataPlatformInstance", mcp=mcp
            )
            yield workunit2

    def _create_schema_metadata(
        self, dataset_name: str, metadata: QueryTableMetadataResponse_extended
    ) -> SchemaMetadata:
        schema_fields = self._get_schema_fields(metadata.metadata)

        version_number = self.get_metadata_version(
            config=self.config, table=metadata.table
        )
        version_int = version_number.delta_table_version

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=version_int,
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
                column_dict=column["type"]
                nativeType = str(column_dict.get("type"))
                datahubDescription = column["metadata"].get("comment")

                # Get avro schema for subfields along with parent complex field
                avro_schema = self.get_avro_schema_from_data_type(
                    column_dict, datahubName
                )

                newfields = schema_util.avro_schema_to_mce_fields(
                    json.dumps(avro_schema), default_nullable=True
                )

                # First field is the parent complex field
                newfields[0].nullable = column["nullable"]
                newfields[0].description = datahubDescription
                newfields[0].nativeDataType = nativeType

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

    def get_avro_schema_from_data_type(
        self, column_dict: Dict[str,Any], column_name: str
    ) -> Dict[str, Any]:
        # Below Record structure represents the dataset level
        # Inner fields represent the complex field (struct/array/map)
        return {
            "type": "record",
            "name": "__struct_",
            "fields": [{"name": column_name, "type": self._parse_datatype(column_dict)}],
        }

    def _parse_datatype(self,column_dict: Dict[str,Any]):
        if column_dict.get("type") == "array":
            # if not atomic
            if isinstance(column_dict.get("elementType"),dict):
                items_payload = column_dict.get("elementType")
            else:
                # remap for easy parsing
                items_payload = {"type": column_dict.get("elementType"), 
                "nullable": column_dict.get("containsNull")}
            return {
                "type": "array",
                "items": self._parse_datatype(self._remap_colum_to_dict(column_dict.get("elementType"))),
                "native_data_type": column_dict.get("type"),
                "_nullable": column_dict.get("containsNull")
            }
        elif column_dict.get("type") == "map":
            kt = self._parse_datatype(self._remap_colum_to_dict(column_dict.get("keyType")))
            vt = self._parse_datatype(self._remap_colum_to_dict(column_dict.get("valueType")))
            # keys are assumed to be strings in avro map
            return {
                "type": "map",
                "values": vt,
                "native_data_type": column_dict.get("type"),#TODO: combined type?
                "key_type": kt,
                "key_native_data_type": column_dict.get("keyType"),
                "_nullable": column_dict.get("valueContainsNull")
            }
        elif column_dict.get("type") == "struct":
            return self._parse_struct_fields(column_dict["fields"])
        else:
            #get atomic datatypes
            return self._parse_basic_datatype(column_dict)

    #function: remap payload dict to basic datatype dict or leave intact
    #TODO: metadata parsing and co?
    def _remap_colum_to_dict(self, column):
        if isinstance(column, dict):
            return column
        else:
            #remap
            remap_dict={"type":column}
            return remap_dict

    def _parse_struct_fields(self, dicts):
        fields = []
        for dict in dicts:
            field_name = dict.get("name","")
            field_type = self._parse_datatype(dict)
            fields.append({"name": field_name, "type": field_type})
        return {
            "type": "record",
            "name": "__struct_{}".format(str(uuid.uuid4()).replace("-", "")),
            "fields": fields,
            "native_data_type": "fields: {}".format(dicts),
        }


    def _parse_basic_datatype(self, column_dict):
        if column_dict.get("type") in _field_avro_mapping:
            return {
                    "type": _field_avro_mapping[column_dict.get("type")],
                    "native_data_type": column_dict.get("type"),
                    "_nullable": column_dict.get("nullable"),
                }

        elif column_dict.get("type") == "date":
            return {
                    "type": "int",
                    "logicalType": "date",
                    "native_data_type": "date",
                    "_nullable": column_dict.get("nullable"),
                }
        elif column_dict.get("type") == "timestamp":
            return {
                    "type": "int",
                    "logicalType": "timestamp-millis",
                    "native_data_type": "timestamp",
                    "_nullable": column_dict.get("nullable"),
                }
        else:
            return {"type": "null", "native_data_type": column_dict.get("type")}

    def get_report(self) -> SourceReport:
        return self.report

    def close(self):
        pass


if __name__ == "__main__":

    import delta_sharing
    from delta_sharing.protocol import Format, Table
    from delta_sharing.rest_client import Metadata, Protocol

    testdata1 = QueryTableMetadataResponse_extended(
        protocol=Protocol(min_reader_version=1),
        metadata=Metadata(
            id="test2",
            name=None,
            description=None,
            format=Format(provider="parquet", options={}),
            schema_string='{"type":"struct","fields":[{"name":"a","type":"integer","nullable":false,"metadata":{"comment":"this is a comment"}},{"name":"b","type":{"type":"struct","fields":[{"name":"d","type":"integer","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"c","type":{"type":"array","elementType":"integer","containsNull":false},"nullable":true,"metadata":{}},{"name":"e","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"d","type":"integer","nullable":false,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}},{"name":"f","type":{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true},"nullable":true,"metadata":{}}]}',
            partition_columns=[],
        ),
        table=Table(name="testdata2", share="delta_sharing", schema="default"),
    )
    source = DeltaLakeSource(
        ctx=PipelineContext(run_id="delta-lake-source-test2"),
        config=DeltaLakeSourceConfig(url="url", token="x"),
    )
    schema_fields = source._get_schema_fields(testdata1.metadata)
    testdata2=QueryTableMetadataResponse_extended(
        protocol=Protocol(min_reader_version=1),
        metadata=Metadata(
            id="test2",
            name=None,
            description=None,
            format=Format(provider="parquet", options={}),
            schema_string='{"type":"struct","fields":[{"name":"a","type":"integer","nullable":false,"metadata":{"comment":"this is a comment"}},{"name":"b","type":{"type":"struct","fields":[{"name":"d","type":"integer","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"c","type":{"type":"array","elementType":"integer","containsNull":false},"nullable":true,"metadata":{}},{"name":"e","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"d","type":"integer","nullable":false,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}},{"name":"f","type":{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true},"nullable":true,"metadata":{}}]}',
            partition_columns=[],
        ),
        table=Table(name="testdata2", share="delta_sharing", schema="default"),
    )
    source = DeltaLakeSource(
        ctx=PipelineContext(run_id="delta-lake-source-test3"),
        config=DeltaLakeSourceConfig(url="url", token="x"),
    )
    schema_fields = source._get_schema_fields(testdata2.metadata)
    # Get the access keys for delta-sharing & start the client
    # profile = delta_sharing.protocol.DeltaSharingProfile(
    #     share_credentials_version=1,
    #     endpoint="https://sharing.delta.io/delta-sharing/",
    #     bearer_token="faaie590d541265bcab1f2de9813274bf233",
    # )
    # client = SharingClient_extended(profile)
    # # get all shared metadata
    # metadata_list = client.query_all_table_metadata()
    # a=2
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
    # from datahub.ingestion.run.pipeline import Pipeline

    # The pipeline configuration is similar to the recipe YAML files provided to the CLI tool.
    # pipeline = Pipeline.create(
    #     {
    #         "source": {
    #             "type": "mysql",
    #             "config": {
    #                 "username": "root",
    #                 "password": "example",
    #                 "database": "metagalaxy",
    #                 "host_port": "localhost:53307",
    #             },
    #         },
    #         "sink": {
    #             "type": "file",
    #             "config": {"filename": "/tmp/myswql_lake_mces.json"},
    #         },
    #     }
    # )
    # # Run the pipeline and report the results.
    # pipeline.run()
    # The pipeline configuration is similar to the recipe YAML files provided to the CLI tool.
    # pipeline = Pipeline.create(
    #     {
    #         "source": {
    #             "type": "delta_lake",
    #             "config": {
    #                 "url": "https://sharing.delta.io/delta-sharing/",
    #                 "token": "faaie590d541265bcab1f2de9813274bf233",
    #                 "share_credentials_version": 1,
    #                 "platform_instance": "core_finance",
    #             },
    #         },
    #         "sink": {
    #             "type": "file",
    #             "config": {"filename": "/tmp/delta_lake_mces.json"},
    #         },
    #     }
    # )

    # # Run the pipeline and report the results.
    # pipeline.run()
