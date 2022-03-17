from curses import meta
import logging

from dataclasses import dataclass, field
from typing import Sequence, List, Optional, Iterable, Union, Dict,Type
import pydantic

from delta_sharing.delta_sharing import SharingClient
from delta_sharing.protocol import Table
from delta_sharing.rest_client import QueryTableMetadataResponse

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BinaryJsonSchema,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemalessClass,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)

LOGGER=logging.getLogger(__name__)

@dataclass(frozen=True)
class QueryTableMetadataResponse_extended(QueryTableMetadataResponse):
    table: Table


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

        return QueryTableMetadataResponse_extended(protocol=response.protocol, metadata=response.metadata, table=table)

    def query_all_table_metadata(self) -> Sequence[QueryTableMetadataResponse_extended]:
        """
        List all metadata in all tables that can be accessed by you in a Delta Sharing Server.
        :return: all metadata that can be accessed.
        """
        tables = self.list_all_tables()
        querytablesmetadata = [self.query_table_metadata(table=table) for table in tables]

        return querytablesmetadata

class DeltaLakeSourceConfig(ConfigModel):

    url: str
    token: str
    shareCredentialsVersion: str ="1"
    
    share_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    schema_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()

# map delta-sharing types to DataHub classes
_field_type_mapping: Dict[Union[Type, str], Type] = {
    "array": ArrayTypeClass,
    "boolean": BooleanTypeClass,
    "binary": BytesTypeClass,#TODO: might need to change
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
    "struct": UnionTypeClass,#TODO: needs custom handling if we want to show nested field in UI
}

@dataclass
class DeltaLakeSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)

@dataclass
class DeltaLakeSource(Source):
    config: DeltaLakeSourceConfig
    report: DeltaLakeSourceReport
    platform: str = "deltalake"

    def __init__(self, ctx: PipelineContext, config: DeltaLakeSourceConfig):
        super().__init__(ctx)
        self.config = config
        self.report = DeltaLakeSourceReport()


    @classmethod
    def create(cls, config_dict, ctx):
        config = DeltaLakeSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    #TODO: test connection / proper error handling
    # def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
    #     for i, obj in enumerate(iterate_generic_file(self.config.filename)):
    #         wu: Union[MetadataWorkUnit, UsageStatsWorkUnit]
    #         if isinstance(obj, UsageAggregationClass):
    #             wu = UsageStatsWorkUnit(f"file://{self.config.filename}:{i}", obj)
    #         elif isinstance(obj, MetadataChangeProposal):
    #             wu = MetadataWorkUnit(f"file://{self.config.filename}:{i}", mcp_raw=obj)
    #         else:
    #             wu = MetadataWorkUnit(f"file://{self.config.filename}:{i}", mce=obj)
    #         self.report.report_workunit(wu)
    #         yield wu

    def get_metadata(self,config: DeltaLakeSourceConfig)-> List[QueryTableMetadataResponse_extended]:
        # Get the access keys for delta-sharing & start the client
        profile = delta_sharing.protocol.DeltaSharingProfile(share_credentials_version=config.share_credentials_version, endpoint=config.url, bearer_token=config.token)
        client = SharingClient_extended(profile)

        # get all shared metadata
        metadata_list=client.query_all_table_metadata()

        #filter data and TODO report dropped items
        metadata_list=[metadata for metadata in metadata_list if config.share_patterns.allowed(metadata.table.share)]
        metadata_list=[metadata for metadata in metadata_list if config.schema_patterns.allowed(metadata.table.schema)]
        metadata_list=[metadata for metadata in metadata_list if config.table_patterns.allowed(metadata.table.name)]

        return metadata_list
    
    def get_field_type(
        self, field_type: Union[Type, str], collection_name: str
    ) -> SchemaFieldDataType:
        """
        Maps types encountered in delta-sharing to corresponding schema types.

        Parameters
        ----------
            field_type:
                type of a Python object
            collection_name:
                name of collection (for logging)
        """
        TypeClass: Optional[Type] = _field_type_mapping.get(field_type)

        if TypeClass is None:
            self.report.report_warning(
                collection_name, f"unable to map type {field_type} to metadata schema"
            )
            TypeClass = NullTypeClass
        
        #TODO: this should be removed if struct nested fields are parsed properly
        if TypeClass is UnionTypeClass:
            self.report.report_warning(
                collection_name, f"Warning {field_type} is a nested field this will not be processed properly and it will displayed poorly in UI."
            )

        return SchemaFieldDataType(type=TypeClass())

    def get_report(self) -> SourceReport:
        return self.report

    def close(self):
        pass

#TODO: report dropped and filtered in WU
#TODO: add message that stats are not implemented yet.


if __name__ == "__main__":

    import delta_sharing

    #get key from recipe

    # Get the access keys for delta-sharing & start the client
    profile = delta_sharing.protocol.DeltaSharingProfile(share_credentials_version=1, endpoint="https://sharing.delta.io/delta-sharing/", bearer_token="faaie590d541265bcab1f2de9813274bf233")
    client = SharingClient_extended(profile)

    # get all shared metadata
    metadata_list=client.query_all_table_metadata()

    #Filter tables, schemas and shares
    filter_table=AllowDenyPattern(allow=["COVID_19_NYT"], deny=["LA"])
    metadata_list=[metadata for metadata in metadata_list if filter_table.allowed(metadata.table.name)]

    filter_schema=AllowDenyPattern(allow=["default"], deny=["LA"])
    metadata_list=[metadata for metadata in metadata_list if filter_schema.allowed(metadata.table.schema)]

    filter_share=AllowDenyPattern(allow=["delta_sharing"], deny=["LA"])
    metadata_list=[metadata for metadata in metadata_list if filter_share.allowed(metadata.table.share)]

    #TODO: catch if no data left to process

    #TODO: transform to datahub dataset entity

    #TODO: publish data


